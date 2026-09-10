package nativesrc

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// newPullClient builds the client for manifest and segment fetches. Unlike the
// stream client below it CARRIES a whole-request Timeout: every fetch here is a
// bounded object (a playlist or one segment), so a slow-loris body must not wedge
// the puller goroutine. Per-source rather than package-level, so a stream's proxy
// and TLS-verification setting reach the segment fetches too.
func newPullClient(opt Options) *http.Client {
	return &http.Client{
		Timeout:   pullRequestTimeout,
		Transport: opt.transport(8 * time.Second),
	}
}

// maxHLSSegmentBytes caps a single segment body so a never-ending
// stream from a misbehaving upstream cannot exhaust the pipe writer
// or memory. 64 MiB covers ~30s of 16 Mbps broadcast UHD, which is
// well above any legitimate HLS segment.
const maxHLSSegmentBytes = 64 << 20

// maxPlaylistBytes caps a manifest read. Playlists are small; anything at this
// size is a misbehaving upstream, not a live window.
const maxPlaylistBytes = 4 << 20

// maxRetainedSegBuf is the largest segment staging buffer a puller will hold on
// to between segments. Comfortably above any real segment (a 16 Mbps feed at a
// 6 s target is ~12 MB) and far below maxHLSSegmentBytes, so reuse covers the
// normal case without one outlier pinning 64 MiB per stream.
const maxRetainedSegBuf = 16 << 20

// maxHLSConsecutiveFails is how many consecutive failures of one KIND (segment
// fetches, or manifest fetch/parse) close the pipe so the puller backs off and
// rotates to the next source URL. The two kinds are counted separately: a
// manifest that keeps arriving fine must not reset the segment counter (a window
// of two segments would then never reach the threshold, and a source serving an
// intact playlist of dead segments would retry forever), and vice versa.
const maxHLSConsecutiveFails = 5

// OpenHLSPull returns an io.ReadCloser that yields concatenated MPEG-TS
// bytes from a live HLS manifest. It supports the common case for IPTV
// live feeds: a `.m3u8` master or media playlist whose segments are
// `.ts` (MPEG-TS). fMP4-segmented HLS (`.m4s`) returns
// ErrUnsupportedSource — those upstreams already carry fragmented MP4
// and don't need the TS demuxer; pkg/remux will grow a separate fast
// path for them later.
//
// The puller runs a single goroutine that:
//
//  1. GETs the manifest at the live cadence (target_duration / 2,
//     floored at 1s).
//  2. Walks new #EXTINF entries (de-duped by absolute URI).
//  3. GETs each new segment and copies the body into the pipe.
//  4. Cancels cleanly on ctx.Done.
//
// Transient errors (segment 5xx, manifest hiccup) are logged-and-
// retried; the demuxer sees a momentary stall but does not abort. A
// run of consecutive failures (~5 manifests in a row) closes the pipe
// so the supervisor's healthy-uptime check advances to the next
// source. Mirrors ffmpeg's `-reconnect` heuristic but in our process.
func OpenHLSPull(ctx context.Context, rawURL string, opt Options) (io.ReadCloser, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("%w: parse: %v", ErrUnsupportedSource, err)
	}
	client := newPullClient(opt)
	// One synchronous fetch up front to (a) classify the playlist and
	// (b) surface obvious 4xx/5xx errors to the caller cleanly instead
	// of letting them turn into a closed-pipe surprise mid-stream.
	body, finalURL, err := hlsFetch(ctx, u, opt, client)
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	return startHLSPull(ctx, finalURL, opt, client, body)
}

// openHLSPullWith starts the pull from a manifest the CALLER already fetched,
// with base the URL it was actually served from (after redirects). It exists so
// the source-dispatch path can hand over the playlist it read while sniffing the
// body, instead of closing that response and fetching the same URL again.
func openHLSPullWith(ctx context.Context, base *url.URL, opt Options, manifest []byte) (io.ReadCloser, error) {
	return startHLSPull(ctx, base, opt, newPullClient(opt), manifest)
}

// startHLSPull classifies a media/master playlist and, if it is one this package
// will take, spawns the puller goroutine behind a pipe. On every refusal it
// drains the client's pool before returning: the caller falls back to ffmpeg and
// will never touch this client again.
func startHLSPull(ctx context.Context, base *url.URL, opt Options, client *http.Client, manifest []byte) (io.ReadCloser, error) {
	refuse := func(err error) (io.ReadCloser, error) {
		client.CloseIdleConnections()
		return nil, err
	}
	pl, perr := parseHLSPlaylist(manifest, base)
	if perr != nil {
		return refuse(fmt.Errorf("%w: parse manifest: %v", ErrUnsupportedSource, perr))
	}
	if pl.IsMaster {
		// Master playlist → pick the highest bandwidth variant whose
		// URI parses, fetch that, and use it as the media playlist.
		variant := pickVariant(pl)
		if variant == nil {
			return refuse(fmt.Errorf("%w: master playlist has no usable variant", ErrUnsupportedSource))
		}
		body, finalURL, err := hlsFetch(ctx, variant, opt, client)
		if err != nil {
			return refuse(err)
		}
		base = finalURL
		pl, perr = parseHLSPlaylist(body, base)
		if perr != nil {
			return refuse(fmt.Errorf("%w: parse media: %v", ErrUnsupportedSource, perr))
		}
		if pl.IsMaster {
			return refuse(fmt.Errorf("%w: master pointed at another master", ErrUnsupportedSource))
		}
	}
	if pl.HasFMP4 {
		// Distinct sentinel so the caller can tell "this upstream is fMP4" apart
		// from a generic refusal — it is worth seeing in a log.
		return refuse(ErrHLSIsFMP4)
	}
	pr, pw := io.Pipe()
	go (&hlsPuller{
		ctx:    ctx,
		base:   base,
		client: client,
		opt:    opt,
		seen:   map[string]bool{},
		pw:     pw,
	}).run(pl)
	return pr, nil
}

type hlsPuller struct {
	ctx    context.Context
	base   *url.URL
	client *http.Client
	opt    Options
	seen   map[string]bool
	pw     *io.PipeWriter

	// segBuf stages one segment body at a time. run() is the only goroutine that
	// touches it and pw.Write blocks until the consumer has drained what it was
	// handed, so the buffer is free again by the next iteration. Allocating it per
	// segment instead meant a multi-megabyte allocation (two, when the upstream
	// sends no Content-Length and io.Copy has to grow) per segment per stream —
	// pure garbage on the ingest hot path.
	segBuf bytes.Buffer
}

func (p *hlsPuller) run(initial *hlsPlaylist) {
	defer p.pw.Close()
	// This client is ours alone and nothing can reach it once this goroutine
	// returns, so drain its pool on the way out. A connection left in it would
	// stay open, with its reader goroutine, until IdleConnTimeout expired it 90s
	// later: one stranded socket per reconnect on a flapping source.
	defer p.client.CloseIdleConnections()

	pl := initial
	// Segment failures and manifest failures are counted SEPARATELY. A shared
	// counter that any success reset meant a source serving a perfectly valid
	// playlist of dead segments could never trip it whenever the live window held
	// fewer segments than the threshold: each manifest poll wiped the tally.
	segFails, manifestFails := 0, 0
	for {
		// First pass: enqueue any new segments from the current pl.
		for _, seg := range pl.Segments {
			if p.ctx.Err() != nil {
				return
			}
			uri := seg.URI.String()
			if p.seen[uri] {
				continue
			}
			if err := p.streamSegment(seg.URI); err != nil {
				segFails++
				if segFails >= maxHLSConsecutiveFails {
					p.pw.CloseWithError(fmt.Errorf("hls pull: %d consecutive segment failures: %w", segFails, err))
					return
				}
				continue
			}
			// Mark seen only after a successful stream so a transient fetch
			// failure is retried on the next manifest poll instead of being
			// permanently skipped by retainSeenInWindow.
			p.seen[uri] = true
			segFails = 0
		}
		if pl.Endlist {
			return // VOD: source-side ended.
		}
		// Sleep before re-fetching the manifest. HLS spec says clients
		// should poll at most every target_duration; / 2 is reasonable
		// for live and gives us low latency without hammering the
		// upstream.
		wait := pl.TargetDuration / 2
		if wait < 1 {
			wait = 1
		}
		t := time.NewTimer(time.Duration(wait) * time.Second)
		select {
		case <-p.ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		body, _, err := hlsFetch(p.ctx, p.base, p.opt, p.client)
		if err != nil {
			manifestFails++
			if manifestFails >= maxHLSConsecutiveFails {
				p.pw.CloseWithError(fmt.Errorf("hls pull: manifest unreachable: %w", err))
				return
			}
			continue
		}
		next, perr := parseHLSPlaylist(body, p.base)
		if perr != nil {
			// This used to increment and loop with no threshold check, so a
			// permanently unparseable manifest span forever at the poll cadence:
			// the pipe never closed, so the puller never errored, so it never
			// backed off and never rotated to the next source URL. A silently
			// dead channel that looked healthy from the outside.
			manifestFails++
			if manifestFails >= maxHLSConsecutiveFails {
				p.pw.CloseWithError(fmt.Errorf("hls pull: manifest unparseable %d polls running: %w", manifestFails, perr))
				return
			}
			continue
		}
		manifestFails = 0
		// Bound the de-dup map to the live window: a live HLS window only
		// slides forward, so a URI that has rolled out of the manifest can
		// never reappear. Forgetting evicted URIs keeps `seen` from growing
		// for the life of the run while preserving de-dup of still-listed
		// segments (a slow leak otherwise).
		p.seen = retainSeenInWindow(p.seen, next)
		pl = next
	}
}

// retainSeenInWindow rebuilds the segment de-dup set to only the URIs still
// present in pl. Both the TS puller and the fMP4 mirror call this on each
// manifest refresh so the `seen` map tracks the live window instead of every
// segment ever observed — a window-evicted URI can never reappear, so it is
// safe to forget while keeping de-dup correctness for still-listed segments.
func retainSeenInWindow(seen map[string]bool, pl *hlsPlaylist) map[string]bool {
	next := make(map[string]bool, len(pl.Segments))
	for _, seg := range pl.Segments {
		k := seg.URI.String()
		if seen[k] {
			next[k] = true
		}
	}
	return next
}

func (p *hlsPuller) streamSegment(u *url.URL) error {
	req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	p.opt.apply(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("segment %s: http %d", u.Redacted(), resp.StatusCode)
	}
	// Buffer the segment fully before writing to the pipe, so a mid-body
	// read failure never emits a partial prefix into the live stream (which
	// the retry — now that we only mark a segment `seen` after success —
	// would otherwise duplicate). Pre-size from Content-Length to avoid the
	// io.ReadAll doubling-realloc churn on the ingest hot path. Cap the body:
	// hitting the cap means a runaway/hostile upstream, so treat it as an
	// error (fail + fail-over) rather than emitting a truncated mid-packet
	// blob as a "successful" segment — matching the fMP4 mirror path.
	p.segBuf.Reset()
	// Reuse keeps the buffer at the high-water mark of the segments seen so far,
	// which is the point — but one freak oversized segment must not pin that much
	// for the life of the stream, so drop the buffer when it has grown past
	// anything a real segment justifies.
	if p.segBuf.Cap() > maxRetainedSegBuf {
		p.segBuf = bytes.Buffer{}
	}
	if cl := resp.ContentLength; cl > 0 && cl <= maxHLSSegmentBytes {
		p.segBuf.Grow(int(cl))
	}
	n, err := io.Copy(&p.segBuf, io.LimitReader(resp.Body, maxHLSSegmentBytes))
	if err != nil {
		return err
	}
	if n >= maxHLSSegmentBytes {
		return fmt.Errorf("segment %s: exceeds %d-byte cap (runaway upstream)", u.Redacted(), maxHLSSegmentBytes)
	}
	_, err = p.pw.Write(p.segBuf.Bytes())
	return err
}

// hlsFetch GETs a playlist URL and returns the body plus the URL that
// was actually served (after redirects), needed to resolve relative
// segment URIs.
func hlsFetch(ctx context.Context, u *url.URL, opt Options, c *http.Client) ([]byte, *url.URL, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	opt.apply(req)
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, nil, fmt.Errorf("playlist %s: http %d", u.Redacted(), resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPlaylistBytes))
	if err != nil {
		return nil, nil, err
	}
	final := resp.Request.URL // honour redirects
	if final == nil {
		final = u
	}
	return body, final, nil
}

// hlsPlaylist is the parsed shape of an .m3u8 — only the fields the
// puller / mirror actually use.
type hlsPlaylist struct {
	IsMaster       bool
	HasFMP4        bool
	Endlist        bool
	TargetDuration int      // seconds
	MapURI         *url.URL // EXT-X-MAP target (fMP4 init)
	Segments       []hlsSegment
	Variants       []hlsVariant
}

type hlsSegment struct {
	URI      *url.URL
	Duration float64
}

type hlsVariant struct {
	URI       *url.URL
	Bandwidth int
}

func parseHLSPlaylist(body []byte, base *url.URL) (*hlsPlaylist, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("empty body")
	}
	pl := &hlsPlaylist{}
	// bytes.NewReader, not strings.NewReader(string(body)): the conversion copied
	// the entire manifest on every poll of every native-HLS stream, for nothing.
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var pendingDur float64
	var pendingBW int
	expectVariantURI := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		switch {
		case line == "#EXTM3U":
			continue
		case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
			pl.TargetDuration = atoiSafe(line[len("#EXT-X-TARGETDURATION:"):])
		case strings.HasPrefix(line, "#EXT-X-ENDLIST"):
			pl.Endlist = true
		case strings.HasPrefix(line, "#EXT-X-MAP:"):
			pl.HasFMP4 = true
			for _, kv := range splitAttrs(strings.TrimPrefix(line, "#EXT-X-MAP:")) {
				if strings.HasPrefix(kv, "URI=") {
					raw := strings.Trim(strings.TrimPrefix(kv, "URI="), `"`)
					if u, err := resolveURI(base, raw); err == nil {
						pl.MapURI = u
					}
				}
			}
		case strings.HasPrefix(line, "#EXTINF:"):
			rest := strings.TrimPrefix(line, "#EXTINF:")
			if comma := strings.IndexByte(rest, ','); comma >= 0 {
				rest = rest[:comma]
			}
			pendingDur, _ = strconv.ParseFloat(rest, 64)
		case strings.HasPrefix(line, "#EXT-X-STREAM-INF:"):
			pl.IsMaster = true
			pendingBW = parseBandwidth(line)
			expectVariantURI = true
		case strings.HasPrefix(line, "#"):
			// Other tag we don't model — keep walking.
		default:
			// URI line.
			u, err := resolveURI(base, line)
			if err != nil {
				continue
			}
			if expectVariantURI {
				pl.Variants = append(pl.Variants, hlsVariant{URI: u, Bandwidth: pendingBW})
				expectVariantURI = false
				pendingBW = 0
				continue
			}
			// Detect fMP4 by extension as a fallback for upstreams
			// that skip EXT-X-MAP — both signals route to ffmpeg.
			low := strings.ToLower(u.Path)
			if strings.HasSuffix(low, ".m4s") || strings.HasSuffix(low, ".mp4") {
				pl.HasFMP4 = true
			}
			pl.Segments = append(pl.Segments, hlsSegment{URI: u, Duration: pendingDur})
			pendingDur = 0
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return pl, nil
}

func pickVariant(pl *hlsPlaylist) *url.URL {
	var best *url.URL
	bestBW := -1
	for _, v := range pl.Variants {
		if v.URI == nil {
			continue
		}
		if v.Bandwidth > bestBW {
			bestBW = v.Bandwidth
			best = v.URI
		}
	}
	if best == nil && len(pl.Variants) > 0 {
		return pl.Variants[0].URI
	}
	return best
}

func parseBandwidth(line string) int {
	// EXT-X-STREAM-INF:BANDWIDTH=1234567,RESOLUTION=...
	rest := strings.TrimPrefix(line, "#EXT-X-STREAM-INF:")
	for _, kv := range splitAttrs(rest) {
		if strings.HasPrefix(kv, "BANDWIDTH=") {
			return atoiSafe(kv[len("BANDWIDTH="):])
		}
	}
	return 0
}

// splitAttrs tokenises an HLS attribute list (k=v,k="v with comma",…).
// Plenty good enough for BANDWIDTH lookup; we don't need full RFC
// 8216 attribute parsing here.
func splitAttrs(s string) []string {
	var out []string
	depth := 0
	start := 0
	for i, r := range s {
		switch r {
		case '"':
			if depth == 0 {
				depth = 1
			} else {
				depth = 0
			}
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if start < len(s) {
		out = append(out, strings.TrimSpace(s[start:]))
	}
	return out
}

func resolveURI(base *url.URL, raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if base == nil {
		return u, nil
	}
	return base.ResolveReference(u), nil
}

func atoiSafe(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}
