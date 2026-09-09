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
		return nil, err
	}
	pl, perr := parseHLSPlaylist(body, finalURL)
	if perr != nil {
		return nil, fmt.Errorf("%w: parse manifest: %v", ErrUnsupportedSource, perr)
	}
	if pl.IsMaster {
		// Master playlist → pick the highest bandwidth variant whose
		// URI parses, fetch that, and use it as the media playlist.
		variant := pickVariant(pl, finalURL)
		if variant == nil {
			return nil, fmt.Errorf("%w: master playlist has no usable variant", ErrUnsupportedSource)
		}
		body, finalURL, err = hlsFetch(ctx, variant, opt, client)
		if err != nil {
			return nil, err
		}
		pl, perr = parseHLSPlaylist(body, finalURL)
		if perr != nil {
			return nil, fmt.Errorf("%w: parse media: %v", ErrUnsupportedSource, perr)
		}
		if pl.IsMaster {
			return nil, fmt.Errorf("%w: master pointed at another master", ErrUnsupportedSource)
		}
	}
	if pl.HasFMP4 {
		// Distinct sentinel so the caller can dispatch to
		// MirrorHLSFMP4 instead of falling all the way to ffmpeg —
		// the pass-through mirror handles fMP4 sources natively.
		return nil, ErrHLSIsFMP4
	}
	pr, pw := io.Pipe()
	go (&hlsPuller{
		ctx:    ctx,
		base:   finalURL,
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
}

func (p *hlsPuller) run(initial *hlsPlaylist) {
	defer p.pw.Close()
	pl := initial
	consecutiveFails := 0
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
				consecutiveFails++
				if consecutiveFails >= 5 {
					p.pw.CloseWithError(fmt.Errorf("hls pull: %d consecutive segment failures: %w", consecutiveFails, err))
					return
				}
				continue
			}
			// Mark seen only after a successful stream so a transient fetch
			// failure is retried on the next manifest poll instead of being
			// permanently skipped by retainSeenInWindow.
			p.seen[uri] = true
			consecutiveFails = 0
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
			consecutiveFails++
			if consecutiveFails >= 5 {
				p.pw.CloseWithError(fmt.Errorf("hls pull: manifest unreachable: %w", err))
				return
			}
			continue
		}
		next, perr := parseHLSPlaylist(body, p.base)
		if perr != nil {
			consecutiveFails++
			continue
		}
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
	var buf bytes.Buffer
	if cl := resp.ContentLength; cl > 0 && cl <= maxHLSSegmentBytes {
		buf.Grow(int(cl))
	}
	n, err := io.Copy(&buf, io.LimitReader(resp.Body, maxHLSSegmentBytes))
	if err != nil {
		return err
	}
	if n >= maxHLSSegmentBytes {
		return fmt.Errorf("segment %s: exceeds %d-byte cap (runaway upstream)", u.Redacted(), maxHLSSegmentBytes)
	}
	_, err = p.pw.Write(buf.Bytes())
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20)) // 4 MiB cap; playlists are small
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
	sc := bufio.NewScanner(strings.NewReader(string(body)))
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

func pickVariant(pl *hlsPlaylist, base *url.URL) *url.URL {
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

func cloneHeader(h http.Header) http.Header {
	if h == nil {
		return http.Header{}
	}
	out := make(http.Header, len(h))
	for k, v := range h {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}
