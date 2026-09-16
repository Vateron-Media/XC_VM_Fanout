// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
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

// maxSegmentStalls is how many consecutive manifest polls the pass may be held
// at the SAME failing segment before it is written off and stepped over, in
// order. Retrying a segment in place is what keeps the wire in time order, but
// an unbounded hold turns one segment the packager lost into a dead channel:
// nothing downstream of it goes out, so the consecutive-failure threshold trips
// on it and the pipe closes, and the reconnect rejoins at a live edge where the
// same segment may still be sitting.
//
// Three attempts spans two poll waits — one target duration — so a transient
// 5xx or timeout is retried in place and recovers with no hole at all, while the
// silence before the pull gives up stays well inside the source's own stall
// bound of three target durations. The give-up is not a free pass: the failure
// still counts toward maxHLSConsecutiveFails, so a source whose segments are ALL
// dead still fails over to the next URL.
const maxSegmentStalls = 3

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
	// A proxy that cannot be used is a refusal: the manifest polls and every
	// segment fetch would otherwise go direct from the node's own IP.
	if err := opt.checkProxy(); err != nil {
		return nil, err
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
			return refuse(fmt.Errorf("%w: master playlist has no usable variant", ErrFormat))
		}
		// A variant whose audio is a separate EXT-X-MEDIA rendition has
		// video-only segments, and this package passes segment bytes through
		// unread — so taking it would fan out a silent channel with nothing to
		// say anything was wrong. Refuse: ffmpeg maps the rendition back in.
		if pl.audioIsElsewhere(variant.Audio) {
			return refuse(fmt.Errorf("%w: hls audio is a separate rendition (EXT-X-MEDIA group %q)", ErrFormat, variant.Audio))
		}
		body, finalURL, err := hlsFetch(ctx, variant.URI, opt, client)
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
	if err := servable(pl); err != nil {
		return refuse(err)
	}
	pr, pw := io.Pipe()
	// The puller dies with its READER, not with the stream. It used to run on the
	// caller's ctx — the stream's whole lifetime — while Close() only shut the
	// pipe, so a puller whose reader had gone kept polling. Against a frozen
	// playlist it never writes again, so its segment-failure counter never grows,
	// and every poll succeeds, so its manifest counter keeps resetting: nothing
	// could ever stop it. Every reconnect left another one behind, each with its
	// own transport, all hammering the upstream. Cancelling also aborts an
	// in-flight segment or manifest fetch instead of waiting it out.
	pctx, cancel := context.WithCancel(ctx)
	go (&hlsPuller{
		ctx:    pctx,
		base:   base,
		client: client,
		opt:    opt,
		seen:   map[string]bool{},
		pw:     pw,
	}).run(pl)
	return &hlsReader{PipeReader: pr, idle: hlsIdleBound(pl), cancel: cancel}, nil
}

// hlsReader is the byte stream of a live HLS pull, which knows how long it may
// legitimately go without bytes: see IdleBound.
type hlsReader struct {
	*io.PipeReader
	idle   time.Duration
	cancel context.CancelFunc
}

// Close stops the puller goroutine as well as the pipe. Closing only the pipe
// left the puller running: it notices a dead reader on its next pw.Write, and a
// frozen playlist gives it nothing to write.
func (r *hlsReader) Close() error {
	r.cancel()
	return r.PipeReader.Close()
}

// IdleBound is the longest silence this source can have while healthy. A live
// HLS source arrives a whole segment at a time and says nothing in between, so
// the gap is the upstream's segment duration — ten seconds is common. The
// default stall bound (eight seconds, the ffmpeg path's -rw_timeout) closed
// every such source between two healthy segments; a stall here is three target
// durations without a new segment, which is also how a playlist that stopped
// updating is caught.
func (r *hlsReader) IdleBound() time.Duration { return r.idle }

func hlsIdleBound(pl *hlsPlaylist) time.Duration {
	d := 3 * time.Duration(pl.TargetDuration) * time.Second
	if d < DefaultSourceIdleTimeout {
		d = DefaultSourceIdleTimeout
	}
	return d
}

// hlsPollWait is how long to wait before re-fetching the manifest. The spec says
// a client should poll at most every target duration; half of it keeps latency
// down without hammering the upstream, and the 1s floor covers a playlist that
// advertises no target duration at all.
func hlsPollWait(pl *hlsPlaylist) time.Duration {
	wait := pl.TargetDuration / 2
	if wait < 1 {
		wait = 1
	}
	return time.Duration(wait) * time.Second
}

// hlsLiveStartSegments is how many of a live playlist's newest segments a pull
// starts with — ffmpeg's default live_start_index of -3.
const hlsLiveStartSegments = 3

type hlsPuller struct {
	ctx    context.Context
	base   *url.URL
	client *http.Client
	opt    Options
	pw     *io.PipeWriter

	// How the pull knows what it has already sent. The media sequence is the
	// primary key when the playlist carries one, as it is for ffmpeg: it is the
	// identity of a segment's PLACE in the stream, which the URI is not. An
	// upstream that re-signs every segment URL per response (seg.ts?token=…)
	// made the whole window look new on every poll and replayed it into the
	// ring; an encoder restarting inside one window length reuses its names, and
	// every one of them looked already-sent. seen is the fallback for a playlist
	// with no usable sequence.
	seen  map[string]bool
	seq   int64 // next media sequence to stream
	bySeq bool  // de-dup follows the media sequence rather than the URI
	first int64 // MEDIA-SEQUENCE of the playlist the state was last synced to

	// Which segment the pass is currently held at, and for how many polls
	// running — see maxSegmentStalls. The key follows the same identity the
	// de-dup does, so an upstream that re-signs every URL is still recognised as
	// stalling on the same segment rather than on a new one each poll.
	stallKey   string
	stallTries int

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
	p.join(pl)
	// Segment failures and manifest failures are counted SEPARATELY. A shared
	// counter that any success reset meant a source serving a perfectly valid
	// playlist of dead segments could never trip it whenever the live window held
	// fewer segments than the threshold: each manifest poll wiped the tally.
	// notTS counts segment BODIES in a row that were not MPEG-TS, separately
	// again: what the upstream is has to be read off a run of bodies, not off
	// one. See the escalation below.
	segFails, manifestFails, notTS := 0, 0, 0
	for {
		// First pass: enqueue any new segments from the current pl.
		stalled := false
		for i, seg := range pl.Segments {
			if p.ctx.Err() != nil {
				return
			}
			if p.streamed(pl, i) {
				continue
			}
			if err := p.streamSegment(seg.URI); err != nil {
				// A segment body that is not MPEG-TS says the upstream is
				// something this package cannot pass through — but ONE of them
				// does not. An origin over its connection limit answers a
				// segment with 200 and an HTML page and serves the same segment
				// correctly a second later, and a format refusal is what hands
				// the stream to ffmpeg for the life of its spec. So it takes a
				// run of them, the same evidence the playlist's own extensions
				// give at once (see servable): a provider really serving packed
				// audio from .ts URLs reaches ffmpeg a few polls later, while a
				// blip costs a retry.
				if IsFormat(err) {
					notTS++
					if notTS >= maxHLSConsecutiveFails {
						p.pw.CloseWithError(fmt.Errorf("hls pull: %d segment bodies running: %w", notTS, err))
						return
					}
				} else {
					notTS = 0
				}
				segFails++
				if segFails >= maxHLSConsecutiveFails {
					p.pw.CloseWithError(fmt.Errorf("hls pull: %d consecutive segment failures: %w", segFails, err))
					return
				}
				// A segment that has been failing for maxSegmentStalls polls
				// running is not coming back, and holding the pass at it holds
				// the whole channel: write it off and step over it, IN ORDER, so
				// what is behind it reaches viewers. markStreamed is what makes
				// the skip permanent — the retry that would otherwise land
				// behind newer content can no longer happen. Only worth doing
				// when something newer is actually waiting: with nothing behind
				// it there is nothing to release, and retiring the segment would
				// take the evidence away from the failure threshold and leave
				// the pull silently polling a window it will never take from.
				if p.stalledOn(pl, i) >= maxSegmentStalls && p.unseenAfter(pl, i) {
					dlog.Logf("hls", "segment %s failed %d polls running, skipping it: %v",
						redactURL(seg.URI), p.stallTries, err)
					p.markStreamed(pl, i)
					p.clearStall()
					continue
				}
				// End the pass here rather than stepping over the hole. The
				// segment stays unseen so the next poll retries it — but its
				// successors must not go out first, or that retry lands BEHIND
				// newer content already on the wire: PTS and PCR jump backwards
				// and the ring and the HLS cutter see time reverse.
				stalled = true
				break
			}
			// Mark seen only after a successful stream so a transient fetch
			// failure is retried on the next manifest poll instead of being
			// permanently skipped by retainSeenInWindow.
			p.markStreamed(pl, i)
			p.clearStall()
			segFails, notTS = 0, 0
		}
		if pl.Endlist && !stalled {
			return // VOD: source-side ended.
		}
		// Sleep before re-fetching the manifest.
		t := time.NewTimer(hlsPollWait(pl))
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
		// A live playlist can change flavour under us — a provider switching a
		// channel to fMP4, or turning on encryption. Passing the new segments
		// through would put ciphertext or MP4 boxes on the wire, so end the pull
		// with the same refusal a fresh open would give.
		if err := servable(next); err != nil {
			p.pw.CloseWithError(fmt.Errorf("hls pull: playlist changed: %w", err))
			return
		}
		p.resync(pl, next)
		pl = next
	}
}

// nonTSSegmentExt are segment suffixes that are definitively NOT MPEG-TS. The
// audio ones are RFC 8216 packed audio (ID3 + ADTS/MP3 straight in the segment,
// no TS wrapper), which is how radio channels ship; the caption ones are WebVTT.
// Refusing on the name catches these before a single byte goes out, which is
// what gets the source onto its ffmpeg fallback — ffmpeg reads packed audio.
var nonTSSegmentExt = map[string]bool{
	".aac": true, ".ac3": true, ".ec3": true, ".mp3": true,
	".vtt": true, ".webvtt": true,
}

// servable reports why a media playlist cannot be passed through, or nil.
func servable(pl *hlsPlaylist) error {
	switch {
	case pl.HasFMP4:
		// Distinct sentinel so the caller can tell "this upstream is fMP4" apart
		// from a generic refusal — it is worth seeing in a log.
		return ErrHLSIsFMP4
	case pl.Encrypted:
		return ErrHLSEncrypted
	case pl.HasByteRange:
		// Segments are ranges of one resource. streamSegment sends no Range
		// header and de-dups on the URI, so serving this would fetch the whole
		// resource once and drop every later entry — and fetch nothing at all
		// once it outgrows the runaway limit. Refuse rather than half-serve.
		return ErrHLSByteRange
	}
	for _, seg := range pl.Segments {
		if ext := strings.ToLower(path.Ext(seg.URI.Path)); nonTSSegmentExt[ext] {
			return fmt.Errorf("%w (%s segments)", ErrHLSNotTS, ext)
		}
	}
	return nil
}

// checkSegmentIsTS is the guard the filename cannot give: a provider serving
// packed audio, or an error page, from a .ts URL. Segment bytes are passed
// through unread, so the body is the last place to notice before they reach
// viewers as 188-byte chunks of something that is not TS.
func checkSegmentIsTS(b []byte, u *url.URL) error {
	if len(b) < tsPacketSize {
		// Too short to be even one packet: a truncated or empty fetch. That says
		// nothing about WHAT the upstream is, so it stays an ordinary transport
		// failure — retried, then failed over once the threshold trips — rather
		// than a format refusal, which would move the source to ffmpeg for good.
		return fmt.Errorf("segment %s: %d bytes, short of one %d-byte packet", redactURL(u), len(b), tsPacketSize)
	}
	head := b
	if len(head) > tsSyncProbePackets*tsPacketSize {
		head = head[:tsSyncProbePackets*tsPacketSize]
	}
	if !looksLikeTS(head) {
		return fmt.Errorf("%w: segment %s", ErrHLSNotTS, redactURL(u))
	}
	return nil
}

// join positions the de-dup state on a playlist the pull is starting to read —
// at open, and again whenever the upstream turns out to be a different stream
// from the one that was being followed.
//
// A live playlist is joined near its edge, as ffmpeg's HLS demuxer does
// (live_start_index -3), not from its oldest entry. Streaming the whole window
// first put up to a minute of old content into the pipeline in a few seconds at
// download speed — and every reconnect did it again. A VOD playlist (ENDLIST)
// still plays from its start.
func (p *hlsPuller) join(pl *hlsPlaylist) {
	p.bySeq = pl.HasMediaSeq
	p.first = pl.MediaSequence
	p.seen = map[string]bool{}
	edge := 0
	if !pl.Endlist && len(pl.Segments) > hlsLiveStartSegments {
		edge = len(pl.Segments) - hlsLiveStartSegments
	}
	if p.bySeq {
		p.seq = pl.MediaSequence + int64(edge)
		return
	}
	for _, seg := range pl.Segments[:edge] {
		p.seen[seg.URI.String()] = true
	}
}

// streamed reports whether the i'th segment of pl has already gone out.
func (p *hlsPuller) streamed(pl *hlsPlaylist, i int) bool {
	if p.bySeq {
		return pl.MediaSequence+int64(i) < p.seq
	}
	return p.seen[pl.Segments[i].URI.String()]
}

// segKey identifies the segment at index i the way the de-dup does: by its place
// in the stream when the playlist carries a media sequence, by its URI when it
// does not. Used to tell "still stuck on the same segment" from "stuck on a new
// one", which the URI alone cannot do for an upstream that re-signs every URL.
func (p *hlsPuller) segKey(pl *hlsPlaylist, i int) string {
	if p.bySeq {
		return strconv.FormatInt(pl.MediaSequence+int64(i), 10)
	}
	return pl.Segments[i].URI.String()
}

// stalledOn counts this pass's failure at i against the segment it is stuck on
// and returns how many polls running that has now been. A failure at a different
// segment starts the count again: the bound is on ONE segment holding the pass,
// not on the source's failures in general, which maxHLSConsecutiveFails covers.
func (p *hlsPuller) stalledOn(pl *hlsPlaylist, i int) int {
	if k := p.segKey(pl, i); k != p.stallKey {
		p.stallKey, p.stallTries = k, 0
	}
	p.stallTries++
	return p.stallTries
}

// clearStall forgets the held segment, after a successful stream or once the
// pass has given up on it.
func (p *hlsPuller) clearStall() { p.stallKey, p.stallTries = "", 0 }

// unseenAfter reports whether any segment after i is still unstreamed — whether
// giving up on i would release anything at all.
func (p *hlsPuller) unseenAfter(pl *hlsPlaylist, i int) bool {
	for j := i + 1; j < len(pl.Segments); j++ {
		if !p.streamed(pl, j) {
			return true
		}
	}
	return false
}

// markStreamed records that it has. Called only after a successful stream, so a
// transient fetch failure is retried on the next poll rather than skipped.
func (p *hlsPuller) markStreamed(pl *hlsPlaylist, i int) {
	if p.bySeq {
		p.seq = pl.MediaSequence + int64(i) + 1
		return
	}
	p.seen[pl.Segments[i].URI.String()] = true
}

// resync moves the de-dup state from the playlist just finished onto the one
// just fetched.
func (p *hlsPuller) resync(pl, next *hlsPlaylist) {
	if !p.bySeq {
		// Bound the de-dup map to the live window: a live window only slides
		// forward, so a URI that has rolled out of the manifest can never
		// reappear. Forgetting evicted URIs keeps `seen` from growing for the
		// life of the run while still de-duping what is still listed.
		p.seen = retainSeenInWindow(p.seen, next)
		return
	}
	switch {
	case !next.HasMediaSeq:
		// The tag we were following vanished; nothing left to follow.
		p.uriFallback(pl, next)
	case next.MediaSequence < p.first:
		// The sequence went BACKWARDS: this is a new stream on the same URL, an
		// encoder that restarted. Its segments are new content however familiar
		// their names are, so rejoin as if opening the playlist fresh.
		p.join(next)
	case next.MediaSequence == p.first && rolledUnderAFrozenSequence(pl, next):
		p.uriFallback(pl, next)
	default:
		p.first = next.MediaSequence
	}
}

// uriFallback abandons media-sequence de-dup for the rest of the run, carrying
// over what has already been streamed so the switch itself replays nothing.
func (p *hlsPuller) uriFallback(pl, next *hlsPlaylist) {
	p.seen = map[string]bool{}
	for i := range pl.Segments {
		if pl.MediaSequence+int64(i) < p.seq {
			p.seen[pl.Segments[i].URI.String()] = true
		}
	}
	p.bySeq = false
	p.seen = retainSeenInWindow(p.seen, next)
}

// rolledUnderAFrozenSequence reports whether next's window has moved on while
// #EXT-X-MEDIA-SEQUENCE stood still — an encoder that writes a constant
// sequence, which some do. Trusting the number there would freeze the channel
// at the first window, so the URI is the only thing left to follow.
//
// The test is the first entry's PATH, with its query dropped: a rolled window
// names a different segment, while an upstream that merely re-signs its URLs
// per response names the same one under a new token. Comparing whole URIs would
// mistake the re-signed window for a rolled one and put the replay back.
func rolledUnderAFrozenSequence(pl, next *hlsPlaylist) bool {
	if len(pl.Segments) == 0 || len(next.Segments) == 0 {
		return false
	}
	return pl.Segments[0].URI.Path != next.Segments[0].URI.Path
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
		return fmt.Errorf("segment %s: http %d", redactURL(u), resp.StatusCode)
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
		return fmt.Errorf("segment %s: exceeds %d-byte cap (runaway upstream)", redactURL(u), maxHLSSegmentBytes)
	}
	if err := checkSegmentIsTS(p.segBuf.Bytes(), u); err != nil {
		return err
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
		return nil, nil, fmt.Errorf("playlist %s: http %d", redactURL(u), resp.StatusCode)
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
	Encrypted      bool // an #EXT-X-KEY with a METHOD other than NONE
	Endlist        bool
	TargetDuration int // seconds
	// MediaSequence is #EXT-X-MEDIA-SEQUENCE: the sequence number of the first
	// segment listed, each later one counting up from it. It is what identifies
	// a segment's place in the stream — the URI does not, because upstreams
	// re-sign URLs per response and encoders reuse names after a restart.
	// HasMediaSeq says the playlist actually carried a usable one, since a
	// missing tag means 0 and a de-dup keyed on a number that never moves would
	// freeze the channel.
	MediaSequence int64
	HasMediaSeq   bool
	// HasByteRange is set by #EXT-X-BYTERANGE: the segments are slices of a
	// larger resource, not whole files. This puller fetches a segment URI whole,
	// so it cannot serve one of those.
	HasByteRange bool
	MapURI       *url.URL // EXT-X-MAP target (fMP4 init)
	Segments       []hlsSegment
	Variants       []hlsVariant
	// DemuxedAudio holds the GROUP-IDs of #EXT-X-MEDIA TYPE=AUDIO renditions
	// that carry their OWN URI. Per RFC 8216 such a rendition lives outside the
	// variant, so a variant referencing one has video-only segments.
	DemuxedAudio map[string]bool
	// MuxedAudio holds the GROUP-IDs that have at least one AUDIO rendition with
	// NO URI — audio that is already in the variant's own segments. A group can
	// hold both shapes (a muxed default language plus separate alternates), and
	// then the variant does carry sound: see audioIsElsewhere.
	MuxedAudio map[string]bool
}

// audioIsElsewhere reports whether a variant's audio group leaves the variant's
// own segments silent — every rendition in the group having its own URI. A group
// with even one URI-less AUDIO rendition is muxed into the variant, which is the
// common multi-language shape (default language in the segments, alternates as
// renditions) and plays natively with sound.
func (pl *hlsPlaylist) audioIsElsewhere(group string) bool {
	return pl.DemuxedAudio[group] && !pl.MuxedAudio[group]
}

type hlsSegment struct {
	URI      *url.URL
	Duration float64
}

type hlsVariant struct {
	URI       *url.URL
	Bandwidth int
	Audio     string // AUDIO="<group-id>", empty when the variant names none
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
	var pendingAudio string
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
			pl.TargetDuration = clampTargetDuration(atoiSafe(line[len("#EXT-X-TARGETDURATION:"):]))
		case strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:"):
			// Only a value that can be counted on: negative is nonsense, and one
			// close to the int64 ceiling would wrap when the segment index is
			// added to it and make every segment look already-sent.
			if n, err := strconv.ParseInt(strings.TrimSpace(line[len("#EXT-X-MEDIA-SEQUENCE:"):]), 10, 64); err == nil && n >= 0 && n < 1<<62 {
				pl.MediaSequence = n
				pl.HasMediaSeq = true
			}
		case strings.HasPrefix(line, "#EXT-X-BYTERANGE:"):
			// The segment that follows is a slice of its URI, not the whole of
			// it. Noticed, never honoured — see servable.
			pl.HasByteRange = true
		case strings.HasPrefix(line, "#EXT-X-ENDLIST"):
			pl.Endlist = true
		case strings.HasPrefix(line, "#EXT-X-KEY:"):
			// METHOD=NONE switches encryption back off for what follows; any
			// other method (AES-128, SAMPLE-AES…) means ciphertext segments.
			for _, kv := range splitAttrs(strings.TrimPrefix(line, "#EXT-X-KEY:")) {
				if m, ok := strings.CutPrefix(kv, "METHOD="); ok && !strings.EqualFold(strings.Trim(m, `"`), "NONE") {
					pl.Encrypted = true
				}
			}
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
		case strings.HasPrefix(line, "#EXT-X-MEDIA:"):
			// Both halves matter. A rendition WITH a URI has its media outside
			// the variant; one WITHOUT has it muxed into the variant's own
			// segments. RFC 8216 makes URI optional for AUDIO, and a
			// multi-language group commonly holds one of each — so the group is
			// only demuxed when EVERY rendition in it has its own URI.
			attrs := splitAttrs(strings.TrimPrefix(line, "#EXT-X-MEDIA:"))
			if !strings.EqualFold(attrValue(attrs, "TYPE"), "AUDIO") {
				continue
			}
			g := attrValue(attrs, "GROUP-ID")
			if g == "" {
				continue
			}
			if attrValue(attrs, "URI") == "" {
				if pl.MuxedAudio == nil {
					pl.MuxedAudio = map[string]bool{}
				}
				pl.MuxedAudio[g] = true
				continue
			}
			if pl.DemuxedAudio == nil {
				pl.DemuxedAudio = map[string]bool{}
			}
			pl.DemuxedAudio[g] = true
		case strings.HasPrefix(line, "#EXTINF:"):
			rest := strings.TrimPrefix(line, "#EXTINF:")
			if comma := strings.IndexByte(rest, ','); comma >= 0 {
				rest = rest[:comma]
			}
			pendingDur, _ = strconv.ParseFloat(rest, 64)
		case strings.HasPrefix(line, "#EXT-X-STREAM-INF:"):
			pl.IsMaster = true
			attrs := splitAttrs(strings.TrimPrefix(line, "#EXT-X-STREAM-INF:"))
			pendingBW = atoiSafe(attrValue(attrs, "BANDWIDTH"))
			pendingAudio = attrValue(attrs, "AUDIO")
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
				pl.Variants = append(pl.Variants, hlsVariant{URI: u, Bandwidth: pendingBW, Audio: pendingAudio})
				expectVariantURI = false
				pendingBW = 0
				pendingAudio = ""
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

func pickVariant(pl *hlsPlaylist) *hlsVariant {
	var best *hlsVariant
	bestBW := -1
	for i := range pl.Variants {
		v := &pl.Variants[i]
		if v.URI == nil {
			continue
		}
		if v.Bandwidth > bestBW {
			bestBW = v.Bandwidth
			best = v
		}
	}
	return best
}

// attrValue reads one value out of a tokenised HLS attribute list, dropping the
// quotes an enumerated-string value carries.
func attrValue(attrs []string, key string) string {
	for _, kv := range attrs {
		if v, ok := strings.CutPrefix(kv, key+"="); ok {
			return strings.Trim(v, `"`)
		}
	}
	return ""
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

// Bounds on a playlist's advertised #EXT-X-TARGETDURATION. Both the poll cadence
// and the stall bound are derived from it, so an unbounded value is an unbounded
// timer: `#EXT-X-TARGETDURATION:9223372036854775807` overflowed
// time.Duration to a NEGATIVE poll wait, so the timer fired at once and the
// puller re-fetched the manifest in a tight loop against the provider; an
// upstream writing milliseconds (6000) put the polls 50 minutes apart under a
// five-hour stall bound, so the channel played its first segments and then sat
// frozen for hours with the watchdog asleep. 60s is well past any real live
// target duration (10s is typical, 6s common), and anything outside the range is
// a broken value rather than a cadence, so it falls back to the usual one.
const (
	maxTargetDuration     = 60
	defaultTargetDuration = 10
)

// clampTargetDuration keeps a parsed target duration inside those bounds. A
// playlist with NO target duration tag keeps 0 — that is the absence of a value,
// not a broken one, and it already has its own floor (a 1s poll, the default
// stall bound).
func clampTargetDuration(v int) int {
	if v < 1 || v > maxTargetDuration {
		return defaultTargetDuration
	}
	return v
}
