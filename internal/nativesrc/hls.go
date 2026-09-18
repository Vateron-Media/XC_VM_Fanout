// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/fmp4"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/hlscrypt"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsmux"
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
	// Every fetch from here — the manifest polls, the variant, each segment —
	// carries the source's headers. A Host override among them belongs to this
	// host only.
	opt = opt.scopedTo(u)
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
	var initSeg *fmp4.Init
	if pl.HasFMP4 {
		// One fetch, before any segment: everything a media segment needs to be
		// read is in here — the timescales, and the parameter sets the segments
		// do not repeat — and a source whose init segment cannot be read is one
		// to refuse now rather than to discover halfway through.
		body, _, ferr := hlsFetch(ctx, pl.MapURI, opt, client)
		if ferr != nil {
			return refuse(fmt.Errorf("%w: init segment: %v", ErrHLSIsFMP4, ferr))
		}
		parsed, perr := fmp4.ParseInit(body)
		if perr != nil {
			return refuse(fmt.Errorf("%w: init segment: %v", ErrHLSIsFMP4, perr))
		}
		initSeg = parsed
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
		keys:   map[string][]byte{},
		init:   initSeg,
		mux:    muxFor(pl, initSeg),
		pw:     pw,
	}).run(pl)
	return &hlsReader{PipeReader: pr, idle: hlsIdleBound(pl), cancel: cancel}, nil
}

// muxFor returns the muxer this playlist's segments need, or nil for one whose
// segments are already MPEG-TS: an audio-only muxer for packed audio, and a
// full programme for fMP4, whose segments carry video too.
func muxFor(pl *hlsPlaylist, in *fmp4.Init) *tsmux.Muxer {
	switch {
	case in != nil:
		if in.Video() == nil {
			return tsmux.New()
		}
		return tsmux.NewAV()
	case pl.packedAudio():
		return tsmux.New()
	}
	return nil
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

	// nalBuf and adtsBuf stage one sample at a time on the fMP4 path, reused
	// across samples so a segment of a few hundred frames does not allocate one
	// buffer per frame.
	nalBuf  []byte
	adtsBuf []byte

	// muxBuf stages the muxed form of one segment, reused across segments for
	// the same reason segBuf is: a radio channel publishes one every few
	// seconds for the life of the node.
	muxBuf []byte

	// init is the parsed #EXT-X-MAP for an fMP4 source: the tracks its media
	// segments are read against. nil for a plain TS or packed-audio playlist.
	init *fmp4.Init

	// mux wraps packed-audio segments in MPEG-TS, and is nil for an ordinary
	// .ts playlist. It holds the stream's clock and continuity counters, so
	// there is one per pull and it lives as long as the pull does.
	mux *tsmux.Muxer

	// keys caches the AES-128 keys the playlist names, by URI. A live upstream
	// names the same key on every segment of a window and often for the whole
	// channel, so fetching it per segment would double the requests for nothing;
	// a re-key simply appears under a new URI. Guarded because the fetch happens
	// on the puller goroutine but the map outlives any single pass.
	keys   map[string][]byte
	keysMu sync.Mutex

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
			if err := p.streamSegment(seg, pl.MediaSequence+int64(i)); err != nil {
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

// nonTSSegmentExt are segment suffixes that are definitively NOT MPEG-TS and
// that this package cannot turn into it: AC-3 and its enhanced form, MP3, and
// WebVTT captions. Refusing on the name catches them before a single byte goes
// out, which is what gets the source onto its ffmpeg fallback.
//
// .aac is NOT here: RFC 8216 packed AAC (an ID3 tag then bare ADTS frames, no
// transport layer) is wrapped in MPEG-TS by internal/tsmux instead, since AAC
// is already the elementary stream and framing it costs no process.
var nonTSSegmentExt = map[string]bool{
	".ac3": true, ".ec3": true, ".mp3": true,
	".vtt": true, ".webvtt": true,
}

// packedAudioExt are the suffixes this package muxes rather than passes through.
var packedAudioExt = map[string]bool{".aac": true, ".adts": true}

// packedAudio reports whether every segment of the playlist is packed AAC, in
// which case the pull wraps each one in MPEG-TS. A playlist mixing packed audio
// with .ts segments is not something a real packager writes, and muxing half a
// window would put two framings on one wire, so it is left to servable to
// refuse through nonTSSegmentExt.
func (pl *hlsPlaylist) packedAudio() bool {
	for _, seg := range pl.Segments {
		if !packedAudioExt[strings.ToLower(path.Ext(seg.URI.Path))] {
			return false
		}
	}
	return len(pl.Segments) > 0
}

// servable reports why a media playlist cannot be passed through, or nil.
func servable(pl *hlsPlaylist) error {
	switch {
	case pl.HasFMP4 && pl.MapURI == nil:
		// fMP4 segments are read by internal/fmp4 — but only against the init
		// segment #EXT-X-MAP names, which holds the timescales and the
		// parameter sets the media segments do not repeat. Without it there is
		// nothing to read them with. Distinct sentinel so the caller can tell
		// "this upstream is fMP4" apart from a generic refusal.
		return ErrHLSIsFMP4
	case pl.Encrypted && !pl.everyKeyIsAES128():
		// AES-128 this package decrypts (it fetches the key the playlist names);
		// SAMPLE-AES and friends encrypt inside the elementary streams, which
		// needs a demuxer this package does not have. A key line with no URI is
		// unusable for the same practical purpose.
		return ErrHLSEncrypted
	case pl.HasByteRange && !pl.everyRangeIsUsable():
		// A #EXT-X-BYTERANGE this package cannot turn into a Range request —
		// a malformed length, or a first slice with no offset and nothing
		// before it — would be fetched as the whole resource and served as if
		// it were the segment. Refuse rather than half-serve.
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
		p.seen[segIdent(seg)] = true
	}
}

// streamed reports whether the i'th segment of pl has already gone out.
func (p *hlsPuller) streamed(pl *hlsPlaylist, i int) bool {
	if p.bySeq {
		return pl.MediaSequence+int64(i) < p.seq
	}
	return p.seen[segIdent(pl.Segments[i])]
}

// segKey identifies the segment at index i the way the de-dup does: by its place
// in the stream when the playlist carries a media sequence, by its URI when it
// does not. Used to tell "still stuck on the same segment" from "stuck on a new
// one", which the URI alone cannot do for an upstream that re-signs every URL.
func (p *hlsPuller) segKey(pl *hlsPlaylist, i int) string {
	if p.bySeq {
		return strconv.FormatInt(pl.MediaSequence+int64(i), 10)
	}
	return segIdent(pl.Segments[i])
}

// segIdent is a segment's identity for de-dup and stall tracking when the
// playlist carries no media sequence: its URI, plus its byte range when it has
// one — a byte-range playlist gives every slice of a file the same URI, so the
// URI alone would mark the whole file streamed after its first slice.
func segIdent(seg hlsSegment) string {
	if seg.Range == nil {
		return seg.URI.String()
	}
	return seg.URI.String() + "#" + strconv.FormatInt(seg.Range.Offset, 10) + "-" + strconv.FormatInt(seg.Range.Length, 10)
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
	p.seen[segIdent(pl.Segments[i])] = true
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
	case next.MediaSequence < p.first && !overlapsWindow(pl, next):
		// The sequence went BACKWARDS and lands nowhere near the window it
		// replaced: this is a new stream on the same URL, an encoder that
		// restarted and began numbering again. Its segments are new content
		// however familiar their names are, so rejoin as if opening the playlist
		// fresh.
		p.join(next)
	case next.MediaSequence < p.first:
		// Backwards, but still over the same numbering: one poll answered by a
		// CDN edge holding a slightly older copy, or by a second origin behind
		// the same hostname running a few segments behind. That is the SAME
		// stream lagging, not a new one. Rejoining would stream its window
		// again, putting PTS and PCR backwards into the ring. Keep the position
		// already reached — everything at or past it is still unseen and will go
		// out when this copy catches up — and follow the sequence down so the
		// next poll is measured against what was actually served.
		p.first = next.MediaSequence
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
			p.seen[segIdent(pl.Segments[i])] = true
		}
	}
	p.bySeq = false
	p.seen = retainSeenInWindow(p.seen, next)
}

// overlapsWindow reports whether next's media sequences reach into the window
// pl covered. It is what separates a stream that restarted its numbering from
// one whose playlist merely came back a little stale: a restart begins again
// from zero (or from wherever the new encoder starts), far below the window it
// replaced, while a lagging copy of the same stream still overlaps it.
func overlapsWindow(pl, next *hlsPlaylist) bool {
	if len(pl.Segments) == 0 || len(next.Segments) == 0 {
		return false
	}
	return next.MediaSequence+int64(len(next.Segments)) > pl.MediaSequence
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
		k := segIdent(seg)
		if seen[k] {
			next[k] = true
		}
	}
	return next
}

func (p *hlsPuller) streamSegment(seg hlsSegment, seq int64) error {
	u := seg.URI
	req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	p.opt.apply(req)
	if seg.Range != nil {
		req.Header.Set("Range", seg.Range.header())
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("segment %s: http %d", redactURL(u), resp.StatusCode)
	}
	// An origin free to ignore Range answers 200 with the WHOLE resource. Then
	// the slice has to be cut here, and the read has to be bounded by where the
	// slice ends rather than by the segment cap: the resource behind a
	// byte-range playlist is the whole file, which is what the cap is for.
	full := seg.Range != nil && resp.StatusCode != http.StatusPartialContent
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
	limit := int64(maxHLSSegmentBytes)
	if full {
		// Read only as far as the slice: everything past it belongs to later
		// segments, and on a large file that is most of the download.
		if end := seg.Range.Offset + seg.Range.Length; end < limit {
			limit = end
		}
	}
	n, err := io.Copy(&p.segBuf, io.LimitReader(resp.Body, limit))
	if err != nil {
		return err
	}
	if n >= maxHLSSegmentBytes {
		return fmt.Errorf("segment %s: exceeds %d-byte cap (runaway upstream)", redactURL(u), maxHLSSegmentBytes)
	}
	body := p.segBuf.Bytes()
	if full {
		o, l := seg.Range.Offset, seg.Range.Length
		if int64(len(body)) < o+l {
			return fmt.Errorf("segment %s: %d bytes, short of the %d..%d range the playlist asked for",
				redactURL(u), len(body), o, o+l)
		}
		body = body[o : o+l]
	}
	if seg.Key != nil {
		// Decrypt in place, before anything looks at the bytes: ciphertext never
		// looks like MPEG-TS, so the sync-byte check below would refuse every
		// segment of a perfectly good encrypted upstream. The plaintext goes to
		// the ring exactly as a clear source's would — whether the daemon then
		// RE-encrypts what it serves is the panel's own encrypt_hls setting,
		// decided per stream in internal/server, and has nothing to do with this.
		key, kerr := p.keyFor(seg.Key)
		if kerr != nil {
			return kerr
		}
		plain, derr := hlscrypt.DecryptCBC(body, key, seg.Key.iv(seq))
		if derr != nil {
			return fmt.Errorf("segment %s: %w", redactURL(u), derr)
		}
		body = plain
	}
	if p.init != nil {
		// fMP4: read the fragment against the init segment and rebuild it as
		// MPEG-TS. Same rule as packed audio for what a bad body means — a
		// format refusal only until the stream is running, an ordinary failed
		// segment after that.
		muxed, merr := p.remuxFragment(body)
		if merr != nil {
			if p.mux.Started() {
				return fmt.Errorf("segment %s: %w", redactURL(u), merr)
			}
			return fmt.Errorf("%w: segment %s: %v", ErrHLSIsFMP4, redactURL(u), merr)
		}
		_, err = p.pw.Write(muxed)
		return err
	}
	if p.mux != nil {
		// Packed audio: frame it as MPEG-TS rather than checking whether it
		// already is. A body that is not ADTS after all — an origin answering a
		// .aac URL with an error page — fails here, and is a format refusal only
		// if it happens before any segment has been muxed: once the stream is
		// running, one bad body is a failed segment like any other.
		muxed, merr := p.mux.Segment(p.muxBuf[:0], body)
		if merr != nil {
			if p.mux.Started() {
				return fmt.Errorf("segment %s: %w", redactURL(u), merr)
			}
			return fmt.Errorf("%w: segment %s: %v", ErrHLSNotTS, redactURL(u), merr)
		}
		p.muxBuf = muxed
		_, err = p.pw.Write(muxed)
		return err
	}
	if err := checkSegmentIsTS(body, u); err != nil {
		return err
	}
	_, err = p.pw.Write(body)
	return err
}

// remuxFragment turns one fMP4 media segment into MPEG-TS.
//
// Every segment opens with the tables, so a viewer joining mid-stream has the
// programme within one segment. The samples go out in the order the fragment
// lists them, video before audio, which is the order they were written in.
func (p *hlsPuller) remuxFragment(body []byte) ([]byte, error) {
	frag, err := fmp4.ParseFragment(body, p.init)
	if err != nil {
		return nil, err
	}
	out := p.mux.Tables(p.muxBuf[:0])
	if v := p.init.Video(); v != nil {
		for _, s := range frag.Video {
			annexB, aerr := fmp4.AnnexB(p.nalBuf[:0], s, v)
			if aerr != nil {
				return nil, aerr
			}
			p.nalBuf = annexB
			out = p.mux.WriteVideo(out, annexB,
				fmp4.Scale(s.PTS, v.TimeScale), fmp4.Scale(s.DTS, v.TimeScale), s.Sync)
		}
	}
	if a := p.init.Audio(); a != nil {
		for _, s := range frag.Audio {
			adts, aerr := fmp4.ADTS(p.adtsBuf[:0], s.Data, a)
			if aerr != nil {
				return nil, aerr
			}
			p.adtsBuf = adts
			out = p.mux.WriteAudio(out, adts, fmp4.Scale(s.PTS, a.TimeScale))
		}
	}
	p.muxBuf = out
	return out, nil
}

// keyFor returns the AES-128 key bytes for k, fetching them once per URI.
//
// The key is fetched with the source's own client, headers and proxy: an
// upstream that gates its segments on a Referer or a token gates the key the
// same way, and a key fetched from the node's own IP when the segments go
// through a proxy would be the one request that gives the node away.
func (p *hlsPuller) keyFor(k *hlsKey) ([]byte, error) {
	id := k.URI.String()
	p.keysMu.Lock()
	cached, ok := p.keys[id]
	p.keysMu.Unlock()
	if ok {
		return cached, nil
	}
	body, _, err := hlsFetch(p.ctx, k.URI, p.opt, p.client)
	if err != nil {
		return nil, fmt.Errorf("key %s: %w", redactURL(k.URI), err)
	}
	if len(body) != aes.BlockSize {
		// An origin that answers a key URI with an error page is the common way
		// to get here, so say what arrived rather than just "bad key".
		return nil, fmt.Errorf("key %s: %d bytes, want %d", redactURL(k.URI), len(body), aes.BlockSize)
	}
	p.keysMu.Lock()
	p.keys[id] = body
	p.keysMu.Unlock()
	return body, nil
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
	Segments     []hlsSegment
	Variants     []hlsVariant
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

// parseByteRange reads "<n>[@<o>]". A missing or unreadable offset comes back
// as -1, which the parse loop resolves against the previous slice of the same
// URI; a length that does not read leaves the range nil, which servable then
// refuses rather than guessing.
func parseByteRange(v string) *hlsRange {
	v = strings.TrimSpace(v)
	lenPart, offPart, hasOff := strings.Cut(v, "@")
	n, err := strconv.ParseInt(strings.TrimSpace(lenPart), 10, 64)
	if err != nil || n <= 0 || n > maxHLSSegmentBytes {
		return nil
	}
	r := &hlsRange{Length: n, Offset: -1}
	if hasOff {
		o, oerr := strconv.ParseInt(strings.TrimSpace(offPart), 10, 64)
		if oerr != nil || o < 0 {
			return nil
		}
		r.Offset = o
	}
	return r
}

// everyRangeIsUsable reports whether every segment of a byte-range playlist
// resolved to a fetchable slice. A tag this package could not read leaves the
// segment without a range while the playlist is marked as using them, which
// would fetch the whole resource in place of one slice of it.
func (pl *hlsPlaylist) everyRangeIsUsable() bool {
	for _, seg := range pl.Segments {
		if seg.Range == nil || seg.Range.Offset < 0 || seg.Range.Length <= 0 {
			return false
		}
	}
	return len(pl.Segments) > 0
}

// everyKeyIsAES128 reports whether every segment this playlist lists is either
// in the clear or AES-128 encrypted with a key we know where to fetch. One
// segment we cannot read is a hole in the channel, so the whole playlist is
// refused and ffmpeg takes it.
func (pl *hlsPlaylist) everyKeyIsAES128() bool {
	for _, seg := range pl.Segments {
		if seg.Key != nil && !seg.Key.aes128() {
			return false
		}
	}
	return true
}

type hlsSegment struct {
	URI      *url.URL
	Duration float64
	// Range is the #EXT-X-BYTERANGE slice of URI this segment is, or nil when
	// the segment is the whole resource. Several segments of a byte-range
	// playlist share one URI and differ only here, which is why every identity
	// this puller keeps — de-dup, stall tracking — has to include it.
	Range *hlsRange
	// Key is the #EXT-X-KEY in force for this segment, or nil when it is in the
	// clear. An upstream may re-key mid-window, and METHOD=NONE switches back
	// off, so the key belongs to the segment rather than to the playlist.
	Key *hlsKey
}

// hlsKey is one #EXT-X-KEY line: how the segments that follow it are encrypted,
// where to fetch the key, and the IV to use if the line names one.
//
// AES-128 is the whole of what this package decrypts — the panel's own scheme,
// and what an ordinary IPTV upstream serves. SAMPLE-AES encrypts inside the
// elementary streams rather than the segment, so it needs a demuxer this
// package does not have, and it stays a refusal that routes to ffmpeg.
type hlsKey struct {
	Method string   // as written, upper-cased: AES-128, SAMPLE-AES, …
	URI    *url.URL // resolved against the playlist
	IV     []byte   // 16 bytes from IV=0x…, or nil to derive it from the sequence
}

// aes128 reports whether this key is the one flavour we can read.
func (k *hlsKey) aes128() bool { return k != nil && k.Method == "AES-128" }

// iv returns the initialisation vector for the segment at media sequence seq.
// An explicit IV= on the key wins; otherwise RFC 8216 §5.2 says the sequence
// number, big-endian, in the low 8 bytes of the block — which is what every
// encoder that omits IV expects, and getting it wrong yields plausible-looking
// garbage rather than an error.
func (k *hlsKey) iv(seq int64) []byte {
	if len(k.IV) == aes.BlockSize {
		return k.IV
	}
	var iv [aes.BlockSize]byte
	binary.BigEndian.PutUint64(iv[8:], uint64(seq))
	return iv[:]
}

// hlsRange is one #EXT-X-BYTERANGE: n bytes at offset o. A tag written without
// an offset (#EXT-X-BYTERANGE:<n>) means "the byte after the previous segment of
// the same URI", which is how a packager writes a series of slices of one file.
type hlsRange struct {
	Offset int64
	Length int64
}

// header renders the range as an HTTP Range request value.
func (r *hlsRange) header() string {
	return fmt.Sprintf("bytes=%d-%d", r.Offset, r.Offset+r.Length-1)
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
	var curKey *hlsKey               // the #EXT-X-KEY in force, carried down the segment list
	var pendingRange *hlsRange       // the #EXT-X-BYTERANGE waiting for its URI line
	nextOffset := map[string]int64{} // per URI: where an offset-less range continues
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
			// it: <n>[@<o>], and a missing offset continues from the previous
			// slice of the same URI.
			pl.HasByteRange = true
			pendingRange = parseByteRange(strings.TrimPrefix(line, "#EXT-X-BYTERANGE:"))
		case strings.HasPrefix(line, "#EXT-X-ENDLIST"):
			pl.Endlist = true
		case strings.HasPrefix(line, "#EXT-X-KEY:"):
			// METHOD=NONE switches encryption back off for what follows; any
			// other method (AES-128, SAMPLE-AES…) means ciphertext segments.
			// The key applies to every segment until the next #EXT-X-KEY, so it
			// is carried forward rather than recorded once for the playlist.
			key := &hlsKey{}
			for _, kv := range splitAttrs(strings.TrimPrefix(line, "#EXT-X-KEY:")) {
				switch {
				case strings.HasPrefix(kv, "METHOD="):
					key.Method = strings.ToUpper(strings.Trim(strings.TrimPrefix(kv, "METHOD="), `"`))
				case strings.HasPrefix(kv, "URI="):
					if u, err := resolveURI(base, strings.Trim(strings.TrimPrefix(kv, "URI="), `"`)); err == nil {
						key.URI = u
					}
				case strings.HasPrefix(kv, "IV="):
					raw := strings.Trim(strings.TrimPrefix(kv, "IV="), `"`)
					raw = strings.TrimPrefix(strings.TrimPrefix(raw, "0x"), "0X")
					if b, err := hex.DecodeString(raw); err == nil && len(b) == aes.BlockSize {
						key.IV = b
					}
				}
			}
			if key.Method == "" || key.Method == "NONE" {
				curKey = nil
				break
			}
			pl.Encrypted = true
			// A key we cannot fetch is a key we cannot use: servable() refuses
			// the playlist rather than letting the pull discover it per segment.
			if key.URI == nil {
				key.Method = "" // unusable; servable reports it as unreadable
			}
			curKey = key
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
			if pendingRange != nil && pendingRange.Offset < 0 {
				// An offset-less range continues the previous slice of the SAME
				// URI. With no previous slice there is nothing to continue from —
				// RFC 8216 leaves it undefined — and assuming 0 would quietly
				// fetch the start of the file in place of a live slice, which is
				// old content on the wire rather than an error. Leave it
				// unresolved; servable refuses the playlist.
				if prev, seen := nextOffset[u.String()]; seen {
					pendingRange.Offset = prev
				}
			}
			if pendingRange != nil {
				nextOffset[u.String()] = pendingRange.Offset + pendingRange.Length
			}
			pl.Segments = append(pl.Segments, hlsSegment{URI: u, Duration: pendingDur, Key: curKey, Range: pendingRange})
			pendingRange = nil
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
