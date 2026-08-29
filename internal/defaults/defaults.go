// Package defaults is the single home for the daemon's operational tuning
// constants — the values that used to live as inline literals or per-package
// consts scattered across the tree. Collecting them here means an operator can
// see, in one file, every knob the code bakes in, instead of grepping for a
// magic number.
//
// Scope: this holds *behavioural defaults* (timeouts, buffer sizes, retry
// bounds, encode fallbacks). It deliberately does NOT hold:
//   - CLI flag defaults — those are the operator-facing surface and live with
//     the flags in cmd/xc_fanout/main.go (the single source of truth for them);
//   - MPEG-TS protocol invariants (188-byte packets, the 90 kHz PTS/PCR clock,
//     the 33-bit PTS wrap) — those are format facts, not tunables, and stay
//     next to the code that parses them.
package defaults

import "time"

// ── HTTP servers ───────────────────────────────────────────────────────────

// HTTPReadHeaderTimeout bounds how long a client may take to send its request
// headers before the server drops it. It bounds only the header read, not the
// (long-lived) response body, so it is safe for the live-TS/HLS streaming
// handlers while still cutting off a slow-loris client that opens a connection
// and never finishes its request line.
const HTTPReadHeaderTimeout = 10 * time.Second

// ── Live-TS fan-out ────────────────────────────────────────────────────────

const (
	// IngestChunk is the fallback source read size (bytes) for a stream created
	// without an explicit chunk. Aligned down to a multiple of 188 by the reader.
	IngestChunk = 12032

	// WriteTimeout bounds a single write to a live-TS viewer. A viewer that
	// cannot accept the next chunk within this window (its OS socket buffer is
	// full because it stopped draining — a backgrounded/force-switched player, a
	// dropped mobile link) is dropped so it can never pin the serveLive goroutine
	// forever. A healthy real-time viewer produces at most ~1s of backlog per
	// second, so this only ever fires on a genuinely stalled connection.
	WriteTimeout = 15 * time.Second

	// SubscriberQueue bounds how many pending chunks a single hub subscriber may
	// fall behind before it is dropped rather than allowed to stall the producer.
	SubscriberQueue = 256
)

// ── Source puller (reconnect + ffmpeg cold start) ──────────────────────────

const (
	// PullBackoffInitial and PullBackoffMax bound the exponential reconnect
	// backoff when a source ends or errors: the wait starts at Initial and
	// doubles up to Max.
	PullBackoffInitial = 1 * time.Second
	PullBackoffMax     = 8 * time.Second

	// PullFfmpegProbeSize and PullFfmpegAnalyzeDuration cap ffmpeg's input
	// analysis on a cold on-demand join (bytes / microseconds), so the first
	// mpegts bytes appear quickly instead of ffmpeg spending its default 5s/5MB
	// probing. 1s/1MB still identifies the PAT/PMT + codecs a live TS/HLS source
	// presents. Passed to ffmpeg as -probesize / -analyzeduration.
	PullFfmpegProbeSize       = "1000000"
	PullFfmpegAnalyzeDuration = "1000000"
)

// ── TS join / prebuffer ring ───────────────────────────────────────────────

// JoinRingBytesPerMS backstops the prebuffer ring at a generous high-bitrate
// estimate (~24 Mbit/s ≈ 3000 bytes/ms) so a stream whose PCR cannot be parsed
// can never grow the ring without bound. Duration-based pruning does the real
// work when PCR is present. Ring capacity = prebuffer-ms × this.
const JoinRingBytesPerMS = 3000

// ── Operator tuning: config-file seeds ─────────────────────────────────────
//
// These seed the panel-editable JSON config (internal/config). They are what
// the daemon writes when the config file is absent, and what it backfills for
// any key an (older) panel omits — so the on-disk file always carries the full
// current schema and a version skew between panel and daemon never throws. An
// admin overrides these from the panel; here they are only the fallback.
//
// Unlike the invariants above, these ARE the operator-facing defaults. They used
// to be CLI-flag literals in cmd/xc_fanout/main.go; that surface is retired (the
// panel never set the flags) and the values live here now.
const (
	CfgPrebufferMaxSec = 20       // live-TS history kept per stream for client_prebuffer (seconds)
	CfgHLSTargetSec    = 6.0      // HLS target segment duration (seconds)
	CfgHLSWindow       = 6        // HLS segments kept in the sliding window (in RAM)
	CfgGraceSec        = 10       // idle-stop grace for control-managed streams (seconds)
	CfgWriteTimeoutSec = 15       // per-write deadline for a live-TS viewer (seconds)
	CfgChunkBytes      = 12032    // source read size for daemon-pulled streams (aligned down to 188)
	CfgMaxGOPBytes     = 10528000 // cap on a single join-snapshot GOP (bytes)
	CfgSourceInsecure  = true     // skip upstream TLS verification when pulling HTTPS sources
)

// ── /probe off-air prewarm ─────────────────────────────────────────────────

const (
	// ProbeDefaultWaitMS is how long /probe waits for the source to produce data
	// when the caller gives no ?wait=. ProbeMaxWaitMS caps any caller-supplied
	// value. ProbePollInterval is how often the wait loop checks for first data.
	ProbeDefaultWaitMS = 5000
	ProbeMaxWaitMS     = 30000
	ProbePollInterval  = 50 * time.Millisecond
)

// ── Admin "send message" drawtext overlay ──────────────────────────────────

const (
	// OverlayTSDuration is how long an admin "send message" banner stays burned
	// onto a live-TS viewer's stream before it rejoins the raw fan-out. Kept short
	// since it costs a transient per-viewer re-encode.
	OverlayTSDuration = 5 * time.Second

	// OverlayTSWindowGrace is extra headroom on the live-TS overlay ffmpeg's hard
	// kill timeout, on top of OverlayTSDuration, so a slow encode can finish the
	// window before the context deadline fires.
	OverlayTSWindowGrace = 10 * time.Second

	// OverlaySegmentTimeout hard-caps the per-segment overlay re-encode; on
	// timeout the plain segment is served so a signal never breaks playback.
	OverlaySegmentTimeout = 15 * time.Second

	// OverlayDefaultCodec is the video codec assumed for the drawtext re-encode
	// when the viewer's token carries no ?vc=.
	OverlayDefaultCodec = "h264"
)
