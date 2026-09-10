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

// HLSSegCacheEntries is how many recently-served HLS segments a stream keeps
// ready-to-send (assembled from the ring and, when the stream has a key, already
// AES-encrypted). Every viewer of a channel fetches the SAME segment bytes, so
// without this each one paid a full re-assembly plus a full AES pass over
// identical data — the dominant CPU cost of an HLS audience, growing linearly
// with viewer count. A handful of entries covers the segments a playlist window
// actually hands out; the cache is dropped when the stream idles or its key
// changes, so it costs nothing on a channel nobody is watching.
const HLSSegCacheEntries = 3

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

// Bounds on the HTTP transport a puller uses to reach a source. The live body is
// deliberately unbounded — it is a stream that should never end — so these bound
// only getting to the point where it starts flowing, plus how long an unused
// pooled connection may sit around.
const (
	// PullDialTimeout bounds the TCP connect to a source, PullTLSTimeout its TLS
	// handshake, and PullHeaderTimeout the wait for response headers once the
	// request is sent. Without them a source that accepts a connection and then
	// says nothing pinned its puller until the stream was stopped — never
	// erroring, so never retrying another URL and never backing off, while the
	// panel saw a "running" stream that had no data.
	PullDialTimeout   = 10 * time.Second
	PullTLSTimeout    = 10 * time.Second
	PullHeaderTimeout = 15 * time.Second

	// PullIdleConnTimeout expires a pooled keep-alive connection. A zero-value
	// http.Transport never expires one, so a connection returned to the pool when
	// a source ended cleanly stayed open — with its reader goroutine — for the
	// life of the process.
	PullIdleConnTimeout = 90 * time.Second

	// PullKeepAlive is the TCP keep-alive probe interval on a source connection.
	PullKeepAlive = 30 * time.Second
)

// ── TS join / prebuffer ring ───────────────────────────────────────────────

// JoinRingBytesPerMS backstops the prebuffer ring at a generous high-bitrate
// estimate (~24 Mbit/s ≈ 3000 bytes/ms) so a stream whose PCR cannot be parsed
// can never grow the ring without bound. Duration-based pruning does the real
// work when PCR is present. Ring capacity = prebuffer-ms × this.
const JoinRingBytesPerMS = 3000

// JoinFreeGOPBufs caps the per-stream free list of recycled GOP data buffers. Each
// keyframe-aligned GOP is stored in its own byte array; without recycling, every
// GOP that ages out of the ring is freshly allocated on open and thrown to GC on
// prune — an allocate-and-discard rate ≈ the stream bitrate, which is the GC
// sawtooth. Recycling the dropped arrays through this list makes steady-state GOP
// allocation ~0. Open/prune alternate, so occupancy is ~1-2; a gating ring-shrink
// frees many at once and the excess past this cap is dropped to GC (the point
// being to release memory then). Kept small so retention is ~a few GOPs per stream.
const JoinFreeGOPBufs = 4

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
	CfgPrebufferMaxSec = 40 // the buffer/ring size per stream (seconds); covers TS prebuffer AND the HLS window
	// CfgDefaultPrebufferSec: fallback per-viewer join burst ONLY when a request
	// carries no ?prebuffer=. The panel is authoritative (client vs restreamer) and
	// passes the value per-request; 0 = the daemon imposes nothing.
	CfgDefaultPrebufferSec = 0
	CfgHLSTargetSec        = 6.0      // HLS target segment duration (seconds)
	CfgHLSWindow           = 6        // HLS segments listed in the playlist (display cap)
	CfgGraceSec            = 10       // idle-stop grace for control-managed streams (seconds)
	CfgWriteTimeoutSec     = 15       // per-write deadline for a live-TS viewer (seconds)
	CfgChunkBytes          = 12032    // source read size for daemon-pulled streams (aligned down to 188)
	CfgMaxGOPBytes         = 10528000 // cap on a single join-snapshot GOP (bytes)
	CfgSourceInsecure      = true     // skip upstream TLS verification when pulling HTTPS sources
	CfgIdleBufferGraceSec  = 30       // no-viewer window before the ring collapses (seconds); 0 = gate off
	CfgIdleBufferRatio     = 0.5      // fraction of the buffer kept while unwatched (HLS still cut from it)
	// CfgViewerIdleTimeoutSec bounds how long a live-TS viewer may sit receiving
	// NOTHING before it is dropped (0 = never). The per-write deadline only fires
	// while there are bytes to write, so a viewer whose stream went off-air AND
	// whose socket is half-open (a dropped mobile link that sent no FIN/RST) is
	// never written to, never times out, and lingers in /connections forever as a
	// ghost. Comfortably longer than a source blip (reconnect backoff tops out at
	// PullBackoffMax plus an ffmpeg cold start), so a recovering source does not
	// cost viewers their connection.
	CfgViewerIdleTimeoutSec = 30
	// CfgSourceBackend picks how a non-mp2t source is turned into MPEG-TS:
	//
	//	"auto"   — native where the source allows it, ffmpeg otherwise
	//	"ffmpeg" — always ffmpeg (the pre-0.12 behaviour, and the kill-switch)
	//	"native" — native only; a source the native reader will not take FAILS
	//	           rather than falling back. For testing what is actually
	//	           eligible on a given node — not for production.
	//
	// "auto" is the default because the fallback makes it strictly safer than
	// ffmpeg-always: anything the native reader declines runs exactly the
	// pipeline it ran before, while the common IPTV case (HLS with TS segments)
	// stops costing a child process per stream.
	CfgSourceBackend = "auto"
	// CfgMemLimitMB is an explicit ceiling (MiB) for the Go soft memory limit,
	// 0 = derive it from the cgroup/host budget (see MemLimitFraction). The daemon
	// usually shares a panel box with nginx, MySQL, PHP-FPM and the streams' own
	// ffmpeg processes, so an operator who knows the split should be able to say
	// what this process may use instead of it inferring a share of the machine.
	CfgMemLimitMB = 0
)

// ── Memory management (soft limit + idle-heap scavenge) ────────────────────
//
// The daemon holds only a bounded working set (per-stream prebuffer rings, sized
// by prebuffer_max_sec × bitrate), but the Go runtime keeps freed pages resident
// and returns them to the OS only lazily. These bound that: a soft heap limit
// caps runaway growth (so 500 streams degrade into harder GC, not unbounded RSS),
// and a periodic scavenge returns idle heap so RSS tracks the working set. Both
// are O(1) in stream count and need no per-stream tuning.
const (
	// MemLimitFraction is the fraction of a CGROUP memory budget used as the
	// runtime soft memory limit (debug.SetMemoryLimit / GOMEMLIMIT). A cgroup
	// limit is this process's own budget, so most of it is fairly claimed. A
	// ceiling, not a reservation: the GC only intensifies as usage nears it, so a
	// daemon whose working set stays well below never feels it. An explicit
	// GOMEMLIMIT in the environment, or mem_limit_mb in the config, overrides this.
	MemLimitFraction = 0.80

	// MemLimitHostFraction is the fraction used when there is no cgroup limit and
	// the budget falls back to the box's physical RAM. Deliberately lower than
	// MemLimitFraction: an XC_VM node runs nginx, MySQL, PHP-FPM and one ffmpeg
	// per stream alongside this daemon, so 80% of the machine is not ours to take
	// — claiming it lets the fan-out grow until it starves the very processes
	// feeding it. Operators who know their split set mem_limit_mb instead.
	MemLimitHostFraction = 0.50

	// GCPercent is the GC target growth (GOGC): the heap may grow this percent over
	// the live set before a collection. The default 100 lets the heap reach ~2×
	// live between GCs, which on a many-stream fan-out is a large RSS swing on top
	// of a working set that is itself inflated ~1.4× by GOP append-growth capacity.
	// 50 halves that headroom (heap ~1.5× live) for a much tighter RSS band; the
	// extra GC cost is negligible here (the daemon is I/O-bound, CPU near idle).
	// An explicit GOGC in the environment overrides this.
	GCPercent = 50

	// MemScavengeInterval is how often the idle-heap sweep runs; MemScavengeIdleMin
	// is how much freed-but-unreturned heap (heap free − heap released) must be
	// present before it forces a release, so a quiet daemon does not GC for
	// nothing. The check itself is cheap — runtime/metrics, which does NOT stop
	// the world (runtime.ReadMemStats does, and running it every sweep taxed every
	// stream to answer a question about none of them).
	MemScavengeInterval = 20 * time.Second
	MemScavengeIdleMin  = 64 << 20 // 64 MiB

	// MemScavengeMinGap is the floor between two FORCED releases. debug.FreeOSMemory
	// is a full stop-the-world GC plus a page-return sweep; on a busy daemon the
	// idle-heap threshold is met almost continuously, so without a floor the
	// process spends its life in back-to-back full collections and thrashes the
	// pages it just handed back. A collapse of a stream's ring (the reaper's idle
	// gate) is real, sizeable garbage and bypasses the floor — that is the event
	// worth releasing promptly for.
	MemScavengeMinGap = 2 * time.Minute
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
