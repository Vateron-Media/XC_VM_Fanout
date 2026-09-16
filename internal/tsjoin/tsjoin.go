// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package tsjoin tracks the MPEG-TS state a new subscriber needs to start
// decoding cleanly mid-stream: the latest PAT + PMT, plus a keyframe-aligned
// ring of recent packets.
//
// A subscriber joining with no prebuffer gets the current GOP (the most recent
// random-access point up to now) — the minimal clean join. One that asks for N
// seconds of prebuffer gets the ring rewound to a keyframe ~N seconds back, so
// its player starts with a filled cache. This mirrors client_prebuffer in the
// legacy live.php byte path, which the daemon X-Accel hand-off otherwise
// bypasses.
//
// The original clean-join heuristic (PAT/PMT + first keyframe) came from
// ProxyCommand.php; here the TS structure is parsed properly instead of matched
// at fixed byte offsets.
package tsjoin

import (
	"fmt"
	"math"
	"strings"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// PacketSize is the fixed MPEG-TS packet length.
const PacketSize = 188

// pcrHz is the MPEG-TS system clock: PCR base is measured in 90 kHz ticks, so
// one millisecond is 90 ticks.
const pcrHz = 90

// gop is one keyframe-aligned block: packets from a random-access point up to
// (but not including) the next one, tagged with the stream clock (PCR base, in
// 90 kHz ticks) seen as it opened. pcr is -1 until a PCR has been observed. id is
// monotonic and assigned at creation, so the HLS segment view can reference GOPs
// stably across ring prunes (slice positions shift; ids do not).
type gop struct {
	id   int64
	data []byte
	// t is the ring clock at open (90 kHz), or -1 before the stream has shown a
	// PCR — see ringClock. It drives retention and the prebuffer walk-back, and it
	// only ever moves forward, which the raw PCR does not.
	t   int64
	pts int64 // HLS clock at open: the keyframe's PES PTS, or the PCR fallback, or -1
	// video reports that this block opens on a VIDEO random-access point, i.e. a
	// decoder handed these bytes starts producing pictures. A block opened by the
	// maxGOP cut (a source with no detectable keyframes) or by the pre-roll before
	// the first one does not, and a join must not begin there — see snapshotStart.
	video bool
}

// ptsWrap is the 33-bit PTS/PCR modulus; a duration delta that comes out negative
// straddled it.
const ptsWrap = int64(1) << 33

// hlsSeg is an HLS segment as a VIEW over the ring: a run of consecutive GOPs
// [startID..endID] plus its duration. It holds NO bytes — Segment assembles them
// from the ring on request. seq is committed when the segment closes and never
// recomputed, so the playlist stays stable as the ring slides.
type hlsSeg struct {
	seq     int
	startID int64
	endID   int64
	durMS   int64
	// disc: the stream's timeline jumped just before this segment (a producer
	// restart, a failover, a splice) — the playlist marks it
	// #EXT-X-DISCONTINUITY so a player resets its clock instead of stalling.
	disc bool
}

// State accumulates join information from a packet-aligned byte stream.
// It is not safe for concurrent use; the caller (Hub) serialises access.
type State struct {
	lastPAT  []byte // one 188-byte PAT packet, or nil
	lastPMT  []byte // one 188-byte PMT packet, or nil
	pmtPID   int    // PID carrying the PMT, or -1 if unknown
	videoPID int    // PID carrying the video ES (for HLS segment PTS), or -1
	audioPID int    // PID carrying the audio ES, or -1 when the source has none
	// videoType is the PMT stream_type of videoPID. It decides whether a PES
	// start on that PID can be recognised as a keyframe from its own bytes when
	// the source sets no random_access_indicator (see tspes.StartsKeyframe).
	videoType byte

	// audioPkts and videoFrames feed the supervisor's health checks: "has audio
	// stopped while video keeps flowing" and "has the frame rate collapsed". Both
	// are counters the caller samples and differences, rather than state kept
	// here, so this package stays a parser and the policy lives with the watchdog.
	audioPkts   int64
	videoFrames int64
	gops        []gop // oldest→newest; the last element is the open (current) GOP
	lastPCR     int64 // most recent PCR base seen (90 kHz), or -1
	// pcrPID is the PMT's PCR_PID, or -1. Once a PCR has been seen on it, only
	// that PID drives the clock: a source carrying a second, unrelated PCR (another
	// programme, a passthrough of a muxer that stamps several PIDs) would otherwise
	// interleave two clocks, and the ring's duration would be noise.
	pcrPID   int
	pcrOnPID bool
	// The ring clock (ringClock): the PCR it last advanced from, the monotonic
	// time it has reached, and the last plausible step, reused across a
	// discontinuity.
	clockPCR  int64
	clockT    int64
	clockStep int64
	maxGOP    int   // cap on a single GOP's length (bytes) — memory guard
	ring90    int64 // history retained, in 90 kHz ticks (0 = current GOP only)
	maxRing   int   // absolute byte ceiling for the whole ring — memory backstop

	// pinned counts, per block id, the readers holding slices into that block
	// while they copy or write with the lock released (a Pin). prune must not
	// RECYCLE a pinned block's array — handing it to a new GOP would rewrite bytes
	// under the reader — so a pinned block goes to GC instead. Counting per block
	// matters since every live viewer pins as it writes (ADR 0004): one global
	// count let a single viewer stuck in a write suspend recycling for the whole
	// stream, and every GOP opened meanwhile grew a fresh array from nothing —
	// over five bytes allocated per byte published.
	pinned map[int64]int32

	// freeBufs recycles the byte arrays of GOPs dropped by prune so opening the
	// next GOP reuses one instead of allocating a fresh array every keyframe. This
	// turns the steady-state GOP allocate-and-discard (≈ bitrate, the GC sawtooth)
	// into ~zero. Capped at defaults.JoinFreeGOPBufs; buffers hold len 0, cap kept.
	freeBufs [][]byte

	// prunedID / prunedLen are the id and final length of the newest block to
	// leave the ring (prunedID -1 = none yet). A block is final once a newer one
	// opens, so a reader whose cursor sits at prunedLen in prunedID took every byte
	// of it and continues at the next block — see ReadFrom.
	prunedID  int64
	prunedLen int

	// HLS segment view over the ring (hlsTargetMS == 0 disables it). The ring is
	// the single cache; HLS segments are cut from these GOPs on demand rather than
	// buffered a second time.
	prebufMS    int64    // TS client-prebuffer depth (ms) = the buffer / ring size
	hlsTargetMS int64    // HLS target segment duration (ms); 0 = HLS view off
	hlsWindow   int      // HLS segments listed in the playlist window (display cap)
	nextGOPID   int64    // next GOP id to assign
	segs        []hlsSeg // closed segments, oldest→newest
	nextSeq     int      // next HLS media sequence number
	segStartID  int64    // open segment's first GOP id, or -1 = none open
	segLastPTS  int64    // HLS clock of the newest keyframe in the open segment, or -1
	segSpan     int64    // media the open segment holds so far (90 kHz), gaps excluded
	segDisc     bool     // the open segment starts after a timeline jump
	discDropped int      // discontinuities in segments pruned out of s.segs
	// hlsStep is the last keyframe-to-keyframe step a cadence explains (90 kHz):
	// this source's GOP length as observed. It is what the next step is judged
	// against, and what the final GOP of a segment cut short by a jump is timed
	// at. 0 until the stream has shown two keyframes.
	hlsStep int64
	// hlsMaxDur is the largest #EXT-X-TARGETDURATION this configuration has
	// published (seconds). RFC 8216 requires the tag not to change for the life
	// of the playlist, and rendering it from the window alone moved it every time
	// a longer-than-usual segment slid out — the same client saw it change
	// between two reloads of the same URL. Cleared when the target changes
	// (Configure) or the stream is torn down (Reset).
	hlsMaxDur int

	// plCache is the last rendered playlist, valid until the segment list changes.
	// Every HLS viewer polls index.m3u8 on its own schedule, so an audience of a few
	// hundred re-rendered an identical string hundreds of times a second, each time
	// holding the hub lock against the producer. The list only changes when a
	// segment closes or ages out, which is once per hls_target_sec.
	plCache string
	plValid bool

	// noKeyframeCuts counts blocks closed because the source produced no
	// random-access point within maxGOP bytes — see Update. A non-zero count is
	// the signal that a source carries no random_access_indicator at all, which is
	// also why it yields no HLS segments.
	noKeyframeCuts int64

	// Viewer gate: when gated (no audience), the ring collapses to prebufMS ×
	// idleRatio to free memory, HLS keeps cutting from the smaller ring so the
	// playlist stays openable, and it grows back when SetGated(false) restores it.
	gated     bool
	idleRatio float64 // fraction of the buffer kept while gated (0 < r ≤ 1)
}

// New returns a State. maxGOP caps a single GOP (bytes, guarding streams with no
// keyframes); maxPrebufMS is how many milliseconds of keyframe-aligned history
// to retain for client prebuffer (0 = keep only the current GOP, the original
// behaviour). The HLS view starts disabled; call Configure to enable it.
func New(maxGOP int, maxPrebufMS int64) *State {
	s := &State{pmtPID: -1, videoPID: -1, audioPID: -1, lastPCR: -1, pcrPID: -1, clockPCR: -1, maxGOP: maxGOP, prebufMS: maxPrebufMS, segStartID: -1, segLastPTS: -1, prunedID: -1, idleRatio: 0.5}
	s.recompute()
	return s
}

// recompute sizes the ring to the buffer (prebufMS) and its byte backstop
// (~24 Mbit/s ≈ 3000 bytes/ms, so a stream whose PCR cannot be parsed can never
// grow the ring without bound). The HLS window is a DISPLAY cap, not a ring
// driver: HLS is cut from whatever the ring holds. When gated the ring collapses
// to prebufMS × idleRatio; a small floor keeps ~2 segments so the HLS playlist
// stays openable. Prunes immediately when it shrinks.
func (s *State) recompute() {
	need := s.prebufMS
	if s.gated {
		need = int64(float64(need) * s.idleRatio)
	}
	// HLS must retain ≥ ~2 segments or its playlist goes empty and the stream
	// won't open; floor the ring there whenever the HLS view is on.
	if s.hlsTargetMS > 0 && need < 2*s.hlsTargetMS {
		need = 2 * s.hlsTargetMS
	}
	if need < 0 {
		need = 0
	}
	s.ring90 = need * pcrHz
	s.maxRing = int(need) * defaults.JoinRingBytesPerMS
	if len(s.gops) > 0 {
		s.prune()
	}
}

// SetGated collapses the ring to the idle fraction (true) or restores it to the
// full buffer (false). HLS keeps cutting either way. Caller (Hub) serialises.
func (s *State) SetGated(gated bool) {
	if s.gated == gated {
		return
	}
	s.gated = gated
	s.recompute()
}

// SetIdleRatio sets the fraction of the buffer retained while gated (clamped to
// (0, 1]). Caller (Hub) serialises access.
func (s *State) SetIdleRatio(r float64) {
	if r <= 0 {
		r = 0.5
	} else if r > 1 {
		r = 1
	}
	if s.idleRatio == r {
		return
	}
	s.idleRatio = r
	s.recompute()
}

// Configure live-reconfigures the TS prebuffer depth AND the HLS segment view
// (target seconds→ms, window count). hlsTargetMS==0 disables the HLS view.
// Caller (Hub) serialises access.
func (s *State) Configure(prebufMS, hlsTargetMS int64, hlsWindow int) {
	if prebufMS < 0 {
		prebufMS = 0
	}
	if hlsTargetMS < 0 {
		hlsTargetMS = 0
	}
	if hlsWindow < 0 {
		hlsWindow = 0
	}
	if hlsTargetMS != s.hlsTargetMS {
		// A new target means a new playlist: the published target duration is
		// free to move to it, and must not stay pinned to the old segment length.
		s.hlsMaxDur = 0
	}
	s.prebufMS, s.hlsTargetMS, s.hlsWindow = prebufMS, hlsTargetMS, hlsWindow
	s.plValid = false // the window (and whether HLS runs at all) may have changed
	s.recompute()
}

// Update scans a packet-aligned chunk and folds it into the join state.
// Bytes that are not 188-aligned or lack the 0x47 sync byte are skipped.
func (s *State) Update(chunk []byte) {
	for off := 0; off+PacketSize <= len(chunk); off += PacketSize {
		pkt := chunk[off : off+PacketSize]
		if pkt[0] != 0x47 {
			continue
		}
		pid := (int(pkt[1]&0x1f) << 8) | int(pkt[2])
		pusi := pkt[1]&0x40 != 0

		// Health counters. A payload-unit-start on the video PID begins a new
		// access unit, which is close enough to "a frame" for a rate that is only
		// ever compared against this same stream's own baseline.
		if pusi {
			switch pid {
			case s.audioPID:
				if s.audioPID >= 0 {
					s.audioPkts++
				}
			case s.videoPID:
				if s.videoPID >= 0 {
					s.videoFrames++
				}
			}
		}
		afc := (pkt[3] >> 4) & 0x3
		hasAdaptation := afc == 2 || afc == 3

		// A random-access point opens a new block. It must be a VIDEO one when the
		// stream has video: random_access_indicator is set on the AUDIO PID too (by
		// ffmpeg's own muxer among others, since every audio frame is a random
		// access point), and a block opened there starts with audio packets and a
		// video slice mid-GOP. A viewer joining on it gets pictures referencing an
		// SPS/PPS it never received — "non-existing PPS 0 referenced", a black
		// picture until the next real keyframe — and with no prebuffer configured
		// (only the open block is kept) that was EVERY join. A source with no video
		// at all (radio) keeps the plain random-access rule.
		rap := false
		if hasAdaptation && pkt[4] > 0 {
			flags := pkt[5]
			// PCR_flag: refresh the stream clock — from the programme's own PCR
			// PID once that has shown one, from any PID until then. The field
			// must be long enough to HOLD the PCR (1 flags byte + 6): a corrupt
			// or truncated adaptation field otherwise had its payload read as a
			// clock, and one such packet moves the whole ring's timeline.
			if flags&0x10 != 0 && pkt[4] >= 7 && (pid == s.pcrPID || !s.pcrOnPID) {
				if pid == s.pcrPID {
					s.pcrOnPID = true
				}
				s.lastPCR = readPCR(pkt)
			}
			if flags&0x40 != 0 { // random_access_indicator
				rap = true
			}
		}
		onVideo := s.videoPID >= 0 && pid == s.videoPID
		// No random_access_indicator: look at the video PES itself. This is the
		// case the native remuxer brings in — ffmpeg's muxer always flagged its
		// keyframes, but a source passed through byte-for-byte carries only what
		// its own encoder set, and some set nothing. Without this such a stream
		// cut no HLS segments at all.
		if !rap && pusi && onVideo && tspes.StartsKeyframe(pkt, s.videoType) {
			rap = true
		}
		keyframe := rap && (onVideo || s.videoPID < 0)

		switch {
		case pid == 0 && pusi:
			s.lastPAT = cloneInto(s.lastPAT, pkt)
			if p := parsePMTPID(pkt); p >= 0 {
				s.pmtPID = p
			}
		// A section is read only from the packet that STARTS it. A PMT too long
		// for one packet continues in the next, which carries no PUSI; parsing
		// those continuation bytes as a section of their own read them as
		// pointer_field and ES entries, which could move videoPID or audioPID to
		// whatever they happened to spell — and left lastPMT holding a packet that
		// is not a table, which is then the packet every join burst and every HLS
		// segment starts with. Multi-packet PMTs are ordinary on DVB passthrough.
		case s.pmtPID >= 0 && pid == s.pmtPID && pusi:
			s.lastPMT = cloneInto(s.lastPMT, pkt)
			if v, t := parseVideoPID(pkt); v >= 0 {
				s.videoPID, s.videoType = v, t
			}
			if m, ok := tspes.ParsePMT(pkt); ok && int(m.PCRPID) != s.pcrPID {
				s.pcrPID, s.pcrOnPID = int(m.PCRPID), false
			}
			if a := parseAudioPID(pkt); a >= 0 {
				s.audioPID = a
			}
		}

		switch {
		case keyframe:
			// Open a fresh GOP at this random-access point. Bookkeep the HLS
			// segment boundary BEFORE appending, so the closing segment ends at
			// the previous last GOP and the new keyframe opens the next segment.
			//
			// Only a VIDEO-PID keyframe carrying a PES PTS cuts an HLS segment, and
			// its PTS is the segment clock. RAI is set on other PIDs too, and mixing
			// their PCR fallback with the video PTS (two different 90 kHz offsets)
			// made segment durations never accumulate — segments never closed. A
			// non-video RAI still opens a ring GOP (needed for the live clean-join);
			// it just does not drive HLS. gop.pts is the video PTS, else -1.
			id := s.nextGOPID
			s.nextGOPID++
			t := s.ringClock()
			pts := int64(-1)
			if s.videoPID >= 0 && pid == s.videoPID {
				if p, ok := parsePTS(pkt); ok {
					pts = p
					s.hlsOnKeyframe(id, pts)
				}
			}
			s.gops = append(s.gops, gop{id: id, data: append(s.getBuf(), pkt...), t: t, pts: pts, video: onVideo})
			s.prune()
		case len(s.gops) > 0 && len(s.gops[len(s.gops)-1].data)+PacketSize <= s.maxGOP:
			g := &s.gops[len(s.gops)-1]
			g.data = append(g.data, pkt...)
		default:
			// Two cases open a fresh block here, neither of them a keyframe (so
			// neither opens an HLS segment):
			//
			//  1. Pre-roll before the first keyframe — keep a provisional block so an
			//     early join still gets recent packets.
			//  2. The open block has reached maxGOP without a random-access point.
			//     Some sources never set random_access_indicator at all; those used
			//     to grow one block to the cap and then SILENTLY DISCARD every packet
			//     after it. The ring froze — one block, never pruned (pruning needs
			//     two), so the stream held maxGOP forever, and every joining viewer
			//     was served that same stale block from whenever the cap was hit,
			//     ahead of the live tail. Cutting a new block instead keeps the ring
			//     rolling: prune bounds it like any other stream and a joiner gets
			//     recent bytes. (HLS still yields nothing for such a source — a
			//     segment must start at a random-access point to decode — which is
			//     what noKeyframeCuts surfaces.)
			if len(s.gops) > 0 {
				s.noKeyframeCuts++
			}
			id := s.nextGOPID
			s.nextGOPID++
			s.gops = append(s.gops, gop{id: id, data: append(s.getBuf(), pkt...), t: s.ringClock(), pts: -1})
			s.prune()
		}
	}
}

// maxCadence bounds the step ringClock takes at face value, and so the one it
// remembers as the stream's cadence — what it advances by across a
// discontinuity. A GOP is seconds; a longer step is a timeline jump, not time
// passing, unless no cadence is known yet (a slow source with no detectable
// keyframes, whose maxGOP-cut blocks can each span minutes).
const maxCadence = 60 * 90000

// maxWrap is how far past the 33-bit rollover a backwards step may land and
// still be read as the wrap rather than a reset.
const maxWrap = 10 * 60 * 90000

// ringClock stamps a new block with the ring's own clock: the PCR, made monotonic.
//
// Retention used to subtract raw PCRs — newest minus oldest — and the PCR is not
// monotonic. It restarts near zero with every producer restart (a fresh ffmpeg
// starts its own clock), jumps with a source failover, and wraps at 2^33 every
// 26.5 hours. Across any of those the difference went NEGATIVE, which the prune
// read as "still inside the window" — so it stopped dropping anything, and the
// ring grew to its byte backstop (prebuffer × 24 Mbit/s: 120 MB at the default
// 40 s, whatever the stream's bitrate) and sat there until every pre-splice block
// had been pushed out. On a node restarting producers or carrying a source with
// two interleaved PCR clocks, that was most of the daemon's memory.
//
// A backwards step that is the 33-bit wrap is unwrapped. Any other step no
// cadence can explain — a restart, a failover, a splice, in EITHER direction —
// advances by the last plausible cadence instead, so the window stays a window:
// under ADR 0004 the ring is the live tail, and an hour's forward jump aged the
// whole of it out in one Update, dropping every follower and emptying the HLS
// playlist. (It used only to cost memory, hence the old "a forward splice can
// only make the ring prune sooner".) The HLS segment clock already reads a jump
// this way; this is the ring clock agreeing with it.
//
// Until a cadence is known (clockStep == 0) a forward step is taken in full
// whatever its size, which is what keeps keyframe-less radio moving: its
// maxGOP-cut blocks each span minutes, more than any cadence, and capping them
// froze the clock so nothing was ever pruned. -1 until the stream has shown a
// PCR; the blocks opened before that are stamped when it starts.
func (s *State) ringClock() int64 {
	if s.lastPCR < 0 {
		return -1
	}
	if s.clockPCR < 0 {
		s.clockPCR, s.clockT = s.lastPCR, 0
		// The clock starts HERE, so the blocks already in the ring — the pre-roll
		// opened before the stream showed a PCR, carrying t=-1 — happened at its
		// origin, not before the beginning of time. Left at -1 they read as older
		// than any window, and prune dropped them in the very Update that had just
		// appended to them: on a cold channel (the puller starts on the first
		// viewer's attach) that is the block the first viewer is reading, and it
		// was dropped as behind at the stream's first keyframe, 0 bytes sent.
		// lastPCR is never unset, so this backfill runs at most once per stream.
		for i := range s.gops {
			if s.gops[i].t < 0 {
				s.gops[i].t = 0
			}
		}
		return 0
	}
	d := s.lastPCR - s.clockPCR
	if d < 0 && d+ptsWrap <= maxWrap {
		d += ptsWrap // the 33-bit PCR wrap: a forward step after all
	}
	switch {
	case d < 0:
		d = s.clockStep // a discontinuity: carry on at the last known cadence
	case d <= maxCadence:
		if d > 0 {
			s.clockStep = d
		}
	case s.clockStep > 0:
		d = s.clockStep // a jump forward no cadence explains: another discontinuity
	}
	s.clockPCR = s.lastPCR
	s.clockT += d
	return s.clockT
}

// prune drops the oldest GOPs once the ring exceeds the configured duration
// (when PCR timing is available) or the absolute byte backstop. With no
// prebuffer configured it collapses to just the current GOP.
func (s *State) prune() {
	if s.ring90 <= 0 {
		if len(s.gops) > 1 {
			for i := 0; i < len(s.gops)-1; i++ {
				s.release(s.gops[i])
			}
			s.gops = append(s.gops[:0], s.gops[len(s.gops)-1])
		}
		return
	}
	// Duration-based prune, on the ring clock (needs one on both ends).
	if newest := s.gops[len(s.gops)-1].t; newest >= 0 {
		drop := 0
		for drop < len(s.gops)-1 {
			if p := s.gops[drop].t; p >= 0 && newest-p <= s.ring90 {
				break
			}
			drop++
		}
		if drop > 0 {
			for i := 0; i < drop; i++ {
				s.release(s.gops[i])
			}
			s.gops = append(s.gops[:0], s.gops[drop:]...)
		}
	}
	// Byte backstop (also covers streams with no parseable PCR).
	total := 0
	for i := range s.gops {
		total += len(s.gops[i].data)
	}
	for total > s.maxRing && len(s.gops) > 1 {
		total -= len(s.gops[0].data)
		s.release(s.gops[0])
		s.gops = append(s.gops[:0], s.gops[1:]...)
	}
	s.hlsPrune()
}

// hlsOnKeyframe advances the HLS segment view when a new keyframe (a new GOP,
// id=newID, carrying PES PTS newPTS) arrives. If the currently-open segment now
// holds at least hlsTargetMS of media, it is closed — ending at the previous last
// GOP — and this keyframe opens the next one. Called before the new GOP is
// appended, so s.gops[last] is the closing segment's final GOP. No-op unless the
// HLS view is enabled and a PES clock is available.
//
// A segment is timed KEYFRAME BY KEYFRAME, not by the span from its first
// keyframe to the one that closes it. The two agree while the stream is
// continuous, and differ by exactly the gap when it is not: a source reconnect,
// an upstream HLS source skipping segments, an ffmpeg -copyts restart all step
// the PTS forward with no media in between. Measured across the whole span, the
// segment straddling such a gap was listed with #EXTINF (and so
// #EXT-X-TARGETDURATION) of media PLUS gap — 16 s of #EXTINF for 6 s of video
// after a 10 s gap — and with no #EXT-X-DISCONTINUITY, so a player buffered
// against a duration the segment could not fill and nothing told it the timeline
// had moved. Per step, the gap is visible as one step far outside the cadence:
// the segment closes on the media it holds and the next one is a discontinuity.
func (s *State) hlsOnKeyframe(newID, newPTS int64) {
	if s.hlsTargetMS <= 0 {
		return
	}
	if newPTS < 0 {
		return // cannot time a segment without a clock on this end
	}
	if s.segStartID < 0 || s.segLastPTS < 0 {
		s.segStartID, s.segLastPTS, s.segSpan = newID, newPTS, 0
		return
	}
	step := newPTS - s.segLastPTS
	if step < 0 && step+ptsWrap <= maxWrap {
		step += ptsWrap // straddled the 33-bit wrap: a forward step after all
	}
	// A step no cadence explains is a jump, not time passing: a producer restart
	// starts its PTS over, a failover lands on another clock, a splice skips
	// ahead. (Adding 2^33 to every backwards step read a restart as the wrap and
	// closed a segment of ~26 hours — #EXTINF and #EXT-X-TARGETDURATION of
	// ~95000 s, and players broken until it left the window.) Until the stream
	// has shown a cadence any forward step is taken at face value, which is what
	// keeps a source whose keyframes are further apart than the target — one long
	// GOP per segment — reading as media rather than as a jump.
	jump := step < 0 || step > maxCadence ||
		(s.hlsStep > 0 && step > 2*s.hlsStep+s.hlsTargetMS*pcrHz)
	if jump {
		// The open segment ends here — a segment must not span two timelines —
		// timed on the media it actually holds: every step up to its last
		// keyframe, plus one cadence for that last GOP, whose own end is what the
		// jump swallowed.
		dur := (s.segSpan + s.hlsStep) / pcrHz
		if dur <= 0 {
			dur = s.hlsTargetMS
		}
		s.closeSegment(newID, dur)
		s.segDisc = true
		s.segStartID, s.segLastPTS, s.segSpan = newID, newPTS, 0
		return
	}
	if step > 0 {
		s.hlsStep = step
	}
	s.segLastPTS = newPTS
	s.segSpan += step
	if s.segSpan >= s.hlsTargetMS*pcrHz {
		s.closeSegment(newID, s.segSpan/pcrHz)
		s.segStartID, s.segLastPTS, s.segSpan = newID, newPTS, 0
	}
}

// closeSegment ends the open segment at the GOP before newID, lasting durMS.
func (s *State) closeSegment(newID, durMS int64) {
	endID := s.segStartID
	if n := len(s.gops); n > 0 {
		endID = s.gops[n-1].id
	}
	s.segs = append(s.segs, hlsSeg{seq: s.nextSeq, startID: s.segStartID, endID: endID, durMS: durMS, disc: s.segDisc})
	s.segDisc = false
	s.nextSeq++
	s.plValid = false
}

// hlsPrune drops segments the ring can no longer assemble, so the playlist only
// ever lists segments HLSSegmentPin will serve.
//
// The test is the same one HLSSegmentPin applies: the segment's FIRST GOP must
// still be in the ring, since a segment handed over without its own keyframe is
// a decode error rather than a skip. Dropping only when the LAST GOP had gone
// left every partially-pruned segment listed and 404ing for as long as it took
// the ring to pass its end — the steady state on a ring shorter than the HLS
// floor, which is what the byte backstop (defaults.JoinRingBytesPerMS, ≈24
// Mbit/s) makes of a higher-bitrate channel: four listed segments in five
// returned nothing. The one-segment case was not covered by HLSPlaylist's fetch
// margin either, which only holds a segment back when there is more than one.
func (s *State) hlsPrune() {
	if len(s.segs) == 0 || len(s.gops) == 0 {
		return
	}
	oldest := s.gops[0].id
	drop := 0
	for drop < len(s.segs) && s.segs[drop].startID < oldest {
		drop++
	}
	if drop > 0 {
		for _, sg := range s.segs[:drop] {
			if sg.disc {
				s.discDropped++
			}
		}
		s.segs = append(s.segs[:0], s.segs[drop:]...)
		s.plValid = false
	}
}

// HLSPlaylist renders the media playlist over the last hlsWindow closed segments,
// or "" when none are ready yet. Segment URIs are "<seq>.ts". Caller serialises.
func (s *State) HLSPlaylist() string {
	if len(s.segs) == 0 {
		return ""
	}
	if s.plValid {
		return s.plCache
	}
	// Reserve the single oldest in-ring segment as a fetch margin, so a listed
	// segment can't age out between playlist render and HLSSegment fetch (the ring
	// no longer carries a +1-segment margin). Only when more than one exists, so a
	// minimal (gated) ring still serves its one segment.
	avail := s.segs
	if len(avail) > 1 {
		avail = avail[1:]
	}
	start := 0
	if s.hlsWindow > 0 && len(avail) > s.hlsWindow {
		start = len(avail) - s.hlsWindow
	}
	win := avail[start:]
	// Discontinuities before the window — pruned, or in the reserved margin —
	// set #EXT-X-DISCONTINUITY-SEQUENCE, as the HLS spec requires once a
	// segment carrying one has left the playlist.
	discSeq := s.discDropped
	for _, sg := range s.segs[:len(s.segs)-len(win)] {
		if sg.disc {
			discSeq++
		}
	}
	// #EXT-X-TARGETDURATION must not change for the life of the playlist (RFC
	// 8216): a player sizes its reload timer and its startup buffer from it.
	// Taken from the window alone it moved every time a longer-than-usual segment
	// slid out, so the same client saw it change between two reloads of the same
	// URL. It only ever rises here, and is cleared when the target itself changes
	// (Configure) or the stream is torn down (Reset).
	maxDur := 0.0
	for _, sg := range win {
		if d := float64(sg.durMS) / 1000.0; d > maxDur {
			maxDur = d
		}
	}
	if td := int(math.Ceil(maxDur)); td > s.hlsMaxDur {
		s.hlsMaxDur = td
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", s.hlsMaxDur)
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", win[0].seq)
	if discSeq > 0 {
		fmt.Fprintf(&b, "#EXT-X-DISCONTINUITY-SEQUENCE:%d\n", discSeq)
	}
	for _, sg := range win {
		if sg.disc {
			b.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%d.ts\n", float64(sg.durMS)/1000.0, sg.seq)
	}
	s.plCache, s.plValid = b.String(), true
	return s.plCache
}

// HLSSegment assembles segment seq from the ring on demand: latest PAT + PMT
// followed by the bytes of the GOPs in the segment's [startID..endID] range.
// Returns nil if seq is unknown or its GOPs have aged out.
//
// This concatenates inline, so a caller holding a lock holds it for the whole
// multi-megabyte copy. Hub uses the two-phase HLSSegmentPin/Unpin below instead,
// to copy with the lock released. Caller serialises access.
func (s *State) HLSSegment(seq int) []byte {
	head, parts, pin, ok := s.HLSSegmentPin(nil, seq)
	if !ok {
		return nil
	}
	n := len(head)
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	out = append(out, head...)
	for _, p := range parts {
		out = append(out, p...)
	}
	s.Unpin(pin)
	return out
}

// HLSSegmentPin is HLSSegment split the way SnapshotPin splits a join snapshot:
// it returns the header (latest PAT + PMT, copied here because they are mutated
// in place) plus the segment's GOP byte slices, and PINS them so the caller can
// release its lock, concatenate at leisure, and then Unpin.
//
// Assembling under the lock meant a multi-megabyte copy out of the ring with the
// stream's producer blocked behind it, once per segment per stream. The pin
// machinery to avoid that already existed for the join burst; this just uses it.
// ok=false means the segment is unknown or has aged out, and NOTHING is pinned.
func (s *State) HLSSegmentPin(dst []byte, seq int) ([]byte, [][]byte, Pin, bool) {
	for i := range s.segs {
		if s.segs[i].seq != seq {
			continue
		}
		sg := s.segs[i]
		// The segment's FIRST GOP must still be in the ring. Accepting it whenever
		// ANY of its GOPs survived served a segment that starts mid-range — no
		// keyframe at the head, and shorter than its own #EXTINF — every time the
		// ring pruned past startID but not yet past endID, which is the steady
		// state at the tail of a tight (gated) ring. A player gets a decode error
		// from that, where a 404 just makes it skip. gops is id-ascending, so one
		// comparison against the oldest settles it.
		if len(s.gops) == 0 || s.gops[0].id > sg.startID {
			return nil, nil, Pin{}, false // aged out between playlist render and fetch
		}
		out := append(dst[:0], s.lastPAT...)
		out = append(out, s.lastPMT...)
		parts := make([][]byte, 0, len(s.gops))
		lo, hi := int64(-1), int64(-1)
		for j := range s.gops {
			if s.gops[j].id >= sg.startID && s.gops[j].id <= sg.endID {
				parts = append(parts, s.gops[j].data)
				if lo < 0 {
					lo = s.gops[j].id
				}
				hi = s.gops[j].id
			}
		}
		if len(parts) == 0 {
			return nil, nil, Pin{}, false
		}
		return out, parts, s.pin(lo, hi), true
	}
	return nil, nil, Pin{}, false
}

// Snapshot returns the bytes a new subscriber should receive before the live
// tail: latest PAT, latest PMT, then a keyframe-aligned run of recent GOPs.
// reqMS is the prebuffer the subscriber wants, in milliseconds; 0 (or a stream
// with no parseable PCR) yields just the current GOP. The run is clamped to what
// the ring holds. The returned slice is a fresh copy owned by the caller.
func (s *State) Snapshot(reqMS int64) []byte {
	return s.SnapshotInto(nil, reqMS)
}

// SnapshotInto is Snapshot writing into dst (its contents are overwritten; grown
// only if it is too small). It lets the caller supply a pooled buffer so a viewer
// join does not allocate a fresh full-ring copy each time — the dominant transient
// allocation on the connect path, which otherwise sets the heap high-water mark
// under many concurrent joins. The returned slice aliases dst when it fit. Caller
// serialises access and must not retain dst past the returned slice's use.
//
// This does the whole copy inline, so a caller holding a lock holds it for the
// duration. Hub uses the two-phase SnapshotPin/Unpin below instead, to copy with
// the lock released.
func (s *State) SnapshotInto(dst []byte, reqMS int64) []byte {
	out, parts, pin := s.SnapshotPin(dst, reqMS)
	for _, p := range parts {
		out = append(out, p...)
	}
	s.Unpin(pin)
	return out
}

// SnapshotPin is the first half of a join snapshot: it writes the header
// (latest PAT + PMT, which are mutated in place as new ones arrive and so must be
// copied while the caller still holds its lock) into dst, and returns it together
// with the GOP byte slices that follow. It PINS those buffers, so the caller may
// release its lock, append the parts at leisure, and then call Unpin.
//
// The point is that appending them is the expensive part — up to the whole ring,
// measured at ~5 ms for a 40 s / 14 MB prebuffer — and doing it under the hub
// lock stalls the stream's producer for that long on every viewer join. A join
// storm (a channel going live, an EPG event) serialised those stalls: a hundred
// restreamers joining at once took the stream off the air for half a second.
//
// The captured slices have the lengths they had at this instant, so the open GOP
// growing afterwards is invisible to the copy — which is also what makes this
// safe to pair with registering a subscriber under the same lock: everything
// published after that point reaches the viewer through its channel, everything
// before it is in these bytes, with no gap and no duplication.
func (s *State) SnapshotPin(dst []byte, reqMS int64) ([]byte, [][]byte, Pin) {
	start := s.joinIndex(reqMS)

	out := append(dst[:0], s.lastPAT...)
	out = append(out, s.lastPMT...)

	parts := make([][]byte, 0, len(s.gops)-start)
	for i := start; i < len(s.gops); i++ {
		parts = append(parts, s.gops[i].data)
	}
	if len(parts) == 0 {
		return out, parts, Pin{}
	}
	return out, parts, s.pin(s.gops[start].id, s.gops[len(s.gops)-1].id)
}

// joinIndex is the ring index a join with a reqMS prebuffer starts at: that far
// back on the ring clock (0 or no clock: the current block), then moved to a
// block a decoder can begin on.
func (s *State) joinIndex(reqMS int64) int {
	start := len(s.gops) - 1
	if start < 0 {
		start = 0
	}
	if reqMS > 0 && len(s.gops) > 0 {
		if newest := s.gops[len(s.gops)-1].t; newest >= 0 {
			req := reqMS * pcrHz
			i := len(s.gops) - 1
			for i > 0 {
				if p := s.gops[i-1].t; p < 0 || newest-p > req {
					break
				}
				i--
			}
			start = i
		}
	}
	return s.snapshotStart(start)
}

// Cursor is a joining viewer's place in the ring: the block it is in, and how
// many of that block's bytes it has already been written.
type Cursor struct {
	GOP int64
	Off int
}

// JoinStart begins a join: the header a viewer is written first (latest PAT and
// PMT, copied — they are mutated in place) and the cursor its history starts
// at. Nothing is pinned and nothing registered; the viewer catches up with
// ReadFrom. Caller (Hub) serialises access.
func (s *State) JoinStart(dst []byte, reqMS int64) ([]byte, Cursor) {
	head := append(dst[:0], s.lastPAT...)
	head = append(head, s.lastPMT...)
	if len(s.gops) == 0 {
		return head, Cursor{GOP: s.nextGOPID} // nothing retained yet: start at the edge
	}
	i := s.joinIndex(reqMS)
	// A viewer takes its history over time, at its own link speed, so it must not
	// start in the block the next keyframe prunes: that left it one GOP to take a
	// whole block, and any slower (a storm of joins, a busy node) was dropped as
	// behind. A request for the whole ring — or more — starts one block in.
	if i == 0 && len(s.gops) > 1 {
		i = s.snapshotStart(1)
	}
	return head, Cursor{GOP: s.gops[i].id}
}

// ReadFrom returns the ring's bytes from c onwards — about max of them, cut on
// a packet boundary — and the cursor after them, PINNED (the caller writes them
// with the lock released, then Unpins; nothing is pinned when parts is empty).
// atEnd reports that they reach the live edge: everything the ring holds. behind
// reports that c's block has left the ring — the reader fell further behind than
// the ring reaches.
//
// This is how a viewer takes its join history without a copy AND without a
// queue: it reads the ring forward in runs until it is at the edge, and only
// then subscribes to the live tail. The live tail used to queue behind the whole
// burst instead, and a queue short enough to be cheap dropped every viewer whose
// link could not take a 30 s burst in ~5 s. Caller (Hub) serialises access.
func (s *State) ReadFrom(c Cursor, max int) (parts [][]byte, next Cursor, atEnd, behind bool, pin Pin) {
	if len(s.gops) == 0 {
		return nil, c, true, false, Pin{}
	}
	first := s.gops[0].id
	if c.GOP < first {
		// Its block has left the ring. If the reader had taken every byte of it —
		// a live viewer parked at the edge when a ring shorter than one GOP prunes
		// the block it just finished — nothing was lost: go on at the next block.
		if c.GOP != s.prunedID || first != s.prunedID+1 || c.Off < s.prunedLen {
			return nil, c, false, true, Pin{}
		}
		c = Cursor{GOP: first}
	}
	i := int(c.GOP - first) // ids are consecutive: blocks are appended in order and pruned from the front
	if i >= len(s.gops) {
		return nil, c, true, false, Pin{}
	}
	if max < PacketSize {
		max = PacketSize
	}
	n := 0
	next = c
	lo, hi := int64(-1), int64(-1) // blocks the run's parts point into
	for ; i < len(s.gops); i++ {
		g := s.gops[i]
		off := 0
		if g.id == c.GOP {
			off = c.Off
		}
		if off > len(g.data) {
			off = len(g.data)
		}
		rem := g.data[off:]
		if n+len(rem) > max {
			take := (max - n) / PacketSize * PacketSize
			if take > 0 {
				parts = append(parts, rem[:take])
				n += take
				if lo < 0 {
					lo = g.id
				}
				hi = g.id
			}
			next = Cursor{GOP: g.id, Off: off + take}
			if len(parts) > 0 {
				pin = s.pin(lo, hi)
			}
			return parts, next, false, false, pin
		}
		if len(rem) > 0 {
			parts = append(parts, rem)
			n += len(rem)
			if lo < 0 {
				lo = g.id
			}
			hi = g.id
		}
		next = Cursor{GOP: g.id, Off: len(g.data)}
	}
	if len(parts) > 0 {
		pin = s.pin(lo, hi)
	}
	return parts, next, true, false, pin
}

// snapshotStart moves a join's first block to one a decoder can start on: the
// earliest video random-access block at or after it, else — when the ring holds
// none after it — the newest one before it, which costs the joiner a little more
// buffer but hands it a picture. A stream with no video block at all (radio, or a
// source whose keyframes cannot be recognised, where the blocks are maxGOP cuts)
// is left alone: there is nothing better to start on.
func (s *State) snapshotStart(start int) int {
	if start < 0 || start >= len(s.gops) {
		return start
	}
	for i := start; i < len(s.gops); i++ {
		if s.gops[i].video {
			return i
		}
	}
	for i := start - 1; i >= 0; i-- {
		if s.gops[i].video {
			return i
		}
	}
	return start
}

// Reset drops the entire cache: every retained GOP, the HLS segment view and the
// rendered playlist. Used when a stream's producer stops for good (the reaper's
// idle-stop), where the bytes still in the ring are not a buffer any more — they
// are a frozen snapshot of whenever the source was last alive, worth nothing to a
// future viewer and, at ~idle_buffer_ratio × prebuffer_max_sec of video per
// registered channel, the daemon's largest idle memory term by a wide margin.
//
// The GOP arrays go to GC rather than onto the free list: releasing the memory is
// the entire point, so recycling a few of them would defeat it. PAT/PMT are kept
// (two packets) so a restarted puller still has program info before its first
// keyframe. Caller (Hub) serialises access.
func (s *State) Reset() {
	for _, sg := range s.segs {
		if sg.disc {
			s.discDropped++
		}
	}
	if n := len(s.gops); n > 0 {
		s.prunedID, s.prunedLen = s.gops[n-1].id, len(s.gops[n-1].data)
	}
	s.gops = nil
	s.segs = nil
	s.freeBufs = nil
	s.segStartID, s.segLastPTS, s.segSpan = -1, -1, 0
	// Whatever the producer sends next follows a gap: its first segment is a
	// discontinuity to anyone who saw the ones before.
	s.segDisc = s.nextSeq > 0
	s.hlsStep, s.hlsMaxDur = 0, 0
	s.plCache, s.plValid = "", false
}

// RingStats reports what the ring holds: its bytes, how much stream time they
// span on the ring clock (ms; 0 without one), and its GOP count. For the memory
// report — the ring is most of the daemon's heap. Caller (Hub) serialises access.
func (s *State) RingStats() (bytes int, spanMS int64, gops int) {
	for i := range s.gops {
		bytes += len(s.gops[i].data)
	}
	if n := len(s.gops); n > 1 && s.gops[0].t >= 0 && s.gops[n-1].t >= 0 {
		spanMS = (s.gops[n-1].t - s.gops[0].t) / pcrHz
	}
	return bytes, spanMS, len(s.gops)
}

// Pin is a reader's hold on the blocks its slices point into (ids lo..hi), taken
// by ReadFrom, SnapshotPin or HLSSegmentPin and given back with Unpin, exactly
// once. The zero Pin holds nothing.
type Pin struct {
	lo, hi int64
	held   bool
}

// pin holds blocks lo..hi. Caller (Hub) serialises access.
func (s *State) pin(lo, hi int64) Pin {
	if s.pinned == nil {
		s.pinned = make(map[int64]int32)
	}
	for id := lo; id <= hi; id++ {
		s.pinned[id]++
	}
	return Pin{lo: lo, hi: hi, held: true}
}

// Unpin gives back a Pin, letting prune recycle those blocks' arrays again once
// no other reader holds them. Caller (Hub) serialises access.
func (s *State) Unpin(p Pin) {
	if !p.held {
		return
	}
	for id := p.lo; id <= p.hi; id++ {
		if n := s.pinned[id]; n > 1 {
			s.pinned[id] = n - 1
		} else {
			delete(s.pinned, id)
		}
	}
}

// NoKeyframeCuts reports how many blocks were closed because the source gave no
// random-access point within maxGOP bytes. Non-zero means this source carries no
// random_access_indicator — which is also why it produces no HLS segments.
// Caller (Hub) serialises access.
func (s *State) NoKeyframeCuts() int64 { return s.noKeyframeCuts }

// readPCR extracts the 33-bit PCR base (90 kHz) from a packet whose adaptation
// field carries it. The caller has verified afc has adaptation, the PCR_flag is
// set and adaptation_field_length is at least 7 — the flags byte plus the six
// PCR bytes — so bytes 6..10 hold the base.
func readPCR(pkt []byte) int64 {
	return int64(pkt[6])<<25 | int64(pkt[7])<<17 | int64(pkt[8])<<9 |
		int64(pkt[9])<<1 | int64(pkt[10])>>7
}

func cloneInto(dst, src []byte) []byte {
	return append(dst[:0], src...)
}

// getBuf returns a recycled GOP data buffer (length 0, capacity preserved), or a
// nil slice when the free list is empty — the caller's append then allocates a
// fresh array exactly as before, so warm-up behaviour is byte-identical. Used when
// opening a new GOP (or a pre-roll block). Caller (Hub) serialises access.
func (s *State) getBuf() []byte {
	n := len(s.freeBufs)
	if n == 0 {
		return nil
	}
	b := s.freeBufs[n-1]
	s.freeBufs[n-1] = nil // drop the slot's reference so it can't pin the array
	s.freeBufs = s.freeBufs[:n-1]
	return b[:0]
}

// release is how prune lets a block leave the ring: it is remembered for ReadFrom
// (blocks leave oldest first, so the last one released is the newest gone) and its
// array recycled — unless a reader still holds it, when it goes to GC instead.
func (s *State) release(g gop) {
	s.prunedID, s.prunedLen = g.id, len(g.data)
	if s.pinned[g.id] > 0 {
		return
	}
	s.putBuf(g.data)
}

// putBuf recycles a dropped GOP's backing array for reuse, capacity preserved.
// Called only from prune (under the hub lock), where the GOP has just left the
// ring. Buffers past the cap are left to GC, which is what we want on a gating
// ring-shrink (the goal there is to release memory).
//
// A block a reader still holds never gets here (see release): its array goes to
// GC, which the reader's own slices keep alive for exactly as long as it needs it.
func (s *State) putBuf(b []byte) {
	if cap(b) == 0 || len(s.freeBufs) >= defaults.JoinFreeGOPBufs {
		return
	}
	s.freeBufs = append(s.freeBufs, b[:0])
}

// parsePMTPID extracts the first program's PMT PID from a PAT packet.
// Returns -1 if it cannot be parsed.
func parsePMTPID(pkt []byte) int {
	afc := (pkt[3] >> 4) & 0x3
	payloadStart := 4
	switch afc {
	case 2: // adaptation only, no payload
		return -1
	case 3: // adaptation + payload
		payloadStart = 5 + int(pkt[4])
	}
	if payloadStart >= len(pkt) {
		return -1
	}
	// PUSI is set for PAT, so the first payload byte is the pointer_field.
	p := payloadStart + 1 + int(pkt[payloadStart])
	// Section header is 8 bytes; the program loop follows.
	prog := p + 8
	for prog+4 <= len(pkt) {
		programNumber := (int(pkt[prog]) << 8) | int(pkt[prog+1])
		pid := ((int(pkt[prog+2]) & 0x1f) << 8) | int(pkt[prog+3])
		if programNumber != 0 {
			return pid
		}
		prog += 4
	}
	return -1
}

// ── HLS PTS parsing (ported from the retired hlsseg; MPEG-TS PES) ────────────

// payloadOffset returns the byte index where a packet's payload begins, or -1 if
// the packet carries no payload (adaptation-only).
func payloadOffset(pkt []byte) int {
	switch (pkt[3] >> 4) & 0x3 {
	case 2: // adaptation only
		return -1
	case 3: // adaptation + payload
		return 5 + int(pkt[4])
	default: // payload only
		return 4
	}
}

// Counters reports the health counters: audio packets and video access units
// seen since this State was created, and whether the source declares an audio
// stream at all. hasAudio distinguishes "audio has stopped" from "there was
// never any audio", which decides whether silence is a fault worth a restart.
// Caller (Hub) serialises access.
func (s *State) Counters() (audioPkts, videoFrames int64, hasAudio bool) {
	return s.audioPkts, s.videoFrames, s.audioPID >= 0
}

// parseAudioPID reads the first audio elementary-stream PID out of a PMT packet,
// or -1. Mirrors parseVideoPID; the two differ only in the stream types they
// accept.
func parseAudioPID(pkt []byte) int {
	es, _ := tspes.ParsePMTStreams(pkt)
	for _, e := range es {
		if isAudioStreamType(e.Type) {
			return int(e.PID)
		}
	}
	return -1
}

// isAudioStreamType covers the audio codecs an IPTV source realistically
// carries: MPEG-1/2 audio, AAC (ADTS and LATM), AC-3 and E-AC-3 — including the
// 0x06 private-stream form the latter two are usually signalled as in DVB.
func isAudioStreamType(t byte) bool {
	switch t {
	case 0x03, 0x04, 0x0f, 0x11, 0x81, 0x87:
		return true
	}
	return false
}

// parseVideoPID reads the first video elementary-stream PID out of a PMT packet,
// with its stream_type, or -1. Used to locate the PES that carries the HLS
// segment clock (PTS), and to know how to read a keyframe off that PES.
//
// The ES loop is tspes's, which reads the table_id and walks bounded by
// section_length. The loop here ran to the end of the 188-byte PACKET instead,
// so on a table whose entries do not fill it — an audio-only PMT, one AAC stream
// — it stepped straight onto the section's CRC32 and read that as another entry.
// About six CRC values in 256 begin with a video stream_type, and a table's CRC
// is fixed, so an affected channel stayed affected across restarts: it then had a
// video PID nothing ever arrived on, `rap && (onVideo || videoPID < 0)` was never
// true, and the stream was cut into blocks only by the maxGOP cap — minutes at
// radio bitrates, which a listener joins at the start of and never catches up
// from.
func parseVideoPID(pkt []byte) (int, byte) {
	es, _ := tspes.ParsePMTStreams(pkt)
	for _, e := range es {
		if isVideoStreamType(e.Type) {
			return int(e.PID), e.Type
		}
	}
	return -1, 0
}

func isVideoStreamType(t byte) bool {
	switch t {
	case 0x01, 0x02, 0x10, 0x1b, 0x24, 0xea: // MPEG1/2, MPEG4, H.264, HEVC, VC-1
		return true
	}
	return false
}

// parsePTS extracts the 33-bit PTS (90 kHz) from a packet whose payload starts a
// PES with the PTS flag set, or reports ok=false.
func parsePTS(pkt []byte) (int64, bool) {
	ps := payloadOffset(pkt)
	if ps < 0 || ps+14 > len(pkt) {
		return 0, false
	}
	if pkt[ps] != 0 || pkt[ps+1] != 0 || pkt[ps+2] != 1 {
		return 0, false // no PES start code
	}
	if pkt[ps+7]&0x80 == 0 {
		return 0, false // PTS not present
	}
	p := ps + 9
	pts := (int64(pkt[p]>>1&0x07) << 30) |
		(int64(pkt[p+1]) << 22) |
		(int64(pkt[p+2]>>1&0x7f) << 15) |
		(int64(pkt[p+3]) << 7) |
		(int64(pkt[p+4] >> 1 & 0x7f))
	return pts, true
}
