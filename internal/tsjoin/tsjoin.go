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
	pcr  int64 // PCR base at open (90 kHz), or -1 — drives ring retention
	pts  int64 // HLS clock at open: the keyframe's PES PTS, or the PCR fallback, or -1
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
}

// State accumulates join information from a packet-aligned byte stream.
// It is not safe for concurrent use; the caller (Hub) serialises access.
type State struct {
	lastPAT  []byte // one 188-byte PAT packet, or nil
	lastPMT  []byte // one 188-byte PMT packet, or nil
	pmtPID   int    // PID carrying the PMT, or -1 if unknown
	videoPID int    // PID carrying the video ES (for HLS segment PTS), or -1
	gops     []gop  // oldest→newest; the last element is the open (current) GOP
	lastPCR  int64  // most recent PCR base seen (90 kHz), or -1
	maxGOP   int    // cap on a single GOP's length (bytes) — memory guard
	ring90   int64  // history retained, in 90 kHz ticks (0 = current GOP only)
	maxRing  int    // absolute byte ceiling for the whole ring — memory backstop

	// pins counts snapshots currently copying out of the ring. While non-zero,
	// prune must not RECYCLE a dropped GOP's array (it leaves it to GC instead):
	// a snapshot in flight holds slices into those arrays and is copying them
	// with the lock released, so handing one to a new GOP would rewrite bytes
	// mid-copy. See SnapshotPin.
	pins int

	// freeBufs recycles the byte arrays of GOPs dropped by prune so opening the
	// next GOP reuses one instead of allocating a fresh array every keyframe. This
	// turns the steady-state GOP allocate-and-discard (≈ bitrate, the GC sawtooth)
	// into ~zero. Capped at defaults.JoinFreeGOPBufs; buffers hold len 0, cap kept.
	freeBufs [][]byte

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
	segStartPTS int64    // open segment's start HLS clock (90 kHz)

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
	s := &State{pmtPID: -1, videoPID: -1, lastPCR: -1, maxGOP: maxGOP, prebufMS: maxPrebufMS, segStartID: -1, idleRatio: 0.5}
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

// SetRing live-reconfigures the buffer depth (ms). Caller (Hub) serialises access.
func (s *State) SetRing(maxPrebufMS int64) {
	if maxPrebufMS < 0 {
		maxPrebufMS = 0
	}
	s.prebufMS = maxPrebufMS
	s.recompute()
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
		afc := (pkt[3] >> 4) & 0x3
		hasAdaptation := afc == 2 || afc == 3

		keyframe := false
		if hasAdaptation && pkt[4] > 0 {
			flags := pkt[5]
			if flags&0x10 != 0 { // PCR_flag: refresh the stream clock
				s.lastPCR = readPCR(pkt)
			}
			if flags&0x40 != 0 { // random_access_indicator
				keyframe = true
			}
		}

		switch {
		case pid == 0 && pusi:
			s.lastPAT = cloneInto(s.lastPAT, pkt)
			if p := parsePMTPID(pkt); p >= 0 {
				s.pmtPID = p
			}
		case s.pmtPID >= 0 && pid == s.pmtPID:
			s.lastPMT = cloneInto(s.lastPMT, pkt)
			if v := parseVideoPID(pkt); v >= 0 {
				s.videoPID = v
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
			pts := int64(-1)
			if s.videoPID >= 0 && pid == s.videoPID {
				if p, ok := parsePTS(pkt); ok {
					pts = p
					s.hlsOnKeyframe(id, pts)
				}
			}
			s.gops = append(s.gops, gop{id: id, data: append(s.getBuf(), pkt...), pcr: s.lastPCR, pts: pts})
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
			s.gops = append(s.gops, gop{id: id, data: append(s.getBuf(), pkt...), pcr: s.lastPCR, pts: -1})
			s.prune()
		}
	}
}

// prune drops the oldest GOPs once the ring exceeds the configured duration
// (when PCR timing is available) or the absolute byte backstop. With no
// prebuffer configured it collapses to just the current GOP.
func (s *State) prune() {
	if s.ring90 <= 0 {
		if len(s.gops) > 1 {
			for i := 0; i < len(s.gops)-1; i++ {
				s.putBuf(s.gops[i].data)
			}
			s.gops = append(s.gops[:0], s.gops[len(s.gops)-1])
		}
		return
	}
	// Duration-based prune (needs valid PCR on both ends).
	if newest := s.gops[len(s.gops)-1].pcr; newest >= 0 {
		drop := 0
		for drop < len(s.gops)-1 {
			if p := s.gops[drop].pcr; p >= 0 && newest-p <= s.ring90 {
				break
			}
			drop++
		}
		if drop > 0 {
			for i := 0; i < drop; i++ {
				s.putBuf(s.gops[i].data)
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
		s.putBuf(s.gops[0].data)
		s.gops = append(s.gops[:0], s.gops[1:]...)
	}
	s.hlsPrune()
}

// hlsOnKeyframe advances the HLS segment view when a new keyframe (a new GOP,
// id=newID, opening at newPCR) arrives. If the currently-open segment now spans
// at least hlsTargetMS, it is closed — ending at the previous last GOP — and this
// keyframe opens the next one. Called before the new GOP is appended, so
// s.gops[last] is the closing segment's final GOP. No-op unless the HLS view is
// enabled and PCR timing is available.
func (s *State) hlsOnKeyframe(newID, newPTS int64) {
	if s.hlsTargetMS <= 0 {
		return
	}
	if s.segStartID < 0 {
		s.segStartID, s.segStartPTS = newID, newPTS
		return
	}
	if newPTS < 0 || s.segStartPTS < 0 {
		return // cannot measure duration without a clock on both ends
	}
	d := newPTS - s.segStartPTS
	if d < 0 {
		d += ptsWrap // straddled the 33-bit wrap
	}
	if d >= s.hlsTargetMS*pcrHz {
		endID := s.segStartID
		if n := len(s.gops); n > 0 {
			endID = s.gops[n-1].id
		}
		s.segs = append(s.segs, hlsSeg{seq: s.nextSeq, startID: s.segStartID, endID: endID, durMS: d / pcrHz})
		s.nextSeq++
		s.plValid = false
		s.segStartID, s.segStartPTS = newID, newPTS
	}
}

// hlsPrune drops segments whose GOPs have fully aged out of the ring, so the
// playlist only ever lists segments Segment can still assemble.
func (s *State) hlsPrune() {
	if len(s.segs) == 0 || len(s.gops) == 0 {
		return
	}
	oldest := s.gops[0].id
	drop := 0
	for drop < len(s.segs) && s.segs[drop].endID < oldest {
		drop++
	}
	if drop > 0 {
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
	maxDur := 0.0
	for _, sg := range win {
		if d := float64(sg.durMS) / 1000.0; d > maxDur {
			maxDur = d
		}
	}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(maxDur)))
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", win[0].seq)
	for _, sg := range win {
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%d.ts\n", float64(sg.durMS)/1000.0, sg.seq)
	}
	s.plCache, s.plValid = b.String(), true
	return s.plCache
}

// HLSSegment assembles segment seq from the ring on demand: latest PAT + PMT
// followed by the bytes of the GOPs in the segment's [startID..endID] range.
// Returns nil if seq is unknown or its GOPs have aged out. Caller serialises.
func (s *State) HLSSegment(seq int) []byte {
	for i := range s.segs {
		if s.segs[i].seq != seq {
			continue
		}
		sg := s.segs[i]
		n := len(s.lastPAT) + len(s.lastPMT)
		got := false
		for j := range s.gops {
			if s.gops[j].id >= sg.startID && s.gops[j].id <= sg.endID {
				n += len(s.gops[j].data)
				got = true
			}
		}
		if !got {
			return nil // aged out between playlist render and fetch
		}
		out := make([]byte, 0, n)
		out = append(out, s.lastPAT...)
		out = append(out, s.lastPMT...)
		for j := range s.gops {
			if s.gops[j].id >= sg.startID && s.gops[j].id <= sg.endID {
				out = append(out, s.gops[j].data...)
			}
		}
		return out
	}
	return nil
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
	out, parts := s.SnapshotPin(dst, reqMS)
	for _, p := range parts {
		out = append(out, p...)
	}
	s.Unpin()
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
func (s *State) SnapshotPin(dst []byte, reqMS int64) ([]byte, [][]byte) {
	start := len(s.gops) - 1
	if start < 0 {
		start = 0
	}
	if reqMS > 0 && len(s.gops) > 0 {
		if newest := s.gops[len(s.gops)-1].pcr; newest >= 0 {
			req := reqMS * pcrHz
			i := len(s.gops) - 1
			for i > 0 {
				if p := s.gops[i-1].pcr; p < 0 || newest-p > req {
					break
				}
				i--
			}
			start = i
		}
	}

	out := append(dst[:0], s.lastPAT...)
	out = append(out, s.lastPMT...)

	parts := make([][]byte, 0, len(s.gops)-start)
	for i := start; i < len(s.gops); i++ {
		parts = append(parts, s.gops[i].data)
	}
	s.pins++
	return out, parts
}

// Unpin releases a SnapshotPin, letting prune recycle dropped GOP buffers again.
// Caller (Hub) serialises access, and must call it exactly once per SnapshotPin.
func (s *State) Unpin() {
	if s.pins > 0 {
		s.pins--
	}
}

// NoKeyframeCuts reports how many blocks were closed because the source gave no
// random-access point within maxGOP bytes. Non-zero means this source carries no
// random_access_indicator — which is also why it produces no HLS segments.
// Caller (Hub) serialises access.
func (s *State) NoKeyframeCuts() int64 { return s.noKeyframeCuts }

// readPCR extracts the 33-bit PCR base (90 kHz) from a packet whose adaptation
// field carries it. The caller has verified afc has adaptation, pkt[4] > 0 and
// the PCR_flag is set, so bytes 6..10 hold the base.
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

// putBuf recycles a dropped GOP's backing array for reuse, capacity preserved.
// Called only from prune (under the hub lock), where the GOP has just left the
// ring. Buffers past the cap are left to GC, which is what we want on a gating
// ring-shrink (the goal there is to release memory).
//
// Recycling is suspended while a snapshot is pinned: that reader is copying GOP
// bytes with the lock released, so its arrays must not be handed to a new GOP
// underneath it. They go to GC for the duration instead, which the reader's own
// slices keep alive for exactly as long as it needs them.
func (s *State) putBuf(b []byte) {
	if cap(b) == 0 || s.pins > 0 || len(s.freeBufs) >= defaults.JoinFreeGOPBufs {
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

// parseVideoPID reads the first video elementary-stream PID out of a PMT packet,
// or -1. Used to locate the PES that carries the HLS segment clock (PTS).
func parseVideoPID(pkt []byte) int {
	ps := payloadOffset(pkt)
	if ps < 0 || ps >= len(pkt) {
		return -1
	}
	p := ps + 1 + int(pkt[ps]) // skip pointer_field
	if p+12 > len(pkt) {
		return -1
	}
	pil := ((int(pkt[p+10]) & 0x0f) << 8) | int(pkt[p+11]) // program_info_length
	es := p + 12 + pil
	for es+5 <= len(pkt) {
		if isVideoStreamType(pkt[es]) {
			return ((int(pkt[es+1]) & 0x1f) << 8) | int(pkt[es+2])
		}
		es += 5 + (((int(pkt[es+3]) & 0x0f) << 8) | int(pkt[es+4]))
	}
	return -1
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
