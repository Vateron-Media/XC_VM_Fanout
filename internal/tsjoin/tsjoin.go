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

	// HLS segment view over the ring (hlsTargetMS == 0 disables it). The ring is
	// the single cache; HLS segments are cut from these GOPs on demand rather than
	// buffered a second time.
	prebufMS    int64    // TS client-prebuffer depth (ms)
	hlsTargetMS int64    // HLS target segment duration (ms); 0 = HLS view off
	hlsWindow   int      // HLS segments kept in the playlist window
	nextGOPID   int64    // next GOP id to assign
	segs        []hlsSeg // closed segments, oldest→newest
	nextSeq     int      // next HLS media sequence number
	segStartID  int64    // open segment's first GOP id, or -1 = none open
	segStartPTS int64    // open segment's start HLS clock (90 kHz)
}

// New returns a State. maxGOP caps a single GOP (bytes, guarding streams with no
// keyframes); maxPrebufMS is how many milliseconds of keyframe-aligned history
// to retain for client prebuffer (0 = keep only the current GOP, the original
// behaviour). The HLS view starts disabled; call Configure to enable it.
func New(maxGOP int, maxPrebufMS int64) *State {
	s := &State{pmtPID: -1, videoPID: -1, lastPCR: -1, maxGOP: maxGOP, prebufMS: maxPrebufMS, segStartID: -1}
	s.recompute()
	return s
}

// recompute sizes the ring to cover BOTH the TS prebuffer and the HLS window
// (plus one segment of margin, so a segment still listed in the playlist is
// guaranteed assemblable when the client fetches it), and its byte backstop
// (~24 Mbit/s ≈ 3000 bytes/ms, so a stream whose PCR cannot be parsed can never
// grow the ring without bound). Prunes immediately when it shrinks.
func (s *State) recompute() {
	need := s.prebufMS
	if s.hlsWindow > 0 && s.hlsTargetMS > 0 {
		if w := int64(s.hlsWindow+1) * s.hlsTargetMS; w > need {
			need = w
		}
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

// SetRing live-reconfigures the TS client-prebuffer depth (ms). The ring still
// also covers the HLS window. Caller (Hub) serialises access.
func (s *State) SetRing(maxPrebufMS int64) {
	if maxPrebufMS < 0 {
		maxPrebufMS = 0
	}
	s.prebufMS = maxPrebufMS
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
			s.gops = append(s.gops, gop{id: id, data: append([]byte(nil), pkt...), pcr: s.lastPCR, pts: pts})
			s.prune()
		case len(s.gops) > 0:
			g := &s.gops[len(s.gops)-1]
			if len(g.data) < s.maxGOP {
				g.data = append(g.data, pkt...)
			}
		default:
			// Pre-roll before the first keyframe: keep a provisional block so an
			// early join still gets recent packets (matches the old
			// accumulate-until-keyframe behaviour). Not a keyframe, so it does not
			// open an HLS segment.
			id := s.nextGOPID
			s.nextGOPID++
			s.gops = append(s.gops, gop{id: id, data: append([]byte(nil), pkt...), pcr: s.lastPCR, pts: -1})
		}
	}
}

// prune drops the oldest GOPs once the ring exceeds the configured duration
// (when PCR timing is available) or the absolute byte backstop. With no
// prebuffer configured it collapses to just the current GOP.
func (s *State) prune() {
	if s.ring90 <= 0 {
		if len(s.gops) > 1 {
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
	}
}

// HLSPlaylist renders the media playlist over the last hlsWindow closed segments,
// or "" when none are ready yet. Segment URIs are "<seq>.ts". Caller serialises.
func (s *State) HLSPlaylist() string {
	if len(s.segs) == 0 {
		return ""
	}
	start := 0
	if s.hlsWindow > 0 && len(s.segs) > s.hlsWindow {
		start = len(s.segs) - s.hlsWindow
	}
	win := s.segs[start:]
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
	return b.String()
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

	n := len(s.lastPAT) + len(s.lastPMT)
	for i := start; i < len(s.gops); i++ {
		n += len(s.gops[i].data)
	}
	out := make([]byte, 0, n)
	out = append(out, s.lastPAT...)
	out = append(out, s.lastPMT...)
	for i := start; i < len(s.gops); i++ {
		out = append(out, s.gops[i].data...)
	}
	return out
}

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
