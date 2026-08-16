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

// PacketSize is the fixed MPEG-TS packet length.
const PacketSize = 188

// pcrHz is the MPEG-TS system clock: PCR base is measured in 90 kHz ticks, so
// one millisecond is 90 ticks.
const pcrHz = 90

// gop is one keyframe-aligned block: packets from a random-access point up to
// (but not including) the next one, tagged with the stream clock (PCR base, in
// 90 kHz ticks) seen as it opened. pcr is -1 until a PCR has been observed.
type gop struct {
	data []byte
	pcr  int64
}

// State accumulates join information from a packet-aligned byte stream.
// It is not safe for concurrent use; the caller (Hub) serialises access.
type State struct {
	lastPAT []byte // one 188-byte PAT packet, or nil
	lastPMT []byte // one 188-byte PMT packet, or nil
	pmtPID  int    // PID carrying the PMT, or -1 if unknown
	gops    []gop  // oldest→newest; the last element is the open (current) GOP
	lastPCR int64  // most recent PCR base seen (90 kHz), or -1
	maxGOP  int    // cap on a single GOP's length (bytes) — memory guard
	ring90  int64  // history retained for prebuffer, in 90 kHz ticks (0 = current GOP only)
	maxRing int    // absolute byte ceiling for the whole ring — memory backstop
}

// New returns a State. maxGOP caps a single GOP (bytes, guarding streams with no
// keyframes); maxPrebufMS is how many milliseconds of keyframe-aligned history
// to retain for client prebuffer (0 = keep only the current GOP, the original
// behaviour).
func New(maxGOP int, maxPrebufMS int64) *State {
	s := &State{pmtPID: -1, lastPCR: -1, maxGOP: maxGOP, ring90: maxPrebufMS * pcrHz}
	// Backstop the ring at a generous high-bitrate estimate (~24 Mbit/s ≈ 3000
	// bytes/ms) so a stream whose PCR cannot be parsed can never grow it without
	// bound. Duration-based pruning does the real work when PCR is present.
	s.maxRing = int(maxPrebufMS) * 3000
	return s
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
		}

		switch {
		case keyframe:
			// Open a fresh GOP at this random-access point.
			s.gops = append(s.gops, gop{data: append([]byte(nil), pkt...), pcr: s.lastPCR})
			s.prune()
		case len(s.gops) > 0:
			g := &s.gops[len(s.gops)-1]
			if len(g.data) < s.maxGOP {
				g.data = append(g.data, pkt...)
			}
		default:
			// Pre-roll before the first keyframe: keep a provisional block so an
			// early join still gets recent packets (matches the old
			// accumulate-until-keyframe behaviour).
			s.gops = append(s.gops, gop{data: append([]byte(nil), pkt...), pcr: s.lastPCR})
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
