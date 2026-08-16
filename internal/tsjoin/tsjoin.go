// Package tsjoin tracks the minimal MPEG-TS state a new subscriber needs to
// start decoding cleanly mid-stream: the latest PAT + PMT and the packets from
// the most recent keyframe (random-access point) up to now.
//
// This mirrors the prebuffer heuristic in the legacy ProxyCommand.php
// (PAT/PMT headers + first keyframe), but parses the TS structure properly
// instead of matching fixed byte offsets.
package tsjoin

// PacketSize is the fixed MPEG-TS packet length.
const PacketSize = 188

// State accumulates join information from a packet-aligned byte stream.
// It is not safe for concurrent use; the caller (Hub) serialises access.
type State struct {
	lastPAT []byte // one 188-byte PAT packet, or nil
	lastPMT []byte // one 188-byte PMT packet, or nil
	pmtPID  int    // PID carrying the PMT, or -1 if unknown
	gop     []byte // packets from the most recent keyframe up to now
	maxGOP  int    // cap on gop length (bytes) to bound memory
}

// New returns a State whose join snapshot is capped at maxGOP bytes.
func New(maxGOP int) *State {
	return &State{pmtPID: -1, maxGOP: maxGOP}
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
		if hasAdaptation && pkt[4] > 0 && (pkt[5]&0x40) != 0 {
			// adaptation_field_length > 0 and random_access_indicator set
			keyframe = true
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

		if keyframe {
			// Start a fresh GOP at this random-access point.
			s.gop = append(s.gop[:0], pkt...)
		} else if len(s.gop) < s.maxGOP {
			s.gop = append(s.gop, pkt...)
		}
	}
}

// Snapshot returns the bytes a new subscriber should receive before the live
// tail: latest PAT, latest PMT, then the current GOP. The returned slice is a
// fresh copy owned by the caller.
func (s *State) Snapshot() []byte {
	out := make([]byte, 0, len(s.lastPAT)+len(s.lastPMT)+len(s.gop))
	out = append(out, s.lastPAT...)
	out = append(out, s.lastPMT...)
	out = append(out, s.gop...)
	return out
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
