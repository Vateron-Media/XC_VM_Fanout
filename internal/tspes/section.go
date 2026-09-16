// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tspes

// MaxSectionBytes is the largest PSI section this package will hold: the
// 1021-byte maximum a private section may declare, plus the three bytes of
// header the length is measured from.
const MaxSectionBytes = 1024

// SectionAssembler folds a PSI section that spans more than one transport
// packet back into one section.
//
// A PMT that lists many elementary streams, or carries descriptors of any size
// — DVB passthrough emits both — does not fit in the 184 payload bytes of one
// packet. Read packet by packet, its first packet alone parses as a section
// that simply ends early: the entries that fit are there and the rest are
// silently missing. That is worse than failing, because callers then act on a
// PARTIAL truth: the segmenter adopted a table with no video PID and gave up on
// the stream, and the ring withdrew a stream's audio declaration because the
// audio entry happened to sit in the continuation packet.
//
// Feed every packet of the table's PID through one assembler and act only on
// what it returns.
type SectionAssembler struct {
	sec []byte
	cc  byte // continuity_counter of the last packet taken
}

// Feed takes one packet of the section's PID and returns the complete section
// once it has one, else nil. The returned bytes alias the assembler's buffer
// and stay valid only until the next Feed.
//
// Continuity is enforced: a continuation packet must be the next in the PID's
// counter sequence, so a packet lost upstream discards the partial section
// rather than splicing two halves of different tables together. An exact
// duplicate — the same counter again, which a remultiplexer may emit — is
// ignored rather than appended.
func (a *SectionAssembler) Feed(pkt []byte) []byte {
	off := PayloadOffset(pkt)
	if off < 0 {
		return nil // adaptation only: carries no part of the section
	}
	cc := pkt[3] & 0x0f
	if PUSI(pkt) {
		off += 1 + int(pkt[off]) // pointer_field
		if off >= len(pkt) {
			a.sec = a.sec[:0]
			return nil
		}
		a.sec = append(a.sec[:0], pkt[off:]...)
	} else {
		if len(a.sec) == 0 {
			return nil // joined mid-section, or the section is already complete
		}
		if cc == a.cc {
			return nil // duplicate packet
		}
		if cc != (a.cc+1)&0x0f {
			a.sec = a.sec[:0] // a packet was lost: this section cannot be trusted
			return nil
		}
		a.sec = append(a.sec, pkt[off:]...)
	}
	a.cc = cc

	sec := a.sec
	if len(sec) < 3 {
		return nil
	}
	total := 3 + (int(sec[1]&0x0f)<<8 | int(sec[2]))
	if total > MaxSectionBytes {
		a.sec = a.sec[:0]
		return nil
	}
	if len(sec) < total {
		return nil // continues in the next packet
	}
	a.sec = a.sec[:0] // the bytes stay readable until the next Feed overwrites them
	return sec[:total]
}

// Reset drops any partial section, for a caller that has lost its place in the
// stream (a source change, a ring flush).
func (a *SectionAssembler) Reset() { a.sec = a.sec[:0] }

// ParsePMTSection reads a COMPLETE PMT section, as returned by a
// SectionAssembler. ok is false when it is not a PMT or names no video.
func ParsePMTSection(sec []byte) (PMT, bool) {
	var out PMT
	if len(sec) < 12 || sec[0] != 0x02 {
		return out, false
	}
	out.PCRPID = uint16(sec[8]&0x1f)<<8 | uint16(sec[9])
	progInfoLen := int(sec[10]&0x0f)<<8 | int(sec[11])
	for i, end := 12+progInfoLen, esEnd(sec); i+5 <= end; {
		st := sec[i]
		pid := uint16(sec[i+1]&0x1f)<<8 | uint16(sec[i+2])
		if out.VideoPID == 0 && isVideo(st) {
			out.VideoPID, out.VideoType = pid, st
		}
		i += 5 + (int(sec[i+3]&0x0f)<<8 | int(sec[i+4]))
	}
	if out.VideoPID == 0 {
		return out, false
	}
	if out.PCRPID == 0x1fff || out.PCRPID == 0 {
		out.PCRPID = out.VideoPID
	}
	return out, true
}

// PMTStreamsSection lists the elementary streams a COMPLETE PMT section
// declares, in table order. ok is false when it is not a PMT or declares none.
func PMTStreamsSection(sec []byte) ([]ES, bool) {
	if len(sec) < 12 || sec[0] != 0x02 {
		return nil, false
	}
	progInfoLen := int(sec[10]&0x0f)<<8 | int(sec[11])
	var out []ES
	for i, end := 12+progInfoLen, esEnd(sec); i+5 <= end; {
		out = append(out, ES{PID: uint16(sec[i+1]&0x1f)<<8 | uint16(sec[i+2]), Type: sec[i]})
		i += 5 + (int(sec[i+3]&0x0f)<<8 | int(sec[i+4]))
	}
	return out, len(out) > 0
}

// esEnd is where a section's elementary-stream loop stops: at the CRC, or at
// the bytes actually present when the section is shorter than it claims.
func esEnd(sec []byte) int {
	end := 3 + (int(sec[1]&0x0f)<<8 | int(sec[2])) - 4
	if end > len(sec)-4 {
		end = len(sec) - 4
	}
	return end
}
