// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsmeta

import "testing"

// crc32MPEG is the CRC-32/MPEG-2 every PSI section ends with: polynomial
// 0x04c11db7, all-ones seed, no reflection and no final inversion.
func crc32MPEG(b []byte) uint32 {
	crc := uint32(0xffffffff)
	for _, x := range b {
		crc ^= uint32(x) << 24
		for i := 0; i < 8; i++ {
			if crc&0x8000_0000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// psiPacket wraps a section (table_id … last byte before the CRC) the way a
// broadcast multiplexer does: pointer_field, the section, its CRC-32, then 0xFF
// stuffing out to 188 bytes. The zero-padded fixtures elsewhere hide exactly the
// bug these tests are about, because a table walker that runs off the end of the
// section lands on zeroes there and stops.
func psiPacket(pid int, section []byte) []byte {
	p := make([]byte, packetSize)
	for i := range p {
		p[i] = 0xff
	}
	p[0] = 0x47
	p[1] = 0x40 | byte((pid>>8)&0x1f) // payload_unit_start_indicator
	p[2] = byte(pid)
	p[3] = 0x10 // payload only
	p[4] = 0x00 // pointer_field
	copy(p[5:], section)
	crc := crc32MPEG(section)
	n := 5 + len(section)
	p[n], p[n+1], p[n+2], p[n+3] = byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc)
	return p
}

// setSectionLength fills in section_length: the bytes after the length field,
// including the four CRC bytes that follow the section's content.
func setSectionLength(s []byte) []byte {
	n := len(s) - 3 + 4
	s[1] = 0xb0 | byte((n>>8)&0x0f)
	s[2] = byte(n)
	return s
}

// patPacket announces one program. programNumber 0 is the NIT's entry, which is
// not a program at all.
func patPacket(programNumber, pmtPID int) []byte {
	s := []byte{
		0x00,       // table_id: PAT
		0xb0, 0x00, // section_syntax_indicator + section_length
		0x00, 0x01, // transport_stream_id
		0xc1, 0x00, 0x00, // version/current_next, section_number, last_section_number
		byte(programNumber >> 8), byte(programNumber),
		byte(0xe0 | (pmtPID>>8)&0x1f), byte(pmtPID),
	}
	return psiPacket(0, setSectionLength(s))
}

// pmtPacket announces the given {stream_type, PID} elementary streams.
func pmtPacket(pmtPID, pcrPID int, streams ...[2]int) []byte {
	s := []byte{
		0x02,       // table_id: PMT
		0xb0, 0x00, // section_syntax_indicator + section_length
		0x00, 0x01, // program_number
		0xc1, 0x00, 0x00, // version/current_next, section_number, last_section_number
		byte(0xe0 | (pcrPID>>8)&0x1f), byte(pcrPID),
		0xf0, 0x00, // program_info_length = 0
	}
	for _, es := range streams {
		s = append(s, byte(es[0]), byte(0xe0|(es[1]>>8)&0x1f), byte(es[1]), 0xf0, 0x00)
	}
	return psiPacket(pmtPID, setSectionLength(s))
}

// TestPMTStopsAtTheSectionLength: the elementary-stream loop has to end where
// section_length says it does. Running it to the end of the 188-byte packet read
// the section's own CRC-32 as another stream entry, so a video-only channel
// whose checksum happened to open with 0x03, 0x04, 0x0f, 0x11, 0x81 or 0x87 was
// reported as carrying mp2/mp3/aac/aac_latm/ac3/eac3 — and a radio channel whose
// checksum opened with a video type grew a video codec the same way. Info then
// read as Complete, so server/meta.go stopped re-parsing and the panel kept the
// invented codec in streams_servers for the life of the stream.
func TestPMTStopsAtTheSectionLength(t *testing.T) {
	const pmtPID = 0x1000
	for pid := 0x20; pid < 0x1ffe; pid++ {
		if pid == pmtPID {
			continue
		}
		video := append(patPacket(1, pmtPID), pmtPacket(pmtPID, pid, [2]int{0x1b, pid})...)
		if got := Parse(video); got.VideoCodec != "h264" || got.AudioCodec != "" {
			t.Fatalf("video-only PMT on pid %#x: video=%q audio=%q, want h264 and no audio codec at all",
				pid, got.VideoCodec, got.AudioCodec)
		}
		audio := append(patPacket(1, pmtPID), pmtPacket(pmtPID, pid, [2]int{0x0f, pid})...)
		if got := Parse(audio); got.AudioCodec != "aac" || got.VideoCodec != "" {
			t.Fatalf("audio-only PMT on pid %#x: video=%q audio=%q, want aac and no video codec at all",
				pid, got.VideoCodec, got.AudioCodec)
		}
	}
}

// TestPATWithOnlyTheNITNamesNoPMT: the program loop has the same bound. A PAT
// that lists nothing but program 0 names no PMT; walking on to the end of the
// packet read the CRC-32 as a program entry and pointed the whole parse at a PID
// picked by four checksum bytes.
func TestPATWithOnlyTheNITNamesNoPMT(t *testing.T) {
	if got := parsePMTPID(patPacket(0, 0x0010)); got != -1 {
		t.Fatalf("parsePMTPID = %#x for a PAT listing only the NIT, want -1", got)
	}
}

// TestPMTDescriptorsStillRead: bounding the loop must not cost the descriptor
// loop that identifies DVB's private-PES audio, which is how AC-3 is declared on
// most European muxes.
func TestPMTDescriptorsStillRead(t *testing.T) {
	s := []byte{
		0x02, 0xb0, 0x00,
		0x00, 0x01,
		0xc1, 0x00, 0x00,
		0xe1, 0x01, // PCR_PID 0x101
		0xf0, 0x00, // program_info_length = 0
		0x1b, 0xe1, 0x01, 0xf0, 0x00, // H.264 on 0x101
		0x06, 0xe1, 0x02, 0xf0, 0x03, 0x6a, 0x01, 0x00, // private PES + AC-3_descriptor
	}
	ts := append(patPacket(1, 0x1000), psiPacket(0x1000, setSectionLength(s))...)
	got := Parse(ts)
	if got.VideoCodec != "h264" || got.AudioCodec != "ac3" {
		t.Fatalf("Parse = video %q audio %q, want h264/ac3 from the AC-3_descriptor", got.VideoCodec, got.AudioCodec)
	}
}
