// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// radioPMT builds an audio-only PMT (one AAC elementary stream) on pmtPID, with
// the four CRC32 bytes the caller chooses and 0xff stuffing after the section —
// what a muxer writes. A real table's CRC is fixed for as long as the table is,
// so a channel whose CRC happens to read as an ES entry stays affected across
// restarts.
func radioPMT(pmtPID, audioPID int, crc [4]byte) []byte {
	p := make([]byte, PacketSize)
	p[0] = 0x47
	p[1] = byte((pmtPID>>8)&0x1f) | 0x40 // PUSI
	p[2] = byte(pmtPID)
	p[3] = 0x10 // payload only
	p[4] = 0x00 // pointer_field
	sec := p[5:]
	sec[0] = 0x02                                                  // table_id (PMT)
	sec[1], sec[2] = 0xb0, 0x12                                    // section_length = 18: sec[3]..the CRC
	sec[3], sec[4] = 0x00, 0x01                                    // program_number
	sec[5] = 0xc1                                                  // version, current_next
	sec[6], sec[7] = 0x00, 0x00                                    // section_number, last_section_number
	sec[8], sec[9] = byte(0xe0|(audioPID>>8)&0x1f), byte(audioPID) // PCR_PID
	sec[10], sec[11] = 0xf0, 0x00                                  // program_info_length = 0
	sec[12] = 0x0f                                                 // stream_type: AAC
	sec[13], sec[14] = byte(0xe0|(audioPID>>8)&0x1f), byte(audioPID)
	sec[15], sec[16] = 0xf0, 0x00 // ES_info_length = 0
	copy(sec[17:21], crc[:])
	for i := 21; i < len(sec); i++ {
		sec[i] = 0xff // stuffing
	}
	return p
}

// audioRAP is one audio packet flagged as a random-access point — which is what
// ffmpeg's muxer sets on every audio PES, since every audio frame is one.
func audioRAP(pid int) []byte {
	return pkt(map[int]byte{
		1: byte((pid >> 8) & 0x1f),
		2: byte(pid),
		3: 0x30, // adaptation + payload
		4: 0x01, // adaptation_field_length
		5: 0x40, // random_access_indicator
	})
}

// TestPMTParsingStopsAtTheSectionsEnd: the ES loop ran to the end of the
// 188-byte PACKET rather than to the end of the SECTION, so on an audio-only
// PMT it stepped straight onto the CRC32 and read it as another ES entry. About
// six CRC values in 256 begin with a video stream_type.
func TestPMTParsingStopsAtTheSectionsEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		crc  [4]byte
	}{
		{"a CRC that reads as an H.264 entry", [4]byte{0x1b, 0x3c, 0x55, 0x99}},
		{"a CRC that reads as an MPEG-2 entry", [4]byte{0x02, 0x40, 0x11, 0x7a}},
		{"an ordinary CRC", [4]byte{0xb6, 0x1f, 0x2c, 0x08}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Read it the way Update does: fold the section, then decide.
			s := New(1<<20, 0)
			s.Update(tsfixture.Concat(tsfixture.PAT(0x100), radioPMT(0x100, 0x101, tc.crc)))
			if s.videoPID >= 0 {
				t.Errorf("an audio-only PMT yielded videoPID=%#x type=%#x, want none", s.videoPID, s.videoType)
			}
			if s.audioPID != 0x101 {
				t.Errorf("audioPID = %#x, want 0x101", s.audioPID)
			}
		})
	}
}

// TestARadioStreamKeepsCuttingBlocks: what adopting a CRC as the video PID costs
// a listener. With a video PID nothing ever arrives on, `rap && (onVideo ||
// videoPID < 0)` is never true, so blocks are cut only when one reaches maxGOP —
// minutes at radio bitrates. A join then starts at the start of that block, so
// the listener opens minutes behind live and stays there, and every maxGOP cut
// prunes the block the listeners at the edge are still reading.
func TestARadioStreamKeepsCuttingBlocks(t *testing.T) {
	const blocks = 10
	s := New(10<<20, 40_000)
	s.Update(tsfixture.PAT(0x100))
	s.Update(radioPMT(0x100, 0x101, [4]byte{0x1b, 0x3c, 0x55, 0x99}))
	for i := 0; i < blocks; i++ {
		s.Update(audioRAP(0x101))
		for k := 0; k < 20; k++ {
			s.Update(tsfixture.Fill(0x101))
		}
	}

	if n := len(s.gops); n < blocks {
		t.Fatalf("%d blocks cut from %d audio random-access points: the stream is being read as one endless block", n, blocks)
	}
	_, c := s.JoinStart(nil, 0)
	if c.GOP == s.gops[0].id {
		t.Fatalf("a no-prebuffer join starts at the ring's oldest block (%+v): the listener opens the whole retained block behind live", c)
	}
}

// TestAPMTIsReadOnlyFromThePacketThatStartsIt: a PMT section too long for one
// packet continues in the next, which carries no payload_unit_start_indicator.
// Those continuation bytes were parsed as a section of their own — pointer_field,
// ES entries and all — so they could flip videoPID or audioPID, and lastPMT (the
// packet every join burst and every HLS segment starts with) ended up holding a
// packet that is not a table at all.
func TestAPMTIsReadOnlyFromThePacketThatStartsIt(t *testing.T) {
	s := New(10<<20, 40_000)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	if s.videoPID != 0x101 {
		t.Fatalf("videoPID = %#x after the real PMT, want 0x101", s.videoPID)
	}

	// A continuation packet on the PMT PID. Its payload stands in for the rest of
	// a long section; read as a section it declares another video PID.
	cont := tsfixture.PMT(0x100, 0x200)
	cont[1] &^= 0x40 // no PUSI: this packet does not start a section
	s.Update(cont)

	if s.videoPID != 0x101 {
		t.Errorf("a continuation packet moved videoPID to %#x", s.videoPID)
	}
	if len(s.lastPMT) == 0 || s.lastPMT[1]&0x40 == 0 {
		t.Error("lastPMT holds a continuation packet: every join burst and HLS segment starts with it")
	}
}
