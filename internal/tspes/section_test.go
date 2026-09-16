// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tspes

import "testing"

// pmtSectionBytes builds a PMT section declaring one H.264 stream, then
// `extraES` AAC streams, with a program_info descriptor block of descLen bytes.
// Long enough values push the later entries past the first packet's payload.
func pmtSectionBytes(videoPID, audioPID int, descLen, extraES int) []byte {
	body := []byte{
		0x00, 0x01, // program_number
		0xc1,       // version, current_next
		0x00, 0x00, // section_number, last_section_number
		byte(0xe0 | (videoPID>>8)&0x1f), byte(videoPID), // PCR_PID
		byte(0xf0 | (descLen>>8)&0x0f), byte(descLen), // program_info_length
	}
	for i := 0; i < descLen; i++ {
		body = append(body, 0xff) // an opaque descriptor block
	}
	body = append(body, 0x1b, byte(0xe0|(videoPID>>8)&0x1f), byte(videoPID), 0xf0, 0x00)
	for i := 0; i < extraES; i++ {
		pid := audioPID + i
		body = append(body, 0x0f, byte(0xe0|(pid>>8)&0x1f), byte(pid), 0xf0, 0x00)
	}
	body = append(body, 0xde, 0xad, 0xbe, 0xef) // CRC32
	secLen := len(body)
	return append([]byte{0x02, byte(0xb0 | (secLen>>8)&0x0f), byte(secLen)}, body...)
}

// packetize splits a section into transport packets on pid, the first with
// PUSI and a pointer_field, the rest as continuations.
func packetize(pid int, sec []byte, firstCC byte) [][]byte {
	var out [][]byte
	cc := firstCC
	for first := true; len(sec) > 0 || first; first = false {
		p := make([]byte, PacketSize)
		for i := range p {
			p[i] = 0xff // stuffing
		}
		p[0] = 0x47
		p[1] = byte((pid >> 8) & 0x1f)
		if first {
			p[1] |= 0x40 // PUSI
		}
		p[2] = byte(pid)
		p[3] = 0x10 | cc // payload only
		cc = (cc + 1) & 0x0f
		off := 4
		if first {
			p[4] = 0x00 // pointer_field
			off = 5
		}
		n := copy(p[off:], sec)
		sec = sec[n:]
		out = append(out, p)
	}
	return out
}

func TestSectionAssemblerFoldsAMultiPacketPMT(t *testing.T) {
	// A descriptor block that pushes the audio entry into the second packet.
	sec := pmtSectionBytes(0x101, 0x102, 162, 1)
	pkts := packetize(0x100, sec, 3)
	if len(pkts) < 2 {
		t.Fatalf("fixture fits in one packet (%d); the test proves nothing", len(pkts))
	}

	var a SectionAssembler
	if got := a.Feed(pkts[0]); got != nil {
		t.Fatalf("the first packet of a two-packet section returned a section (%d bytes)", len(got))
	}
	got := a.Feed(pkts[1])
	if got == nil {
		t.Fatal("the completing packet returned no section")
	}
	es, ok := PMTStreamsSection(got)
	if !ok || len(es) != 2 {
		t.Fatalf("PMTStreamsSection = %v, ok=%v; want both streams", es, ok)
	}
	if es[0].PID != 0x101 || es[1].PID != 0x102 || es[1].Type != 0x0f {
		t.Errorf("streams = %+v, want video 0x101 then AAC 0x102", es)
	}
	m, ok := ParsePMTSection(got)
	if !ok || m.VideoPID != 0x101 || m.PCRPID != 0x101 {
		t.Errorf("ParsePMTSection = %+v ok=%v, want video and PCR on 0x101", m, ok)
	}
}

func TestSectionAssemblerSinglePacketSection(t *testing.T) {
	sec := pmtSectionBytes(0x101, 0x102, 0, 1)
	pkts := packetize(0x100, sec, 0)
	if len(pkts) != 1 {
		t.Fatalf("fixture needs %d packets, want 1", len(pkts))
	}
	var a SectionAssembler
	got := a.Feed(pkts[0])
	if got == nil {
		t.Fatal("a section complete in one packet returned nil")
	}
	if es, ok := PMTStreamsSection(got); !ok || len(es) != 2 {
		t.Errorf("streams = %v ok=%v, want 2", es, ok)
	}
}

// A lost packet must discard the partial section rather than splice two halves
// of different tables together.
func TestSectionAssemblerDropsASectionWithAGap(t *testing.T) {
	sec := pmtSectionBytes(0x101, 0x102, 162, 1)
	pkts := packetize(0x100, sec, 0)
	if len(pkts) < 2 {
		t.Fatal("fixture fits in one packet")
	}
	gapped := append([]byte(nil), pkts[1]...)
	gapped[3] = 0x10 | ((pkts[1][3] + 1) & 0x0f) // as if one packet went missing

	var a SectionAssembler
	a.Feed(pkts[0])
	if got := a.Feed(gapped); got != nil {
		t.Error("a section with a continuity gap was returned as complete")
	}
	// And it recovers on the next whole copy of the table.
	a2 := packetize(0x100, sec, 7)
	a.Feed(a2[0])
	if got := a.Feed(a2[1]); got == nil {
		t.Error("the assembler did not recover on the next copy of the table")
	}
}

func TestSectionAssemblerIgnoresADuplicatePacket(t *testing.T) {
	sec := pmtSectionBytes(0x101, 0x102, 162, 1)
	pkts := packetize(0x100, sec, 0)
	if len(pkts) < 2 {
		t.Fatal("fixture fits in one packet")
	}
	var a SectionAssembler
	a.Feed(pkts[0])
	if got := a.Feed(pkts[0]); got != nil {
		t.Error("a duplicate of the PUSI packet completed a section")
	}
	if got := a.Feed(pkts[1]); got == nil {
		t.Error("the section did not complete after a duplicate was ignored")
	}
}

// A section whose declared length is absurd must not be buffered for ever.
func TestSectionAssemblerRefusesAnOversizedSection(t *testing.T) {
	sec := pmtSectionBytes(0x101, 0x102, 0, 1)
	sec[1], sec[2] = 0xbf, 0xff // section_length = 4095, past the PSI maximum
	pkts := packetize(0x100, sec, 0)
	var a SectionAssembler
	if got := a.Feed(pkts[0]); got != nil {
		t.Error("an oversized section was returned")
	}
	if len(a.sec) != 0 {
		t.Errorf("an oversized section left %d bytes buffered", len(a.sec))
	}
}

// Joining a stream mid-section: continuations with no PUSI seen yet are not a
// section, and the next complete table is picked up normally.
func TestSectionAssemblerWaitsForAStart(t *testing.T) {
	sec := pmtSectionBytes(0x101, 0x102, 162, 1)
	pkts := packetize(0x100, sec, 0)
	var a SectionAssembler
	if got := a.Feed(pkts[len(pkts)-1]); got != nil {
		t.Error("a continuation packet with no start was read as a section")
	}
	a.Feed(pkts[0])
	if got := a.Feed(pkts[1]); got == nil {
		t.Error("the assembler did not pick up the next whole table")
	}
}
