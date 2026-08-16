package tsjoin

import "testing"

// pkt returns a 188-byte packet with the given header bytes applied.
func pkt(set map[int]byte) []byte {
	p := make([]byte, PacketSize)
	p[0] = 0x47
	for i, b := range set {
		p[i] = b
	}
	return p
}

func patPacket(pmtPID int) []byte {
	p := pkt(map[int]byte{
		1:  0x40, // PUSI, PID hi = 0
		2:  0x00, // PID lo = 0
		3:  0x10, // payload only
		4:  0x00, // pointer_field
		5:  0x00, // table_id (PAT)
		6:  0xb0, // section syntax + length hi
		7:  0x0d, // section_length
		8:  0x00, // tsid hi
		9:  0x01, // tsid lo
		10: 0xc1, // version/current-next
		11: 0x00, // section_number
		12: 0x00, // last_section_number
		13: 0x00, // program_number hi
		14: 0x01, // program_number lo (=1)
		15: byte(0xe0 | (pmtPID>>8)&0x1f),
		16: byte(pmtPID & 0xff),
	})
	return p
}

func TestSnapshotPicksPatPmtAndGopFromKeyframe(t *testing.T) {
	const pmtPID = 0x100
	pat := patPacket(pmtPID)
	pmt := pkt(map[int]byte{1: byte(pmtPID >> 8), 2: byte(pmtPID & 0xff), 3: 0x10})
	key := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x30, 4: 0x07, 5: 0x40}) // AFC=3, adaptLen=7, RAI set
	non := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})                    // payload only, no RAI

	s := New(10 * 1024 * 1024)
	// Pre-keyframe PAT/PMT land in the GOP but must be wiped by the keyframe reset.
	chunk := concat(pat, pmt, key, non)
	s.Update(chunk)

	snap := s.Snapshot()
	want := PacketSize * 4 // PAT + PMT + (keyframe + non-keyframe GOP)
	if len(snap) != want {
		t.Fatalf("snapshot len = %d, want %d", len(snap), want)
	}
	if snap[0] != 0x47 || snap[1] != 0x40 || snap[2] != 0x00 {
		t.Fatalf("snapshot must start with the PAT packet")
	}
	// GOP portion (after PAT+PMT) must start at the keyframe packet.
	gop := snap[2*PacketSize:]
	if gop[3] != 0x30 || gop[5] != 0x40 {
		t.Fatalf("GOP must start at the keyframe packet")
	}
}

func TestKeyframeResetsGop(t *testing.T) {
	s := New(10 * 1024 * 1024)
	non := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})
	key := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x30, 4: 0x07, 5: 0x40})
	// Many non-keyframe packets, then a keyframe: GOP should shrink to just the keyframe.
	var chunk []byte
	for i := 0; i < 50; i++ {
		chunk = append(chunk, non...)
	}
	chunk = append(chunk, key...)
	s.Update(chunk)
	if got := len(s.Snapshot()); got != PacketSize {
		t.Fatalf("after keyframe reset snapshot = %d bytes, want %d", got, PacketSize)
	}
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
