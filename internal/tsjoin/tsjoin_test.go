// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

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

	s := New(10*1024*1024, 0)
	// Pre-keyframe PAT/PMT land in the GOP but must be wiped by the keyframe reset.
	chunk := concat(pat, pmt, key, non)
	s.Update(chunk)

	snap := s.Snapshot(0)
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
	s := New(10*1024*1024, 0)
	non := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})
	key := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x30, 4: 0x07, 5: 0x40})
	// Many non-keyframe packets, then a keyframe: GOP should shrink to just the keyframe.
	var chunk []byte
	for i := 0; i < 50; i++ {
		chunk = append(chunk, non...)
	}
	chunk = append(chunk, key...)
	s.Update(chunk)
	if got := len(s.Snapshot(0)); got != PacketSize {
		t.Fatalf("after keyframe reset snapshot = %d bytes, want %d", got, PacketSize)
	}
}

// keyPCR builds a keyframe packet (PID 0x101, AFC=3) carrying the given PCR base
// (90 kHz ticks) with the random_access_indicator and PCR_flag set — the inverse
// of readPCR.
func keyPCR(pcr90 int64) []byte {
	return pkt(map[int]byte{
		1:  0x01, // PID hi
		2:  0x01, // PID lo → 0x101 (video)
		3:  0x30, // AFC = 3 (adaptation + payload)
		4:  0x07, // adaptation_field_length (room for the 6 PCR bytes)
		5:  0x50, // random_access_indicator (0x40) + PCR_flag (0x10)
		6:  byte(pcr90 >> 25),
		7:  byte(pcr90 >> 17),
		8:  byte(pcr90 >> 9),
		9:  byte(pcr90 >> 1),
		10: byte((pcr90 & 1) << 7),
	})
}

func TestSnapshotPrebufferRewindsByPCR(t *testing.T) {
	const tick = 90000        // 1s in 90 kHz PCR ticks
	s := New(10*1024*1024, 10_000) // retain 10s of history
	filler := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})

	// 6 keyframe GOPs 2s apart (PCR 0,2,4,6,8,10s); each GOP is key + one filler.
	for i := 0; i < 6; i++ {
		s.Update(keyPCR(int64(i) * 2 * tick))
		s.Update(filler)
	}

	// No prebuffer → current GOP only (key + filler = 2 packets).
	if got := len(s.Snapshot(0)); got != 2*PacketSize {
		t.Fatalf("Snapshot(0) = %d bytes, want %d", got, 2*PacketSize)
	}
	// 4s prebuffer → the 3 newest GOPs (PCR 6,8,10s span 4s) = 6 packets.
	if got := len(s.Snapshot(4000)); got != 6*PacketSize {
		t.Fatalf("Snapshot(4000ms) = %d bytes, want %d", got, 6*PacketSize)
	}
	// More than the ring holds → clamped to all 6 GOPs = 12 packets.
	if got := len(s.Snapshot(60000)); got != 12*PacketSize {
		t.Fatalf("Snapshot(60000ms) = %d bytes, want %d", got, 12*PacketSize)
	}
}

func TestPruneDropsHistoryBeyondRing(t *testing.T) {
	const tick = 90000
	s := New(10*1024*1024, 6_000) // retain 6s
	filler := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})

	// 8 GOPs 2s apart (PCR 0..14s); a 6s ring keeps the newest 4 (PCR 8,10,12,14).
	for i := 0; i < 8; i++ {
		s.Update(keyPCR(int64(i) * 2 * tick))
		s.Update(filler)
	}
	if got := len(s.Snapshot(60000)); got != 8*PacketSize {
		t.Fatalf("pruned ring snapshot = %d bytes, want %d (4 GOPs)", got, 8*PacketSize)
	}
}

// TestSnapshotIntoMatchesAndReuses checks the pooled-buffer path: SnapshotInto
// yields byte-identical output to Snapshot, reuses a supplied buffer's backing
// array when it is large enough (no allocation), and grows a too-small one.
func TestSnapshotIntoMatchesAndReuses(t *testing.T) {
	const tick = 90000
	s := New(10*1024*1024, 10_000)
	filler := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})
	for i := 0; i < 6; i++ {
		s.Update(keyPCR(int64(i) * 2 * tick))
		s.Update(filler)
	}

	want := s.Snapshot(4000)

	// Nil dst behaves exactly like Snapshot.
	if got := s.SnapshotInto(nil, 4000); string(got) != string(want) {
		t.Fatalf("SnapshotInto(nil) mismatch: got %d bytes, want %d", len(got), len(want))
	}

	// A buffer with ample capacity is reused in place (same backing array).
	buf := make([]byte, 0, len(want)+512)
	got := s.SnapshotInto(buf, 4000)
	if string(got) != string(want) {
		t.Fatalf("SnapshotInto(buf) content mismatch: got %d bytes, want %d", len(got), len(want))
	}
	if &got[:cap(got)][0] != &buf[:cap(buf)][0] {
		t.Fatalf("SnapshotInto did not reuse the supplied buffer's backing array")
	}

	// A short (but non-nil) buffer still yields correct content (grown as needed).
	if got := s.SnapshotInto(make([]byte, 0, 1), 4000); string(got) != string(want) {
		t.Fatalf("SnapshotInto(short) content mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

// TestPruneRecyclesGopBuffers proves the free-list: a GOP dropped by prune has its
// backing array recycled, and the next opened GOP reuses that same array instead of
// allocating a fresh one (pointer identity). This is what turns the steady-state
// GOP allocate-and-discard into ~zero.
func TestPruneRecyclesGopBuffers(t *testing.T) {
	const tick = 90000
	s := New(10*1024*1024, 2_000) // 2s ring → old GOPs prune quickly
	filler := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})

	// GOPs 2s apart (PCR 0,2,4,6,8s); the 2s ring drops the older ones, feeding the
	// free list. Each GOP is key + filler so its buffer has real capacity.
	for i := 0; i < 5; i++ {
		s.Update(keyPCR(int64(i) * 2 * tick))
		s.Update(filler)
	}
	if len(s.freeBufs) == 0 {
		t.Fatalf("prune recycled no buffers; free list is empty")
	}

	// getBuf pops the last entry, so that is the array the next GOP will reuse.
	want := s.freeBufs[len(s.freeBufs)-1]
	if cap(want) == 0 {
		t.Fatalf("recycled buffer has zero capacity")
	}
	wantPtr := &want[:1][0]

	// Open one more GOP: its data must land in the recycled backing array.
	s.Update(keyPCR(int64(5) * 2 * tick))
	got := &s.gops[len(s.gops)-1].data[0]
	if got != wantPtr {
		t.Fatalf("new GOP did not reuse the recycled backing array")
	}
}

// TestUpdateSteadyStateAllocations guards the whole point of the refactor: once the
// free list is warm, an Update that opens a GOP and appends a packet allocates no
// GOP data arrays. Inputs are pre-built so the measurement sees only Update's own
// allocations (the per-GOP data churn — the sawtooth — which must be gone).
func TestUpdateSteadyStateAllocations(t *testing.T) {
	const tick = 90000
	const runs = 300
	s := New(10*1024*1024, 2_000)
	filler := pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10})

	// Pre-build all keyframe packets (advancing PCR) so building them doesn't count
	// against Update's allocation budget.
	keys := make([][]byte, 40+runs+2)
	for i := range keys {
		keys[i] = keyPCR(int64(i) * 2 * tick)
	}

	// Warm up: reach steady state so recycled buffers have grown to GOP size.
	for i := 0; i < 40; i++ {
		s.Update(keys[i])
		s.Update(filler)
	}

	i := 40
	avg := testing.AllocsPerRun(runs, func() {
		s.Update(keys[i])
		s.Update(filler)
		i++
	})
	if avg > 0.1 {
		t.Fatalf("steady-state Update allocates %.2f objs/op; recycling should keep it ~0", avg)
	}
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
