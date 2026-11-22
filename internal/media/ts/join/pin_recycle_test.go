// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"bytes"
	"runtime"
	"testing"
)

// feedGOP publishes one ~1.2 MB block at pcr (a keyframe, then 100 chunks of
// 64 packets carrying fill): a 2 s GOP of a ~4.8 Mbit/s channel.
func feedGOP(s *State, pcr int64, fill byte) {
	s.Update(keyPCR(pcr))
	chunk := bytes.Repeat(pkt(map[int]byte{1: 0x01, 2: 0x01, 3: 0x10, 4: fill}), 64)
	for k := 0; k < 100; k++ {
		s.Update(chunk)
	}
}

// TestAHeldReadDoesNotStopTheRingRecycling: every live viewer pins the blocks
// it is writing (ADR 0004), and a viewer stuck in a write holds its pin until
// the write deadline. With one pin count for the whole ring, that suspended
// recycling for the stream: each GOP opened meanwhile grew a new array from
// nothing — 5.4 bytes allocated per byte published, where recycling costs none.
// Only the held block may be kept from recycling.
func TestAHeldReadDoesNotStopTheRingRecycling(t *testing.T) {
	const tick = 90000
	s := New(16<<20, 40_000)
	for i := 0; i < 30; i++ {
		feedGOP(s, int64(i)*2*tick, 0x11)
	}
	_, c := s.JoinStart(nil, 0)
	parts, _, _, _, pin := s.ReadFrom(c, 1<<30) // a reader stuck mid-write
	if len(parts) == 0 {
		t.Fatal("nothing to hold")
	}

	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	for i := 30; i < 70; i++ {
		feedGOP(s, int64(i)*2*tick, 0x11)
	}
	runtime.ReadMemStats(&m1)
	s.Unpin(pin)

	published := 40 * (PacketSize + 100*64*PacketSize)
	perByte := float64(m1.TotalAlloc-m0.TotalAlloc) / float64(published)
	// ~0.15 remains: the held block, once pruned, goes to GC, and one new GOP
	// array is grown in its place.
	if perByte > 0.5 {
		t.Fatalf("%.2f bytes allocated per byte published while one reader held a block (want ~0: the ring recycles the rest)", perByte)
	}
}

// TestAHeldBlockIsNotRewrittenUnderItsReader: the reason pins exist. A block a
// reader holds may leave the ring, but its array must not be handed to a new
// GOP until the reader lets go — and after that, recycling resumes.
func TestAHeldBlockIsNotRewrittenUnderItsReader(t *testing.T) {
	const tick = 90000
	s := New(16<<20, 2_000) // a 2 s ring: each new GOP prunes the one before
	feedGOP(s, 0, 0x11)
	_, c := s.JoinStart(nil, 0)
	parts, _, _, _, pin := s.ReadFrom(c, 1<<30)
	held := append([]byte(nil), parts[0]...)

	for i := 1; i < 8; i++ {
		feedGOP(s, int64(i)*2*tick, 0xEE) // different bytes: a reused array would show them
	}
	if !bytes.Equal(parts[0], held) {
		t.Fatal("a block the reader still held was recycled into a new GOP and rewritten")
	}

	s.Unpin(pin)
	if len(s.pinned) != 0 {
		t.Fatalf("pins left after Unpin: %v", s.pinned)
	}
	free := len(s.freeBufs)
	feedGOP(s, 8*2*tick, 0xEE)
	feedGOP(s, 9*2*tick, 0xEE)
	if len(s.freeBufs) == 0 && free == 0 {
		t.Fatal("recycling did not resume after the reader let go")
	}
}
