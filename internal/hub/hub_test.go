// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hub

import (
	"bytes"
	"testing"
)

func mkPkt(marker byte) []byte {
	p := make([]byte, 188)
	p[0] = 0x47
	p[3] = marker
	return p
}

// TestSnapshotReturnsCleanEntry: Snapshot yields the current join snapshot
// (PAT/PMT + recent ring bytes) as a fresh copy, for re-seeding a decoder
// (the overlay ffmpeg) without disturbing the ring.
func TestSnapshotReturnsCleanEntry(t *testing.T) {
	h := New(1<<20, 0)
	h.Publish(mkPkt(1))

	snap := h.Snapshot(0)
	if len(snap) == 0 || snap[0] != 0x47 {
		t.Fatalf("Snapshot must return TS-aligned bytes, got %d bytes", len(snap))
	}
}

// TestPublishFoldsIntoRingIndependently: Publish folds the chunk in place (no
// per-chunk copy), but the ring must still hold an INDEPENDENT copy — mutating
// the caller's buffer after Publish must not corrupt the buffered data. This pins
// the safety of the no-copy publish path (the whole point of ADR 0004's Publish).
func TestPublishFoldsIntoRingIndependently(t *testing.T) {
	h := New(1<<20, 0)
	chunk := mkPkt(9) // p[0]=0x47, p[3]=9
	h.Publish(chunk)  // folded in place

	// Overwrite the caller's buffer; the ring must be unaffected.
	for i := range chunk {
		chunk[i] = 0xEE
	}
	snap := h.Snapshot(0)
	if len(snap) != 188 || snap[0] != 0x47 || snap[3] != 9 {
		t.Fatalf("ring must hold an independent copy; got len=%d byte[0]=%#x byte[3]=%d", len(snap), snap[0], snap[3])
	}
}

// TestBurstIsTheRingNotACopy: a Follow burst is the ring's own bytes (no
// per-viewer copy of the history), it reads back correctly through Bytes()/Len(),
// a second read from the same cursor yields the same bytes, and Release is safe to
// call more than once — serveLive releases right after the write and again,
// deferred, on every exit path.
func TestBurstIsTheRingNotACopy(t *testing.T) {
	h := New(1<<20, 0)
	_, cur := h.Join(0) // empty ring: cursor at the edge
	h.Publish(mkPkt(7))

	b1, _, _, _, behind, ended := h.Follow(cur, 1<<20)
	if behind || ended {
		t.Fatalf("Follow: behind=%v ended=%v, want both false", behind, ended)
	}
	want := b1.Bytes()
	if len(want) == 0 || want[0] != 0x47 {
		t.Fatalf("Follow burst must be TS-aligned, got %d bytes", len(want))
	}
	if b1.Len() != len(want) {
		t.Fatalf("Len() = %d, Bytes() gave %d", b1.Len(), len(want))
	}
	b1.Release()
	b1.Release() // idempotent

	b2, _, _, _, _, _ := h.Follow(cur, 1<<20)
	if !bytes.Equal(b2.Bytes(), want) {
		t.Fatalf("second read from the same cursor differs: got %d bytes, want %d", b2.Len(), len(want))
	}
	b2.Release()
}

// TestCloseAllIdempotentAndPublishSafe: teardown may be requested more than once
// (racing DELETEs), and the producer may still publish for a moment after it
// (the puller notices its cancellation asynchronously). Neither must panic — in
// particular CloseAll must not double-close the wake channel, and Publish after a
// teardown must be a no-op, not a close-of-closed-channel panic.
func TestCloseAllIdempotentAndPublishSafe(t *testing.T) {
	h := New(1<<20, 0)
	h.Publish(mkPkt(1))

	h.CloseAll()
	h.CloseAll() // idempotent: must not panic (no double close of wake)

	// Publishing after a teardown must not panic or block.
	h.Publish([]byte("x")) // sub-packet garbage
	h.Publish(mkPkt(2))    // a valid packet

	// A viewer that tries to follow a torn-down hub is told the stream ended.
	_, cur := h.Join(0)
	if _, _, _, _, _, ended := h.Follow(cur, 1<<20); !ended {
		t.Fatal("Follow on a torn-down hub must report ended")
	}
}
