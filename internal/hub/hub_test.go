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

// TestFanoutIdentical asserts every subscriber receives the exact same live
// byte stream from a single producer.
func TestFanoutIdentical(t *testing.T) {
	h := New(1<<20, 0)

	const nSubs = 3
	subs := make([]*Sub, nSubs)
	for i := range subs {
		subs[i], _ = h.Subscribe(0)
	}

	const nChunks = 100 // < subQueue, so nothing is dropped
	go func() {
		for i := 0; i < nChunks; i++ {
			h.Publish(mkPkt(byte(i)))
		}
	}()

	outs := make([]*bytes.Buffer, nSubs)
	for i, s := range subs {
		outs[i] = &bytes.Buffer{}
		for j := 0; j < nChunks; j++ {
			outs[i].Write(<-s.C())
		}
	}

	if outs[0].Len() != nChunks*188 {
		t.Fatalf("sub 0 got %d bytes, want %d", outs[0].Len(), nChunks*188)
	}
	for i := 1; i < nSubs; i++ {
		if !bytes.Equal(outs[0].Bytes(), outs[i].Bytes()) {
			t.Fatalf("subscriber %d received a different byte stream than subscriber 0", i)
		}
	}
}

// TestSlowSubscriberDropped asserts a subscriber that never reads is dropped
// once its queue overflows, and does not block the producer.
func TestSlowSubscriberDropped(t *testing.T) {
	h := New(1<<20, 0)
	slow, _ := h.Subscribe(0)

	// Publish well past the queue depth without ever reading `slow`.
	for i := 0; i < subQueue+50; i++ {
		h.Publish(mkPkt(byte(i)))
	}

	select {
	case <-slow.Done():
		// dropped as expected
	default:
		t.Fatalf("slow subscriber should have been dropped")
	}
	if h.Count() != 0 {
		t.Fatalf("hub should have 0 subscribers after dropping the slow one, got %d", h.Count())
	}
}

// TestUnsubscribeRemovesAndClosesDone: Unsubscribe drops the subscriber from the
// fan-out and closes its Done channel; a second call is a harmless no-op.
func TestUnsubscribeRemovesAndClosesDone(t *testing.T) {
	h := New(1<<20, 0)
	sub, _ := h.Subscribe(0)
	if h.Count() != 1 {
		t.Fatalf("Count after Subscribe = %d, want 1", h.Count())
	}

	h.Unsubscribe(sub)
	if h.Count() != 0 {
		t.Fatalf("Count after Unsubscribe = %d, want 0", h.Count())
	}
	select {
	case <-sub.Done():
		// closed as expected
	default:
		t.Fatal("Unsubscribe must close the subscriber's Done channel")
	}

	h.Unsubscribe(sub) // idempotent: must not panic on a double close
}

// TestSnapshotReturnsCleanEntryWithoutSubscribing: Snapshot yields the current
// join snapshot (here PAT+PMT+keyframe) without registering a subscriber.
func TestSnapshotReturnsCleanEntryWithoutSubscribing(t *testing.T) {
	h := New(1<<20, 0)
	h.Publish(mkPkt(1))

	snap := h.Snapshot(0)
	if len(snap) == 0 || snap[0] != 0x47 {
		t.Fatalf("Snapshot must return TS-aligned bytes, got %d bytes", len(snap))
	}
	if h.Count() != 0 {
		t.Fatalf("Snapshot must not subscribe; Count = %d, want 0", h.Count())
	}
}

// TestPublishNoSubsCopiesIntoRing: with no subscribers Publish folds the chunk in
// place (no per-chunk copy), but the ring must still hold an INDEPENDENT copy —
// mutating the caller's buffer after Publish must not corrupt the buffered data.
// This pins the safety of the no-copy fast path.
func TestPublishNoSubsCopiesIntoRing(t *testing.T) {
	h := New(1<<20, 0)
	chunk := mkPkt(9) // p[0]=0x47, p[3]=9
	h.Publish(chunk)  // no subscribers → in-place fold

	// Overwrite the caller's buffer; the ring must be unaffected.
	for i := range chunk {
		chunk[i] = 0xEE
	}
	snap := h.Snapshot(0)
	if len(snap) != 188 || snap[0] != 0x47 || snap[3] != 9 {
		t.Fatalf("ring must hold an independent copy; got len=%d byte[0]=%#x byte[3]=%d", len(snap), snap[0], snap[3])
	}

	// And a subscriber that joins afterwards still receives the buffered content.
	sub, burst := h.Subscribe(0)
	s2 := burst.Bytes()
	if len(s2) != 188 || s2[3] != 9 {
		t.Fatalf("late subscriber join burst wrong; got len=%d byte[3]=%d", len(s2), s2[3])
	}
	burst.Release()
	h.Unsubscribe(sub)
}

// TestSubscribeBurstIsTheRingNotACopy: the join burst is the ring's own bytes
// (no per-viewer copy of the history), it reads back correctly, and Release is
// safe to call more than once — serveLive releases right after the write and
// again, deferred, on every exit path.
func TestSubscribeBurstIsTheRingNotACopy(t *testing.T) {
	h := New(1<<20, 0)
	h.Publish(mkPkt(7))

	sub1, b1 := h.Subscribe(0)
	want := b1.Bytes()
	if len(want) == 0 || want[0] != 0x47 {
		t.Fatalf("Subscribe join burst must be TS-aligned, got %d bytes", len(want))
	}
	if b1.Len() != len(want) {
		t.Fatalf("Len() = %d, Bytes() gave %d", b1.Len(), len(want))
	}
	b1.Release()
	b1.Release() // idempotent
	h.Unsubscribe(sub1)

	sub2, b2 := h.Subscribe(0)
	if !bytes.Equal(b2.Bytes(), want) {
		t.Fatalf("second join burst differs from the first: got %d bytes, want %d", b2.Len(), len(want))
	}
	b2.Release()
	h.Unsubscribe(sub2)
}

// TestCloseAll: a stream teardown must release every subscriber at once, so the
// serveLive goroutines blocked on a hub that will never publish again can exit
// and let the Stream (hub, ring and all) be collected.
func TestCloseAll(t *testing.T) {
	h := New(1<<20, 0)
	subs := make([]*Sub, 5)
	for i := range subs {
		subs[i], _ = h.Subscribe(0)
	}
	if got := h.CloseAll(); got != len(subs) {
		t.Fatalf("CloseAll reported %d subscribers, want %d", got, len(subs))
	}
	for i, s := range subs {
		select {
		case <-s.Done():
		default:
			t.Errorf("subscriber %d not released by CloseAll", i)
		}
	}
	if h.Count() != 0 {
		t.Errorf("hub still holds %d subscriber(s)", h.Count())
	}
	if got := h.CloseAll(); got != 0 { // idempotent
		t.Errorf("second CloseAll reported %d", got)
	}
	// Publishing after a teardown must not panic or block.
	h.Publish([]byte("x"))
}
