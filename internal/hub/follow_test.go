// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hub

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// drainToEdge follows the ring from cur, concatenating every run, until Follow
// reports the live edge. It fails the test on behind/ended and returns the bytes
// read, the cursor at the edge, and the wake channel to park on.
func drainToEdge(t *testing.T, h *Hub, cur tsjoin.Cursor) ([]byte, tsjoin.Cursor, <-chan struct{}) {
	t.Helper()
	var got []byte
	for {
		b, next, atEnd, wake, behind, ended := h.Follow(cur, 1<<20)
		if behind {
			t.Fatalf("Follow reported behind while draining to the edge")
		}
		if ended {
			t.Fatalf("Follow reported ended while draining to the edge")
		}
		for _, p := range b.Parts {
			got = append(got, p...)
		}
		b.Release()
		cur = next
		if atEnd {
			return got, cur, wake
		}
	}
}

// TestFollowReadsRingForwardThenParks: a follower reads the ring forward run by
// run to the live edge (no gap, no duplication — the bytes read back equal the
// bytes published, in order), then parks on wake until the next Publish delivers
// the new bytes. The whole ADR 0004 pull loop in miniature.
func TestFollowReadsRingForwardThenParks(t *testing.T) {
	h := New(1<<20, 40*1000) // 40s prebuffer so nothing prunes during the test

	// Capture the cursor at the (empty-ring) edge BEFORE publishing, so Follow
	// then reads exactly the packets we append. The fixtures carry no PCR, so Join
	// cannot walk back a history depth — it starts at the edge either way.
	_, cur := h.Join(0)

	// Track every published packet; Follow must read the identical stream back.
	// PAT+PMT ahead of the first keyframe form a pre-roll block, which is
	// legitimately part of the ring history a follower receives.
	var published []byte
	pub := func(p []byte) { published = append(published, p...); h.Publish(p) }
	pub(tsfixture.PAT(0x100))
	pub(tsfixture.PMT(0x100, 0x101))
	for i := 0; i < 3; i++ {
		pub(tsfixture.Keyframe(0x101, int64(i)*90000))
	}

	got, cur, wake := drainToEdge(t, h, cur)
	if !bytes.Equal(got, published) {
		t.Fatalf("ring read back %d bytes, want the %d published (in order, no gap/dup)", len(got), len(published))
	}
	if wake == nil {
		t.Fatal("Follow at the edge must return a non-nil wake channel to park on")
	}

	// Parked at the edge: a Publish must close the wake channel we hold.
	next := tsfixture.Keyframe(0x101, 3*90000)
	go h.Publish(next)
	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("wake did not fire after a Publish while parked at the edge")
	}

	// And the woken follower reads exactly the one new packet, continuing from cur
	// with no gap and no duplication.
	b, _, _, _, behind, ended := h.Follow(cur, 1<<20)
	if behind || ended {
		t.Fatalf("Follow after wake: behind=%v ended=%v, want both false", behind, ended)
	}
	tail := bytes.Join(b.Parts, nil)
	b.Release()
	if !bytes.Equal(tail, next) {
		t.Fatalf("after wake read %d bytes, want the one new keyframe (%d) with no duplication", len(tail), len(next))
	}
}

// TestFollowBehindWhenCursorPrunedOut: a cursor whose GOP has aged off the ring
// tail (a reader slower than the stream) is reported behind — the pull-path
// equivalent of the old "slow subscriber dropped".
func TestFollowBehindWhenCursorPrunedOut(t *testing.T) {
	h := New(1<<20, 0) // prebuffer 0: the ring keeps only the current GOP

	_, cur := h.Join(0) // cursor at the edge, GOP id 0 (the first keyframe to come)
	h.Publish(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))
	h.Publish(tsfixture.Keyframe(0x101, 0))       // GOP 0
	h.Publish(tsfixture.Keyframe(0x101, 1*90000)) // GOP 1 → GOP 0 pruned (prebuffer 0)

	_, _, _, _, behind, ended := h.Follow(cur, 1<<20)
	if ended {
		t.Fatal("Follow reported ended, want behind")
	}
	if !behind {
		t.Fatal("Follow must report behind when the cursor's GOP has been pruned out")
	}
}

// TestFollowEndedOnCloseAll: teardown wakes a parked follower and Follow then
// reports ended, so serveLive unwinds instead of blocking on a dead hub — the
// pull-path teardown.
func TestFollowEndedOnCloseAll(t *testing.T) {
	h := New(1<<20, 40*1000)
	_, cur := h.Join(0)
	h.Publish(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))
	h.Publish(tsfixture.Keyframe(0x101, 0))

	_, cur, wake := drainToEdge(t, h, cur)
	if wake == nil {
		t.Fatal("expected a wake channel at the edge")
	}

	h.CloseAll()
	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("CloseAll must wake a parked follower")
	}
	if _, _, _, _, _, ended := h.Follow(cur, 1<<20); !ended {
		t.Fatal("Follow after CloseAll must report ended")
	}
}

// TestFollowConcurrentReadersSeeContiguousStream: many followers reading in
// parallel with a live producer each see the same, gap-free, duplication-free
// byte stream. Run under -race, this also guards the wake/lock discipline.
func TestFollowConcurrentReadersSeeContiguousStream(t *testing.T) {
	h := New(1<<20, 40*1000)

	// One cursor start shared by every reader, captured before any publish, so
	// every reader receives the entire stream from the beginning of the ring.
	_, start := h.Join(0)

	const nGOP = 200
	const nReaders = 8

	// The exact byte stream the producer will emit (pre-roll PAT+PMT then GOPs).
	var published []byte
	published = append(published, tsfixture.PAT(0x100)...)
	published = append(published, tsfixture.PMT(0x100, 0x101)...)
	for i := 0; i < nGOP; i++ {
		published = append(published, tsfixture.Keyframe(0x101, int64(i)*90000)...)
	}
	target := len(published)

	var wg sync.WaitGroup
	results := make([][]byte, nReaders)
	for r := 0; r < nReaders; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			cur := start
			var got []byte
			deadline := time.After(5 * time.Second)
			for len(got) < target {
				b, next, atEnd, wake, behind, ended := h.Follow(cur, 4096)
				if behind || ended {
					return // recorded as a short read → asserted below
				}
				for _, p := range b.Parts {
					got = append(got, p...)
				}
				b.Release()
				cur = next
				if atEnd {
					select {
					case <-wake:
					case <-deadline:
						results[r] = got
						return
					}
				}
			}
			results[r] = got
		}(r)
	}

	h.Publish(tsfixture.PAT(0x100))
	h.Publish(tsfixture.PMT(0x100, 0x101))
	for i := 0; i < nGOP; i++ {
		h.Publish(tsfixture.Keyframe(0x101, int64(i)*90000))
	}
	wg.Wait()

	for r := 0; r < nReaders; r++ {
		if !bytes.Equal(results[r], published) {
			t.Fatalf("reader %d got %d bytes, want the %d published (a gap, a drop, or a slow reader)", r, len(results[r]), target)
		}
	}
}
