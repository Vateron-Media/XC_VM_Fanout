// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hub

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// TestFollowStableWhileProducing is the safety case for reading the ring with the
// hub lock RELEASED. A follower reads GOP buffers straight out of the ring while
// the producer keeps publishing — opening GOPs, pruning old ones and recycling
// their arrays for reuse. If a run's pin failed to hold off that recycling, a
// buffer would be handed to a new GOP and rewritten mid-write, and the viewer
// would receive a splice of two different points in the stream.
//
// Every fill packet carries the generation of the GOP that produced it, and GOPs
// sit in the ring oldest→newest, so across a follow consecutive generations may
// only stay the same or step up by one. A buffer recycled underneath the read
// splices in some unrelated generation, which shows up as a large jump in either
// direction. The comparison is done in uint16 arithmetic so the counter wrapping
// (a long run does hundreds of thousands of joins) reads as the step of 1 it
// really is. Run with -race for the other half of the proof.
// maxGenStep is the largest generation step legitimately possible between two
// consecutive fill packets of one follow: they are either the same GOP (0) or
// adjacent ones (1). Anything larger is a splice. Generous, so the check can
// never be tripped by ordinary scheduling.
const maxGenStep = 1000

func TestFollowStableWhileProducing(t *testing.T) {
	// A deliberately small ring: GOPs are pruned (and their buffers recycled)
	// constantly, which is exactly the condition the pin has to survive.
	h := New(1<<20, 300)
	h.Configure(300, 0, 0)
	h.Publish(tsfixture.PAT(0x100))
	h.Publish(tsfixture.PMT(0x100, 0x101))

	var stop atomic.Bool
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		var gen uint16
		var pcr int64
		for !stop.Load() {
			gen++
			pcr += 90 * 30 // 30 ms per GOP, so a 300 ms ring holds ~10
			h.Publish(tsfixture.KeyframePCR(0x101, pcr, pcr))
			blk := make([]byte, 0, 188*40)
			for i := 0; i < 40; i++ {
				blk = append(blk, tsfixture.FillGen(0x101, gen)...)
			}
			h.Publish(blk)
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	joins, checked := 0, 0
	for time.Now().Before(deadline) {
		_, cur := h.Join(300) // ask for the whole ring
		joins++

		// Follow the ring forward to the edge, reading each run IN PLACE as
		// serveLive writes it — straight out of the ring's pinned GOP buffers while
		// the producer keeps publishing. A pause every few packets stands in for a
		// slow viewer's socket, so the pin has to hold for the whole write of a run,
		// not just a quick copy. A slow reader on this tiny ring may be pruned past
		// (behind) mid-catch-up; whatever it did read must still be intact.
		var snap []byte
		i := 0
		for {
			burst, next, atEnd, _, behind, ended := h.Follow(cur, 1<<20)
			if behind || ended {
				burst.Release()
				break
			}
			for _, part := range burst.Parts {
				if i%4 == 3 {
					time.Sleep(50 * time.Microsecond)
				}
				snap = append(snap, part...)
				i++
			}
			burst.Release()
			cur = next
			if atEnd {
				break
			}
		}

		var last uint16
		checked0 := checked
		for off := 0; off+188 <= len(snap); off += 188 {
			p := snap[off : off+188]
			if p[0] != 0x47 {
				t.Fatalf("join %d: packet at %d lost its sync byte (0x%02x) — buffer rewritten mid-write", joins, off, p[0])
			}
			// fill packets only: PID 0x101, no PUSI, payload-only
			pid := int(p[1]&0x1f)<<8 | int(p[2])
			if pid != 0x101 || p[1]&0x40 != 0 || (p[3]>>4)&0x3 != 1 {
				continue
			}
			g := tsfixture.ReadGen(p)
			if step := g - last; checked > checked0 && step > maxGenStep {
				t.Fatalf("join %d: generation jumped at offset %d (%d after %d) — a GOP buffer was recycled mid-write",
					joins, off, g, last)
			}
			last = g
			checked++
		}
	}

	stop.Store(true)
	wg.Wait()
	t.Logf("%d joins, %d packets verified against a live producer", joins, checked)
	if joins < 50 || checked < 1000 {
		t.Fatalf("test did not exercise the path: %d joins, %d packets", joins, checked)
	}
}

// TestFollowNoGapNoDuplication: reading the ring forward by cursor with the lock
// released must not break the contract that the live tail continues exactly where
// the history ends. Everything published before the reader reaches the edge is in
// the history it reads; everything after it is in the live tail it reads next —
// each byte in exactly one of the two, no gap and no duplication at the hand-over.
func TestFollowNoGapNoDuplication(t *testing.T) {
	h := New(1<<20, 10000)
	h.Configure(10000, 0, 0)
	h.Publish(tsfixture.PAT(0x100))
	h.Publish(tsfixture.PMT(0x100, 0x101))

	var gen uint16
	var pcr int64
	publishGOP := func() {
		gen++
		pcr += 90 * 100
		h.Publish(tsfixture.KeyframePCR(0x101, pcr, pcr))
		blk := make([]byte, 0, 188*10)
		for i := 0; i < 10; i++ {
			blk = append(blk, tsfixture.FillGen(0x101, gen)...)
		}
		h.Publish(blk)
	}
	for i := 0; i < 3; i++ {
		publishGOP()
	}

	// Read the whole history to the edge.
	_, cur := h.Join(10000)
	var hist []byte
	hist, cur = readToEdge(t, h, cur, hist)
	if maxHist := lastGen(hist); maxHist != 3 {
		t.Fatalf("history ends at generation %d, want 3 (the last one published before the edge)", maxHist)
	}

	// Everything published from here on must arrive as the live tail, from cur.
	for i := 0; i < 3; i++ {
		publishGOP()
	}
	var tail []byte
	tail, _ = readToEdge(t, h, cur, tail)
	first := firstGen(tail)
	if first != 4 {
		t.Fatalf("live tail starts at generation %d, want 4: %s", first,
			map[bool]string{true: "a gap", false: "duplicated bytes"}[first > 4])
	}
}

// readToEdge follows the ring from cur, appending every run's bytes to dst, until
// it reaches the live edge. Fails on behind/ended (the caller's window is sized so
// neither happens).
func readToEdge(t *testing.T, h *Hub, cur tsjoin.Cursor, dst []byte) ([]byte, tsjoin.Cursor) {
	t.Helper()
	for {
		burst, next, atEnd, _, behind, ended := h.Follow(cur, 1<<24)
		if behind || ended {
			burst.Release()
			t.Fatalf("read did not reach the edge (behind=%v ended=%v)", behind, ended)
		}
		for _, p := range burst.Parts {
			dst = append(dst, p...)
		}
		burst.Release()
		cur = next
		if atEnd {
			return dst, cur
		}
	}
}

func lastGen(b []byte) uint16 {
	var g uint16
	for off := 0; off+188 <= len(b); off += 188 {
		p := b[off : off+188]
		if p[0] == 0x47 && int(p[1]&0x1f)<<8|int(p[2]) == 0x101 && p[1]&0x40 == 0 && (p[3]>>4)&0x3 == 1 {
			g = tsfixture.ReadGen(p)
		}
	}
	return g
}

func firstGen(b []byte) uint16 {
	for off := 0; off+188 <= len(b); off += 188 {
		p := b[off : off+188]
		if p[0] == 0x47 && int(p[1]&0x1f)<<8|int(p[2]) == 0x101 && p[1]&0x40 == 0 && (p[3]>>4)&0x3 == 1 {
			return tsfixture.ReadGen(p)
		}
	}
	return 0
}
