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
)

// TestSubscribeSnapshotStableWhileProducing is the safety case for copying a join
// snapshot with the hub lock RELEASED. The copy reads GOP buffers straight out of
// the ring while the producer keeps publishing — opening GOPs, pruning old ones
// and recycling their arrays for reuse. If a snapshot's pin failed to hold off
// that recycling, a buffer would be handed to a new GOP and rewritten mid-copy,
// and the joining viewer would receive a splice of two different points in the
// stream.
//
// Every fill packet carries the generation of the GOP that produced it, and GOPs
// sit in the ring oldest→newest, so within a snapshot consecutive generations may
// only stay the same or step up by one. A buffer recycled underneath the copy
// splices in some unrelated generation, which shows up as a large jump in either
// direction. The comparison is done in uint16 arithmetic so the counter wrapping
// (a long run does hundreds of thousands of joins) reads as the step of 1 it
// really is. Run with -race for the other half of the proof.
// maxGenStep is the largest generation step legitimately possible between two
// consecutive packets of one snapshot: they are either the same GOP (0) or
// adjacent ones (1). Anything larger is a splice. Generous, so the check can
// never be tripped by ordinary scheduling.
const maxGenStep = 1000

func TestSubscribeSnapshotStableWhileProducing(t *testing.T) {
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
		sub, burst := h.Subscribe(300) // ask for the whole ring
		joins++

		// Read the burst IN PLACE, as serveLive writes it: straight out of the
		// ring's pinned GOP buffers, while the producer keeps publishing. Walking
		// the parts one at a time with a pause between them stands in for a slow
		// viewer's socket — the pin has to hold for the whole write, not just a
		// quick copy.
		var snap []byte
		for i, part := range burst.Parts {
			if i%4 == 3 {
				time.Sleep(50 * time.Microsecond)
			}
			snap = append(snap, part...)
		}

		var last uint16
		checked0 := checked
		for off := 0; off+188 <= len(snap); off += 188 {
			p := snap[off : off+188]
			if p[0] != 0x47 {
				t.Fatalf("join %d: packet at %d lost its sync byte (0x%02x) — buffer rewritten mid-copy", joins, off, p[0])
			}
			// fill packets only: PID 0x101, no PUSI, payload-only
			pid := int(p[1]&0x1f)<<8 | int(p[2])
			if pid != 0x101 || p[1]&0x40 != 0 || (p[3]>>4)&0x3 != 1 {
				continue
			}
			g := tsfixture.ReadGen(p)
			if step := g - last; checked > checked0 && step > maxGenStep {
				t.Fatalf("join %d: generation jumped at offset %d (%d after %d) — a GOP buffer was recycled mid-copy",
					joins, off, g, last)
			}
			last = g
			checked++
		}
		burst.Release()
		h.Unsubscribe(sub)
	}

	stop.Store(true)
	wg.Wait()
	t.Logf("%d joins, %d packets verified against a live producer", joins, checked)
	if joins < 50 || checked < 1000 {
		t.Fatalf("test did not exercise the path: %d joins, %d packets", joins, checked)
	}
}

// TestSubscribeNoGapNoDuplication: releasing the lock before the copy must not
// break the contract that the live tail continues exactly where the snapshot
// ends. Everything published before registration belongs in the snapshot,
// everything after it in the channel — each byte in exactly one of the two.
func TestSubscribeNoGapNoDuplication(t *testing.T) {
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

	sub, burst := h.Subscribe(10000)
	defer h.Unsubscribe(sub)
	snap := burst.Bytes()
	burst.Release()

	// Everything published from here on must arrive on the channel, not the snapshot.
	for i := 0; i < 3; i++ {
		publishGOP()
	}

	maxSnap := lastGen(snap)
	if maxSnap != 3 {
		t.Fatalf("snapshot ends at generation %d, want 3 (the last one published before the join)", maxSnap)
	}

	var tail []byte
	for {
		select {
		case b := <-sub.C():
			tail = append(tail, b...)
			continue
		default:
		}
		break
	}
	first := firstGen(tail)
	if first != 4 {
		t.Fatalf("live tail starts at generation %d, want 4: %s", first,
			map[bool]string{true: "a gap", false: "duplicated bytes"}[first > 4])
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
