// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hub

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// genStream publishes one-second GOPs of 12 KB chunks (a real stream's size),
// each fill packet stamped with a running generation so a reader can check it
// saw every packet exactly once.
type genStream struct {
	h   *Hub
	gen uint16
	sec int64
}

func newGenStream(ringSecs int) *genStream {
	h := New(1<<24, int64(ringSecs)*1000)
	h.Configure(int64(ringSecs)*1000, 0, 0)
	h.Publish(tsfixture.PAT(0x100))
	h.Publish(tsfixture.PMT(0x100, 0x101))
	return &genStream{h: h}
}

// second publishes one second of stream: a keyframe and 50 chunks of 64 packets.
func (g *genStream) second() {
	pcr := g.sec * 90000
	g.sec++
	g.h.Publish(tsfixture.KeyframePCR(0x101, pcr, pcr))
	for c := 0; c < 50; c++ {
		chunk := make([]byte, 0, 188*64)
		for i := 0; i < 64; i++ {
			g.gen++
			chunk = append(chunk, tsfixture.FillGen(0x101, g.gen)...)
		}
		g.h.Publish(chunk)
	}
}

const secondBytes = 50 * 64 * 188 // one second of genStream, ~600 KB

// gens appends the generation of every fill packet in b.
func gens(dst []uint16, b []byte) []uint16 {
	for off := 0; off+188 <= len(b); off += 188 {
		p := b[off : off+188]
		if int(p[1]&0x1f)<<8|int(p[2]) == 0x101 && p[1]&0x40 == 0 && (p[3]>>4)&0x3 == 1 {
			dst = append(dst, tsfixture.ReadGen(p))
		}
	}
	return dst
}

// catchUp drives a joining viewer that takes readPerStep bytes of history for
// every second of stream published meanwhile — its link speed relative to the
// stream's. It follows the ring by cursor (ADR 0004) and returns the generations
// it received, the cursor it reached, whether it reached the live edge, and
// whether it fell behind.
func catchUp(t *testing.T, g *genStream, prebufMS int64, readPerStep int, maxSteps int) (seen []uint16, cur tsjoin.Cursor, reachedEdge, behind bool) {
	t.Helper()
	_, cur = g.h.Join(prebufMS)
	for step := 0; step < maxSteps; step++ {
		burst, next, atEnd, _, bh, ended := g.h.Follow(cur, readPerStep)
		if bh {
			burst.Release()
			return seen, cur, false, true
		}
		if ended {
			burst.Release()
			return seen, cur, false, false
		}
		for _, p := range burst.Parts {
			seen = gens(seen, p)
		}
		burst.Release()
		cur = next
		if atEnd {
			return seen, cur, true, false
		}
		g.second() // the stream moves on while the viewer writes that run
	}
	t.Fatalf("still catching up after %d steps", maxSteps)
	return
}

// TestSlowJoinerCatchesUpThroughTheRing: a viewer on a link twice the stream's
// rate takes its 30 s of history straight out of the ring and reaches the live
// edge. It used to be subscribed first with the live tail queued behind the
// history, and a queue short enough to be cheap (256 chunks) dropped it as too
// slow before the history was through; following the ring has no such queue.
func TestSlowJoinerCatchesUpThroughTheRing(t *testing.T) {
	g := newGenStream(40)
	for i := 0; i < 40; i++ {
		g.second()
	}
	_, _, edge, behind := catchUp(t, g, 30000, 2*secondBytes, 1000)
	if behind || !edge {
		t.Fatalf("a viewer on a link twice the stream's rate did not catch up (behind=%v, edge=%v)", behind, edge)
	}
}

// TestHopelesslySlowJoinerFallsBehind: a link slower than the stream can never
// reach the edge; its place leaves the ring's tail and it is let go.
func TestHopelesslySlowJoinerFallsBehind(t *testing.T) {
	g := newGenStream(10)
	for i := 0; i < 10; i++ {
		g.second()
	}
	_, _, edge, behind := catchUp(t, g, 10000, secondBytes/2, 1000)
	if !behind || edge {
		t.Fatalf("a viewer at half the stream's rate was not let go (behind=%v, edge=%v)", behind, edge)
	}
}

// TestCatchUpHasNoGapOrDuplicate: the history a joiner reads through the ring,
// followed by the live tail it reads once it reaches the edge, is every packet
// from its starting keyframe on, exactly once — the atomicity Subscribe gave,
// kept by the single-cursor follow.
func TestCatchUpHasNoGapOrDuplicate(t *testing.T) {
	g := newGenStream(20)
	for i := 0; i < 20; i++ {
		g.second()
	}
	seen, cur, edge, behind := catchUp(t, g, 5000, 3*secondBytes, 1000)
	if behind || !edge {
		t.Fatal("did not catch up")
	}
	for i := 0; i < 3; i++ {
		g.second()
	}
	// Read the live tail from where catch-up left off, right up to the edge.
	for {
		burst, next, atEnd, _, bh, ended := g.h.Follow(cur, 1<<24)
		if bh || ended {
			burst.Release()
			t.Fatalf("tail read did not complete (behind=%v ended=%v)", bh, ended)
		}
		for _, p := range burst.Parts {
			seen = gens(seen, p)
		}
		burst.Release()
		cur = next
		if atEnd {
			break
		}
	}
	if len(seen) == 0 {
		t.Fatal("nothing received")
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] != seen[i-1]+1 {
			t.Fatalf("packet %d: generation %d after %d — a gap or a duplicate at the hand-over", i, seen[i], seen[i-1])
		}
	}
	if last := seen[len(seen)-1]; last != g.gen {
		t.Fatalf("last packet received is generation %d, the stream is at %d", last, g.gen)
	}
}
