// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hub

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// joinerRig publishes `secs` one-second GOPs of 12 KB chunks, a stream the size
// of a real one, and returns the hub and one chunk to keep publishing.
func joinerRig(secs int) (*Hub, []byte) {
	h := New(1<<24, int64(secs)*1000)
	h.Configure(int64(secs)*1000, 0, 0)
	h.Publish(tsfixture.PAT(0x100))
	h.Publish(tsfixture.PMT(0x100, 0x101))
	chunk := make([]byte, 0, 188*64)
	for i := 0; i < 64; i++ {
		chunk = append(chunk, tsfixture.Fill(0x101)...)
	}
	for g := 0; g < secs; g++ {
		pcr := int64(g) * 90000
		h.Publish(tsfixture.KeyframePCR(0x101, pcr, pcr))
		for c := 0; c < 50; c++ {
			h.Publish(chunk)
		}
	}
	return h, chunk
}

// TestSlowJoinerIsNotDroppedWhileTakingItsBurst: the live tail queues behind a
// viewer's join burst for as long as the burst takes to write. A fixed queue
// (256 chunks, ~5 s of a 4.7 Mbit/s stream) dropped every viewer that could not
// take a 30 s burst within that — anything under ~30 Mbit/s — which then
// reconnected into another burst. A burst's worth of chunks must fit.
func TestSlowJoinerIsNotDroppedWhileTakingItsBurst(t *testing.T) {
	h, chunk := joinerRig(30)
	sub, burst := h.Subscribe(30000)
	defer h.Unsubscribe(sub)
	defer burst.Release()

	behind := burst.Len() / len(chunk) // what arrives while a just-fast-enough link takes the burst
	if behind <= subQueue {
		t.Fatalf("rig too small to test: burst of %d chunks", behind)
	}
	for i := 0; i < behind; i++ {
		h.Publish(chunk)
	}
	select {
	case <-sub.Done():
		t.Fatalf("a viewer taking its %d MB burst was dropped with %d chunks queued behind it", burst.Len()>>20, behind)
	default:
	}
}

// TestHopelesslySlowJoinerIsStillDropped: a link slower than the stream never
// catches up, and must still be let go.
func TestHopelesslySlowJoinerIsStillDropped(t *testing.T) {
	h, chunk := joinerRig(30)
	sub, burst := h.Subscribe(30000)
	defer h.Unsubscribe(sub)
	defer burst.Release()

	for i := 0; i < 4*burst.Len()/len(chunk); i++ {
		h.Publish(chunk)
	}
	select {
	case <-sub.Done():
	default:
		t.Fatal("a viewer four bursts behind was not dropped")
	}
}
