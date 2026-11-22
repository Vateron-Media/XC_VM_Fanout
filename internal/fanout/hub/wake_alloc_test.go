// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hub

import "testing"

// TestPublishToAnUnwatchedStreamAllocatesNoWakeSignal: the wake signal exists
// for viewers parked at the live edge. Re-arming it on every chunk made a new
// channel per Publish on every stream, watched or not — most of a node's
// streams, most of the time.
func TestPublishToAnUnwatchedStreamAllocatesNoWakeSignal(t *testing.T) {
	h := New(1<<20, 0)
	p := mkPkt(1)
	for i := 0; i < 2000; i++ { // grow the open block well past what the run below adds
		h.Publish(p)
	}
	if n := testing.AllocsPerRun(200, func() { h.Publish(p) }); n > 0.5 {
		t.Fatalf("%.2f allocations per Publish with no viewer parked (want 0)", n)
	}
}

// TestAParkedViewerIsStillWoken: arming the signal only when it is handed out
// must not lose the wakeup it exists for.
func TestAParkedViewerIsStillWoken(t *testing.T) {
	h := New(1<<20, 0)
	h.Publish(mkPkt(1))
	_, cur := h.Join(0)
	b, cur, atEnd, wake, _, _ := h.Follow(cur, 1<<20)
	b.Release()
	if !atEnd || wake == nil {
		t.Fatal("expected to park at the live edge")
	}
	for i := 0; i < 3; i++ {
		h.Publish(mkPkt(2))
		select {
		case <-wake:
		default:
			t.Fatalf("publish %d did not wake the parked viewer", i)
		}
		b, cur, atEnd, wake, _, _ = h.Follow(cur, 1<<20)
		b.Release()
		if !atEnd || wake == nil {
			t.Fatal("expected to park at the live edge again")
		}
	}
}
