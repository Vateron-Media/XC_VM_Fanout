// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"strconv"
	"testing"
	"time"
)

// TestSignalStorePeekDoesNotScanWhileNothingCanHaveExpired: peek is the live-TS
// hot path. It runs once per chunk delivered to every viewer of every stream on
// the node, and the atomic count exists so that, with nothing queued, it is one
// load and no lock at all.
//
// Sweeping stranded signals fixed the case where that fast path never came back
// — but it sweeps on EVERY miss, which is every viewer's every chunk for as long
// as one signal is pending. Each miss then walks the whole map and writes to the
// very atomic the lock-free path reads, although nothing has expired and nothing
// has changed. A signal only has to be pending; the panel sends one per viewer
// row when an admin fingerprints a batch.
//
// The map is small in real life, so this magnifies the per-miss cost — 20k
// queued signals — until the difference between "walk the map" and "look at a
// watermark" is unmistakable. It is the per-miss cost that is being measured,
// not the size.
func TestSignalStorePeekDoesNotScanWhileNothingCanHaveExpired(t *testing.T) {
	const (
		queued = 20000
		peeks  = 5000
		budget = 500 * time.Millisecond
	)

	s := newSignalStore()
	// Queued through the store's own door would be quadratic (set sweeps too),
	// so seed the map directly: this is about what a LOOKUP costs.
	deadline := time.Now().Add(time.Hour)
	s.mu.Lock()
	for i := 0; i < queued; i++ {
		s.m[strconv.Itoa(i)] = pendingSignal{uuid: strconv.Itoa(i), text: "fingerprint", expires: deadline}
	}
	s.mu.Unlock()
	s.set("addressee", pendingSignal{text: "for one viewer", expires: deadline})

	start := time.Now()
	for i := 0; i < peeks; i++ {
		if s.peek("some-other-viewer") {
			t.Fatal("peek matched a uuid that has no signal queued")
		}
	}
	if took := time.Since(start); took > budget {
		t.Fatalf("%d hot-path misses took %v (budget %v) with %d signals queued and none of them "+
			"expired: every miss walks the whole map and re-stores the atomic count that the "+
			"lock-free fast path reads", peeks, took, budget, queued+1)
	}

	// The saving must not cost the sweep. A signal nobody comes back for still
	// has to leave the map on its own, or the fast path never returns.
	s.mu.Lock()
	s.m = map[string]pendingSignal{}
	s.mu.Unlock()
	s.set("gone", pendingSignal{text: "to a viewer that already left", expires: time.Now().Add(20 * time.Millisecond)})
	time.Sleep(40 * time.Millisecond)
	if s.peek("some-other-viewer") {
		t.Fatal("peek matched a uuid that has no signal queued")
	}
	if n := s.n.Load(); n != 0 {
		t.Fatalf("count = %d after an expired signal was left stranded; the watermark must not "+
			"suppress the sweep that clears it", n)
	}
}
