// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"sync"
	"testing"
	"time"
)

// TestHLSSegmentCacheServesOneAssembly: every viewer of a channel fetches
// byte-identical segments, so assembling one out of the ring and running AES over
// it per request cost several ms of CPU and megabytes of garbage per viewer per
// segment, scaling linearly with the audience. The work must happen once.
func TestHLSSegmentCacheServesOneAssembly(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	st := mgr.GetOrCreate("5")
	feedStream(st)

	first := st.hlsSegment(0)
	if first == nil {
		t.Fatal("no segment to serve")
	}
	// A cache hit must return the very same backing array, not an equal copy.
	second := st.hlsSegment(0)
	if &first[0] != &second[0] {
		t.Error("segment re-assembled for the second viewer instead of being served from cache")
	}

	// Concurrent joins onto a fresh segment — the exact shape of an HLS audience
	// rolling onto the newest segment together — must single-flight to one result.
	var wg sync.WaitGroup
	got := make([][]byte, 32)
	for i := range got {
		wg.Add(1)
		go func(i int) { defer wg.Done(); got[i] = st.hlsSegment(0) }(i)
	}
	wg.Wait()
	for i, g := range got {
		if g == nil || &g[0] != &first[0] {
			t.Fatalf("concurrent viewer %d got a separately assembled segment", i)
		}
	}
}

// TestHLSSegmentCacheMissNotPinned: a seq that has not closed yet must not leave
// a nil cached behind, or the segment would stay invisible once it does exist.
func TestHLSSegmentCacheMissNotPinned(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	st := mgr.GetOrCreate("5")

	if st.hlsSegment(0) != nil {
		t.Fatal("expected no segment before the stream is fed")
	}
	feedStream(st)
	if st.hlsSegment(0) == nil {
		t.Fatal("segment stayed invisible after it closed — a miss was cached")
	}
}

// TestHLSSegmentCacheDroppedOnKeyChange: cached bytes carry the ciphertext of the
// key in force when they were built.
func TestHLSSegmentCacheDroppedOnKeyChange(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	st := mgr.GetOrCreate("5")
	feedStream(st)

	plain := append([]byte(nil), st.hlsSegment(0)...)

	key := hex.EncodeToString(bytes.Repeat([]byte{0xab}, 16))
	iv := hex.EncodeToString(bytes.Repeat([]byte{0xcd}, 16))
	st.setEnc(key, iv)

	enc := st.hlsSegment(0)
	if bytes.Equal(enc, plain) {
		t.Fatal("stale plaintext served after a key was set")
	}
	st.setEnc("", "")
	if again := st.hlsSegment(0); !bytes.Equal(again, plain) {
		t.Fatal("stale ciphertext served after the key was cleared")
	}
}

// TestGateDropsSegmentCache: a channel nobody watches must not hold segment
// copies on top of its (already collapsed) ring.
func TestGateDropsSegmentCache(t *testing.T) {
	mgr := NewManager(1<<20, 20000, 2, 6, time.Second)
	mgr.idleBufferGraceNS.Store(int64(time.Millisecond))
	st := mgr.GetOrCreate("5")
	feedStream(st)
	if st.hlsSegment(0) == nil {
		t.Fatal("no segment to cache")
	}

	st.lastAccess.Store(time.Now().Add(-time.Second).UnixNano())
	st.mu.Lock()
	gated := st.gateIdleBufferLocked(time.Now())
	st.mu.Unlock()
	if !gated {
		t.Fatal("stream did not gate")
	}
	st.segMu.Lock()
	n := len(st.segCache)
	st.segMu.Unlock()
	if n != 0 {
		t.Errorf("gated stream still holds %d cached segment(s)", n)
	}
}

// TestMemoryScavengerRateFloor: debug.FreeOSMemory is a full stop-the-world GC
// plus a page-return sweep, and on a busy daemon the idle-heap threshold is met
// almost continuously. Releases must be floored apart, and a ring collapse (the
// reaper's gate — known-real garbage) must bypass that floor.
func TestMemoryScavengerRateFloor(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// threshold 0 ⇒ every sweep sees "enough" retained heap, so only the floor
	// and the gate signal decide whether a release happens.
	m.startMemoryScavenger(ctx, 10*time.Millisecond, 0, time.Hour)

	time.Sleep(150 * time.Millisecond)
	if m.gatedSinceScavenge.Load() {
		t.Fatal("scavenger never ran")
	}

	// With an hour-long floor, a gate is the only thing that may release again.
	m.gatedSinceScavenge.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && m.gatedSinceScavenge.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if m.gatedSinceScavenge.Load() {
		t.Error("a ring collapse did not trigger a release: the gate must bypass the rate floor")
	}
}

// TestRingCollapseReleasesEvenBelowTheThreshold: the GOPs a gate drops are
// garbage, not the swept free heap the threshold measures, and on a steady
// ingest no GC comes along to sweep them — so the threshold is never met on
// their account. A collapse must force the release anyway, or the collapsed
// ring stays resident: 30 channels' rings held 513 MB while the heap sat at 1.9 GB.
func TestRingCollapseReleasesEvenBelowTheThreshold(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A threshold no heap can reach: only the gate may cause a release.
	m.startMemoryScavenger(ctx, 10*time.Millisecond, 1<<62, 0)

	m.gatedSinceScavenge.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && m.gatedSinceScavenge.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if m.gatedSinceScavenge.Load() {
		t.Fatal("a ring collapse did not trigger a release while the swept-free heap was under the threshold")
	}
}
