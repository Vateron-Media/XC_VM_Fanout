// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// withFastEnsure lowers the EnsureRunning tunables so the timeout case is a
// fraction of a second, and restores them after the test.
func withFastEnsure(t *testing.T) {
	t.Helper()
	oldTimeout, oldPoll := ensureRunningTimeout, ensureRunningPoll
	ensureRunningTimeout, ensureRunningPoll = 500*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { ensureRunningTimeout, ensureRunningPoll = oldTimeout, oldPoll })
}

func (s *Stream) isRunning() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.running }

// An unknown id has nothing to start: EnsureRunning fails closed, fast, without
// creating a stream or waiting out the timeout.
func TestEnsureRunningUnknownIdReturnsFalseFast(t *testing.T) {
	withFastEnsure(t)
	m := NewManager(1<<20, 0, 2, 6, time.Second)

	start := time.Now()
	if m.EnsureRunning("nope") {
		t.Fatal("unknown id must not be ready")
	}
	if time.Since(start) >= ensureRunningTimeout {
		t.Fatal("unknown id must return fast, not wait out the timeout")
	}
	if m.Get("nope") != nil {
		t.Fatal("EnsureRunning must not create a stream for an unknown id")
	}
}

// A registered but not-yet-on-air stream: EnsureRunning starts the puller
// (touch) and waits until data flows, then reports ready.
func TestEnsureRunningStartsRegisteredPullerAndWaits(t *testing.T) {
	withFastEnsure(t)
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	// Unreachable source — the real puller produces nothing; we drive onAir by
	// feeding the stream shortly after start, standing in for the puller's output.
	m.Register("7", puller.Source{URLs: []string{"http://127.0.0.1:0/x.ts"}}, 0)
	st := m.Get("7")
	if st.onAir() {
		t.Fatal("precondition: stream must not be on air yet")
	}

	go func() { time.Sleep(30 * time.Millisecond); feedStream(st) }()

	if !m.EnsureRunning("7") {
		t.Fatal("EnsureRunning should report ready once data flows")
	}
	if !st.isRunning() {
		t.Fatal("EnsureRunning must have started the registered puller (touch)")
	}
}

// A registered stream whose source never produces: EnsureRunning gives up at the
// bound and reports not-ready, rather than blocking forever.
func TestEnsureRunningTimesOutWhenNeverOnAir(t *testing.T) {
	withFastEnsure(t)
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.Register("7", puller.Source{URLs: []string{"http://127.0.0.1:0/x.ts"}}, 0)

	start := time.Now()
	if m.EnsureRunning("7") {
		t.Fatal("a stream that never goes on air must not report ready")
	}
	if elapsed := time.Since(start); elapsed < ensureRunningTimeout {
		t.Fatalf("EnsureRunning returned after %s, before the %s bound", elapsed, ensureRunningTimeout)
	}
}

// An already-on-air stream is ready immediately (idempotent — no wait, no
// disruptive restart).
func TestEnsureRunningIdempotentForAlreadyOnAir(t *testing.T) {
	withFastEnsure(t)
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.Register("7", puller.Source{URLs: []string{"http://127.0.0.1:0/x.ts"}}, 0)
	st := m.Get("7")
	feedStream(st) // already on air

	start := time.Now()
	if !m.EnsureRunning("7") {
		t.Fatal("an on-air stream must report ready")
	}
	if time.Since(start) >= ensureRunningTimeout {
		t.Fatal("an on-air stream must return promptly, not wait")
	}
}

// Review Focus #1 — the concurrent-warmup race: many viewers of the same idle
// stream call EnsureRunning at once. The puller is launched exactly once
// (touch()/startLocked() single-flight under s.mu), never once per caller.
// startLocked logs "puller starting" once per launch, so the count is the
// number of launches; a broken running-guard would show more than one.
func TestEnsureRunningConcurrentCallersStartPullerOnce(t *testing.T) {
	withFastEnsure(t)
	logs := captureDebugLog(t)
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.Register("7", puller.Source{URLs: []string{"http://127.0.0.1:0/x.ts"}}, 0)
	st := m.Get("7")

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() { defer wg.Done(); m.EnsureRunning("7") }()
	}
	feedStream(st) // flip onAir so the callers return without each waiting the full bound
	wg.Wait()

	if !st.isRunning() {
		t.Fatal("concurrent EnsureRunning must have started the puller")
	}
	if got := strings.Count(logs.String(), "puller starting"); got != 1 {
		t.Fatalf("puller launched %d times under concurrency; want exactly 1", got)
	}
}

// An externally-fed stream (cfg == nil, e.g. an ingest-fed transcode): touch is
// a no-op (the encoder feeds it), so EnsureRunning only waits for readiness and
// reports ready once data arrives from the external feed.
func TestEnsureRunningExternallyFedCfgNilWaitsForData(t *testing.T) {
	withFastEnsure(t)
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	st := m.GetOrCreate("7") // created, no pull config (cfg == nil)

	go func() { time.Sleep(30 * time.Millisecond); feedStream(st) }()

	if !m.EnsureRunning("7") {
		t.Fatal("EnsureRunning should report ready once external data flows")
	}
	if st.isRunning() {
		t.Fatal("touch must be a no-op for a cfg==nil externally-fed stream")
	}
}
