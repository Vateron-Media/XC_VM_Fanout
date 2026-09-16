// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// What GET /monitor/<id> says is not decoration: PHP's supervisedRowUpdate
// turns it into streams_servers, and a stream that is not running with failures
// on the clock is written as stream_status=1, "failed".

// TestFailuresAreClearedByAStartThatWorked: a stream that stumbled on the way up
// and has run ever since is not a failing stream. The tally was never lowered,
// so /monitor/<id> reported failures=3 for a healthy channel for as long as it
// ran — and every later restart gap of it was written to the panel as a failure
// rather than as a start in progress.
func TestFailuresAreClearedByAStartThatWorked(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	h.failNextLaunches(errTest, errTest, errTest)

	spec := baseSpec(dir)
	spec.Policy.StopFailures = 0 // keep trying
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	p := h.nextProcess(t)
	waitConfirmed(t, h, p)

	if st := h.sup.State("5"); st.Failures != 0 {
		t.Errorf("Failures = %d after a confirmed start, want 0 — the run of failures is over", st.Failures)
	}
}

// TestAnEncoderExitIsExplained: an encoder that ends on its own is the commonest
// thing an operator has to diagnose, and its exit status is the only evidence
// the daemon has. It was thrown away: the state reported the restart with no
// reason at all unless the run had been too short to count as healthy.
func TestAnEncoderExitIsExplained(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	var mu sync.Mutex
	clock := time.Now()
	h.sup.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }

	spec := baseSpec(dir)
	spec.Policy.StartTimeoutSec = 10
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	p := h.nextProcess(t)
	waitConfirmed(t, h, p)

	// A long, healthy run — so the "ended before it proved itself" note, the one
	// place an exit was ever explained, does not fire.
	advance(time.Hour)
	p.exit <- errors.New("exit status 8")

	h.nextProcess(t)
	waitFor(t, "the restart", func() bool { return h.sup.State("5").Running })
	if st := h.sup.State("5"); !strings.Contains(st.LastError, "exit status 8") {
		t.Errorf("LastError = %q, want it to carry the encoder's exit status", st.LastError)
	}
}
