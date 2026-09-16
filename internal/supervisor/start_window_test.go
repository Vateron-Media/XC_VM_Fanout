// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
	"time"
)

// The start-confirmation window is where a stream spends its first seconds (up
// to start_timeout_sec), and it is also when the panel is most likely to change
// its mind: a DELETE, a re-PUT after a source change, or a daemon shutdown.
// These pin what those must and must not do to a start still in flight.

// TestStopDuringTheStartWindowIsNotAFailedStart: cancelling a start is our own
// doing, not the source's. Counting it as a failed start writes a
// STREAM_START_FAIL row the panel's cron copies into the database, charges
// stop_failures for it and walks the source list — so every re-PUT of a starting
// stream left the panel a failure that never happened.
func TestStopDuringTheStartWindowIsNotAFailedStart(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(false) // nothing confirms: the start sits in its window

	spec := baseSpec(dir)
	spec.Policy.StartTimeoutSec = 30 // long: the test ends the start itself
	spec.Policy.StopFailures = 1
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "the start window", func() bool { return h.sup.State("5").Running })

	h.sup.Release("5")

	time.Sleep(100 * time.Millisecond)
	if got := actions(readLog(t, dirLog(dir))); len(got) != 0 {
		t.Errorf("event trail = %v, want nothing at all: a stop during a start is not a %s",
			got, EventStreamStartFail)
	}
	if n := h.launchCount(); n != 1 {
		t.Errorf("launched %d times, want 1 — a cancelled start must not be retried", n)
	}
}
