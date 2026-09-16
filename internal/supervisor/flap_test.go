// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestConfirmedThenCrashingSourceFailsOver: a source that accepts the
// connection, sends a few seconds of TS and drops — an upstream connection
// limit, or a short error clip on a loop — used to flap on the same index
// forever. The start was confirmed, so the exit went down the "exited on its
// own" path, which restarted it on the SAME source: failover never happened and
// a working backup was never tried. The runbook's rule for any ordinary exit is
// that it walks the source list, which is what PHP did by re-running startStream
// over the whole list on every restart.
func TestConfirmedThenCrashingSourceFailsOver(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}
	spec.Policy.PriorityBackupSec = 0 // plain rotation: this is not the priority-backup bug
	spec.Policy.StartTimeoutSec = 30  // a run this short is nowhere near healthy
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	// Two cycles of "connects, delivers a little, drops".
	for i := 0; i < 2; i++ {
		p := h.nextProcess(t)
		waitConfirmed(t, h, p)
		p.exit <- errors.New("upstream closed the stream")
	}
	h.nextProcess(t)

	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	if len(got) < 2 || got[1] != "ffmpeg -i backup" {
		t.Fatalf("launched %q, want the second attempt on the backup", got)
	}
	if st := h.sup.State("5"); st.LastError == "" {
		t.Error("state must explain the failover to an operator reading /monitor/<id>")
	}
}

// TestALongRunStaysOnItsSource is the other half of the rule: only a run that
// ended before it could be called healthy is a failure. A channel whose upstream
// recycles the connection every few hours is running on a source that WORKS, and
// demoting it to a backup on every such restart would take a good feed off a
// good source.
func TestALongRunStaysOnItsSource(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	var mu sync.Mutex
	clock := time.Now()
	h.sup.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}
	spec.Policy.StartTimeoutSec = 10
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	// Three encoders that each ran for an hour before ending.
	for i := 0; i < 3; i++ {
		p := h.nextProcess(t)
		waitConfirmed(t, h, p)
		advance(time.Hour)
		p.exit <- errors.New("upstream restarted")
	}
	h.nextProcess(t)

	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	for _, c := range got {
		if c != "ffmpeg -i primary" {
			t.Fatalf("launched %q, want every restart back on the source that was working", got)
		}
	}
	if st := h.sup.State("5"); st.SourceIdx != 0 {
		t.Errorf("SourceIdx = %d, want 0 — a long run is not a failed source", st.SourceIdx)
	}
}

// waitConfirmed waits until p is the stream's running, confirmed encoder — so a
// test that then ends p is ending a run, not a start still inside its
// confirmation window.
func waitConfirmed(t *testing.T, h *harness, p *fakeProcess) {
	t.Helper()
	waitFor(t, "a confirmed start", func() bool {
		st := h.sup.State("5")
		return st.PID == p.Pid() && st.Confirmed
	})
}
