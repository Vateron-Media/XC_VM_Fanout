// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
	"time"
)

// TestAStreamWhereNoSourceLastsGivesUp: failing over a source that confirms and
// then dies is right, but on a stream where EVERY source does that the walk just
// goes round and round. Each lap is a start, a few seconds of TS and a kill, for
// ever, and stop_failures — the operator's "stop trying" — never fired, because
// only a start that never produced anything was counted. An operator who set the
// limit got no give-up for the one fault that never resolves itself.
func TestAStreamWhereNoSourceLastsGivesUp(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true) // every start confirms; none of them lasts

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}
	spec.Policy.StopFailures = 3
	spec.Policy.StartTimeoutSec = 30 // a run this short is nowhere near healthy
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for !h.sup.State("5").GaveUp && time.Now().Before(deadline) {
		select {
		case p := <-h.spawned:
			waitConfirmed(t, h, p)
			p.exit <- errTest
		case <-time.After(20 * time.Millisecond):
		}
	}

	st := h.sup.State("5")
	if !st.GaveUp {
		t.Fatalf("still flapping after %d starts with stop_failures=3: %+v", h.launchCount(), st)
	}
	if n := h.launchCount(); n != 3 {
		t.Errorf("launched %d times, want 3 — the limit counts flaps once every source has been tried", n)
	}
	if st.Running {
		t.Error("gave up but still reports running")
	}
}

// TestASingleFlappingSourceIsNeverGivenUpOn: with one source there is nothing to
// walk to and nothing better to switch to, so a channel that comes up, delivers
// a few seconds and drops is left flapping rather than turned off. Viewers get
// something; giving up would leave them nothing, and PHP's stop_failures never
// counted this fault at all.
func TestASingleFlappingSourceIsNeverGivenUpOn(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	spec := baseSpec(dir) // one source
	spec.Policy.StopFailures = 2
	spec.Policy.StartTimeoutSec = 30
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		p := h.nextProcess(t)
		waitConfirmed(t, h, p)
		p.exit <- errTest
	}
	h.nextProcess(t) // still trying
	if st := h.sup.State("5"); st.GaveUp {
		t.Errorf("gave up on the only source there is: %+v", st)
	}
}
