// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
	"time"
)

// TestForceBackToTheRunningSourceCancelsTheQueuedOne: an operator who picks a
// backup and changes their mind before the next health tick expects the stream
// to stay where it is. The second call took the "already on it, nothing to do"
// shortcut and returned WITHOUT clearing the switch the first one queued, so the
// tick that followed killed the healthy primary and moved to the backup the
// operator had just cancelled.
func TestForceBackToTheRunningSourceCancelsTheQueuedOne(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 200 * time.Millisecond

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	p := h.nextProcess(t)
	waitConfirmed(t, h, p)

	if err := h.sup.ForceSource("5", 1); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	if err := h.sup.ForceSource("5", 0); err != nil { // changed their mind
		t.Fatalf("ForceSource: %v", err)
	}

	time.Sleep(700 * time.Millisecond) // several health ticks
	if st := h.sup.State("5"); st.Source != "primary" {
		t.Errorf("source = %q, want the primary: the second force cancelled the first", st.Source)
	}
	if n := h.launchCount(); n != 1 {
		t.Errorf("launched %d times, want 1 — a cancelled force must not restart anything", n)
	}
}

// TestAQueuedForceTheStreamHasSinceLandedOnIsDropped: a force queued while
// starts were failing is acted on by the first health tick of the run that
// finally worked. If the failover walk has meanwhile put the stream on exactly
// that source, acting on it kills a healthy encoder and starts the same command
// again, logged as a FORCE_SOURCE the operator never asked for.
func TestAQueuedForceTheStreamHasSinceLandedOnIsDropped(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 10 * time.Millisecond

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	p := h.nextProcess(t)
	waitConfirmed(t, h, p)

	// The queued switch names the source the stream is already running.
	h.sup.mu.Lock()
	st := h.sup.procs["5"]
	h.sup.mu.Unlock()
	st.mu.Lock()
	st.forced = 0
	st.mu.Unlock()

	time.Sleep(200 * time.Millisecond) // many health ticks
	if n := h.launchCount(); n != 1 {
		t.Errorf("launched %d times, want 1 — the stream was already on the forced source", n)
	}
	if got := actions(readLog(t, dirLog(dir))); contains(got, EventForceSource) {
		t.Errorf("event trail = %v, want no %s: there was nothing to switch", got, EventForceSource)
	}
}
