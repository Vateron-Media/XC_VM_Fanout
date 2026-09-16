// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
)

// TestForceIsHonouredWhileTheStreamCannotStart: POST /monitor/<id>/source is the
// operator's manual rescue, and a stream that will not start is exactly when it
// is reached for. The force used to be consumed only inside watch(), which the
// restart loop reaches only AFTER a start has worked — so while starts kept
// failing the call returned 204 and nothing happened, and the queued switch was
// then acted on much later, restarting an encoder that had just come good.
func TestForceIsHonouredWhileTheStreamCannotStart(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(false) // nothing this stream starts ever produces

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
		{Label: "tertiary", Cmd: "ffmpeg -i tertiary"},
	}
	spec.Policy.PriorityBackupSec = 300
	spec.Policy.StartTimeoutSec = 30 // long: the test ends each start itself
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	// The first attempt is up but unconfirmed, which is where a failing stream
	// spends its life. The operator picks the last source.
	p := h.nextProcess(t)
	if err := h.sup.ForceSource("5", 2); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	p.exit <- errTest // the start fails, as every start on this stream does

	// The next attempt must be the operator's choice, not the failover walk's.
	next := h.nextProcess(t)
	_ = next
	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	if len(got) < 2 || got[1] != "ffmpeg -i tertiary" {
		t.Fatalf("launched %q, want the forced source on the attempt after the force", got)
	}
	if st := h.sup.State("5"); st.SourceIdx != 2 {
		t.Errorf("SourceIdx = %d, want the forced 2", st.SourceIdx)
	}
	if acts := actions(readLog(t, dirLog(dir))); !contains(acts, EventForceSource) {
		t.Errorf("event trail = %v, want the panel told about the %s", acts, EventForceSource)
	}
}

// TestForceOnTheCurrentIndexIsQueuedWhileDown: the "already on it, nothing to
// do" shortcut is only true while the stream is RUNNING on that source. A
// failing loop walks the list between attempts, so an operator who picks the
// index it happens to be sitting at this instant would otherwise get a 204 and
// a stream that walks straight off the source they asked for.
func TestForceOnTheCurrentIndexIsQueuedWhileDown(t *testing.T) {
	h := newHarness(t)
	spec := baseSpec(t.TempDir())
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup"},
	}

	// A supervised stream with no loop running, so the test alone decides what
	// it is doing: down, on source 0. Its done channel is already closed, so a
	// Release finds nothing to wait for.
	st := &stream{id: "5", sup: h.sup, spec: spec, forced: -1, cancel: func() {}, done: make(chan struct{})}
	close(st.done)
	h.sup.mu.Lock()
	h.sup.procs["5"] = st
	h.sup.mu.Unlock()

	if err := h.sup.ForceSource("5", 0); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	if got := st.takeForced(); got != 0 {
		t.Errorf("queued force = %d, want 0 — a force on a stream that is down must be honoured", got)
	}

	// Running on it: there really is nothing to do, and restarting a healthy
	// encoder onto the source it already has would be an outage for nothing.
	st.mu.Lock()
	st.running = true
	st.mu.Unlock()
	if err := h.sup.ForceSource("5", 0); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	if got := st.takeForced(); got != -1 {
		t.Errorf("queued force = %d, want none while already running on that source", got)
	}
}
