// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"testing"
	"time"
)

// TestAForceOnTheSourceTheLoopIsAlreadyOnIsNotAnEvent: a force is queued even
// when it names the index a failing loop happens to be sitting on, because the
// walk would otherwise carry the stream off the source the operator picked. But
// acting on it when nothing changes writes a FORCE_SOURCE into the panel's
// stream_log — which its cron copies into the database as a source switch that
// never happened — and restarts the failure pass. watch() grew this guard; the
// restart loop did not.
func TestAForceOnTheSourceTheLoopIsAlreadyOnIsNotAnEvent(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	h.failNextLaunches(errTest) // the first attempt fails; the stream is down

	// Leave room between the attempts for the operator to act, as a real
	// stream_fail_sleep does.
	h.sup.sleep = func(ctx context.Context, d time.Duration) bool {
		if d > 300*time.Millisecond {
			d = 300 * time.Millisecond
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}

	spec := baseSpec(dir) // one source: the loop cannot walk anywhere else
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the failed start", func() bool {
		return h.launchCount() == 1 && !h.sup.State("5").Running
	})

	// The operator picks the source it is already trying.
	if err := h.sup.ForceSource("5", 0); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	waitFor(t, "the next start", func() bool { return h.sup.State("5").Running })
	time.Sleep(50 * time.Millisecond)

	got := actions(readLog(t, dirLog(dir)))
	if contains(got, EventForceSource) {
		t.Errorf("event trail = %v, want no %s: the stream was already on source 0", got, EventForceSource)
	}
	if st := h.sup.State("5"); st.SourceIdx != 0 || !st.Running {
		t.Errorf("state = %+v, want it running on the source the operator asked for", st)
	}
}

// TestAForceSurvivesAdoptionPuttingTheStreamElsewhere: the force is taken at the
// top of the restart loop, before startOnce — which may then ADOPT a survivor
// and put the stream on the source that survivor is actually running. The
// operator's choice was consumed and silently dropped, leaving a FORCE_SOURCE in
// the panel's log naming a source the stream is not on. It is queued again
// instead, so the watch loop carries it out.
func TestAForceSurvivesAdoptionPuttingTheStreamElsewhere(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	w := newWorld()
	h.sup.find = w.find
	h.sup.killPID = w.kill

	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 300 * time.Millisecond // slow enough to set the scene first

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary /home/xc_vm/streams/5_.m3u8"},
		{Label: "backup", Cmd: "ffmpeg -i backup /home/xc_vm/streams/5_.m3u8"},
		{Label: "tertiary", Cmd: "ffmpeg -i tertiary /home/xc_vm/streams/5_.m3u8"},
	}
	spec.AdoptMatch = "/streams/5_"
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	first := h.nextProcess(t)
	waitFor(t, "the first start", func() bool { return h.sup.State("5").Running })

	// That encoder outlives the loop's view of it: when it ends, its pid is
	// still alive and running the BACKUP's command line, which is what adoption
	// will find in the pid file.
	w.add(first.Pid(), "ffmpeg -i backup /home/xc_vm/streams/5_.m3u8")

	// The operator picks the third source, and the encoder dies before the
	// health tick that would have carried it out.
	if err := h.sup.ForceSource("5", 2); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	first.exit <- errTest

	// Adoption wins the restart — there is a live encoder on this stream's pid
	// and killing it would take the channel off air — but the operator's choice
	// must not be lost.
	waitFor(t, "the survivor to be adopted", func() bool {
		st := h.sup.State("5")
		return st.Adopted && st.SourceIdx == 1
	})
	waitFor(t, "the forced switch to happen anyway", func() bool {
		return h.sup.State("5").Source == "tertiary"
	})

	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	if got[len(got)-1] != "ffmpeg -i tertiary /home/xc_vm/streams/5_.m3u8" {
		t.Errorf("launched %q, want the operator's source in the end", got)
	}
}
