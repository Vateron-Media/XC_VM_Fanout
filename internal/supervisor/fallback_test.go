package supervisor

import (
	"errors"
	"path/filepath"
	"testing"
)

// exitErr is a process exit carrying a status, the shape exec.ExitError has.
type exitErr int

func (e exitErr) Error() string { return "exit status " + string(rune('0'+int(e))) }
func (e exitErr) ExitCode() int { return int(e) }

func fallbackSpec(dir string) Spec {
	s := baseSpec(dir)
	s.Sources = []Source{{
		Label:       "http://src/one.m3u8",
		Cmd:         "xc_fanout remux -i one",
		FallbackCmd: "ffmpeg -i one",
	}}
	return s
}

// TestUnsupportedSwitchesToFallback: a remuxer that says it cannot serve the
// source (ExitUnsupported) hands that source to the panel's fallback command at
// once — no fail sleep, no stop_failures charge — and stays there.
func TestUnsupportedSwitchesToFallback(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	spec := fallbackSpec(dir)
	spec.Policy.StopFailures = 1 // a counted failure would end supervision

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	native := h.nextProcess(t)
	native.exit <- exitErr(ExitUnsupported)

	h.setData(true)
	fb := h.nextProcess(t)
	waitFor(t, "fallback running", func() bool {
		st := h.sup.State("5")
		return st.Running && st.PID == fb.Pid()
	})

	st := h.sup.State("5")
	if !st.Fallback {
		t.Error("state does not report the fallback command")
	}
	if st.GaveUp {
		t.Error("an unsupported source was charged as a failed start and ended supervision")
	}
	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	if len(got) != 2 || got[0] != spec.Sources[0].Cmd || got[1] != spec.Sources[0].FallbackCmd {
		t.Fatalf("launched %q, want the command then its fallback", got)
	}

	// The switch is sticky: a restart goes straight to the fallback rather than
	// paying for another refused native start.
	fb.exit <- errors.New("ffmpeg died")
	again := h.nextProcess(t)
	waitFor(t, "restart", func() bool { return h.sup.State("5").PID == again.Pid() })
	h.mu.Lock()
	last := h.launched[len(h.launched)-1]
	h.mu.Unlock()
	if last != spec.Sources[0].FallbackCmd {
		t.Errorf("restart ran %q, want the fallback to stick", last)
	}
}

// TestUnsupportedWithoutFallbackIsAFailure: with no fallback (backend=native)
// the same exit is simply a failed start, counted like any other.
func TestUnsupportedWithoutFallbackIsAFailure(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	spec := fallbackSpec(dir)
	spec.Sources[0].FallbackCmd = ""
	spec.Policy.StopFailures = 1

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t).exit <- exitErr(ExitUnsupported)

	waitFor(t, "give up", func() bool { return h.sup.State("5").GaveUp })
	if h.sup.State("5").Fallback {
		t.Error("reported a fallback that does not exist")
	}
}

// TestOrdinaryExitDoesNotFallBack: only ExitUnsupported selects the fallback. A
// source that is merely down fails over through the source list as before —
// switching pipelines would not make an unreachable upstream answer.
func TestOrdinaryExitDoesNotFallBack(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	spec := fallbackSpec(dir)

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t).exit <- exitErr(1)
	h.nextProcess(t)

	h.mu.Lock()
	second := h.launched[1]
	h.mu.Unlock()
	if second != spec.Sources[0].Cmd {
		t.Errorf("an ordinary failure ran %q, want the primary command again", second)
	}
}

// TestUnsupportedMidStreamSwitches: a source can turn unservable after the start
// was confirmed (an HLS that changes segment format); the same exit then moves
// it to the fallback instead of logging a failure.
func TestUnsupportedMidStreamSwitches(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	spec := fallbackSpec(dir)

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	native := h.nextProcess(t)
	waitFor(t, "native running", func() bool { return h.sup.State("5").Running })
	native.exit <- exitErr(ExitUnsupported)

	fb := h.nextProcess(t)
	waitFor(t, "fallback running", func() bool {
		st := h.sup.State("5")
		return st.Running && st.PID == fb.Pid() && st.Fallback
	})
	for _, a := range actions(readLog(t, filepath.Join(dir, "stream_log.log"))) {
		if a == EventStreamFailed {
			t.Error("a pipeline switch was logged as STREAM_FAILED")
		}
	}
}

// TestConfirmedOnlyOnceDataArrives: Running covers a start in progress; the panel
// shows "starting" until Confirmed, which needs bytes to have arrived.
func TestConfirmedOnlyOnceDataArrives(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	spec := baseSpec(dir)
	spec.Policy.StartTimeoutSec = 30

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "launched", func() bool { return h.sup.State("5").Running })
	if h.sup.State("5").Confirmed {
		t.Fatal("confirmed before any data arrived")
	}
	h.setData(true)
	waitFor(t, "confirmed", func() bool { return h.sup.State("5").Confirmed })
}
