package supervisor

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// vitalsStub is a controllable stream-health source.
type vitalsStub struct {
	mu sync.Mutex
	v  Vitals
	ok bool
}

func (s *vitalsStub) get(string) (Vitals, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.v, s.ok
}

func (s *vitalsStub) set(v Vitals) {
	s.mu.Lock()
	s.v, s.ok = v, true
	s.mu.Unlock()
}

// TestUnhealthyStreamIsKilledAndRestarted is the end-to-end claim of M2: a
// process that is alive but producing nothing gets replaced. Detecting that and
// not acting on it would be worse than not detecting it, because /monitor would
// report a diagnosis while the channel stayed dead.
func TestUnhealthyStreamIsKilledAndRestarted(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 10 * time.Millisecond

	spec := baseSpec(dir)
	spec.Health = Health{StallSec: 1}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	first := h.nextProcess(t)
	waitFor(t, "first start", func() bool { return h.sup.State("5").Running })

	killed := make(chan struct{})
	var once sync.Once
	first.kill = func() { once.Do(func() { close(killed) }) }

	// Freeze the output: LastData stops advancing, so the stall rule trips.
	vit.set(Vitals{LastData: time.Now().Add(-10 * time.Second)})

	select {
	case <-killed:
	case <-time.After(3 * time.Second):
		t.Fatal("a stalled encoder was diagnosed but never killed")
	}

	second := h.nextProcess(t)
	waitFor(t, "restart on a fresh process", func() bool {
		st := h.sup.State("5")
		return st.Running && st.PID == second.Pid()
	})

	// The panel must be told WHY, with its own action name.
	got := actions(readLog(t, dirLog(dir)))
	if !contains(got, EventFfmpegError) {
		t.Errorf("event trail = %v, want it to carry %s", got, EventFfmpegError)
	}
	if st := h.sup.State("5"); st.LastError == "" {
		t.Error("state must explain the restart to an operator reading /monitor/<id>")
	}
}

// TestHealthyStreamIsLeftAlone: the checks must not restart a working channel.
// A false positive here is a self-inflicted outage on every stream at once.
func TestHealthyStreamIsLeftAlone(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	vit := &vitalsStub{}
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 5 * time.Millisecond

	spec := baseSpec(dir)
	spec.Health = Health{StallSec: 1, AudioLossSec: 1, FPSThreshold: 0.5}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "start", func() bool { return h.sup.State("5").Running })

	// Keep it plausibly alive for many health ticks.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		now := time.Now()
		vit.set(Vitals{LastData: now, LastAudio: now, FPS: 25})
		time.Sleep(5 * time.Millisecond)
	}

	if n := h.launchCount(); n != 1 {
		t.Errorf("a healthy stream was restarted %d times", n-1)
	}
	if got := actions(readLog(t, dirLog(dir))); len(got) != 1 || got[0] != EventStreamStart {
		t.Errorf("event trail = %v, want just the initial %s", got, EventStreamStart)
	}
}

// TestNoVitalsMeansNoOpinion: without a vitals source the supervisor still runs
// the process, and must not condemn it for evidence it cannot gather.
func TestNoVitalsMeansNoOpinion(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t) // no WithVitals
	h.setData(true)
	h.sup.healthTick = 5 * time.Millisecond

	spec := baseSpec(dir)
	spec.Health = Health{StallSec: 1, AudioLossSec: 1}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "start", func() bool { return h.sup.State("5").Running })

	time.Sleep(200 * time.Millisecond)
	if n := h.launchCount(); n != 1 {
		t.Errorf("restarted %d time(s) with no way to measure health", n-1)
	}
}

// TestScheduledAutoRestartFires: the panel's auto_restart is a maintenance
// window, and it is logged as its own action rather than as a failure.
func TestScheduledAutoRestartFires(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 10 * time.Millisecond

	// Pin the clock inside the scheduled minute (2026-09-10 is a Thursday).
	fixed := time.Date(2026, 9, 10, 4, 30, 15, 0, time.UTC)
	h.sup.now = func() time.Time { return fixed }

	spec := baseSpec(dir)
	spec.Health = Health{AutoRestart: &AutoRestart{Days: []string{"Thursday"}, At: "04:30"}}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)

	waitFor(t, "the scheduled restart", func() bool {
		return contains(actions(readLog(t, dirLog(dir))), EventAutoRestart)
	})

	// And exactly once: the schedule matches for a whole minute, so a second
	// firing inside it would mean the stream restarts in a tight loop until the
	// clock moves on.
	time.Sleep(150 * time.Millisecond)
	n := 0
	for _, a := range actions(readLog(t, dirLog(dir))) {
		if a == EventAutoRestart {
			n++
		}
	}
	if n != 1 {
		t.Errorf("auto-restart fired %d times in one scheduled minute, want 1", n)
	}
}

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}

func dirLog(dir string) string { return filepath.Join(dir, "stream_log.log") }
