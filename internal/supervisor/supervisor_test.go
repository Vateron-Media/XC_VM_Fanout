// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeProcess is an encoder that exits when the test says so, so the restart
// loop can be exercised without spawning anything.
type fakeProcess struct {
	pid  int
	exit chan error
	once sync.Once
	done chan struct{}
	err  error
	kill func()
}

func newFakeProcess(pid int) *fakeProcess {
	return &fakeProcess{pid: pid, exit: make(chan error, 1), done: make(chan struct{})}
}

func (p *fakeProcess) Pid() int { return p.pid }
func (p *fakeProcess) Wait() error {
	p.once.Do(func() {
		p.err = <-p.exit
		close(p.done)
	})
	<-p.done
	return p.err
}
func (p *fakeProcess) Kill() {
	if p.kill != nil {
		p.kill()
	}
	select {
	case p.exit <- errors.New("killed"):
	default:
	}
}

// harness wires a Supervisor to a controllable launcher and clock.
type harness struct {
	sup *Supervisor

	mu       sync.Mutex
	launched []string       // command lines, in order
	procs    []*fakeProcess // the processes handed out
	launchEr []error        // pre-seeded launch failures, consumed in order
	data     bool           // what hasData reports, whatever the start time
	dataAt   time.Time      // when set: a data event, confirming only starts launched before it
	spawned  chan *fakeProcess
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{spawned: make(chan *fakeProcess, 32)}
	nextPid := 1000
	h.sup = New(func(_ context.Context, cmdline, _ string) (Process, error) {
		h.mu.Lock()
		h.launched = append(h.launched, cmdline)
		if len(h.launchEr) > 0 {
			err := h.launchEr[0]
			h.launchEr = h.launchEr[1:]
			h.mu.Unlock()
			if err != nil {
				return nil, err
			}
			h.mu.Lock()
		}
		nextPid++
		p := newFakeProcess(nextPid)
		h.procs = append(h.procs, p)
		h.mu.Unlock()
		// Never block the supervisor: a test that stops draining this must not
		// wedge the loop it is testing.
		select {
		case h.spawned <- p:
		default:
		}
		return p, nil
	}, func(_ string, since time.Time) bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		if !h.dataAt.IsZero() {
			return h.dataAt.After(since)
		}
		return h.data
	})
	// Keep the retry sleeps out of the test's wall clock.
	h.sup.sleep = func(ctx context.Context, d time.Duration) bool {
		if d > 50*time.Millisecond {
			d = time.Millisecond
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
	t.Cleanup(h.sup.ReleaseAll)
	return h
}

func (h *harness) setData(v bool) { h.mu.Lock(); h.data = v; h.mu.Unlock() }

// dataNow records bytes arriving now: it confirms a start launched before this
// moment, and no start launched after it.
func (h *harness) dataNow() { h.mu.Lock(); h.dataAt = time.Now(); h.mu.Unlock() }

func (h *harness) failNextLaunches(errs ...error) {
	h.mu.Lock()
	h.launchEr = append(h.launchEr, errs...)
	h.mu.Unlock()
}

func (h *harness) launchCount() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.launched) }

func (h *harness) nextProcess(t *testing.T) *fakeProcess {
	t.Helper()
	select {
	case p := <-h.spawned:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("no process was launched")
		return nil
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func baseSpec(dir string) Spec {
	return Spec{
		Sources: []Source{{Label: "http://src/one.ts", Cmd: "ffmpeg -i one"}},
		Policy: Policy{
			StreamFailSleepSec: 1,
			StartTimeoutSec:    1,
		},
		PIDPath:    filepath.Join(dir, "5_.pid"),
		ErrorsPath: filepath.Join(dir, "5.errors"),
		LogPath:    filepath.Join(dir, "stream_log.log"),
		ServerID:   7,
	}
}

// readLog returns the decoded panel stream-log entries written so far. The panel
// format is one base64'd JSON object per line, which is what
// StreamProcess::streamLog writes and StreamsLogsCronJob drains.
func readLog(t *testing.T, path string) []logLine {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []logLine
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(line)
		if err != nil {
			t.Fatalf("stream log line is not base64 (the panel cron will choke): %q", line)
		}
		var e logLine
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("stream log line is not JSON: %q", raw)
		}
		out = append(out, e)
	}
	return out
}

func actions(entries []logLine) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Action)
	}
	return out
}

// TestStartsAndPublishesPID: taking a stream over means starting its encoder and
// publishing the pid where the panel still looks for it — ProcessManager::
// isStreamRunning and stopStream both read `<streams>/<id>_.pid`, so a
// supervised stream has to stay visible to every PHP path that has not moved.
func TestStartsAndPublishesPID(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	spec := baseSpec(dir)

	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatalf("Supervise: %v", err)
	}
	p := h.nextProcess(t)

	waitFor(t, "running state", func() bool { return h.sup.State("5").Running })
	st := h.sup.State("5")
	if !st.Supervised || st.PID != p.Pid() {
		t.Fatalf("state = %+v, want supervised with pid %d", st, p.Pid())
	}
	if st.Source != "http://src/one.ts" {
		t.Errorf("state.Source = %q, want the panel's label for the source", st.Source)
	}

	b, err := os.ReadFile(spec.PIDPath)
	if err != nil {
		t.Fatalf("pid file not written: %v", err)
	}
	if got := strings.TrimSpace(string(b)); got != strconv.Itoa(p.Pid()) {
		t.Errorf("pid file = %q, want %d", got, p.Pid())
	}
}

// TestRestartsOnEncoderExit is the watchdog's whole reason to exist: when the
// encoder dies the stream comes back, and the panel sees the event trail.
func TestRestartsOnEncoderExit(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	if err := h.sup.Supervise("5", baseSpec(dir)); err != nil {
		t.Fatal(err)
	}
	first := h.nextProcess(t)
	waitFor(t, "first start", func() bool { return h.sup.State("5").Running })

	first.exit <- errors.New("ffmpeg died")
	second := h.nextProcess(t)
	waitFor(t, "restart", func() bool {
		st := h.sup.State("5")
		return st.Running && st.PID == second.Pid()
	})

	if st := h.sup.State("5"); st.Restarts < 2 {
		t.Errorf("Restarts = %d, want at least 2 (initial + restart)", st.Restarts)
	}

	got := actions(readLog(t, filepath.Join(dir, "stream_log.log")))
	want := []string{EventStreamStart, EventStreamFailed, EventStreamRestart}
	if len(got) < 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("event trail = %v, want it to begin %v", got, want)
	}
}

// TestStreamLogMatchesPanelFormat: the daemon has no database, so this file IS
// the reporting channel. If its shape drifts from what StreamProcess::streamLog
// writes, the panel's drain silently stops recording restarts.
func TestStreamLogMatchesPanelFormat(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	if err := h.sup.Supervise("5", baseSpec(dir)); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "an event", func() bool {
		return len(readLog(t, filepath.Join(dir, "stream_log.log"))) > 0
	})

	e := readLog(t, filepath.Join(dir, "stream_log.log"))[0]
	if e.ServerID != 7 {
		t.Errorf("server_id = %d, want the spec's 7", e.ServerID)
	}
	if e.StreamID != 5 {
		t.Errorf("stream_id = %d, want 5 parsed from the stream id", e.StreamID)
	}
	if e.Action != EventStreamStart {
		t.Errorf("action = %q, want %q", e.Action, EventStreamStart)
	}
	if e.Source != "http://src/one.ts" {
		t.Errorf("source = %q, want the source label", e.Source)
	}
	if e.Time <= 0 {
		t.Error("time must be a unix timestamp; the panel orders log rows by it")
	}
}

// TestFailedStartRetriesThenGivesUp pins the panel's stop_failures rule: retry
// a failing start, but stop after the configured number of consecutive failures
// instead of hammering a dead source forever.
func TestFailedStartRetriesThenGivesUp(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	h.failNextLaunches(errors.New("boom"), errors.New("boom"), errors.New("boom"))

	spec := baseSpec(dir)
	spec.Policy.StopFailures = 3
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "give-up", func() bool { return h.sup.State("5").GaveUp })
	st := h.sup.State("5")
	if st.Running {
		t.Error("gave up but still reports running")
	}
	if st.Failures != 3 {
		t.Errorf("Failures = %d, want exactly the stop_failures limit of 3", st.Failures)
	}
	if n := h.launchCount(); n != 3 {
		t.Errorf("launched %d times, want 3 — it must stop at the limit, not keep trying", n)
	}
	if got := actions(readLog(t, filepath.Join(dir, "stream_log.log"))); len(got) != 3 {
		t.Errorf("event trail = %v, want three %s entries", got, EventStreamStartFail)
	}
	if _, err := os.Stat(spec.PIDPath); !os.IsNotExist(err) {
		t.Error("giving up must clear the pid file, or the panel keeps thinking the stream runs")
	}
}

// TestOnDemandFailureExitsImmediately: an on-demand stream is started because
// somebody is waiting for it. The panel's on_demand_failure_exit says not to
// keep retrying at that viewer's expense.
func TestOnDemandFailureExitsImmediately(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	h.failNextLaunches(errors.New("nope"))

	spec := baseSpec(dir)
	spec.Policy.OnDemand = true
	spec.Policy.OnDemandFailureExit = true
	spec.Policy.StopFailures = 0 // unlimited: on-demand must still bail out
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "give-up", func() bool { return h.sup.State("5").GaveUp })
	if n := h.launchCount(); n != 1 {
		t.Errorf("launched %d times, want 1 — on_demand_failure_exit must not retry", n)
	}
}

// TestStartWithoutDataIsAFailedStart: a process that starts and then produces
// nothing is not a running stream. The panel discovered this by waiting for a
// playlist file to appear; the daemon asks whether bytes arrived, and must treat
// silence the same way — otherwise a wedged encoder is supervised forever as
// "running" and the channel is dead with nobody noticing.
func TestStartWithoutDataIsAFailedStart(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(false) // the encoder runs but never produces

	spec := baseSpec(dir)
	spec.Policy.StartTimeoutSec = 1
	spec.Policy.StopFailures = 1
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "give-up on a silent encoder", func() bool { return h.sup.State("5").GaveUp })
	if st := h.sup.State("5"); !strings.Contains(st.LastError, "no data") {
		t.Errorf("LastError = %q, want it to name the missing data", st.LastError)
	}
	got := actions(readLog(t, filepath.Join(dir, "stream_log.log")))
	if len(got) != 1 || got[0] != EventStreamStartFail {
		t.Errorf("event trail = %v, want a single %s", got, EventStreamStartFail)
	}
}

// TestReleaseStopsAndDoesNotRestart: DELETE means stop, and the restart loop
// must not treat our own kill as a fault worth restarting from.
func TestReleaseStopsAndDoesNotRestart(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	spec := baseSpec(dir)
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "running", func() bool { return h.sup.State("5").Running })

	before := h.launchCount()
	if !h.sup.Release("5") {
		t.Fatal("Release reported nothing to release")
	}
	if st := h.sup.State("5"); st.Supervised {
		t.Error("released stream still reports as supervised")
	}
	time.Sleep(50 * time.Millisecond)
	if n := h.launchCount(); n != before {
		t.Errorf("launched %d more time(s) after Release; our own kill must not trigger a restart", n-before)
	}
	if _, err := os.Stat(spec.PIDPath); !os.IsNotExist(err) {
		t.Error("Release must clear the pid file")
	}
	if h.sup.Release("5") {
		t.Error("Release must be idempotent")
	}
}

// TestReSuperviseReplacesSpec: a PUT for a stream already running is how the
// panel changes its source, so it must replace the command and restart rather
// than being ignored or leaving two encoders up.
func TestReSuperviseReplacesSpec(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	if err := h.sup.Supervise("5", baseSpec(dir)); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "first start", func() bool { return h.sup.State("5").Running })

	next := baseSpec(dir)
	next.Sources = []Source{{Label: "http://src/two.ts", Cmd: "ffmpeg -i two"}}
	if err := h.sup.Supervise("5", next); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "restart on the new source", func() bool {
		return h.sup.State("5").Source == "http://src/two.ts"
	})

	h.mu.Lock()
	last := h.launched[len(h.launched)-1]
	h.mu.Unlock()
	if last != "ffmpeg -i two" {
		t.Errorf("last launched %q, want the replacement command", last)
	}
}

// TestRejectsUnrunnableSpec: the daemon runs commands it is handed and composes
// none, so a spec with nothing to run is a client error, not a stream that sits
// there failing.
func TestRejectsUnrunnableSpec(t *testing.T) {
	h := newHarness(t)
	if err := h.sup.Supervise("5", Spec{}); err == nil {
		t.Error("accepted a spec with no sources")
	}
	if err := h.sup.Supervise("5", Spec{Sources: []Source{{Label: "x"}}}); err == nil {
		t.Error("accepted a source with an empty command")
	}
	if ids := h.sup.IDs(); len(ids) != 0 {
		t.Errorf("a rejected spec left %v supervised", ids)
	}
}
