// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
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

// TestDetachDuringTheStartWindowLeavesTheEncoder: DetachAll is the shutdown
// path, and its whole contract is that the encoders keep running for the next
// daemon to adopt. A start still inside its confirmation window was killed
// anyway, so a daemon upgrade that landed while a slow source was coming up took
// that channel off air and left a pid file naming a dead pid.
func TestDetachDuringTheStartWindowLeavesTheEncoder(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(false)

	spec := baseSpec(dir)
	spec.Policy.StartTimeoutSec = 30
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	p := h.nextProcess(t)
	var killed atomic.Bool
	p.kill = func() { killed.Store(true) }
	waitFor(t, "the start window", func() bool { return h.sup.State("5").Running })

	if n := h.sup.DetachAll(); n != 1 {
		t.Fatalf("DetachAll reported %d streams, want 1", n)
	}
	if killed.Load() {
		t.Error("detach killed an encoder still in its start window; the next daemon has nothing to adopt")
	}
	if _, err := os.Stat(spec.PIDPath); err != nil {
		t.Errorf("detach left no pid file (%v); adoption reads it to find the survivor", err)
	}
}

// slowProcess is an encoder that takes a moment to die, so a test can tell
// "asked it to stop" from "waited for it to be gone".
type slowProcess struct {
	pid    int
	lag    time.Duration
	killed chan struct{}
	once   sync.Once
	reaped atomic.Bool
}

func (p *slowProcess) Pid() int { return p.pid }

func (p *slowProcess) Wait() error {
	<-p.killed
	time.Sleep(p.lag)
	p.reaped.Store(true)
	return nil
}

func (p *slowProcess) Kill() { p.once.Do(func() { close(p.killed) }) }

// TestStopDuringTheStartWindowReapsTheEncoder: stop() promises that nothing of
// the stream is still running when it returns — Release then hands the source
// over, and a re-PUT launches the replacement immediately. The cancellation
// branch of the start window killed the process and returned without waiting for
// it, so the new encoder could come up beside one that had not died yet.
func TestStopDuringTheStartWindowReapsTheEncoder(t *testing.T) {
	proc := &slowProcess{pid: 4321, lag: 120 * time.Millisecond, killed: make(chan struct{})}
	sup := New(
		func(context.Context, string, string) (Process, error) { return proc, nil },
		func(string, time.Time) bool { return false }, // never confirms
	)
	// A sleep that never reports the cancellation itself, so the wait for data
	// always comes back round to the select that watches ctx — the branch a real
	// cancel lands in when it arrives while the loop is between sleeps.
	sup.sleep = func(context.Context, time.Duration) bool {
		time.Sleep(time.Millisecond)
		return true
	}
	spec := baseSpec(t.TempDir())
	spec.Policy.StartTimeoutSec = 30
	if err := sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the start window", func() bool { return sup.State("5").Running })

	sup.Release("5")
	if !proc.reaped.Load() {
		t.Error("Release returned while the encoder it killed was still being reaped")
	}
}
