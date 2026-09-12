// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRestartIsConfirmedOnlyByItsOwnData: a start is confirmed by bytes that
// arrived after it was launched. The daemon's stream outlives every encoder, so
// "has this stream ever had data" confirmed every start after the first on its
// first poll: a dead primary never counted as a failed start, and the backup was
// never tried.
func TestRestartIsConfirmedOnlyByItsOwnData(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "http://src/primary.ts", Cmd: "ffmpeg -i primary"},
		{Label: "http://src/backup.ts", Cmd: "ffmpeg -i backup"},
	}
	spec.Policy.StartTimeoutSec = 1
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}

	// The primary works once...
	p := h.nextProcess(t)
	h.dataNow()
	waitFor(t, "first start confirmed", func() bool { return h.sup.State("5").Confirmed })

	// ...then its encoder exits and the source is dead: no more bytes, ever.
	p.exit <- nil

	waitFor(t, "a start on the backup", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, c := range h.launched {
			if strings.Contains(c, "backup") {
				return true
			}
		}
		return false
	})
}

// TestStopEndsTheEncoderFromTheLoop: ending the encoder is the loop's job on
// its way out of a stop, not stop()'s. stop() used to cancel the loop and then
// read the process to kill — and the loop, woken by the cancel, clears that
// field as it leaves, so a stop that lost the race killed nothing and the
// encoder outlived its stream. Here nobody but the loop can kill it: the
// cancellation alone, flagged as a stop, must end it.
func TestStopEndsTheEncoderFromTheLoop(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stop     bool
		wantKill bool
	}{
		{"stop", true, true},
		{"detach leaves it for the next daemon", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.setData(true)
			if err := h.sup.Supervise("5", baseSpec(t.TempDir())); err != nil {
				t.Fatal(err)
			}
			p := h.nextProcess(t)
			var killed atomic.Bool
			p.kill = func() { killed.Store(true) }
			waitFor(t, "running", func() bool { return h.sup.State("5").Running })

			h.sup.mu.Lock()
			st := h.sup.procs["5"]
			h.sup.mu.Unlock()
			st.killOnExit.Store(tc.stop)
			st.cancel() // the loop alone decides the encoder's fate now
			<-st.done

			if killed.Load() != tc.wantKill {
				t.Fatalf("encoder killed = %v, want %v", killed.Load(), tc.wantKill)
			}
		})
	}
}

// TestConcurrentHandOversLeaveOneEncoder: two PUTs for one stream at once (the
// panel's start and its cron pass, say) must end with exactly one supervised
// encoder. The second used to install its stream while the first was stopping
// the old one, and the first then overwrote it — an orphaned loop and encoder
// feeding the same ingest, which no Release could reach.
func TestConcurrentHandOversLeaveOneEncoder(t *testing.T) {
	h := newHarness(t)
	h.setData(true)
	dir := t.TempDir()
	if err := h.sup.Supervise("5", baseSpec(dir)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "running", func() bool { return h.sup.State("5").Running })

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.sup.Supervise("5", baseSpec(dir))
		}()
	}
	wg.Wait()
	waitFor(t, "running", func() bool { return h.sup.State("5").Running })

	h.sup.Release("5")
	// Every encoder any hand-over started must be dead now.
	h.mu.Lock()
	procs := append([]*fakeProcess(nil), h.procs...)
	h.mu.Unlock()
	for _, p := range procs {
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("encoder pid %d is still running after Release: a hand-over orphaned it", p.pid)
		}
	}
}
