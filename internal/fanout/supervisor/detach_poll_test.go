// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestDetachStopsPollingAnAdoptedEncoder: an adopted encoder is watched by a
// goroutine polling its liveness once a second, because it is not our child and
// cannot be waited on. A detach leaves that encoder running on purpose — that is
// what the next daemon adopts — so the poller has nobody left to report to, and
// nothing ever stopped it: one goroutine per adopted stream, each waking every
// second for as long as its encoder lives.
func TestDetachStopsPollingAnAdoptedEncoder(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(false) // the survivor's feed into this daemon has not come back

	w := newWorld()
	const survivor = 9100
	w.add(survivor, "ffmpeg -i http://src/one.ts -f hls /home/xc_vm/streams/5_.m3u8")
	h.sup.find = w.find
	h.sup.killPID = w.kill

	var mu sync.Mutex
	polls := 0
	inner := h.sup.sleep
	h.sup.sleep = func(ctx context.Context, d time.Duration) bool {
		if d == adoptPollInterval {
			mu.Lock()
			polls++
			mu.Unlock()
		}
		return inner(ctx, d)
	}
	pollCount := func() int { mu.Lock(); defer mu.Unlock(); return polls }

	spec := baseSpec(dir)
	spec.Policy.StartTimeoutSec = 30 // the detach lands inside the start window
	spec.AdoptMatch = "/streams/5_"
	writePID(t, spec.PIDPath, survivor)
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "adoption", func() bool { return h.sup.State("5").Adopted })
	waitFor(t, "the liveness poll to be running", func() bool { return pollCount() > 2 })

	if n := h.sup.DetachAll(); n != 1 {
		t.Fatalf("DetachAll detached %d streams, want 1", n)
	}
	// The encoder is still running — that is what a detach is for.
	if _, alive := w.find(survivor); !alive {
		t.Fatal("the detach killed the encoder it was supposed to leave for the next daemon")
	}

	settled := pollCount()
	time.Sleep(100 * time.Millisecond)
	if n := pollCount(); n > settled+1 {
		t.Errorf("the liveness poll ran %d more times after the detach, want it stopped", n-settled)
	}
}

// TestAVanishedSurvivorIsExplainedWithoutANilError: an adopted encoder's Wait
// reports nil, because its exit status belongs to init and not to us, and a
// process we launched ourselves can exit 0 too. Formatting that with %v told the
// operator the start failed because of "<nil>", which explains nothing.
func TestAVanishedSurvivorIsExplainedWithoutANilError(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(false) // nothing confirms this start

	spec := baseSpec(dir)
	spec.Policy.StartTimeoutSec = 30 // long: the process ends first
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	p := h.nextProcess(t)
	waitFor(t, "the start window", func() bool { return h.sup.State("5").Running })
	p.exit <- nil // gone, with nothing to say about why

	waitFor(t, "the failed start to be reported", func() bool {
		return h.sup.State("5").LastError != ""
	})
	if got := h.sup.State("5").LastError; strings.Contains(got, "nil") {
		t.Errorf("LastError = %q, want an explanation rather than a formatted nil", got)
	}
}
