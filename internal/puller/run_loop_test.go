// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// logSink is the operator log, captured. Run writes from the pull goroutine, so
// the buffer needs a lock of its own.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// captureLog redirects the operator log for the duration of a test.
func captureLog(t *testing.T) *logSink {
	t.Helper()
	sink := &logSink{}
	log.SetOutput(sink)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return sink
}

// recordWaits replaces the reconnect wait with a recorder, so a test can read
// the loop's decisions without spending the seconds. The stopAfter'th wait ends
// the puller, the way a cancelled context does.
func recordWaits(t *testing.T, stopAfter int) func() []time.Duration {
	t.Helper()
	var mu sync.Mutex
	var waits []time.Duration
	orig := waitBackoff
	t.Cleanup(func() { waitBackoff = orig })
	waitBackoff = func(_ context.Context, d time.Duration) bool {
		mu.Lock()
		waits = append(waits, d)
		n := len(waits)
		mu.Unlock()
		return n < stopAfter
	}
	return func() []time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Duration(nil), waits...)
	}
}

// TestRunResetsBackoffBeforeTheWait: a failure that follows a long healthy pull
// must reconnect at the initial backoff, not at whatever the last rough patch
// left behind.
//
// The loop used to sleep first and recompute afterwards, so the reset landed on
// the NEXT failure: a stream that flapped in the morning (backoff at the 8s
// ceiling) and then streamed cleanly for hours still opened its next incident
// with the full 8s of dead air — exactly what the reset was added to prevent.
func TestRunResetsBackoffBeforeTheWait(t *testing.T) {
	// Compress the "healthy run" bound: what is under test is the loop's
	// judgement, not the minute production spends earning it.
	origReset := backoffResetAfter
	t.Cleanup(func() { backoffResetAfter = origReset })
	backoffResetAfter = 20 * time.Millisecond
	healthy := 3 * backoffResetAfter

	captureLog(t)
	waits := recordWaits(t, 5)

	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 4 {
			// A source that is simply down: every attempt fails at once and the
			// backoff climbs to its ceiling.
			http.Error(w, "down", http.StatusNotFound)
			return
		}
		// And now it comes back and streams for longer than backoffResetAfter
		// before ending — a healthy run, whose failure is a new incident.
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(payload)
		w.(http.Flusher).Flush()
		time.Sleep(healthy)
	}))
	defer srv.Close()

	Run(context.Background(), Source{URLs: []string{srv.URL}, Label: "t"}, 12032, func([]byte) {})

	got := waits()
	if len(got) != 5 {
		t.Fatalf("recorded waits %v, want 5", got)
	}
	// The first four are the climb to the ceiling: that must keep working.
	want := []time.Duration{
		defaults.PullBackoffInitial, 2 * defaults.PullBackoffInitial,
		4 * defaults.PullBackoffInitial, defaults.PullBackoffMax,
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("wait %d = %s, want %s (the climb to the ceiling changed)", i+1, got[i], w)
		}
	}
	if got[4] != defaults.PullBackoffInitial {
		t.Fatalf("the reconnect after a healthy run waits %s, want %s: every new incident still opens with the stale ceiling",
			got[4], defaults.PullBackoffInitial)
	}
}
