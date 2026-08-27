package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// syncBuffer is a concurrency-safe io.Writer: the debug-stats goroutine writes
// to it from another goroutine while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// captureDebugLog turns debug mode on, redirects the standard logger to a
// concurrency-safe buffer, and restores both afterwards.
func captureDebugLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	old := log.Writer()
	log.SetOutput(buf)
	dlog.Enable(true)
	t.Cleanup(func() {
		dlog.Enable(false)
		log.SetOutput(old)
	})
	return buf
}

// waitFor polls until cond is true or the deadline elapses.
func waitFor(cond func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func TestStartDebugStatsSnapshotsStreamState(t *testing.T) {
	buf := captureDebugLog(t)

	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	feedStream(mgr.GetOrCreate("42"))

	ctx, cancel := context.WithCancel(context.Background())
	mgr.StartDebugStats(ctx, 20*time.Millisecond)

	if !waitFor(func() bool { return strings.Contains(buf.String(), "[dbg stats") }) {
		t.Fatalf("no stats snapshot emitted; log=%q", buf.String())
	}
	cancel()
	// Let the goroutine observe ctx.Done() before Cleanup restores the writer.
	time.Sleep(40 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "id=42") || !strings.Contains(out, "data_age=") {
		t.Fatalf("stats snapshot missing stream state: %q", out)
	}
}

func TestStartDebugStatsReportsNoStreams(t *testing.T) {
	buf := captureDebugLog(t)

	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	mgr.StartDebugStats(ctx, 20*time.Millisecond)

	if !waitFor(func() bool { return strings.Contains(buf.String(), "no streams registered") }) {
		t.Fatalf("empty-manager snapshot missing; log=%q", buf.String())
	}
	cancel()
	time.Sleep(40 * time.Millisecond)
}

func TestStartDebugStatsIsNoOpWhenIdle(t *testing.T) {
	// every <= 0 with debug on: must return without starting a goroutine.
	buf := captureDebugLog(t)
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	feedStream(mgr.GetOrCreate("x"))
	mgr.StartDebugStats(context.Background(), 0)
	time.Sleep(30 * time.Millisecond)
	if strings.Contains(buf.String(), "[dbg stats") {
		t.Fatalf("stats emitted despite every<=0: %q", buf.String())
	}

	// debug off: must return immediately regardless of interval.
	dlog.Enable(false)
	mgr.StartDebugStats(context.Background(), 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if strings.Contains(buf.String(), "[dbg stats") {
		t.Fatalf("stats emitted while debug disabled: %q", buf.String())
	}
}

// timeoutErr is a net.Error whose Timeout() is true — a stalled-write signal.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestWriteFailReason(t *testing.T) {
	if got := writeFailReason(timeoutErr{}); !strings.Contains(got, "stalled past timeout") {
		t.Fatalf("timeout error → %q, want stalled-past-timeout", got)
	}
	if got := writeFailReason(errors.New("broken pipe")); got != "write failed: broken pipe" {
		t.Fatalf("plain error → %q, want %q", got, "write failed: broken pipe")
	}
}

// TestStartReaperIdleStops drives the reaper goroutine end-to-end: a running,
// control-managed stream with no viewers and stale access gets its puller
// stopped within the grace window.
func TestStartReaperIdleStops(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, 40*time.Millisecond)
	st := mgr.GetOrCreate("r")

	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.mu.Lock()
	st.cfg = &puller.Source{URLs: []string{"x"}}
	st.running = true
	st.cancel = cancel
	st.mu.Unlock()
	st.lastAccess.Store(time.Now().Add(-time.Second).UnixNano())

	rctx, rcancel := context.WithCancel(context.Background())
	defer rcancel()
	mgr.StartReaper(rctx)

	stopped := waitFor(func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return !st.running
	})
	if !stopped {
		t.Fatal("reaper did not idle-stop an unattended running stream")
	}
}

// TestRunPinnedLifecycle: a launch/test feed marks the stream running with a
// pin ref immediately, is exempt from the idle-stop reaper, and clears running
// when its context is cancelled.
func TestRunPinnedLifecycle(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A source that refuses fast so the puller loops without ever yielding data.
	mgr.RunPinned(ctx, "L", puller.Source{URLs: []string{"http://127.0.0.1:1/dead.ts"}}, 0)

	st := mgr.Get("L")
	if st == nil {
		t.Fatal("pinned stream missing after RunPinned")
	}
	if s := st.status(); !s.Running || s.Refs != 1 {
		t.Fatalf("pinned status = %+v, want running with a pin ref", s)
	}

	// The reaper must leave a pinned stream alone even with stale access (refs>0).
	st.lastAccess.Store(time.Now().Add(-time.Hour).UnixNano())
	st.mu.Lock()
	st.idleStopLocked(time.Now())
	st.mu.Unlock()
	if !st.status().Running {
		t.Fatal("idle-stop hit a pinned launch feed (refs>0 must be exempt)")
	}

	// Cancelling the context stops the puller and clears running.
	cancel()
	stopped := waitFor(func() bool { return !st.status().Running })
	if !stopped {
		t.Fatal("pinned puller did not clear running after context cancel")
	}
}

func TestServeSignalQueuesOverlay(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	if code := ctlRequest(t, ts.URL, http.MethodPost, "/signal/u1", `{"message":"hello","ttl":5}`); code != http.StatusNoContent {
		t.Fatalf("POST /signal/u1: %d, want 204", code)
	}
	if !mgr.signals.peek("u1") {
		t.Fatal("signal was not queued for u1")
	}

	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"empty message", http.MethodPost, "/signal/u2", `{"message":""}`, http.StatusBadRequest},
		{"malformed json", http.MethodPost, "/signal/u3", `{nope`, http.StatusBadRequest},
		{"missing uuid", http.MethodPost, "/signal/", `{"message":"x"}`, http.StatusBadRequest},
		{"wrong method", http.MethodGet, "/signal/u1", "", http.StatusMethodNotAllowed},
	}
	for _, c := range cases {
		if code := ctlRequest(t, ts.URL, c.method, c.path, c.body); code != c.want {
			t.Errorf("%s: %d, want %d", c.name, code, c.want)
		}
	}
}

func TestServeIngestLifecycle(t *testing.T) {
	dir := t.TempDir()
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.SetIngestDir(dir)
	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	// PUT returns the producer socket path and (with key/iv) arms HLS encryption.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/ingest/9",
		strings.NewReader(`{"key":"000102030405060708090a0b0c0d0e0f","iv":"0f0e0d0c0b0a09080706050403020100"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /ingest/9: %v", err)
	}
	var out struct {
		Socket string `json:"socket"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || out.Socket == "" {
		t.Fatalf("PUT /ingest/9 = %d socket=%q, want 200 with a socket", resp.StatusCode, out.Socket)
	}
	if st := mgr.Get("9"); st == nil || st.hlsKey == nil {
		t.Fatal("ingest PUT with key/iv did not arm HLS encryption")
	}

	// DELETE tears it down.
	if code := ctlRequest(t, ts.URL, http.MethodDelete, "/ingest/9", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE /ingest/9: %d, want 204", code)
	}

	// Bad requests.
	if code := ctlRequest(t, ts.URL, http.MethodPatch, "/ingest/9", ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("PATCH /ingest/9: %d, want 405", code)
	}
	if code := ctlRequest(t, ts.URL, http.MethodPut, "/ingest/", ""); code != http.StatusNotFound {
		t.Fatalf("PUT /ingest/ (missing id): %d, want 404", code)
	}
}

// TestServeIngestWithoutDirFails covers the RegisterIngest error path: a manager
// with no ingest dir set returns 500 rather than a socket.
func TestServeIngestWithoutDirFails(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second) // no SetIngestDir
	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	if code := ctlRequest(t, ts.URL, http.MethodPut, "/ingest/9", ""); code != http.StatusInternalServerError {
		t.Fatalf("PUT /ingest/9 without ingest dir: %d, want 500", code)
	}
}
