package clusteragent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The ingest permits' lane refusals (ADR 0004, "The cluster bus (Phase 2,
// fourth increment): ingest permits").

func laneBusy(lane string, ms int64, op string) func() map[string]any {
	return func() map[string]any { return map[string]any{"retry_after_ms": ms, "op": op, "lane": lane} }
}

// logSink collects an agent's log lines.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		t.Logf(format, args...)
		l.mu.Lock()
		l.lines = append(l.lines, format)
		l.mu.Unlock()
	}
}

func (l *logSink) has(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, s) {
			return true
		}
	}
	return false
}

func TestLaneRefusalIsOnlyA503RateLimitedWithALane(t *testing.T) {
	cases := []struct {
		name string
		d    *Denial
		want bool
	}{
		{"p0 lane", &Denial{Status: 503, Reason: "RATE_LIMITED", Lane: "p0", RetryAfterMs: 300}, true},
		{"bulk lane", &Denial{Status: 503, Reason: "RATE_LIMITED", Lane: "bulk", RetryAfterMs: 2000}, true},
		{"per-op semaphore (no lane)", &Denial{Status: 503, Reason: "RATE_LIMITED", RetryAfterMs: 2000}, false},
		{"re-key minute", &Denial{Status: 429, Reason: "RATE_LIMITED", Lane: "bulk"}, false},
		{"starting", &Denial{Status: 503, Reason: "STARTING", Lane: "bulk"}, false},
	}
	for _, c := range cases {
		if got := laneRefusal(c.d) != nil; got != c.want {
			t.Errorf("%s: lane refusal %v, want %v", c.name, got, c.want)
		}
	}
	if laneRefusal(context.Canceled) != nil {
		t.Error("a transport error taken as a lane refusal")
	}
}

func TestP0WaitFollowsRetryAfterWithJitterOnlyAdding(t *testing.T) {
	cases := []struct {
		ms       int64
		min, max time.Duration
	}{
		{250, 250 * time.Millisecond, 275 * time.Millisecond},
		{750, 750 * time.Millisecond, 825 * time.Millisecond},
		{0, 500 * time.Millisecond, 550 * time.Millisecond},  // none said: MAIN's middle
		{20, 100 * time.Millisecond, 100 * time.Millisecond}, // clamped up to 100 ms, no 1 s floor
		{60000, 5 * time.Second, 5 * time.Second},            // clamped down to 5 s
	}
	for _, c := range cases {
		for i := 0; i < 200; i++ {
			w := p0Wait(&Denial{Status: 503, Reason: "RATE_LIMITED", Lane: "p0", RetryAfterMs: c.ms})
			if w < c.min || w > c.max {
				t.Fatalf("retry_after_ms %d: waited %s, want %s–%s", c.ms, w, c.min, c.max)
			}
		}
	}
}

func TestBulkLaneIntervalStretchesAndComesBack(t *testing.T) {
	busy := &Denial{Status: 503, Reason: "RATE_LIMITED", Lane: "bulk", RetryAfterMs: 1000}
	long := &Denial{Status: 503, Reason: "RATE_LIMITED", Lane: "bulk", RetryAfterMs: 50000}
	type step struct {
		refuse *Denial // nil: MAIN served a batch
		cur    time.Duration
		// the wait before the next send: [min, max]
		min, max time.Duration
	}
	cases := []struct {
		name   string
		normal time.Duration
		steps  []step
	}{
		{"P1 (5 s)", 5 * time.Second, []step{
			{busy, 10 * time.Second, 10 * time.Second, 10 * time.Second},
			{busy, 20 * time.Second, 20 * time.Second, 20 * time.Second},
			{busy, 40 * time.Second, 40 * time.Second, 40 * time.Second},
			{busy, 60 * time.Second, 60 * time.Second, 60 * time.Second},
			{busy, 60 * time.Second, 60 * time.Second, 60 * time.Second}, // capped
			{nil, 30 * time.Second, 30 * time.Second, 30 * time.Second},
			{nil, 15 * time.Second, 15 * time.Second, 15 * time.Second},
			{nil, 7500 * time.Millisecond, 7500 * time.Millisecond, 7500 * time.Millisecond},
			{nil, 5 * time.Second, 5 * time.Second, 5 * time.Second},
			{nil, 5 * time.Second, 5 * time.Second, 5 * time.Second}, // never below normal
		}},
		{"P2 (10 s)", 10 * time.Second, []step{
			{busy, 20 * time.Second, 20 * time.Second, 20 * time.Second},
			{nil, 10 * time.Second, 10 * time.Second, 10 * time.Second},
		}},
		{"a busy wait longer than the interval wins", 5 * time.Second, []step{
			{long, 10 * time.Second, 45 * time.Second, 55 * time.Second},
		}},
	}
	for _, c := range cases {
		iv := newLaneInterval(c.normal)
		for i, s := range c.steps {
			var w time.Duration
			if s.refuse != nil {
				w = iv.refused(s.refuse)
			} else {
				w = iv.served()
			}
			if iv.cur != s.cur || w < s.min || w > s.max {
				t.Fatalf("%s step %d: interval %s, wait %s; want %s, %s–%s", c.name, i, iv.cur, w, s.cur, s.min, s.max)
			}
		}
	}
}

// countingTransport counts the requests that go through it.
type countingTransport struct {
	next http.RoundTripper
	n    atomic.Int32
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return c.next.RoundTrip(r)
}

func TestP0LaneRefusalResendsTheSameBatchSoonOnItsOwnConnection(t *testing.T) {
	m, c := newReplayMain(t)
	logs := &logSink{}
	a := &Agent{Client: c, Logf: logs.logf(t), SpoolDir: t.TempDir()}
	p0 := &countingTransport{next: newP0Transport()}
	c.P0HTTP = &http.Client{Timeout: 10 * time.Second, Transport: p0}
	a.cursorP0.Store(1) // MAIN's cursor is 0
	spool(t, a.SpoolDir, "p0", 1, "a", "b")
	m.answers["events"] = map[string]any{"useq": 2, "applied": 2}
	m.queue("events", 2, 503, "RATE_LIMITED", laneBusy("p0", 250, "events"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunEvents(ctx, Lanes[0]); close(done) }()
	for m.count("events") < 3 && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	b := m.bodies["events"]
	if len(b) < 3 {
		t.Fatalf("%d events requests", len(b))
	}
	for i := 1; i < 3; i++ {
		if b[i]["first_useq"] != b[0]["first_useq"] || len(b[i]["events"].([]any)) != 2 || b[i]["lane"] != "p0" {
			t.Fatalf("resend %d is not the refused batch: %v", i, b[i])
		}
	}
	// 250 ms (+ up to 10 %) and the 200 ms interval, no 1 s floor.
	for i := 1; i < 3; i++ {
		if gap := m.stamps["events"][i] - m.stamps["events"][i-1]; gap < 250 || gap >= 1000 {
			t.Fatalf("resend %d after %d ms", i, gap)
		}
	}
	if n := p0.n.Load(); n < 3 {
		t.Fatalf("%d P0 requests over P0's own connection", n)
	}
	if a.BusyRefusals() != 2 || logs.has("cluster: events") {
		t.Fatalf("busy refusals %d, logged: %v", a.BusyRefusals(), logs.lines)
	}
}

func TestBulkLaneRefusalResendsTheSameP1BatchAfterTheBusyWait(t *testing.T) {
	m, c := newReplayMain(t)
	logs := &logSink{}
	a := &Agent{Client: c, Logf: logs.logf(t), SpoolDir: t.TempDir()}
	a.cursorP1.Store(1)
	spool(t, a.SpoolDir, "p1", 1, "x")
	m.answers["events"] = map[string]any{"useq": 1, "applied": 1}
	m.queue("events", 1, 503, "RATE_LIMITED", laneBusy("bulk", 1000, "events"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunEvents(ctx, Lane{Name: "p1", Interval: 50 * time.Millisecond}); close(done) }()
	for m.count("events") < 2 && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	b := m.bodies["events"]
	if len(b) != 2 || b[1]["first_useq"] != b[0]["first_useq"] || b[1]["lane"] != "p1" {
		t.Fatalf("P1 requests: %v", b)
	}
	if gap := m.stamps["events"][1] - m.stamps["events"][0]; gap < 850 {
		t.Fatalf("P1 went again after %d ms, before MAIN's busy wait", gap)
	}
	if a.BusyRefusals() != 1 || logs.has("cluster: events") {
		t.Fatalf("busy refusals %d, logged: %v", a.BusyRefusals(), logs.lines)
	}
}

func TestARateLimitedWithoutALaneIsHandledAsBefore(t *testing.T) {
	m, c := newReplayMain(t)
	logs := &logSink{}
	a := &Agent{Client: c, Logf: logs.logf(t), SpoolDir: t.TempDir()}
	a.cursorP1.Store(1)
	spool(t, a.SpoolDir, "p1", 1, "x")
	m.answers["events"] = map[string]any{"useq": 1, "applied": 1}
	m.queue("events", 1, 503, "RATE_LIMITED", func() map[string]any { return map[string]any{"retry_after_ms": 1000, "op": "events"} })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunEvents(ctx, Lane{Name: "p1", Interval: 50 * time.Millisecond}); close(done) }()
	for m.count("events") < 2 && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if a.BusyRefusals() != 0 || !logs.has("cluster: events") {
		t.Fatalf("a per-op refusal: busy %d, logged %v", a.BusyRefusals(), logs.lines)
	}
}

func TestBulkLaneRefusalOfConfigAsksAgainAfterTheBusyWait(t *testing.T) {
	m, c := newReplayMain(t)
	logs := &logSink{}
	a := &Agent{Client: c, Logf: logs.logf(t), ReplicaDir: t.TempDir()}
	m.answers["config"] = map[string]any{"blocklist": map[string]any{"seq": 0}}
	m.queue("config", 1, 503, "RATE_LIMITED", laneBusy("bulk", 1000, "config"))
	old := ReplicaPoll
	ReplicaPoll = time.Hour
	defer func() { ReplicaPoll = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunReplica(ctx); close(done) }()
	for m.count("config") < 2 && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if m.count("config") != 2 {
		t.Fatalf("config asked %d times", m.count("config"))
	}
	if gap := m.stamps["config"][1] - m.stamps["config"][0]; gap < 850 || gap > 3000 {
		t.Fatalf("config asked again after %d ms", gap)
	}
	if a.BusyRefusals() != 1 || logs.has("cluster: replica") {
		t.Fatalf("busy refusals %d, logged %v", a.BusyRefusals(), logs.lines)
	}
}

func TestBulkLaneRefusalOfASnapshotChunkResendsIt(t *testing.T) {
	m, c := newReplayMain(t)
	a := &Agent{Client: c, Logf: t.Logf}
	reg, _, _ := newTestRegistry(t)
	a.Registry = reg
	reg.Seed([]map[string]any{{"uuid": "a", "user_id": 1}}, true)
	m.queue("conn_snapshot", 2, 503, "RATE_LIMITED", laneBusy("bulk", 1000, "conn_snapshot"))
	if err := a.SendSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := m.bodies["conn_snapshot"]
	if len(b) != 3 || b[2]["snap_id"] != b[0]["snap_id"] || b[2]["seq"] != float64(0) || a.BusyRefusals() != 2 {
		t.Fatalf("chunks %v, busy %d", b, a.BusyRefusals())
	}
}

func TestRecordingCompleteAnswersTheSocketWithinItsDeadline(t *testing.T) {
	cases := []struct {
		name     string
		refusals int
		deadline time.Duration
		status   int
		calls    int
	}{
		// 1 s wait + a 10 s try ends by 12 s: sent again, then served.
		{"one refusal, sent again", 1, 12 * time.Second, http.StatusOK, 2},
		// The busy wait is 1–1.1 s: the first fits (at most 11.1 s), the
		// second, after at least 1 s, would end past 11.5 s and goes back.
		{"two refusals, handed back", 2, 11500 * time.Millisecond, http.StatusConflict, 2},
		// No room for a wait and a whole try at all.
		{"no room", 1, 5 * time.Second, http.StatusConflict, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, cl := newReplayMain(t)
			logs := &logSink{}
			a := &Agent{Client: cl, Logf: logs.logf(t)}
			m.answers["recording_complete"] = map[string]any{"vod_id": 7}
			m.queue("recording_complete", c.refusals, 503, "RATE_LIMITED", laneBusy("bulk", 1000, "recording_complete"))
			old := SocketDeadline
			SocketDeadline = c.deadline
			defer func() { SocketDeadline = old }()
			rec := httptest.NewRecorder()
			start := time.Now()
			a.socketHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/main/recording_complete", strings.NewReader(`{"id":1}`)))
			if rec.Code != c.status || m.count("recording_complete") != c.calls {
				t.Fatalf("status %d after %d call(s): %s", rec.Code, m.count("recording_complete"), rec.Body)
			}
			if time.Since(start) > c.deadline || logs.has("cluster: socket") {
				t.Fatalf("answered after %s, logged %v", time.Since(start), logs.lines)
			}
		})
	}
}

func TestBulkLaneRefusalOfP2TouchesWaitsAndSendsThemAfter(t *testing.T) {
	m, a, _, _ := newTouchAgent(t)
	logs := &logSink{}
	a.Logf = logs.logf(t)
	a.publish(p2Reply)
	a.Registry.Put("h1", rec(7, "10.0.0.1", 100, 100))
	a.Registry.Touch("h1", 101)
	m.status, m.reason, m.extra = 503, "RATE_LIMITED", laneBusy("bulk", 1000, "events")()
	old := TouchLoop
	TouchLoop = 50 * time.Millisecond
	defer func() { TouchLoop = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { a.RunTouches(ctx); close(done) }()
	for len(m.sent()) < 2 && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	m.mu.Lock()
	at := append([]time.Time{}, m.at...)
	m.mu.Unlock()
	sent := m.sent()
	if len(sent) != 2 || sent[1]["d"].(map[string]any)["hls_last_read"] != float64(101) {
		t.Fatalf("touches sent: %v", sent)
	}
	if gap := at[1].Sub(at[0]); gap < 900*time.Millisecond {
		t.Fatalf("P2 went again after %s, before MAIN's busy wait", gap)
	}
	if a.BusyRefusals() != 1 || logs.has("cluster: events p2") {
		t.Fatalf("busy refusals %d, logged %v", a.BusyRefusals(), logs.lines)
	}
}
