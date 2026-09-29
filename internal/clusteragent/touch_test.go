package clusteragent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// p2Main takes P2 batches (or refuses them as told) and records what came.
type p2Main struct {
	*fakeMain
	mu      sync.Mutex
	batches []map[string]any
	status  int // 0: take; else refuse with this status
	reason  string
	extra   map[string]any // the refusal's other fields
	at      []time.Time    // when each batch arrived
}

func newTouchAgent(t *testing.T) (*p2Main, *Agent, *sink, *time.Time) {
	f, st := newFake(t)
	m := &p2Main{fakeMain: f}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		req := f.opened(r, reqCtx)
		if !strings.HasSuffix(r.URL.Path, "/events") || req["lane"] != "p2" {
			http.Error(w, "unexpected", 500)
			return
		}
		m.mu.Lock()
		m.batches = append(m.batches, req)
		m.at = append(m.at, time.Now())
		status, reason, extra := m.status, m.reason, m.extra
		if extra != nil {
			m.status, m.extra = 0, nil // a busy refusal is refused once
		}
		m.mu.Unlock()
		if status != 0 {
			m.refuse(w, status, nonce, reason, extra)
			return
		}
		evs, _ := req["events"].([]any)
		m.box(w, reqCtx, map[string]any{"ok": true, "useq": 0, "applied": len(evs), "dropped": 0, "main_time_ms": time.Now().UnixMilli()})
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	reg, s, now := newTestRegistry(t)
	a.Registry, reg.P2 = reg, a.p2Touch.Load
	return m, a, s, now
}

func (m *p2Main) sent() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []map[string]any
	for _, b := range m.batches {
		for _, e := range b["events"].([]any) {
			out = append(out, e.(map[string]any))
		}
	}
	return out
}

var p2Reply = &Reply{State: "active", Flows: FlowConnections, P2Types: []string{"conn.touch"}}

func TestTouchesGoOnP2OnlyWhileMainTakesThem(t *testing.T) {
	m, a, s, now := newTouchAgent(t)
	ctx := context.Background()
	a.publish(&Reply{State: "active", Flows: FlowConnections}) // an older MAIN: no p2_types
	a.Registry.Put("h1", rec(7, "10.0.0.1", 100, 100))
	*now = now.Add(11 * time.Second)
	a.Registry.Touch("h1", 111)
	if s.types() != "conn.upsert,conn.upsert" {
		t.Fatalf("without P2 a touch is a throttled P0 upsert: %s", s.types())
	}

	a.publish(p2Reply)
	*now = now.Add(11 * time.Second)
	a.Registry.Touch("h1", "122") // a numeric string, as a seeded record may hold
	a.Registry.Touch("h1", 123)
	if s.types() != "conn.upsert,conn.upsert" {
		t.Fatalf("with P2 a touch emitted on P0: %s", s.types())
	}
	if err := a.sendTouches(ctx); err != nil {
		t.Fatal(err)
	}
	got := m.sent()
	if len(got) != 1 || got[0]["type"] != "conn.touch" {
		t.Fatalf("P2 batch: %v", got)
	}
	d := got[0]["d"].(map[string]any)
	if d["uuid"] != "h1" || d["hls_last_read"] != float64(123) || got[0]["t"] == nil {
		t.Fatalf("touch %v", got[0])
	}
	if _, ok := m.batches[0]["first_useq"]; ok {
		t.Fatal("first_useq sent on P2")
	}
	// Within 60 s nothing more goes, whatever changes.
	a.Registry.Touch("h1", 130)
	a.sendTouches(ctx)
	if len(m.sent()) != 1 {
		t.Fatal("a uuid's touch went twice within TouchP2Every")
	}
	*now = now.Add(TouchP2Every)
	a.sendTouches(ctx)
	if got := m.sent(); len(got) != 2 || got[1]["d"].(map[string]any)["hls_last_read"] != float64(130) {
		t.Fatalf("after 60 s the newest value goes: %v", got)
	}
	// A value MAIN already has is not sent again.
	*now = now.Add(TouchP2Every)
	a.Registry.Touch("h1", 130)
	a.sendTouches(ctx)
	if len(m.sent()) != 2 {
		t.Fatal("an unchanged value went again")
	}

	// Any other change goes on P0 at once, with the current hls_last_read,
	// and drops the pending touch.
	a.Registry.Touch("h1", 140)
	moved := rec(7, "10.0.0.1", 100, 141)
	moved["pid"] = 99
	a.Registry.Put("h1", moved)
	if !strings.HasSuffix(s.types(), ",conn.upsert") || len(s.events) != 3 {
		t.Fatalf("a real change: %s", s.types())
	}
	*now = now.Add(TouchP2Every)
	a.sendTouches(ctx)
	if len(m.sent()) != 2 {
		t.Fatal("a touch the P0 upsert carried went on P2 too")
	}
}

func TestLosingP2CatchesMainsStoreUpOnP0(t *testing.T) {
	_, a, s, now := newTouchAgent(t)
	ctx := context.Background()
	a.publish(p2Reply)
	a.Registry.Put("h1", rec(7, "10.0.0.1", 100, 100))
	a.Registry.Put("h2", rec(8, "10.0.0.2", 100, 100))
	ended := rec(9, "10.0.0.3", 100, 100)
	ended["hls_end"] = 1
	a.Registry.Put("h3", ended)
	a.Registry.Touch("h1", 150)
	a.sendTouches(ctx) // h1's 150 reached the bus, not the store
	*now = now.Add(time.Second)
	n := len(s.events)

	// MAIN rolled back: the next reply lists no p2_types.
	a.publish(&Reply{State: "active", Flows: FlowConnections})
	if len(s.events) != n+1 {
		t.Fatalf("catch-up: %d new P0 events, want 1 (h1 only)", len(s.events)-n)
	}
	d := s.events[n]["d"].(map[string]any)["record"].(map[string]any)
	if d["uuid"] != "h1" || intOf(d["hls_last_read"]) != 150 {
		t.Fatalf("catch-up sent %v", d)
	}
	// Then the 10 s P0 touches again.
	*now = now.Add(TouchEvery)
	a.Registry.Touch("h2", 160)
	if len(s.events) != n+2 {
		t.Fatal("the P0 touches did not resume")
	}
}

func TestP2RefusalsAndCONNECTIONSOff(t *testing.T) {
	m, a, s, now := newTouchAgent(t)
	ctx := context.Background()
	a.publish(p2Reply)
	a.Registry.Put("h1", rec(7, "10.0.0.1", 100, 100))
	a.Registry.Touch("h1", 101)

	// A 503 keeps the pending touch for a retry.
	m.status, m.reason = 503, "DB"
	if err := a.sendTouches(ctx); err == nil {
		t.Fatal("a 503 was taken as sent")
	}
	m.status = 0
	a.sendTouches(ctx)
	if got := m.sent(); len(got) != 2 {
		t.Fatalf("the touch was not retried: %v", got)
	}

	// A 400 BAD_REQUEST (a MAIN without P2) stops P2 and catches up on P0.
	*now = now.Add(TouchP2Every)
	a.Registry.Touch("h1", 102)
	m.status, m.reason = 400, "BAD_REQUEST"
	n := len(s.events)
	lctx, stop := context.WithCancel(ctx)
	old := TouchLoop
	TouchLoop = 10 * time.Millisecond
	defer func() { TouchLoop = old }()
	done := make(chan struct{})
	go func() { a.RunTouches(lctx); close(done) }()
	for i := 0; i < 200 && a.p2Touch.Load(); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	<-done
	if a.p2Touch.Load() || len(s.events) != n+1 {
		t.Fatalf("after a 400: P2 %v, %d catch-up events", a.p2Touch.Load(), len(s.events)-n)
	}

	// CONNECTIONS off: nothing is sent, the pending touches are dropped.
	a.publish(p2Reply)
	a.Registry.Touch("h1", 103)
	n = len(s.events)
	a.publish(&Reply{State: "active", Flows: 0, P2Types: []string{"conn.touch"}})
	if len(s.events) != n || len(a.Registry.pending) != 0 {
		t.Fatalf("CONNECTIONS off sent %d events, kept %d touches", len(s.events)-n, len(a.Registry.pending))
	}
}

func TestPendingTouchesLeaveWithTheRecord(t *testing.T) {
	_, a, _, _ := newTouchAgent(t)
	a.publish(p2Reply)
	for _, u := range []string{"a", "b", "c"} {
		a.Registry.Put(u, rec(7, "10.0.0.1", 100, 100))
		a.Registry.Touch(u, 101)
	}
	a.Registry.Delete("a")
	a.Registry.Close("b", true)
	a.Registry.Close("c", false)
	if len(a.Registry.dueTouches(10)) != 0 {
		t.Fatalf("touches outlived their records: %v", a.Registry.dueTouches(10))
	}
	a.Registry.Put("d", rec(7, "10.0.0.1", 100, 100))
	a.Registry.Touch("d", 101)
	a.Registry.Seed(nil, true)
	if len(a.Registry.dueTouches(10)) != 0 {
		t.Fatal("a resetting seed kept a touch")
	}
}

// sendTouches sends every touch due, a batch after another, as the P2 lane
// does.
func (a *Agent) sendTouches(ctx context.Context) error {
	for {
		served, err := a.sendTouchBatch(ctx)
		if err != nil || !served {
			return err
		}
	}
}
