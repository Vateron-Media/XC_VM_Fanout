package clusteragent

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// replayMain refuses the first requests of each op as told, then answers.
type replayMain struct {
	*fakeMain
	mu      sync.Mutex
	refuse  map[string][]func(w http.ResponseWriter, nonce []byte) // per op, in order
	stamps  map[string][]int64
	nonces  map[string][]string
	bodies  map[string][]map[string]any
	answers map[string]map[string]any
}

func newReplayMain(t *testing.T) (*replayMain, *Client) {
	f, st := newFake(t)
	m := &replayMain{fakeMain: f, refuse: map[string][]func(http.ResponseWriter, []byte){}, stamps: map[string][]int64{}, nonces: map[string][]string{}, bodies: map[string][]map[string]any{}, answers: map[string]map[string]any{}}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		ts, _ := strconv.ParseInt(r.Header.Get(cc.HTs), 10, 64)
		m.mu.Lock()
		m.stamps[op] = append(m.stamps[op], ts)
		m.nonces[op] = append(m.nonces[op], r.Header.Get(cc.HNonce))
		m.bodies[op] = append(m.bodies[op], f.opened(r, reqCtx))
		var next func(http.ResponseWriter, []byte)
		if q := m.refuse[op]; len(q) > 0 {
			next, m.refuse[op] = q[0], q[1:]
		}
		answer := m.answers[op]
		m.mu.Unlock()
		if next != nil {
			next(w, nonce)
			return
		}
		if answer == nil {
			answer = map[string]any{"state": "active", "mode": 1}
		}
		f.box(w, reqCtx, answer)
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	return m, NewClient(st, "xc_agent/test")
}

func (m *replayMain) queue(op string, n int, status int, reason string, extra func() map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < n; i++ {
		m.refuse[op] = append(m.refuse[op], func(w http.ResponseWriter, nonce []byte) {
			var x map[string]any
			if extra != nil {
				x = extra()
			}
			m.fakeMain.refuse(w, status, nonce, reason, x)
		})
	}
}

func (m *replayMain) count(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.stamps[op])
}

func TestReplayWithAWaitIsRetriedOnceStampedAnew(t *testing.T) {
	m, c := newReplayMain(t)
	ctx := context.Background()
	var mainAt int64
	m.queue("heartbeat", 1, 401, "REPLAY", func() map[string]any {
		// MAIN's clock runs 4 s ahead of this node's.
		mainAt = time.Now().UnixMilli() + 4000
		return map[string]any{"main_time_ms": mainAt, "retry_after_ms": 120}
	})
	start := time.Now()
	if err := c.Call(ctx, "heartbeat", map[string]any{}, nil, false); err != nil {
		t.Fatalf("the retry failed: %v", err)
	}
	if n := m.count("heartbeat"); n != 2 {
		t.Fatalf("%d requests, want 2", n)
	}
	if d := time.Since(start); d < 120*time.Millisecond {
		t.Fatalf("retried after %s, before retry_after_ms", d)
	}
	if m.nonces["heartbeat"][0] == m.nonces["heartbeat"][1] {
		t.Fatal("the retry reused the nonce")
	}
	if got := m.stamps["heartbeat"][1]; got < mainAt+120 {
		t.Fatalf("the retry was stamped %d, before MAIN's time %d + retry_after_ms (the clock was not taken from the denial)", got, mainAt)
	}
}

func TestReplayWithoutAWaitOrTwiceTakesTheUsualBackoff(t *testing.T) {
	m, c := newReplayMain(t)
	ctx := context.Background()
	m.queue("events", 1, 401, "REPLAY", nil) // a replay proper: no retry_after_ms
	err := c.Call(ctx, "events", map[string]any{}, nil, false)
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "REPLAY" || m.count("events") != 1 {
		t.Fatalf("a replay proper: %v after %d requests", err, m.count("events"))
	}
	wait := func() map[string]any { return map[string]any{"retry_after_ms": 50} }
	m.queue("hello", 3, 401, "REPLAY", wait)
	if err := c.Call(ctx, "hello", map[string]any{}, nil, false); !errors.As(err, &d) || m.count("hello") != 2 {
		t.Fatalf("two REPLAYs in a row: %v after %d requests", err, m.count("hello"))
	}
	// The wait is capped.
	if w, ok := replayWait(&Denial{Status: 401, Reason: "REPLAY", RetryAfterMs: 60000}); !ok || w > ReplayWaitMax {
		t.Fatalf("wait %s", w)
	}
	if _, ok := replayWait(&Denial{Status: 503, Reason: "RATE_LIMITED", RetryAfterMs: 1000}); ok {
		t.Fatal("a busy refusal taken for a REPLAY")
	}
}

func TestRekeyRetriesAReplayWithTheSameChallenge(t *testing.T) {
	m, st, _ := newRekeyMain(t)
	m.replays = 1
	c := NewClient(st, "xc_agent/test")
	if _, err := c.Rekey(context.Background(), map[string]any{"instance_id": "i"}); err != nil {
		t.Fatalf("re-key after a REPLAY: %v", err)
	}
	if len(m.stamps) != 2 || m.nonces[0] == m.nonces[1] || m.stamps[1] <= m.stamps[0] {
		t.Fatalf("the retry: stamps %v, nonces %v", m.stamps, m.nonces)
	}
}

func TestBusyRefusals(t *testing.T) {
	busy := &Denial{Status: 503, Reason: "RATE_LIMITED", RetryAfterMs: 2000, Op: "hello"}
	w, ok := busyWait(busy)
	if !ok || w < 1800*time.Millisecond || w > 2200*time.Millisecond {
		t.Fatalf("busy wait %s %v", w, ok)
	}
	if w, _ := busyWait(&Denial{Status: 503, Reason: "RATE_LIMITED", RetryAfterMs: 999999}); w != time.Minute {
		t.Fatalf("not clamped: %s", w)
	}
	if _, ok := busyWait(&Denial{Status: 429, Reason: "RATE_LIMITED", RetryAfterMs: 2000}); ok {
		t.Fatal("the re-key minute's 429 taken as busy")
	}
	if _, ok := busyWait(errors.New("x")); ok {
		t.Fatal("a transport error taken as busy")
	}
}

func TestSnapshotResendsABusyChunk(t *testing.T) {
	m, c := newReplayMain(t)
	a := &Agent{Client: c, Logf: t.Logf}
	reg, _, _ := newTestRegistry(t)
	a.Registry = reg
	reg.Seed([]map[string]any{{"uuid": "a", "user_id": 1}, {"uuid": "b", "user_id": 2}, {"uuid": "c", "user_id": 3}}, true)
	old := SnapshotChunk
	SnapshotChunk = 2
	defer func() { SnapshotChunk = old }()
	m.queue("conn_snapshot", 1, 503, "RATE_LIMITED", func() map[string]any { return map[string]any{"retry_after_ms": 1000, "op": "conn_snapshot"} })
	if err := a.SendSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := m.bodies["conn_snapshot"]
	if len(b) != 3 || b[0]["seq"] != float64(0) || b[1]["seq"] != float64(0) || b[1]["snap_id"] != b[0]["snap_id"] || b[2]["seq"] != float64(1) {
		t.Fatalf("chunks sent: %v", b)
	}
}

func TestHelloIsRetriedWhenMainIsBusy(t *testing.T) {
	m, c := newReplayMain(t)
	a := &Agent{Client: c, Logf: t.Logf}
	c.State.Enrolled = true
	busy := func() map[string]any { return map[string]any{"retry_after_ms": 1000, "op": "hello"} }
	m.queue("hello", 2, 503, "RATE_LIMITED", busy)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.stopCh = make(chan error, 1)
	a.helloLater(ctx, "test")
	a.helloLater(ctx, "test") // one at a time
	for a.helloing.Load() && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	if n := m.count("hello"); n != 3 {
		t.Fatalf("%d hellos, want 2 refused then 1 served", n)
	}
	// At start, Run waits retry_after_ms rather than the doubling backoff.
	m.queue("hello", 1, 503, "RATE_LIMITED", busy)
	start := time.Now()
	rctx, stop := context.WithCancel(ctx)
	go func() {
		for m.count("hello") < 5 && rctx.Err() == nil {
			time.Sleep(10 * time.Millisecond)
		}
		stop()
	}()
	a.Interval = 2 * time.Second
	a.Run(rctx)
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("start after a busy hello took %s", d)
	}
}

func TestStartingIsNeverFatal(t *testing.T) {
	m, c := newReplayMain(t)
	c.State.Enrolled = true
	a := &Agent{Client: c, Logf: t.Logf, Interval: 50 * time.Millisecond}
	starting := func() map[string]any { return map[string]any{"retry_after_ms": 1000} }
	if w, ok := busyWait(&Denial{Status: 503, Reason: "STARTING"}); !ok || w < 4*time.Second || w > 6*time.Second {
		t.Fatalf("STARTING without retry_after_ms waits %s, want about 5 s", w)
	}
	m.queue("hello", 1, 503, "STARTING", starting)
	m.queue("heartbeat", 2, 503, "STARTING", starting)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	for m.count("heartbeat") < 4 && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run ended with %v", err)
	}
	if m.count("hello") != 2 || m.count("heartbeat") < 4 {
		t.Fatalf("hello %d, heartbeat %d", m.count("hello"), m.count("heartbeat"))
	}
	// The heartbeats after a STARTING waited for it: two STARTINGs of about
	// 1 s each, then the 50 ms cadence.
	st := m.stamps["heartbeat"]
	if gap := st[2] - st[1]; gap < 900 {
		t.Fatalf("a heartbeat followed a STARTING after %d ms", gap)
	}
}

// skewedMain is a MAIN whose clock runs ahead of the node's by ahead, and
// which refuses a stamp more than 90 s off its clock as PHP's ClusterApi does:
// a signed 401 CLOCK_SKEW naming the request, with main_time_ms.
func skewedMain(t *testing.T, ahead time.Duration, deny func(f *fakeMain, w http.ResponseWriter, nonce []byte, mainMs int64)) (*Client, *[]int64) {
	f, st := newFake(t)
	var mu sync.Mutex
	var stamps []int64
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		ts, _ := strconv.ParseInt(r.Header.Get(cc.HTs), 10, 64)
		mu.Lock()
		stamps = append(stamps, ts)
		mu.Unlock()
		mainMs := time.Now().Add(ahead).UnixMilli()
		if d := ts - mainMs; d > 90000 || d < -90000 {
			deny(f, w, nonce, mainMs)
			return
		}
		f.box(w, reqCtx, map[string]any{"state": "active", "mode": 1})
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	return NewClient(st, "xc_agent/test"), &stamps
}

// A node whose clock is off by more than MAIN's 90 s window takes MAIN's time
// from the verified CLOCK_SKEW and is served on the very next request, instead
// of being refused on every op until its token looked expired.
func TestClockSkewResyncsTheClockAndRetriesOnce(t *testing.T) {
	ahead := 10 * time.Minute
	c, stamps := skewedMain(t, ahead, func(f *fakeMain, w http.ResponseWriter, nonce []byte, mainMs int64) {
		f.refuse(w, 401, nonce, "CLOCK_SKEW", map[string]any{"main_time_ms": mainMs})
	})
	var r Reply
	if err := c.Call(context.Background(), "heartbeat", map[string]any{}, &r, false); err != nil || r.State != "active" {
		t.Fatalf("after CLOCK_SKEW: %v %+v", err, r)
	}
	if len(*stamps) != 2 {
		t.Fatalf("%d requests, want 2 (the refused one and its retry)", len(*stamps))
	}
	if off := time.Duration(c.MainNowMs()-time.Now().UnixMilli()) * time.Millisecond; off < ahead-5*time.Second || off > ahead+5*time.Second {
		t.Fatalf("offset %s after resync, want about %s", off, ahead)
	}
	// And it stays in sync: the next op goes through at once.
	if err := c.Call(context.Background(), "heartbeat", map[string]any{}, nil, false); err != nil || len(*stamps) != 3 {
		t.Fatalf("next op: %v after %d requests", err, len(*stamps))
	}
}

// Only a verified CLOCK_SKEW moves the clock: one not signed by the panel, or
// bound to another request's nonce, is a transport failure and changes
// nothing. And a second CLOCK_SKEW in a row is returned, not retried again.
func TestClockSkewIsTakenOnlyFromAVerifiedDenialAndOnce(t *testing.T) {
	_, stranger, _ := ed25519.GenerateKey(nil)
	for name, deny := range map[string]func(f *fakeMain, w http.ResponseWriter, nonce []byte, mainMs int64){
		"signed by another key": func(f *fakeMain, w http.ResponseWriter, nonce []byte, mainMs int64) {
			(&fakeMain{panel: stranger, uuid: f.uuid}).refuse(w, 401, nonce, "CLOCK_SKEW", map[string]any{"main_time_ms": mainMs})
		},
		"for another request": func(f *fakeMain, w http.ResponseWriter, _ []byte, mainMs int64) {
			f.refuse(w, 401, make([]byte, 16), "CLOCK_SKEW", map[string]any{"main_time_ms": mainMs})
		},
	} {
		c, stamps := skewedMain(t, time.Hour, deny)
		err := c.Call(context.Background(), "heartbeat", map[string]any{}, nil, false)
		var d *Denial
		if err == nil || errors.As(err, &d) || !errors.Is(err, ErrTransport) {
			t.Errorf("%s: %v", name, err)
		}
		if len(*stamps) != 1 {
			t.Errorf("%s: %d requests, want 1", name, len(*stamps))
		}
		if off := c.MainNowMs() - time.Now().UnixMilli(); off > 5000 || off < -5000 {
			t.Errorf("%s: the clock moved by %d ms", name, off)
		}
	}

	// MAIN keeps refusing (its clock jumped again): the op fails after one retry.
	c, stamps := skewedMain(t, time.Hour, func(f *fakeMain, w http.ResponseWriter, nonce []byte, mainMs int64) {
		f.refuse(w, 401, nonce, "CLOCK_SKEW", map[string]any{"main_time_ms": mainMs - 3600_000})
	})
	err := c.Call(context.Background(), "heartbeat", map[string]any{}, nil, false)
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "CLOCK_SKEW" || len(*stamps) != 2 {
		t.Fatalf("two CLOCK_SKEWs in a row: %v after %d requests", err, len(*stamps))
	}
	if _, ok := skewClock(&Denial{Status: 401, Reason: "CLOCK_SKEW"}); ok {
		t.Fatal("a CLOCK_SKEW without main_time_ms moved the clock")
	}
}
