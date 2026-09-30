package clusteragent

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// beatMain records when each heartbeat arrived; slow ops hang until the
// request is given up.
type beatMain struct {
	*fakeMain
	mu    sync.Mutex
	beats []time.Time
	slow  map[string]bool
}

func newBeatMain(t *testing.T, urls ...string) (*beatMain, *Agent) {
	f, st := newFake(t)
	m := &beatMain{fakeMain: f, slow: map[string]bool{}}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		m.mu.Lock()
		if op == "heartbeat" {
			m.beats = append(m.beats, time.Now())
		}
		slow := m.slow[op] || (op == "heartbeat" && len(m.beats)%2 == 0 && m.slow["every-other-heartbeat"])
		m.mu.Unlock()
		if slow {
			io.Copy(io.Discard, r.Body) // so the server notices the client giving up
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			return
		}
		f.box(w, reqCtx, map[string]any{"state": "active", "mode": 1})
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = append(urls, srv.URL+"/cluster/v1/")
	st.Enrolled = true
	return m, &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
}

func (m *beatMain) gaps() (n int, worst time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 1; i < len(m.beats); i++ {
		worst = max(worst, m.beats[i].Sub(m.beats[i-1]))
	}
	return len(m.beats), worst
}

func runFor(t *testing.T, a *Agent, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := a.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
}

func TestHeartbeatsStayWithinTheGapWhenMainIsSlow(t *testing.T) {
	old := MaxHeartbeatGap
	MaxHeartbeatGap = 300 * time.Millisecond
	defer func() { MaxHeartbeatGap = old }()
	m, a := newBeatMain(t)
	a.Interval = 10 * time.Second // capped at the gap
	m.slow["every-other-heartbeat"] = true
	runFor(t, a, 2*time.Second)
	n, worst := m.gaps()
	if n < 4 || worst > MaxHeartbeatGap+300*time.Millisecond {
		t.Fatalf("%d heartbeats, worst gap %s", n, worst)
	}
}

func TestATokenRefreshDoesNotHoldTheHeartbeats(t *testing.T) {
	m, a := newBeatMain(t)
	m.slow["token_refresh"] = true
	a.Interval = 100 * time.Millisecond
	// The token is due for refresh from the start.
	s, _ := a.Client.current()
	s.tok.RefreshAt = 0
	runFor(t, a, 1500*time.Millisecond)
	if n, worst := m.gaps(); n < 8 || worst > 500*time.Millisecond {
		t.Fatalf("%d heartbeats, worst gap %s while a refresh hung", n, worst)
	}
}

// A refresh MAIN refuses for want of a licence is asked again after a wait
// that doubles up to RefreshRetryMax, not at every tick: a fleet without a
// licence no longer asks every 2 s, and still gets its token and lease
// within RefreshRetryMax of the licence's return (ADR 0004, "Re-licensing a
// fleet").
func TestARefusedRefreshBacksOffAndIsStillAskedAgain(t *testing.T) {
	oldMin, oldMax := RefreshRetryMin, RefreshRetryMax
	RefreshRetryMin, RefreshRetryMax = 100*time.Millisecond, 400*time.Millisecond
	t.Cleanup(func() { RefreshRetryMin, RefreshRetryMax = oldMin, oldMax })
	f, st := newFake(t)
	var mu sync.Mutex
	var asked []time.Time
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		if strings.HasSuffix(r.URL.Path, "/token_refresh") {
			mu.Lock()
			asked = append(asked, time.Now())
			mu.Unlock()
			f.refuse(w, 403, nonce, "LICENCE_INVALID", nil)
			return
		}
		f.box(w, reqCtx, map[string]any{"state": "active", "mode": 1})
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs, st.Enrolled = []string{srv.URL + "/cluster/v1/"}, true
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Interval: 20 * time.Millisecond, Logf: t.Logf}
	s, _ := a.Client.current()
	s.tok.RefreshAt = 0 // due from the start
	runFor(t, a, 2*time.Second)
	mu.Lock()
	defer mu.Unlock()
	// About 100 ticks: asked at 0, then after 0.1, 0.2, 0.4, 0.4, … s.
	if len(asked) < 4 || len(asked) > 10 {
		t.Fatalf("a refused refresh was asked %d times in 2 s of 20 ms ticks, want the backoff's 4-10", len(asked))
	}
	for i := 2; i < len(asked); i++ {
		if gap := asked[i].Sub(asked[i-1]); gap < 150*time.Millisecond {
			t.Fatalf("ask %d came %s after the one before, inside the backoff", i, gap)
		}
	}
	if gap := asked[len(asked)-1].Sub(asked[len(asked)-2]); gap > 600*time.Millisecond {
		t.Fatalf("asked again only after %s, past RefreshRetryMax", gap)
	}
}

func TestAnUnreachableURLGoesLast(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := "http://" + l.Addr().String() + "/cluster/v1/"
	l.Close() // connection refused
	_, a := newBeatMain(t, dead)
	ctx := context.Background()
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if got := a.Client.urls(); got[len(got)-1] != dead {
		t.Fatalf("order after a failure: %v", got)
	}
	// Retried first once URLRetry has passed.
	a.Client.now = func() time.Time { return time.Now().Add(URLRetry + time.Second) }
	if got := a.Client.urls(); got[0] != dead {
		t.Fatalf("order after URLRetry: %v", got)
	}
}

// run.sh judges a new binary by ReachedFile: an answered heartbeat touches
// it (the first of the run at once, then at most every ReachedEvery), and a
// heartbeat MAIN never answered does not.
func TestAnAnsweredHeartbeatMarksTheNodeReached(t *testing.T) {
	_, a := newBeatMain(t)
	p := filepath.Join(filepath.Dir(a.Client.State.path), ReachedFile)
	ctx := context.Background()
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("not marked after an answer: %v", err)
	}
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	os.Chtimes(p, old, old)
	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(p); !st.ModTime().Equal(old) {
		t.Fatalf("touched again within ReachedEvery (%s)", st.ModTime())
	}

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := "http://" + l.Addr().String() + "/cluster/v1/"
	l.Close()
	_, st := newFake(t)
	st.MainURLs, st.Enrolled = []string{dead}, true
	b := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	if _, err := b.Heartbeat(ctx); err == nil {
		t.Fatal("a heartbeat to nowhere was answered")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(st.path), ReachedFile)); !os.IsNotExist(err) {
		t.Fatalf("marked without an answer: %v", err)
	}
}
