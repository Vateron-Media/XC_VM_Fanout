package mitm

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// echo answers with what it got: the method and URI, a header and the body.
func echo(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Seen", r.Header.Get("X-Test"))
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, r.Method+" "+r.URL.RequestURI()+" "+string(b))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func do(t *testing.T, url, body string) (int, http.Header, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("X-Test", "t1")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, res.Header, string(b)
}

func TestProxyPassesThroughAndRecords(t *testing.T) {
	up, _ := echo(t)
	p := New(up.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()
	st, h, body := do(t, srv.URL+"/cluster/v1/heartbeat?a=1", "hello")
	if st != http.StatusAccepted || h.Get("X-Seen") != "t1" || body != "POST /cluster/v1/heartbeat?a=1 hello" {
		t.Fatalf("passed through: %d %v %q", st, h, body)
	}
	e, ok := p.Last("/heartbeat")
	if !ok || e.URI() != "/cluster/v1/heartbeat?a=1" || string(e.ReqBody) != "hello" || e.Upstream != http.StatusAccepted || string(e.ResBody) != body {
		t.Fatalf("recorded %+v", e)
	}
}

func TestProxyRewritesEitherHalf(t *testing.T) {
	up, _ := echo(t)
	p := New(up.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()
	p.Set(func(e *Exchange) {
		e.Path = "/other"
		e.ReqHeader = With(e.ReqHeader, "X-Test", "t2")
		e.ReqBody = Flip(e.ReqBody, 0)
	}, func(e *Exchange) {
		e.Status = http.StatusTeapot
		e.ResBody = append(e.ResBody, "!!"...)
	})
	st, h, body := do(t, srv.URL+"/x", "abc")
	if st != http.StatusTeapot || h.Get("X-Seen") != "t2" || body != "POST /other `bc!!" {
		t.Fatalf("rewritten: %d %v %q", st, h, body)
	}
	e, _ := p.Last("/other")
	if e.Upstream != http.StatusAccepted || string(e.UpstreamBody) != "POST /other `bc" {
		t.Fatalf("the server's own reply is kept: %+v", e)
	}
	p.Clear()
	if _, _, body := do(t, srv.URL+"/x", "abc"); body != "POST /x abc" {
		t.Fatalf("after Clear: %q", body)
	}
}

func TestProxyAnswersWithoutForwarding(t *testing.T) {
	up, hits := echo(t)
	p := New(up.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()
	_, _, first := do(t, srv.URL+"/a", "one")
	old, _ := p.Last("/a")
	p.Set(Answer(old), nil)
	if st, _, body := do(t, srv.URL+"/b", "two"); st != http.StatusAccepted || body != first {
		t.Fatalf("an old reply: %d %q", st, body)
	}
	p.Set(Refuse(http.StatusBadGateway), nil)
	if st, _, body := do(t, srv.URL+"/c", "three"); st != http.StatusBadGateway || body != "" {
		t.Fatalf("refused: %d %q", st, body)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("%d requests reached the server, want 1", n)
	}
	if e, _ := p.Last("/c"); e.Upstream != 0 {
		t.Fatalf("a request never forwarded has no upstream status: %+v", e)
	}
}

func TestReplaySendsTheSameBytes(t *testing.T) {
	up, hits := echo(t)
	p := New(up.URL)
	srv := httptest.NewServer(p)
	defer srv.Close()
	do(t, srv.URL+"/a?q=1", "body")
	e, _ := p.Last("/a")
	again, err := p.Replay(context.Background(), e, nil)
	if err != nil || again.Status != http.StatusAccepted || string(again.ResBody) != "POST /a?q=1 body" {
		t.Fatalf("replay: %v %+v", err, again)
	}
	edited, _ := p.Replay(context.Background(), e, func(e *Exchange) { e.ReqBody = []byte("other") })
	if string(edited.ResBody) != "POST /a?q=1 other" {
		t.Fatalf("an edited replay: %q", edited.ResBody)
	}
	if hits.Load() != 3 || len(p.Log()) != 1 {
		t.Fatalf("hits %d, log %d: a replay is not logged", hits.Load(), len(p.Log()))
	}
	if string(e.ReqBody) != "body" {
		t.Fatal("an edit reached the captured exchange")
	}
}

func TestEditsLeaveTheirInputAlone(t *testing.T) {
	b := []byte{0, 1, 2}
	if got := Flip(b, -1); !bytes.Equal(got, []byte{0, 1, 3}) || b[2] != 2 {
		t.Fatalf("Flip: %v, input %v", got, b)
	}
	if got := Flip(nil, 3); len(got) != 0 {
		t.Fatalf("Flip of nothing: %v", got)
	}
	h := http.Header{}
	h.Set("X-Sig", "ab0")
	h.Set("X-Ts", "100")
	for name, got := range map[string]string{
		"hex 0":   FlipHeader(h, "X-Sig").Get("X-Sig"),
		"hex a":   FlipHeader(With(h, "X-Sig", "fa"), "X-Sig").Get("X-Sig"),
		"shift":   Shift(h, "X-Ts", -7).Get("X-Ts"),
		"without": Without(h, "X-Ts").Get("X-Ts"),
	} {
		want := map[string]string{"hex 0": "ab1", "hex a": "f9", "shift": "93", "without": ""}[name]
		if got != want {
			t.Errorf("%s: %q, want %q", name, got, want)
		}
	}
	if h.Get("X-Sig") != "ab0" || h.Get("X-Ts") != "100" {
		t.Fatalf("an edit reached its input: %v", h)
	}
}
