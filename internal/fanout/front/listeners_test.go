package front

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// noKeepAlive is a client that never reuses a connection, so a GET to a dropped
// port fails rather than hitting a cached connection from an earlier test.
var noKeepAlive = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })
}

// getBodyRetry gets http://127.0.0.1:<port>/ , retrying until the Serve
// goroutine is accepting (bind is synchronous but Accept runs in a goroutine).
func getBodyRetry(t *testing.T, port int) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := noKeepAlive.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return string(b)
		}
		if time.Now().After(deadline) {
			t.Fatalf("port %d never served: %v", port, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func serving(port int) bool {
	resp, err := noKeepAlive.Get(fmt.Sprintf("http://127.0.0.1:%d/", port))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

func TestReconcileAddsAndServes(t *testing.T) {
	m := NewManager(okHandler(), nil, time.Second)
	defer m.Shutdown(context.Background())
	p1, p2 := freePort(t), freePort(t)
	if err := m.Reconcile([]ListenSpec{{Port: p1}, {Port: p2}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, p := range []int{p1, p2} {
		if body := getBodyRetry(t, p); body != "ok" {
			t.Fatalf("port %d body=%q, want ok", p, body)
		}
	}
}

func TestReconcileDropsRemovedPort(t *testing.T) {
	m := NewManager(okHandler(), nil, time.Second)
	defer m.Shutdown(context.Background())
	p1, p2 := freePort(t), freePort(t)
	if err := m.Reconcile([]ListenSpec{{Port: p1}, {Port: p2}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	getBodyRetry(t, p1)
	getBodyRetry(t, p2)
	if err := m.Reconcile([]ListenSpec{{Port: p1}}); err != nil {
		t.Fatalf("reconcile drop: %v", err)
	}
	if serving(p2) {
		t.Fatalf("port %d still serving after it was dropped", p2)
	}
	if body := getBodyRetry(t, p1); body != "ok" {
		t.Fatalf("port %d body=%q after drop, want ok", p1, body)
	}
}

func TestReconcileEmptyKeepsLastGood(t *testing.T) {
	m := NewManager(okHandler(), nil, time.Second)
	defer m.Shutdown(context.Background())
	p1 := freePort(t)
	if err := m.Reconcile([]ListenSpec{{Port: p1}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	getBodyRetry(t, p1)
	if err := m.Reconcile(nil); err == nil {
		t.Fatalf("empty reconcile must return an error")
	}
	if body := getBodyRetry(t, p1); body != "ok" {
		t.Fatalf("port %d body=%q after empty reconcile, want still ok", p1, body)
	}
}

func TestReconcileBindErrorSkipsOne(t *testing.T) {
	// A plain listener (no SO_REUSEPORT) holds p1, so the manager's bind to p1
	// fails EADDRINUSE; p2 is free and must still come up, and the error names p1.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	defer held.Close()
	p1 := held.Addr().(*net.TCPAddr).Port
	p2 := freePort(t)
	m := NewManager(okHandler(), nil, time.Second)
	defer m.Shutdown(context.Background())
	err = m.Reconcile([]ListenSpec{{Port: p1}, {Port: p2}})
	if err == nil {
		t.Fatalf("expected a bind error for the held port %d", p1)
	}
	if body := getBodyRetry(t, p2); body != "ok" {
		t.Fatalf("free port %d body=%q, want ok despite the bind error", p2, body)
	}
}

func TestDrainLetsInflightFinish(t *testing.T) {
	entered := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		time.Sleep(200 * time.Millisecond)
		fmt.Fprint(w, "done")
	})
	m := NewManager(h, nil, 2*time.Second)
	defer m.Shutdown(context.Background())
	p1, p2 := freePort(t), freePort(t)
	if err := m.Reconcile([]ListenSpec{{Port: p1}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	bodyCh := make(chan string, 1)
	go func() {
		resp, err := noKeepAlive.Get(fmt.Sprintf("http://127.0.0.1:%d/", p1))
		if err != nil {
			bodyCh <- "ERR:" + err.Error()
			return
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		bodyCh <- string(b)
	}()
	<-entered // the handler is now mid-flight on p1
	// Drop p1 (reconcile to p2); the drain must let the in-flight request finish.
	if err := m.Reconcile([]ListenSpec{{Port: p2}}); err != nil {
		t.Fatalf("reconcile drop: %v", err)
	}
	if body := <-bodyCh; body != "done" {
		t.Fatalf("in-flight request got %q, want done (drain cut it)", body)
	}
}

func TestReconcileDoesNotBlockOnDrain(t *testing.T) {
	// A removed port with a live (stuck) connection must not hold up Reconcile:
	// the drain runs outside the lock, in the background. With a long drain and a
	// stuck handler, the old code blocked Reconcile for the whole drain.
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	})
	m := NewManager(h, nil, 5*time.Second) // deliberately long drain
	defer func() { close(release); m.Shutdown(context.Background()) }()
	p1, p2 := freePort(t), freePort(t)
	if err := m.Reconcile([]ListenSpec{{Port: p1}}); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = noKeepAlive.Get(fmt.Sprintf("http://127.0.0.1:%d/", p1)) }()
	<-entered // a request is now stuck in the handler on p1

	done := make(chan struct{})
	go func() { _ = m.Reconcile([]ListenSpec{{Port: p2}}); close(done) }()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Reconcile blocked on the 5s drain of a stuck connection; drain must be outside the lock")
	}
}

func TestReconcileRestartsOnSpecChange(t *testing.T) {
	dir := t.TempDir()
	cp, kp := dir+"/c", dir+"/k"
	writeSelfSigned(t, cp, kp, "flip.example")
	certs, err := NewCertReloader(cp, kp)
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(okHandler(), certs.TLSConfig(), time.Second)
	defer m.Shutdown(context.Background())
	p := freePort(t)

	// Start plaintext, then flip the SAME port to TLS; the listener must restart
	// so the port now speaks TLS (and no longer plaintext).
	if err := m.Reconcile([]ListenSpec{{Port: p, TLS: false}}); err != nil {
		t.Fatal(err)
	}
	if body := getBodyRetry(t, p); body != "ok" {
		t.Fatalf("plaintext body=%q", body)
	}
	if err := m.Reconcile([]ListenSpec{{Port: p, TLS: true}}); err != nil {
		t.Fatal(err)
	}
	if cn := tlsPeerCN(t, p); cn != "flip.example" {
		t.Fatalf("after flip to TLS, peer CN=%q, want flip.example", cn)
	}
}
