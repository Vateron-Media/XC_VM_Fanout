package front

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func startUnixAgent(t *testing.T, h http.Handler) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return sock
}

// The front must hand the agent the request-target byte-for-byte: the fetcher
// signed its relay-auth proof over it, so a re-encoded or reordered query would
// break every /xfile. It must also forward the two X-XCVM-* headers.
func TestXFilePreservesRawTarget(t *testing.T) {
	var gotURI, gotFile, gotAuth string
	sock := startUnixAgent(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI, gotFile, gotAuth = r.RequestURI, r.Header.Get("X-XCVM-File"), r.Header.Get("X-XCVM-File-Auth")
		w.WriteHeader(http.StatusOK)
	}))
	s := NewServer(okHandler(), nil, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(sock)

	target := "/xfile?o=0&n=4194304&rk=AbC-_9"
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XCVM-File", "the-ticket")
	r.Header.Set("X-XCVM-File-Auth", "the-proof")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if gotURI != target {
		t.Fatalf("agent saw RequestURI %q, want %q", gotURI, target)
	}
	if gotFile != "the-ticket" || gotAuth != "the-proof" {
		t.Fatalf("X-XCVM-* not forwarded: file=%q auth=%q", gotFile, gotAuth)
	}
}

// The /xfile limiter is keyed on the raw TCP peer and refuses 429 (not the
// serve routes' 503), matching nginx's zone xfile.
func TestXFileRateLimit429(t *testing.T) {
	sock := startUnixAgent(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	s := NewServer(okHandler(), nil, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(sock)
	frozen := time.Now()
	s.xfileLimiter = newRateLimiter(xfileRate, 1)
	s.xfileLimiter.now = func() time.Time { return frozen } // no refill

	r := httptest.NewRequest(http.MethodGet, "/xfile?o=0&n=10", nil)
	if rec := httptest.NewRecorder(); func() int { s.ServeHTTP(rec, r); return rec.Code }() != http.StatusOK {
		t.Fatal("first request not allowed")
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over-burst status %d, want 429", rec.Code)
	}
}

// If the agent socket cannot be dialed (agent down, or not yet serving), the
// front falls back to nginx during the transition — or 502 when nginx is gone.
func TestXFileFallsBackToNginxOnDialFail(t *testing.T) {
	var nginxHit int32
	ng := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nginxHit, 1)
		io.WriteString(w, "nginx")
	}))
	defer ng.Close()
	u, _ := url.Parse(ng.URL)
	badSock := filepath.Join(t.TempDir(), "nope.sock") // nothing listening

	s := NewServer(okHandler(), u, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(badSock)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/xfile?o=0&n=10", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "nginx" {
		t.Fatalf("dial-fail fallback: %d %q", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt32(&nginxHit) != 1 {
		t.Fatal("nginx not reached on dial failure")
	}

	// No nginx backend (post-cutover): a dial failure is 502.
	s2 := NewServer(okHandler(), nil, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(badSock)
	rec2 := httptest.NewRecorder()
	s2.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/xfile?o=0&n=10", nil))
	if rec2.Code != http.StatusBadGateway {
		t.Fatalf("no-backend dial-fail: %d, want 502", rec2.Code)
	}
}

// An agent response — even a 404 — is authoritative: the front streams it
// through and never falls back to nginx.
func TestXFileAgent404Passthrough(t *testing.T) {
	sock := startUnixAgent(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	var nginxHit int32
	ng := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nginxHit, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ng.Close()
	u, _ := url.Parse(ng.URL)

	s := NewServer(okHandler(), u, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(sock)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/xfile?o=0&n=10", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("agent 404 not passed through: %d", rec.Code)
	}
	if atomic.LoadInt32(&nginxHit) != 0 {
		t.Fatal("front fell back to nginx on an authoritative agent response")
	}
}

// Without an agent socket configured, /xfile proxies to nginx (pre-cutover).
func TestXFileProxiesToNginxWhenNoAgent(t *testing.T) {
	var nginxHit int32
	ng := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nginxHit, 1)
		if r.URL.Path != "/xfile" {
			t.Errorf("nginx path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ng.Close()
	u, _ := url.Parse(ng.URL)
	s := NewServer(okHandler(), u, t.TempDir(), LoadTrustedProxies()) // no WithAgentSocket
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/xfile?o=0&n=10", nil))
	if rec.Code != http.StatusOK || atomic.LoadInt32(&nginxHit) != 1 {
		t.Fatalf("no-agent /xfile did not proxy to nginx: %d, hits %d", rec.Code, nginxHit)
	}
}
