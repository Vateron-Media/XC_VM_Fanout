package front

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// The front routes the loopback /stream/rtmp callback to the agent, preserving
// the raw query (nginx_rtmp's notify args), and refuses a non-loopback peer.
func TestFrontRtmpNotifyLoopbackPreservesTarget(t *testing.T) {
	var gotURI string
	sock := startUnixAgent(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	}))
	s := NewServer(okHandler(), nil, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(sock)

	target := "/stream/rtmp?addr=203.0.113.9&clientid=7&call=play&name=100&password=p"
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.RemoteAddr = "127.0.0.1:40000"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK || gotURI != target {
		t.Fatalf("loopback: status %d, agent uri %q", rec.Code, gotURI)
	}
}

func TestFrontRtmpNotifyRefusesNonLoopback(t *testing.T) {
	var dialed atomic.Bool
	sock := startUnixAgent(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dialed.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	s := NewServer(okHandler(), nil, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(sock)
	r := httptest.NewRequest(http.MethodGet, "/stream/rtmp?call=play&name=100", nil) // default non-loopback peer
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("non-loopback peer: %d, want 404", rec.Code)
	}
	if dialed.Load() {
		t.Fatal("a non-loopback peer must not reach the agent")
	}
}

func TestFrontRtmpNotifyNginxWhenNoAgent(t *testing.T) {
	var hit atomic.Int32
	ng := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ng.Close()
	u, _ := url.Parse(ng.URL)
	s := NewServer(okHandler(), u, t.TempDir(), LoadTrustedProxies()) // no agent socket
	r := httptest.NewRequest(http.MethodGet, "/stream/rtmp?call=play&name=100", nil)
	r.RemoteAddr = "127.0.0.1:40000"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK || hit.Load() != 1 {
		t.Fatalf("no-agent should proxy to nginx: %d hit=%d", rec.Code, hit.Load())
	}
}

func TestFrontRtmpNotifyDialFailFallsBack(t *testing.T) {
	var hit atomic.Int32
	ng := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ng.Close()
	u, _ := url.Parse(ng.URL)
	bad := filepath.Join(t.TempDir(), "nope.sock")

	s := NewServer(okHandler(), u, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(bad)
	r := httptest.NewRequest(http.MethodGet, "/stream/rtmp?call=play&name=100", nil)
	r.RemoteAddr = "127.0.0.1:40000"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK || hit.Load() != 1 {
		t.Fatalf("dial-fail should fall back to nginx: %d hit=%d", rec.Code, hit.Load())
	}

	s2 := NewServer(okHandler(), nil, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(bad)
	r2 := httptest.NewRequest(http.MethodGet, "/stream/rtmp?call=play&name=100", nil)
	r2.RemoteAddr = "127.0.0.1:40000"
	rec2 := httptest.NewRecorder()
	s2.ServeHTTP(rec2, r2)
	if rec2.Code != http.StatusBadGateway {
		t.Fatalf("no backend on a dial failure: %d, want 502", rec2.Code)
	}
}
