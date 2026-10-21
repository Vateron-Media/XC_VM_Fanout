package front

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The front routes /admin/thumb to the agent and forwards the resolved real
// client IP as the only X-XC-Client-IP, dropping any the caller sent (the agent's
// uitoken/password admission keys on it).
func TestFrontRoutesAdminThumbToAgentWithRealIP(t *testing.T) {
	var gotPath, gotIP string
	sock := startUnixAgent(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotIP = r.URL.Path, r.Header.Get("X-XC-Client-IP")
		_, _ = io.WriteString(w, "AGENT")
	}))
	s := NewServer(okHandler(), newNginxStub(t).url, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(sock)

	r := httptest.NewRequest(http.MethodGet, "/admin/thumb?stream=42", nil)
	r.RemoteAddr = "203.0.113.9:5555"
	r.Header.Set("X-XC-Client-IP", "9.9.9.9") // spoofed by the caller
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)

	if rec.Body.String() != "AGENT" || gotPath != "/admin/thumb" {
		t.Fatalf("admin not routed to the agent: body=%q path=%q", rec.Body.String(), gotPath)
	}
	if gotIP != "203.0.113.9" {
		t.Fatalf("front forwarded X-XC-Client-IP=%q, want the real peer 203.0.113.9 (spoof dropped)", gotIP)
	}
}

// The front routes /admin/live to the agent too, with the same real-IP forwarding.
func TestFrontProxiesAdminLive(t *testing.T) {
	var gotPath, gotIP string
	sock := startUnixAgent(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotIP = r.URL.Path, r.Header.Get("X-XC-Client-IP")
		_, _ = io.WriteString(w, "AGENT")
	}))
	s := NewServer(okHandler(), newNginxStub(t).url, t.TempDir(), LoadTrustedProxies()).WithAgentSocket(sock)

	r := httptest.NewRequest(http.MethodGet, "/admin/live?stream=42&extension=ts", nil)
	r.RemoteAddr = "203.0.113.9:5555"
	r.Header.Set("X-XC-Client-IP", "9.9.9.9") // spoofed by the caller
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)

	if rec.Body.String() != "AGENT" || gotPath != "/admin/live" {
		t.Fatalf("admin/live not routed to the agent: body=%q path=%q", rec.Body.String(), gotPath)
	}
	if gotIP != "203.0.113.9" {
		t.Fatalf("front forwarded X-XC-Client-IP=%q, want the real peer 203.0.113.9 (spoof dropped)", gotIP)
	}
}

// With no agent socket, /admin/thumb falls back to nginx during the transition.
func TestFrontAdminFallsBackToNginxWithoutAgent(t *testing.T) {
	nginx := newNginxStub(t)
	s := NewServer(okHandler(), nginx.url, t.TempDir(), LoadTrustedProxies())
	r := httptest.NewRequest(http.MethodGet, "/admin/thumb?stream=42", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, r)
	if rec.Body.String() != "NGINX:/admin/thumb" {
		t.Fatalf("admin not proxied to nginx without an agent: %q", rec.Body.String())
	}
}
