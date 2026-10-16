package front

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// nginxStub stands in for the nginx behind the front, recording what it was
// handed and echoing a marker.
type nginxStub struct {
	srv    *httptest.Server
	url    *url.URL
	path   string
	realIP string
	xff    string
}

func newNginxStub(t *testing.T) *nginxStub {
	t.Helper()
	n := &nginxStub{}
	n.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.path, n.realIP, n.xff = r.URL.Path, r.Header.Get("X-Real-IP"), r.Header.Get("X-Forwarded-For")
		_, _ = io.WriteString(w, "NGINX:"+r.URL.Path)
	}))
	n.url, _ = url.Parse(n.srv.URL)
	t.Cleanup(n.srv.Close)
	return n
}

func TestFrontRoutesServeToGateway(t *testing.T) {
	var gotKind, gotOrig, gotIP string
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKind = r.URL.Path // /stream/<kind>
		gotOrig = r.Header.Get("X-XC-Original-URI")
		gotIP = r.Header.Get("X-XC-Client-IP")
		_, _ = io.WriteString(w, "GATEWAY")
	})
	front := NewServer(gw, newNginxStub(t).url, t.TempDir())

	req := httptest.NewRequest(http.MethodGet, "/vauth/abc123", nil)
	req.RemoteAddr = "203.0.113.5:40000"
	rec := httptest.NewRecorder()
	front.ServeHTTP(rec, req)

	if rec.Body.String() != "GATEWAY" {
		t.Fatalf("serve route not answered by the gateway: %q", rec.Body.String())
	}
	if gotKind != "/stream/vod" || gotOrig != "/vauth/abc123" || gotIP != "203.0.113.5" {
		t.Fatalf("gateway got path=%q orig=%q ip=%q", gotKind, gotOrig, gotIP)
	}
}

func TestFrontDefersToNginx(t *testing.T) {
	// The gateway hands this back to PHP: sets X-Accel-Redirect, no body.
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Accel-Redirect", "@gw_vod_php")
	})
	nginx := newNginxStub(t)
	front := NewServer(gw, nginx.url, t.TempDir())

	req := httptest.NewRequest(http.MethodGet, "/vauth/abc123", nil)
	rec := httptest.NewRecorder()
	front.ServeHTTP(rec, req)

	// The viewer gets nginx's answer for the ORIGINAL path, not the X-Accel header.
	if rec.Body.String() != "NGINX:/vauth/abc123" || rec.Header().Get("X-Accel-Redirect") != "" {
		t.Fatalf("defer not re-proxied: body=%q accel=%q", rec.Body.String(), rec.Header().Get("X-Accel-Redirect"))
	}
	if nginx.path != "/vauth/abc123" {
		t.Fatalf("nginx saw %q, want the original /vauth/abc123", nginx.path)
	}
}

func TestFrontProxiesNonServeAndStripsSpoofedIP(t *testing.T) {
	nginx := newNginxStub(t)
	front := NewServer(http.NotFoundHandler(), nginx.url, t.TempDir())

	req := httptest.NewRequest(http.MethodGet, "/player_api", nil)
	req.RemoteAddr = "203.0.113.9:50000"
	req.Header.Set("X-Real-IP", "8.8.8.8")       // spoofed by the client
	req.Header.Set("X-Forwarded-For", "9.9.9.9") // spoofed by the client
	rec := httptest.NewRecorder()
	front.ServeHTTP(rec, req)

	if rec.Body.String() != "NGINX:/player_api" {
		t.Fatalf("non-serve route not proxied: %q", rec.Body.String())
	}
	if nginx.realIP != "203.0.113.9" {
		t.Fatalf("spoofed X-Real-IP survived: %q", nginx.realIP)
	}
	if nginx.xff != "203.0.113.9" {
		t.Fatalf("spoofed X-Forwarded-For survived: %q", nginx.xff)
	}
}

func TestFrontACMEServesTokenNotTraversal(t *testing.T) {
	webroot := t.TempDir()
	challenge := filepath.Join(webroot, ".well-known", "acme-challenge")
	if err := os.MkdirAll(challenge, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(challenge, "tok123"), []byte("challenge-body"), 0o644)
	_ = os.WriteFile(filepath.Join(webroot, "secret"), []byte("TOPSECRET"), 0o600)
	front := NewServer(http.NotFoundHandler(), newNginxStub(t).url, webroot)

	get := func(p string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		front.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		return rec
	}
	if rec := get(acmePrefix + "tok123"); rec.Code != 200 || rec.Body.String() != "challenge-body" {
		t.Fatalf("valid token: %d %q", rec.Code, rec.Body.String())
	}
	for _, bad := range []string{
		acmePrefix + "../../secret", acmePrefix + "..%2f..%2fsecret", acmePrefix, acmePrefix + "a/b", acmePrefix + "..",
	} {
		if rec := get(bad); rec.Code == 200 {
			t.Errorf("%s served (code 200, body %q), want rejected", bad, rec.Body.String())
		}
	}
}

// The front terminates TLS with the reloading certificate and reverse-proxies
// over a real localhost TLS connection.
func TestFrontTerminatesTLS(t *testing.T) {
	nginx := newNginxStub(t)
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	writeSelfSigned(t, cp, kp, "lb.example")
	certs, err := NewCertReloader(cp, kp)
	if err != nil {
		t.Fatal(err)
	}
	front := NewServer(http.NotFoundHandler(), nginx.url, t.TempDir())
	srv := &http.Server{Handler: front, TLSConfig: certs.TLSConfig()}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go srv.ServeTLS(ln, "", "") //nolint:errcheck // closed via ln/srv at test end
	defer srv.Close()

	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // self-signed test cert
	}
	res, err := client.Get("https://" + ln.Addr().String() + "/player_api")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(body) != "NGINX:/player_api" {
		t.Fatalf("TLS proxy: %q", body)
	}
}
