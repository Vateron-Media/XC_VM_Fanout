package front

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
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
	front := NewServer(gw, newNginxStub(t).url, t.TempDir(), LoadTrustedProxies())

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
	front := NewServer(gw, nginx.url, t.TempDir(), LoadTrustedProxies())

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
	front := NewServer(http.NotFoundHandler(), nginx.url, t.TempDir(), LoadTrustedProxies())

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
	front := NewServer(http.NotFoundHandler(), newNginxStub(t).url, webroot, LoadTrustedProxies())

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

// The front serves /images/ in-process the way nginx does: the file, 403 for a
// script extension, 404 for an auto-index or a directory, and no path that
// escapes the image root.
func TestServeStaticImages(t *testing.T) {
	root := t.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "sub"), 0o755)
	_ = os.WriteFile(filepath.Join(root, "logo.png"), []byte("PNGDATA"), 0o644)
	_ = os.WriteFile(filepath.Join(root, "evil.php"), []byte("<?php bad"), 0o644)
	_ = os.WriteFile(filepath.Join(root, "sub", "index.html"), []byte("listing"), 0o644)
	// A secret one level above the image root; a traversal must not reach it.
	_ = os.WriteFile(filepath.Join(filepath.Dir(root), "secret.txt"), []byte("TOP"), 0o600)

	front := NewServer(http.NotFoundHandler(), newNginxStub(t).url, t.TempDir(), LoadTrustedProxies()).WithImages(root)
	get := func(p string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		front.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		return rec
	}
	if rec := get("/images/logo.png"); rec.Code != 200 || rec.Body.String() != "PNGDATA" {
		t.Fatalf("logo: %d %q", rec.Code, rec.Body.String())
	}
	// Script extensions are 403 — including normalization differentials that
	// clean to a .php file ("evil.php/." → "/evil.php").
	for _, p := range []string{"/images/evil.php", "/images/evil.php/.", "/images/evil.php/", "/images/sub/../evil.php"} {
		if rec := get(p); rec.Code != 403 {
			t.Errorf("%s: code %d (body %q), want 403", p, rec.Code, rec.Body.String())
		}
	}
	for _, p := range []string{"/images/sub/index.html", "/images/index.html/.", "/images/sub/", "/images/../secret.txt", "/images/missing.png"} {
		if rec := get(p); rec.Code == 200 {
			t.Errorf("%s served (200, %q), want rejected", p, rec.Body.String())
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
	front := NewServer(http.NotFoundHandler(), nginx.url, t.TempDir(), LoadTrustedProxies())
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

func TestInternalPathsReturn404(t *testing.T) {
	// nginx marks /xc_hls and /xc_fanout* `internal;` — a direct external request
	// must 404, never be served or proxied (the gateway serves that content
	// in-process; these paths carry no token check).
	nginx := newNginxStub(t)
	front := NewServer(okHandler(), nginx.url, t.TempDir(), LoadTrustedProxies())
	for _, p := range []string{"/xc_hls/7_.ts", "/xc_fanout/42", "/xc_fanout_hls/7_3", "/xc_fanout_file/9"} {
		rec := httptest.NewRecorder()
		front.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s code=%d, want 404", p, rec.Code)
		}
	}
	if nginx.path != "" {
		t.Fatalf("an internal path reached nginx (%q); it must 404 at the front", nginx.path)
	}
}

func TestServeRouteIgnoresSpoofedXFF(t *testing.T) {
	// With no trusted proxies, the front is the true edge: a serve route must key
	// on the TCP peer and never on a client-supplied X-Forwarded-For.
	var gotIP string
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIP = r.Header.Get("X-XC-Client-IP")
	})
	front := NewServer(gw, newNginxStub(t).url, t.TempDir(), LoadTrustedProxies())
	req := httptest.NewRequest(http.MethodGet, "/vauth/abc", nil)
	req.RemoteAddr = "203.0.113.9:5555"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	front.ServeHTTP(httptest.NewRecorder(), req)
	if gotIP != "203.0.113.9" {
		t.Fatalf("serve route used a spoofed XFF: X-XC-Client-IP=%q, want 203.0.113.9", gotIP)
	}
}

func TestServeOnlyNoNginxReproxy(t *testing.T) {
	// A serve-only front may run with no nginx backend, never panicking on a nil
	// one. A gateway defer (which *expected* to re-proxy to nginx) is a
	// misconfiguration with nginx gone → 502; an unknown route is one nginx
	// itself answers 404 → 404 (nginx parity), not 502.
	gwDefer := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Accel-Redirect", "@gw_vod_php") // a PHP defer
	})
	front := NewServer(gwDefer, nil, t.TempDir(), LoadTrustedProxies())

	rec := httptest.NewRecorder()
	front.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/vauth/x", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("serve-route defer with no backend: code=%d, want 502", rec.Code)
	}
	rec = httptest.NewRecorder()
	front.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/something/else", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown route with no backend: code=%d, want 404 (nginx parity)", rec.Code)
	}
}

func TestStatusLoopbackOnly(t *testing.T) {
	nginx := newNginxStub(t)
	front := NewServer(okHandler(), nginx.url, t.TempDir(), LoadTrustedProxies())

	// A remote client is denied at the front (nginx's allow 127.0.0.1; deny all).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.RemoteAddr = "203.0.113.1:4444"
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("remote /status: code=%d, want 403", rec.Code)
	}

	// A spoofed X-Forwarded-For from a remote peer must NOT pass the guard.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/status", nil)
	req.RemoteAddr = "203.0.113.1:4444"
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("spoofed-XFF /status: code=%d, want 403", rec.Code)
	}

	// A loopback caller is answered natively (200, empty body) — NOT proxied.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/status", nil)
	req.RemoteAddr = "127.0.0.1:4444"
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "" {
		t.Fatalf("loopback /status: code=%d body=%q, want 200 empty", rec.Code, rec.Body.String())
	}
}

func TestProgressWritesFile(t *testing.T) {
	streams := t.TempDir()
	front := NewServer(http.NotFoundHandler(), newNginxStub(t).url, t.TempDir(), LoadTrustedProxies()).WithStreams(streams + "/")

	body := "frame=42\nfps=25.0\nbitrate= 1200.0kbits/s\nprogress=continue\n\n"
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/progress?stream_id=12", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:5555"
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("progress: code=%d, want 200", rec.Code)
	}
	b, err := os.ReadFile(filepath.Join(streams, "12_.progress"))
	if err != nil {
		t.Fatalf("read progress file: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("progress JSON: %v (%q)", err, b)
	}
	want := map[string]string{"frame": "42", "fps": "25.0", "bitrate": "1200.0kbits/s", "progress": "continue"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("progress map = %v, want %v", got, want)
	}
}

func TestProgressRejected(t *testing.T) {
	streams := t.TempDir()
	front := NewServer(http.NotFoundHandler(), newNginxStub(t).url, t.TempDir(), LoadTrustedProxies()).WithStreams(streams + "/")
	cases := []struct {
		name, addr, xff, target, body string
	}{
		{"remote peer", "203.0.113.1:9", "", "/progress?stream_id=12", "frame=1\n"},
		{"spoofed xff", "203.0.113.1:9", "127.0.0.1", "/progress?stream_id=12", "frame=1\n"},
		{"missing stream_id", "127.0.0.1:9", "", "/progress", "frame=1\n"},
		{"empty body", "127.0.0.1:9", "", "/progress?stream_id=12", ""},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, c.target, strings.NewReader(c.body))
		req.RemoteAddr = c.addr
		if c.xff != "" {
			req.Header.Set("X-Forwarded-For", c.xff)
		}
		front.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: code=%d, want 404", c.name, rec.Code)
		}
	}
	// No file was created for the rejected calls.
	if entries, _ := os.ReadDir(streams); len(entries) != 0 {
		t.Fatalf("rejected progress wrote files: %v", entries)
	}
}

func TestProgressStreamIDInteger(t *testing.T) {
	streams := t.TempDir()
	front := NewServer(http.NotFoundHandler(), newNginxStub(t).url, t.TempDir(), LoadTrustedProxies()).WithStreams(streams + "/")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/progress?stream_id=../../etc/passwd", strings.NewReader("frame=1\n"))
	req.RemoteAddr = "127.0.0.1:9"
	front.ServeHTTP(rec, req)
	// intval("../../etc/passwd") == 0 → the only file written is 0_.progress, in-dir.
	if _, err := os.Stat(filepath.Join(streams, "0_.progress")); err != nil {
		t.Fatalf("want 0_.progress written in-dir, got err=%v", err)
	}
}

func TestProgressWhitespaceBodyPreservesFile(t *testing.T) {
	streams := t.TempDir()
	front := NewServer(http.NotFoundHandler(), newNginxStub(t).url, t.TempDir(), LoadTrustedProxies()).WithStreams(streams + "/")

	// An existing progress file with real content.
	existing := []byte(`{"frame":"99","progress":"continue"}`)
	if err := os.WriteFile(filepath.Join(streams, "12_.progress"), existing, 0o644); err != nil {
		t.Fatal(err)
	}

	// A whitespace-only POST must 404 and leave the file untouched (PHP trims the
	// body then 404s on empty — it never clobbers the file with an empty object).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/progress?stream_id=12", strings.NewReader("\n   \n"))
	req.RemoteAddr = "127.0.0.1:9"
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("whitespace body: code=%d, want 404", rec.Code)
	}
	got, err := os.ReadFile(filepath.Join(streams, "12_.progress"))
	if err != nil || string(got) != string(existing) {
		t.Fatalf("progress file clobbered: err=%v content=%q, want %q", err, got, existing)
	}
}

func TestNoBackendUnknownPathIs404(t *testing.T) {
	front := NewServer(http.NotFoundHandler(), nil, t.TempDir(), LoadTrustedProxies())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	req.RemoteAddr = "127.0.0.1:9"
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("no-backend unknown path: code=%d, want 404", rec.Code)
	}
}

func TestNonGetPostReturns200(t *testing.T) {
	front := NewServer(http.NotFoundHandler(), nil, t.TempDir(), LoadTrustedProxies())
	for _, m := range []string{http.MethodOptions, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(m, "/auth/sometoken", nil) // even a serve route
		req.RemoteAddr = "203.0.113.9:9"
		front.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
			t.Fatalf("%s: code=%d len=%d, want 200 empty", m, rec.Code, rec.Body.Len())
		}
	}
}

func TestLegacyPhpRedirects(t *testing.T) {
	front := NewServer(http.NotFoundHandler(), nil, t.TempDir(), LoadTrustedProxies())
	redirects := map[string]string{
		"/stream/live.php?token=abc": "/stream/live?token=abc",
		"/stream/key.php?token=z":    "/stream/key?token=z",
		"/admin/vod.php?a=1&b=2":     "/admin/vod?a=1&b=2",
		"/streaming/rtmp.php?x=1":    "/stream/rtmp?x=1",
		"/progress.php?stream_id=12": "/progress?stream_id=12",
		"/stream/segment.php":        "/stream/segment", // no query
	}
	for in, want := range redirects {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, in, nil)
		req.RemoteAddr = "127.0.0.1:9"
		front.ServeHTTP(rec, req)
		loc := rec.Header().Get("Location")
		if rec.Code != http.StatusFound || loc != want {
			t.Fatalf("%s: code=%d loc=%q, want 302 %q", in, rec.Code, loc, want)
		}
		if strings.Contains(loc, "://") { // absolute_redirect off → relative
			t.Fatalf("%s: Location %q is absolute; nginx emits relative", in, loc)
		}
	}
	for _, p := range []string{"/portal.php", "/c/portal.php", "/whatever.php"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, p, nil)
		req.RemoteAddr = "127.0.0.1:9"
		front.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s: code=%d, want 404", p, rec.Code)
		}
	}
}

func TestNginxStatusStub(t *testing.T) {
	front := NewServer(http.NotFoundHandler(), nil, t.TempDir(), LoadTrustedProxies())
	// Remote peer denied (nginx's allow 127.0.0.1; deny all).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nginx_status", nil)
	req.RemoteAddr = "203.0.113.1:9"
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("remote /nginx_status: code=%d, want 403", rec.Code)
	}
	// Loopback: a stub_status body whose line[2] field[2] is numeric — what
	// SystemInfo::nginxRequestCount reads (explode(' ', line[2])[2]).
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/nginx_status", nil)
	req.RemoteAddr = "127.0.0.1:9"
	front.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("loopback /nginx_status: code=%d, want 200", rec.Code)
	}
	lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("stub body has %d lines: %q", len(lines), rec.Body.String())
	}
	fields := strings.Fields(lines[2])
	if len(fields) < 3 {
		t.Fatalf("line[2] = %q, want 3 fields", lines[2])
	}
	if _, err := strconv.Atoi(fields[2]); err != nil {
		t.Fatalf("requests field %q not numeric: %v", fields[2], err)
	}
}
