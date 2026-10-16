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

// The front terminates TLS with the reloading certificate, serves certbot's
// ACME challenge from the webroot, and reverse-proxies everything else to the
// nginx behind it (passing the viewer's address) — proven end to end over a
// real TLS connection on localhost.
func TestFrontTerminatesTLSAndProxies(t *testing.T) {
	// Stand-in for nginx: echoes a marker and the forwarded client address.
	var gotRealIP string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRealIP = r.Header.Get("X-Real-IP")
		_, _ = io.WriteString(w, "NGINX:"+r.URL.Path)
	}))
	defer backend.Close()
	backendURL, _ := url.Parse(backend.URL)

	// certbot's webroot with a challenge file.
	webroot := t.TempDir()
	challenge := filepath.Join(webroot, ".well-known", "acme-challenge")
	if err := os.MkdirAll(challenge, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(challenge, "tok123"), []byte("challenge-body"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The front's TLS certificate (certbot's, here self-signed).
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	writeSelfSigned(t, cp, kp, "lb.example")
	certs, err := NewCertReloader(cp, kp)
	if err != nil {
		t.Fatal(err)
	}

	front := NewServer(backendURL, webroot)
	srv := &http.Server{Handler: front, TLSConfig: certs.TLSConfig()}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go srv.ServeTLS(ln, "", "") //nolint:errcheck // closed via ln at test end
	defer srv.Close()

	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // self-signed test cert
	}
	base := "https://" + ln.Addr().String()

	// A normal request is proxied to nginx with the viewer's address.
	res, err := client.Get(base + "/player_api")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(body) != "NGINX:/player_api" {
		t.Fatalf("proxy: body %q", body)
	}
	if gotRealIP != "127.0.0.1" {
		t.Fatalf("proxy did not pass the viewer address: X-Real-IP %q", gotRealIP)
	}

	// The ACME challenge is served from the webroot, not proxied.
	res, err = client.Get(base + acmePrefix + "tok123")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(body) != "challenge-body" {
		t.Fatalf("acme: %d %q", res.StatusCode, body)
	}
}
