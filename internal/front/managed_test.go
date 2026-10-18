package front

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var insecureTLS = &http.Client{Transport: &http.Transport{
	TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	DisableKeepAlives: true,
}}

func tlsGetRetry(t *testing.T, port int) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := insecureTLS.Get(fmt.Sprintf("https://127.0.0.1:%d/", port))
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return string(b)
		}
		if time.Now().After(deadline) {
			t.Fatalf("port %d never served TLS: %v", port, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func tlsPeerCN(t *testing.T, port int) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			cs := conn.ConnectionState()
			conn.Close()
			if len(cs.PeerCertificates) == 0 {
				return ""
			}
			return cs.PeerCertificates[0].Subject.CommonName
		}
		if time.Now().After(deadline) {
			t.Fatalf("port %d no TLS handshake: %v", port, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func writePorts(t *testing.T, httpPath, httpsPath string, httpPorts, httpsPorts []int) {
	t.Helper()
	var hb, sb strings.Builder
	for _, p := range httpPorts {
		fmt.Fprintf(&hb, "listen %d reuseport;\n", p)
	}
	for _, p := range httpsPorts {
		fmt.Fprintf(&sb, "listen %d ssl reuseport;\n", p)
	}
	if err := os.WriteFile(httpPath, []byte(hb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(httpsPath, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFrontBindsWatchedSet(t *testing.T) {
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	writeSelfSigned(t, cp, kp, "lb.example")
	certs, err := NewCertReloader(cp, kp)
	if err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "backend") }))
	defer backend.Close()
	bu, _ := url.Parse(backend.URL)
	srv := NewServer(okHandler(), bu, dir, LoadTrustedProxies())

	httpPath, httpsPath := filepath.Join(dir, "http.conf"), filepath.Join(dir, "https.conf")
	p1, p2 := freePort(t), freePort(t)
	writePorts(t, httpPath, httpsPath, nil, []int{p1})

	_, stop, err := srv.ServeManaged(certs, httpPath, httpsPath, 20*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("ServeManaged: %v", err)
	}
	defer stop()

	if body := tlsGetRetry(t, p1); body != "backend" {
		t.Fatalf("p1 body=%q, want backend (routed via proxy)", body)
	}
	// MAIN adds a port: the watcher rebinds and p2 comes up serving the same handler.
	writePorts(t, httpPath, httpsPath, nil, []int{p1, p2})
	if body := tlsGetRetry(t, p2); body != "backend" {
		t.Fatalf("p2 body=%q after the port was added, want backend", body)
	}
}

func TestCertRenewalAcrossAllListeners(t *testing.T) {
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	writeSelfSigned(t, cp, kp, "first.example")
	certs, err := NewCertReloader(cp, kp)
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(okHandler(), mustURL(t, "http://127.0.0.1:1"), dir, LoadTrustedProxies())
	httpPath, httpsPath := filepath.Join(dir, "http.conf"), filepath.Join(dir, "https.conf")
	p1, p2 := freePort(t), freePort(t)
	writePorts(t, httpPath, httpsPath, nil, []int{p1, p2})
	_, stop, err := srv.ServeManaged(certs, httpPath, httpsPath, 20*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("ServeManaged: %v", err)
	}
	defer stop()

	for _, p := range []int{p1, p2} {
		if cn := tlsPeerCN(t, p); cn != "first.example" {
			t.Fatalf("port %d initial CN=%q, want first.example", p, cn)
		}
	}
	// certbot renews: the shared CertReloader must present the new cert on EVERY listener.
	writeSelfSigned(t, cp, kp, "second.example")
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(cp, later, later); err != nil {
		t.Fatal(err)
	}
	for _, p := range []int{p1, p2} {
		if cn := tlsPeerCN(t, p); cn != "second.example" {
			t.Fatalf("port %d after renewal CN=%q, want second.example", p, cn)
		}
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestServeManagedProceedsOnPartialBind(t *testing.T) {
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "c"), filepath.Join(dir, "k")
	writeSelfSigned(t, cp, kp, "lb.example")
	certs, err := NewCertReloader(cp, kp)
	if err != nil {
		t.Fatal(err)
	}
	// p1 is held by a non-reuseport squatter, so the front's bind to it fails;
	// p2 is free. ServeManaged must proceed (serve p2), not return a hard error
	// that orphans the started listeners.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	p1 := held.Addr().(*net.TCPAddr).Port
	p2 := freePort(t)
	srv := NewServer(okHandler(), mustURL(t, "http://127.0.0.1:1"), dir, LoadTrustedProxies())
	hp, sp := filepath.Join(dir, "http.conf"), filepath.Join(dir, "https.conf")
	writePorts(t, hp, sp, nil, []int{p1, p2})

	mgr, stop, err := srv.ServeManaged(certs, hp, sp, 20*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("ServeManaged returned a hard error on a partial bind: %v", err)
	}
	if mgr == nil || stop == nil {
		t.Fatal("ServeManaged returned nil manager/stop on a partial bind")
	}
	defer stop()
	if cn := tlsPeerCN(t, p2); cn != "lb.example" {
		t.Fatalf("the free port %d did not come up: CN=%q", p2, cn)
	}
}
