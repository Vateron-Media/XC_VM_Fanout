package fanoutcmd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCert(t *testing.T, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "lb"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// A missing certificate is returned as an error, not a crash: the caller logs
// it and leaves nginx serving.
func TestServeFrontNonFatalOnBadCert(t *testing.T) {
	dir := t.TempDir()
	if _, err := serveFront(http.NotFoundHandler(), "http://127.0.0.1:1", dir,
		filepath.Join(dir, "absent.crt"), filepath.Join(dir, "absent.key"), "", "", "", "",
		filepath.Join(dir, "http.conf"), filepath.Join(dir, "https.conf")); err == nil {
		t.Fatal("want an error for a missing certificate, got nil")
	}
}

// Wired end to end: the front binds the node listen set, a serve route reaches
// the in-process gateway, everything else is proxied to the nginx backend, over
// a real TLS connection.
func TestServeFrontServesAndProxies(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "NGINX")
	}))
	defer backend.Close()

	var gotOrig string
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotOrig = r.Header.Get("X-XC-Original-URI")
		_, _ = io.WriteString(w, "GATEWAY")
	})

	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	writeCert(t, cp, kp)
	hp, sp := filepath.Join(dir, "http.conf"), filepath.Join(dir, "https.conf")
	port := freePort(t)
	if err := os.WriteFile(hp, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sp, []byte(fmt.Sprintf("listen %d ssl reuseport;", port)), 0o644); err != nil {
		t.Fatal(err)
	}

	stop, err := serveFront(gw, backend.URL, dir, cp, kp, "", "", "", "", hp, sp)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true}, //nolint:gosec // self-signed test cert
	}
	base := fmt.Sprintf("https://127.0.0.1:%d", port)

	body := func(path string) string {
		deadline := time.Now().Add(2 * time.Second)
		for {
			res, err := client.Get(base + path)
			if err == nil {
				b, _ := io.ReadAll(res.Body)
				res.Body.Close()
				return string(b)
			}
			if time.Now().After(deadline) {
				t.Fatalf("GET %s never served: %v", path, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	if got := body("/vauth/tok"); got != "GATEWAY" || gotOrig != "/vauth/tok" {
		t.Fatalf("serve route: body=%q orig=%q", got, gotOrig)
	}
	if got := body("/player_api"); got != "NGINX" {
		t.Fatalf("proxy: body=%q", got)
	}
}

func TestServeFrontEmptyNginxAccepted(t *testing.T) {
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "GATEWAY") })
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	writeCert(t, cp, kp)
	hp, sp := filepath.Join(dir, "http.conf"), filepath.Join(dir, "https.conf")
	port := freePort(t)
	if err := os.WriteFile(hp, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sp, []byte(fmt.Sprintf("listen %d ssl reuseport;", port)), 0o644); err != nil {
		t.Fatal(err)
	}

	// An empty -front-nginx must be accepted (no backend) and the front must start.
	stop, err := serveFront(gw, "", dir, cp, kp, "", "", "", "", hp, sp)
	if err != nil {
		t.Fatalf("empty -front-nginx rejected: %v", err)
	}
	defer stop()

	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true}, //nolint:gosec // self-signed test cert
	}
	base := fmt.Sprintf("https://127.0.0.1:%d", port)
	deadline := time.Now().Add(2 * time.Second)
	for {
		res, err := client.Get(base + "/does-not-exist")
		if err == nil {
			res.Body.Close()
			if res.StatusCode != http.StatusNotFound {
				t.Fatalf("no-backend unknown path over the wire: code=%d, want 404", res.StatusCode)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("front never came up with an empty backend: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
