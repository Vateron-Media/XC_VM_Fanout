package fanoutcmd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
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

// A missing certificate is returned as an error, not a crash: the caller logs
// it and leaves nginx serving.
func TestServeFrontNonFatalOnBadCert(t *testing.T) {
	if _, _, err := serveFront("127.0.0.1:0", http.NotFoundHandler(), "http://127.0.0.1:1", t.TempDir(),
		filepath.Join(t.TempDir(), "absent.crt"), filepath.Join(t.TempDir(), "absent.key"), ""); err == nil {
		t.Fatal("want an error for a missing certificate, got nil")
	}
}

// Wired end to end: a serve route reaches the in-process gateway, everything
// else is proxied to the nginx backend, over a real TLS connection.
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

	srv, cleanup, err := serveFront("127.0.0.1:0", gw, backend.URL, dir, cp, kp, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Close(); cleanup() }()

	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // self-signed test cert
	}
	base := "https://" + srv.Addr

	res, err := client.Get(base + "/vauth/tok")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(b) != "GATEWAY" || gotOrig != "/vauth/tok" {
		t.Fatalf("serve route: body=%q orig=%q", b, gotOrig)
	}

	res, err = client.Get(base + "/player_api")
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if string(b) != "NGINX" {
		t.Fatalf("proxy: body=%q", b)
	}
}
