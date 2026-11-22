package front

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSelfSigned writes a throwaway cert/key with the given common name.
func writeSelfSigned(t *testing.T, certPath, keyPath, cn string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func leafCN(t *testing.T, cert *tls.Certificate) string {
	t.Helper()
	if cert == nil || len(cert.Certificate) == 0 {
		t.Fatal("no certificate")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.Subject.CommonName
}

func TestCertReloader(t *testing.T) {
	dir := t.TempDir()
	cp, kp := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	writeSelfSigned(t, cp, kp, "first.example")

	cr, err := NewCertReloader(cp, kp)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := cr.GetCertificate(nil)
	if cn := leafCN(t, c); cn != "first.example" {
		t.Fatalf("initial cert CN = %q, want first.example", cn)
	}

	// A certbot renewal: the files change and the mtime advances.
	writeSelfSigned(t, cp, kp, "second.example")
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(cp, later, later); err != nil {
		t.Fatal(err)
	}
	c, _ = cr.GetCertificate(nil)
	if cn := leafCN(t, c); cn != "second.example" {
		t.Fatalf("after renewal CN = %q, want second.example", cn)
	}

	// A bad reload (the cert vanished) keeps the last good certificate rather
	// than breaking the handshake.
	_ = os.Remove(cp)
	if c, _ := cr.GetCertificate(nil); c == nil {
		t.Fatal("lost the certificate on a failed reload")
	}

	// A missing certificate at startup is fatal.
	if _, err := NewCertReloader(filepath.Join(dir, "nope.crt"), filepath.Join(dir, "nope.key")); err == nil {
		t.Fatal("want an error for a missing certificate at startup")
	}
}
