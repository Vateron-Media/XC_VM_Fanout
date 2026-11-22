// Package front is the LB's public HTTP server — the single Go binary's
// replacement for nginx on a serve-only node (2.8.0 Phase 3). It terminates
// TLS, applies the serve-only rewrites and routes requests to the in-process
// gateway, so an LB needs no nginx. Certificates stay certbot's
// (server.crt/server.key); this package only reads and serves them.
package front

import (
	"crypto/tls"
	"os"
	"sync"
	"time"
)

// CertReloader serves the LB's TLS certificate from the files certbot writes
// (server.crt / server.key) and reloads it when they change, so a renewal
// applies without restarting the binary. Every handshake goes through
// GetCertificate, which tls.Config calls per connection.
type CertReloader struct {
	certPath, keyPath string

	mu      sync.RWMutex
	cert    *tls.Certificate
	modTime time.Time
}

// NewCertReloader loads the pair once. It errors only when that first load
// fails: a missing or invalid certificate at startup is fatal, whereas a bad
// write later is not (GetCertificate keeps the last good one).
func NewCertReloader(certPath, keyPath string) (*CertReloader, error) {
	c := &CertReloader{certPath: certPath, keyPath: keyPath}
	if err := c.reload(); err != nil {
		return nil, err
	}
	return c, nil
}

// GetCertificate returns the current certificate, reloading first when the cert
// file's modification time has advanced (a certbot renewal). A failed reload
// keeps the last good certificate rather than breaking the handshake.
func (c *CertReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	if fi, err := os.Stat(c.certPath); err == nil {
		c.mu.RLock()
		stale := fi.ModTime().After(c.modTime)
		c.mu.RUnlock()
		if stale {
			_ = c.reload() // on failure, keep the last good cert
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cert, nil
}

func (c *CertReloader) reload() error {
	fi, err := os.Stat(c.certPath)
	if err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(c.certPath, c.keyPath)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.cert, c.modTime = &cert, fi.ModTime()
	c.mu.Unlock()
	return nil
}

// TLSConfig serves the reloading certificate, with TLS 1.2 the floor as the
// LB's nginx sets it (ssl_protocols TLSv1.2 TLSv1.3).
func (c *CertReloader) TLSConfig() *tls.Config {
	return &tls.Config{GetCertificate: c.GetCertificate, MinVersion: tls.VersionTLS12}
}
