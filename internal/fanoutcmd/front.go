package fanoutcmd

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/front"
)

// serveFront starts the public HTTPS front for the single-binary LB: it
// terminates TLS with certbot's certificate, answers the serve routes from the
// in-process gateway, and reverse-proxies the rest to nginx during the
// transition. It is opt-in and non-fatal — a bad cert or a taken port is
// returned as an error so the caller logs it and leaves nginx serving, exactly
// as the gateway socket does.
func serveFront(addr string, gw http.Handler, nginxURL, webroot, certPath, keyPath, realipPaths string) (*http.Server, func(), error) {
	certs, err := front.NewCertReloader(certPath, keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("tls certificate: %w", err)
	}
	u, err := url.Parse(nginxURL)
	if err != nil || u.Host == "" {
		return nil, nil, fmt.Errorf("nginx backend url %q is not usable", nginxURL)
	}
	f := front.NewServer(gw, u, webroot, front.LoadTrustedProxies(splitCSV(realipPaths)...))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	srv := &http.Server{Addr: ln.Addr().String(), Handler: f, TLSConfig: certs.TLSConfig(), ReadHeaderTimeout: defaults.HTTPReadHeaderTimeout}
	go func() {
		if err := srv.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
			log.Printf("front: serve: %v", err)
		}
	}()
	log.Printf("front: serving HTTPS on %s (serve-only; proxying the rest to %s)", srv.Addr, nginxURL)
	return srv, func() { _ = ln.Close() }, nil
}
