package front

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// acmePrefix is certbot's http-01 challenge path; it must keep working through
// the front so renewals succeed.
const acmePrefix = "/.well-known/acme-challenge/"

// Server is the LB's public HTTP front (2.8.0 Phase 3, step 3a): it terminates
// TLS and, for now, reverse-proxies every request to the nginx behind it, so it
// can be slid in front with no behaviour change. The ACME challenge is served
// straight from certbot's webroot. Later steps move the serve routes (/hls,
// /auth, …) into the in-process gateway and shrink the proxied set until nginx
// is gone.
type Server struct {
	proxy   http.Handler
	webroot string
}

// NewServer reverse-proxies to the nginx backend (e.g. http://127.0.0.1:81) and
// serves the ACME challenge from webroot (certbot's -w directory).
func NewServer(nginx *url.URL, webroot string) *Server {
	rp := httputil.NewSingleHostReverseProxy(nginx)
	base := rp.Director
	rp.Director = func(r *http.Request) {
		base(r)
		// The viewer's address, for nginx's real-ip behind this front.
		ip := r.RemoteAddr
		if h, _, err := net.SplitHostPort(ip); err == nil {
			ip = h
		}
		r.Header.Set("X-Real-IP", ip)
	}
	return &Server{proxy: rp, webroot: webroot}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, acmePrefix) {
		// Served from certbot's webroot exactly as nginx's acme location does.
		http.FileServer(http.Dir(s.webroot)).ServeHTTP(w, r)
		return
	}
	s.proxy.ServeHTTP(w, r)
}

// ListenAndServe binds addr and serves HTTPS with the reloading certificate.
func (s *Server) ListenAndServe(addr string, certs *CertReloader) error {
	srv := &http.Server{Addr: addr, Handler: s, TLSConfig: certs.TLSConfig()}
	return srv.ListenAndServeTLS("", "") // the certificate comes from TLSConfig.GetCertificate
}
