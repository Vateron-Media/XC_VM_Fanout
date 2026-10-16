package front

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

// acmePrefix is certbot's http-01 challenge path; it must keep working through
// the front so renewals succeed.
const acmePrefix = "/.well-known/acme-challenge/"

// acmeToken is a single http-01 challenge name: base64url characters only, so
// it can name nothing but a file in the challenge directory.
var acmeToken = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// serveKind maps a viewer's public path to the gateway stream kind the front
// answers in-process, as nginx's server-level rewrites do, or "" when the
// request is not a serve route (it goes to nginx). A serve route is a single
// path-token form, e.g. /hls/<token>.
func serveKind(path string) string {
	for prefix, kind := range map[string]string{
		"/hls/": "segment", "/key/": "key", "/auth/": "live", "/vauth/": "vod",
		"/tsauth/": "timeshift", "/thauth/": "thumb", "/subauth/": "subtitle",
	} {
		if rest, ok := strings.CutPrefix(path, prefix); ok && rest != "" && !strings.Contains(rest, "/") {
			return kind
		}
	}
	return ""
}

// Server is the LB's public HTTP front (2.8.0 Phase 3). It terminates TLS,
// serves certbot's ACME challenge, answers the serve routes from the in-process
// gateway, and reverse-proxies everything else — and the gateway's PHP-defer
// verdicts — to the nginx behind it. As more moves in-process the proxied set
// shrinks until nginx is gone.
// serveRate / serveBurst mirror nginx's zone one on the serve routes
// (limit_req zone=one burst=8; zone one = 20 r/s), which the front must apply
// itself since those routes no longer reach nginx.
const (
	serveRate  = 20
	serveBurst = 8
)

type Server struct {
	gateway http.Handler
	proxy   http.Handler
	webroot string
	trusted *TrustedProxies
	limiter *rateLimiter
}

// NewServer answers serve routes from gateway, proxies the rest to the nginx
// backend, and serves the ACME challenge from webroot (certbot's -w directory).
// trusted is the set_real_ip_from list the front uses to resolve the viewer's
// address (see TrustedProxies.RealIP).
func NewServer(gateway http.Handler, nginx *url.URL, webroot string, trusted *TrustedProxies) *Server {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(nginx)
			// The front is the edge and the authority on the viewer's address:
			// resolve it and pass it as the only forwarded value, dropping any the
			// client sent. nginx (trusting the front) reads it straight back.
			ip := trusted.RealIP(pr.In)
			pr.Out.Header.Set("X-Real-IP", ip)
			pr.Out.Header.Set("X-Forwarded-For", ip)
		},
	}
	return &Server{
		gateway: gateway,
		proxy:   rp,
		webroot: webroot,
		trusted: trusted,
		limiter: newRateLimiter(serveRate, serveBurst),
	}
}

// serveACME answers certbot's http-01 challenge from the webroot. Only a single
// validated token name is served (no directory listing, no path that could
// leave the challenge directory).
func (s *Server) serveACME(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.URL.Path, acmePrefix)
	if !acmeToken.MatchString(token) {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, filepath.Join(s.webroot, ".well-known", "acme-challenge", token))
}

func clientIP(r *http.Request) string {
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return h
	}
	return r.RemoteAddr
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, acmePrefix) {
		s.serveACME(w, r)
		return
	}
	if kind := serveKind(r.URL.Path); kind != "" {
		s.serveStream(w, r, kind)
		return
	}
	s.proxy.ServeHTTP(w, r)
}

// serveStream hands a viewer's serve request to the in-process gateway the way
// nginx does — the original URI and address in headers, the path rewritten to
// /stream/<kind>. A verdict the gateway hands back to PHP (X-Accel-Redirect) is
// re-proxied to nginx, so PHP answers it unchanged during the transition.
func (s *Server) serveStream(w http.ResponseWriter, r *http.Request, kind string) {
	ip := s.trusted.RealIP(r)
	if !s.limiter.allow(ip) {
		http.Error(w, "", http.StatusServiceUnavailable) // nginx limit_req's default status
		return
	}
	gr := r.Clone(r.Context())
	gr.Header.Set("X-XC-Original-URI", r.URL.RequestURI())
	gr.Header.Set("X-XC-Client-IP", ip)
	gr.Header.Set("X-XC-Host", r.Host)
	gr.URL = &url.URL{Path: "/stream/" + kind}
	gr.RequestURI = ""

	gi := &interceptor{rw: w, header: make(http.Header)}
	s.gateway.ServeHTTP(gi, gr)
	if gi.toPHP() {
		s.proxy.ServeHTTP(w, r)
	}
}

// interceptor captures the gateway's response so a PHP-defer verdict
// (X-Accel-Redirect, set with no body) becomes a re-proxy to nginx instead of
// reaching the viewer; any real answer (serve, 404, redirect) streams through.
type interceptor struct {
	rw       http.ResponseWriter
	header   http.Header
	wrote    bool
	deferred bool
}

func (i *interceptor) Header() http.Header { return i.header }

func (i *interceptor) WriteHeader(code int) {
	if i.wrote || i.deferred {
		return
	}
	if i.header.Get("X-Accel-Redirect") != "" {
		i.deferred = true
		return
	}
	dst := i.rw.Header()
	for k, v := range i.header {
		dst[k] = v
	}
	i.rw.WriteHeader(code)
	i.wrote = true
}

func (i *interceptor) Write(b []byte) (int, error) {
	if !i.wrote && !i.deferred {
		i.WriteHeader(http.StatusOK)
	}
	if i.deferred {
		return len(b), nil // swallowed: nginx will answer
	}
	return i.rw.Write(b)
}

// Flush streams a served response (playlist/segment) to the viewer as it comes.
func (i *interceptor) Flush() {
	if f, ok := i.rw.(http.Flusher); ok && i.wrote {
		f.Flush()
	}
}

// toPHP reports, after the gateway returned, whether it deferred (so the front
// re-proxies to nginx). A gateway defer sets X-Accel-Redirect and returns
// without writing, so nothing reached the viewer. A no-write return without a
// redirect gets the empty 200 net/http would have sent.
func (i *interceptor) toPHP() bool {
	if i.wrote {
		return false
	}
	if i.deferred || i.header.Get("X-Accel-Redirect") != "" {
		return true
	}
	dst := i.rw.Header()
	for k, v := range i.header {
		dst[k] = v
	}
	i.rw.WriteHeader(http.StatusOK)
	return false
}

// ListenAndServe binds addr and serves HTTPS with the reloading certificate.
func (s *Server) ListenAndServe(addr string, certs *CertReloader) error {
	srv := &http.Server{Addr: addr, Handler: s, TLSConfig: certs.TLSConfig()}
	return srv.ListenAndServeTLS("", "") // the certificate comes from TLSConfig.GetCertificate
}
