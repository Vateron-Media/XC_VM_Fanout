package front

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
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
	// xfileRate / xfileBurst mirror nginx's zone xfile on /xfile (50 r/s, burst
	// 100), keyed on the raw TCP peer (node-to-node), refusing 429.
	xfileRate  = 50
	xfileBurst = 100
)

// imagesPrefix is nginx's static image location (alias storage/images). scriptExt
// is the set of extensions nginx returns 403 for there — the image store never
// executes code.
const imagesPrefix = "/images/"

var scriptExt = regexp.MustCompile(`(?i)\.(php\d*|phtml|phar)$`)

type Server struct {
	gateway      http.Handler
	proxy        http.Handler
	webroot      string
	trusted      *TrustedProxies
	limiter      *rateLimiter
	images       string       // storage/images root; empty leaves /images to the proxy
	agentProxy   http.Handler // /xfile owner, reverse-proxied to the agent socket; nil leaves /xfile to the proxy
	xfileLimiter *rateLimiter // the peer-keyed /xfile limiter (nginx zone xfile)
}

// internalPrefixes are nginx's `internal;` X-Accel targets — reachable only via
// an X-Accel-Redirect, never by an external client. The Go gateway serves that
// content in-process (its own segments handler, "without the hop"), so these
// have no front route; a direct external request gets 404, exactly as nginx's
// `internal;` returns for one. Serving them would hand out segments/files with
// no token check.
var internalPrefixes = []string{"/xc_hls/", "/xc_fanout/", "/xc_fanout_hls/", "/xc_fanout_file/"}

func isInternalPath(p string) bool {
	for _, prefix := range internalPrefixes {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// statusPath is nginx's local-only node status location (allow 127.0.0.1; deny
// all). The front, as the edge, enforces that itself on the TCP peer.
const statusPath = "/status"

// NewServer answers serve routes from gateway, proxies the rest to the nginx
// backend, and serves the ACME challenge from webroot (certbot's -w directory).
// trusted is the set_real_ip_from list the front uses to resolve the viewer's
// address (see TrustedProxies.RealIP). A nil nginx URL means no backend (a
// serve-only front with nginx gone): a request that would proxy gets 502.
func NewServer(gateway http.Handler, nginx *url.URL, webroot string, trusted *TrustedProxies) *Server {
	s := &Server{
		gateway:      gateway,
		webroot:      webroot,
		trusted:      trusted,
		limiter:      newRateLimiter(serveRate, serveBurst),
		xfileLimiter: newRateLimiter(xfileRate, xfileBurst),
	}
	if nginx != nil {
		s.proxy = &httputil.ReverseProxy{
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
	}
	return s
}

// proxyOrError proxies to the nginx backend, or returns 502 when there is none
// (a serve-only front with nginx gone).
func (s *Server) proxyOrError(w http.ResponseWriter, r *http.Request) {
	if s.proxy == nil {
		http.Error(w, "", http.StatusBadGateway)
		return
	}
	s.proxy.ServeHTTP(w, r)
}

// isLoopback reports whether ip is a loopback address.
func isLoopback(ip string) bool {
	parsed := net.ParseIP(ip)
	return parsed != nil && parsed.IsLoopback()
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

// WithImages makes the front serve /images/ from dir (nginx's storage/images
// alias) in-process instead of proxying it. An empty dir leaves /images to the
// proxy. Returns the server for chaining.
func (s *Server) WithImages(dir string) *Server {
	s.images = dir
	return s
}

// serveImages serves a file under /images/ from the image store, matching
// nginx: no code execution (403), no auto-index.html (404), no directory
// listing, and confined to the root.
func (s *Server) serveImages(w http.ResponseWriter, r *http.Request) {
	// Normalise FIRST, then run every check on the cleaned path: a check against
	// the raw remainder is a differential (e.g. "evil.php/." fails a `\.php$`
	// test but cleans to "/evil.php"). The leading / anchors Clean so .. cannot
	// escape the root.
	clean := path.Clean("/" + strings.TrimPrefix(r.URL.Path, imagesPrefix))
	if clean == "/" || clean == "/index.html" || strings.HasSuffix(clean, "/index.html") {
		http.NotFound(w, r)
		return
	}
	if scriptExt.MatchString(clean) {
		http.Error(w, "", http.StatusForbidden)
		return
	}
	full := filepath.Join(s.images, filepath.FromSlash(clean))
	if fi, err := os.Stat(full); err != nil || fi.IsDir() {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, full)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, acmePrefix) {
		s.serveACME(w, r)
		return
	}
	if s.images != "" && strings.HasPrefix(r.URL.Path, imagesPrefix) {
		s.serveImages(w, r)
		return
	}
	// nginx's `internal;` X-Accel targets are never reachable by an external
	// client; the gateway serves their content in-process. 404 a direct request
	// rather than proxy or serve it (no token check lives on these paths).
	if isInternalPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	// /status is local-only; the front enforces it on the TCP peer rather than
	// delegating to a backend it reaches over loopback.
	if r.URL.Path == statusPath && !isLoopback(clientIP(r)) {
		http.Error(w, "", http.StatusForbidden)
		return
	}
	// /xfile is a public, owner-side cross-node file read. With an agent socket
	// configured, the front proxies it there (peer-rate-limited); otherwise nginx
	// still owns it during the transition.
	if r.URL.Path == "/xfile" {
		s.serveXFile(w, r)
		return
	}
	// /stream/rtmp is nginx_rtmp's loopback auth-notify callback; the agent
	// answers it (allow/deny), or nginx during the transition.
	if r.URL.Path == "/stream/rtmp" {
		s.serveRtmpNotify(w, r)
		return
	}
	if kind := serveKind(r.URL.Path); kind != "" {
		s.serveStream(w, r, kind)
		return
	}
	// /admin/<kind> are cross-node relay-source pulls answered by the agent (it
	// holds the keys the admission arms need). Only the paths the agent serves are
	// routed here; the rest still reach nginx during the transition.
	if adminAgentPath(r.URL.Path) {
		s.serveAdmin(w, r)
		return
	}
	s.proxyOrError(w, r)
}

// adminAgentPath reports whether an /admin path is one the agent serves in-process
// now (the serving handlers land slice by slice; the rest stay nginx's).
func adminAgentPath(p string) bool {
	return p == "/admin/thumb" || p == "/admin/vod"
}

// serveAdmin routes an /admin relay-source request to the agent, rate-limited on
// the resolved client IP like nginx's zone one. With no agent socket it falls
// back to nginx during the transition; a dial failure does too (the agentProxy
// ErrorHandler). The agent does the admission (it holds the node keys).
func (s *Server) serveAdmin(w http.ResponseWriter, r *http.Request) {
	if s.agentProxy == nil {
		s.proxyOrError(w, r)
		return
	}
	if !s.limiter.allow(s.trusted.RealIP(r)) {
		http.Error(w, "", http.StatusServiceUnavailable) // nginx's limit_req default status
		return
	}
	s.agentProxy.ServeHTTP(w, r)
}

// serveXFile routes the public /xfile to the owner handler in the agent. The
// limiter is keyed on the raw TCP peer (node-to-node, not the viewer real-IP)
// and refuses 429, matching nginx's zone xfile. A dial failure (agent down or
// not yet serving) falls back to nginx during the transition — an agent
// *response*, even a 404, is authoritative and streams through unchanged. With
// no agent socket configured, nginx still owns /xfile.
func (s *Server) serveXFile(w http.ResponseWriter, r *http.Request) {
	if s.agentProxy == nil {
		s.proxyOrError(w, r)
		return
	}
	if !s.xfileLimiter.allow(clientIP(r)) {
		http.Error(w, "", http.StatusTooManyRequests) // nginx's limit_req_status 429
		return
	}
	s.agentProxy.ServeHTTP(w, r)
}

// serveRtmpNotify routes nginx_rtmp's /stream/rtmp auth-notify callback to the
// agent. The callback is loopback-only (nginx_rtmp runs on this box; rtmp.php
// required REMOTE_ADDR==127.0.0.1), so a non-loopback peer is refused here. With
// an agent socket the request is proxied there (raw query preserved; a dial
// failure falls back to nginx via the proxy's ErrorHandler); without one nginx
// still owns /stream/rtmp during the transition. No rate limiter — the only
// caller is the local nginx_rtmp.
func (s *Server) serveRtmpNotify(w http.ResponseWriter, r *http.Request) {
	if !isLoopback(clientIP(r)) {
		http.NotFound(w, r)
		return
	}
	if s.agentProxy == nil {
		s.proxyOrError(w, r)
		return
	}
	s.agentProxy.ServeHTTP(w, r)
}

// WithAgentSocket makes the front serve /xfile from the owner handler in the
// agent at sock (the agent's local unix socket, which the gateway also dials),
// instead of proxying it to nginx. The raw request-target is preserved verbatim
// so the fetcher's relay-auth proof still verifies. An empty sock leaves /xfile
// to the proxy. Returns the server for chaining.
func (s *Server) WithAgentSocket(sock string) *Server {
	if sock == "" {
		return s
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
		MaxIdleConnsPerHost: 8,
	}
	s.agentProxy = &httputil.ReverseProxy{
		Transport: tr,
		Rewrite: func(pr *httputil.ProxyRequest) {
			// The agent verifies the proof over its raw r.RequestURI, so forward
			// the target byte-for-byte: set RawQuery from the inbound request
			// rather than letting the framework re-encode it.
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = "agent"
			pr.Out.URL.Path = pr.In.URL.Path
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			// The front is the authority on the caller's address: resolve it and
			// forward the only trusted copy (dropping any the client set), so the
			// agent's /admin uitoken/password/loopback arms key on the real peer.
			pr.Out.Header.Set("X-XC-Client-IP", s.trusted.RealIP(pr.In))
		},
		ErrorHandler: func(ew http.ResponseWriter, er *http.Request, err error) {
			// Only a transport error (the agent could not be reached) lands here;
			// an HTTP response from the agent is not an error. Fall back to nginx
			// during the transition, else 502.
			s.proxyOrError(ew, er)
		},
	}
	return s
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
		s.proxyOrError(w, r)
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
