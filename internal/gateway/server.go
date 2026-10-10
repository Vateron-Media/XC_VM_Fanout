package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
)

// Server is the gateway's handler on its own socket (gw.sock), which only
// nginx reaches, with the viewer's original URI and address in
// X-XC-Original-URI and X-XC-Client-IP (the panel's GatewayNginxConfig).
//
//   - POST /shadow (gateway_mode shadow): nginx mirrors each /hls/, /key/ and
//     /auth/ request PHP serves; the gateway judges it as PHP would and counts
//     the verdict. POST /shadow/php: PHP reports what it answered the same
//     request (its nginx request id), and the book counts the two as agreeing
//     or not (ShadowBook), so the decisions are checked before any is served.
//   - /stream/segment, /stream/key (gateway_mode segments): nginx passes the
//     request itself, as its server-level rewrites made it. The
//     gateway serves a live daemon segment from fanout's HLS in-process and a
//     key from its file, answers PHP's 404 or redirect, and hands anything
//     else back to PHP (X-Accel-Redirect to @gw_segment_php or @gw_key_php),
//     which then answers exactly as before.
//   - GET /stats: the verdict counts and how long each decision took (the
//     judgement, not the bytes served after it: what the gateway adds).
type Server struct {
	policy   *PolicyFile
	segments http.Handler
	node     Node
	now      func() time.Time

	agentMu   sync.Mutex
	agentSock string
	agent     *AgentConns

	book *ShadowBook

	mu     sync.Mutex
	counts map[string]uint64
	took   map[string]*[len(latencyBuckets) + 1]uint64
}

// latencyBuckets are the upper bounds /stats sorts each verdict's time into.
var latencyBuckets = [...]time.Duration{100 * time.Microsecond, 500 * time.Microsecond, time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond}

// Node is what the gateway asks of the fanout it runs in (the server
// package's Manager): a file served from an offset, as its file server serves
// a manifest of one part, a stream's HLS playlist while it is on air, and
// whether it is on air.
type Node interface {
	ServeFilePart(w http.ResponseWriter, r *http.Request, path string, offset int64, contentType string)
	FedPlaylist(id string) string
	Fed(id string) bool
}

// NewServer judges with the policy file at policyPath, serves live segments
// and TS through segments, fanout's client handler (its /hls/<id>/<seq>.ts
// and /live/<id>), and catch-up minutes and playlists through node.
func NewServer(policyPath string, segments http.Handler, node Node) *Server {
	return &Server{policy: NewPolicyFile(policyPath), segments: segments, node: node, book: NewShadowBook(""), now: time.Now, counts: map[string]uint64{}, took: map[string]*[len(latencyBuckets) + 1]uint64{}}
}

// KeepShadowIn keeps the shadow comparison in path across restarts (call before serving).
func (s *Server) KeepShadowIn(path string) { s.book = NewShadowBook(path) }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/shadow":
		s.shadow(r)
		w.WriteHeader(http.StatusNoContent)
	case "/shadow/php":
		// What PHP answered a mirrored request (GatewayShadow): its other side.
		s.book.PHP(r.Header.Get("X-XC-Request-ID"), r.Header.Get("X-XC-Kind"), r.Header.Get("X-XC-Outcome"))
		w.WriteHeader(http.StatusNoContent)
	case "/stream/segment", "/stream/key", "/stream/live":
		s.serve(w, r)
	case "/stats":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"counts": s.Stats(), "judge_us_le": s.latency(), "shadow": s.book.State()})
	default:
		http.NotFound(w, r)
	}
}

// judged is a request's verdict, with what serving a playlist refresh needs.
type judged struct {
	kind    string
	policy  *Policy
	verdict Verdict
	refresh *Refresh
}

// judge is the verdict on the request nginx describes. touch: the viewer's
// record is touched as PHP's heartbeat does (serving); else only read (shadow).
func (s *Server) judge(r *http.Request, now time.Time, touch bool) judged {
	kind, token := ParseURI(r.Header.Get("X-XC-Original-URI"))
	ip := r.Header.Get("X-XC-Client-IP")
	p := s.policy.Get(now)
	j := judged{kind: kind, policy: p}
	if kind == "" {
		j.kind, j.verdict = "other", verdict(PHP, "uri")
		return j
	}
	if p != nil {
		if why := BootstrapRefuses(p, ip, r.Header.Get("X-XC-Host"), exists); why != "" {
			j.verdict = verdict(PHP, why)
			return j
		}
	}
	switch kind {
	case "segment":
		j.verdict = JudgeSegment(p, token, ip, now.Unix(), s.env(p, now, touch))
	case "key":
		j.verdict = JudgeKey(p, token, ip, now.Unix())
	case "live":
		j.verdict, j.refresh = JudgeLive(p, token, ip, r.Header.Get("User-Agent"), now.Unix(), s.liveEnv(p, now))
	}
	return j
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// env is the node as segment.php sees it: its markers and files, and its agent.
func (s *Server) env(p *Policy, now time.Time, touch bool) Env {
	return Env{
		Cons: func(uuid string) bool {
			_, err := os.Stat(filepath.Join(p.Paths.Cons, uuid))
			return err == nil
		},
		File: func(path string) bool {
			_, err := os.Stat(path)
			return err == nil
		},
		Heard: func(uuid string) (bool, bool) {
			if p.Paths.AgentSock == "" {
				return false, false
			}
			a := s.agentFor(p.Paths.AgentSock)
			if touch {
				return a.Touch(uuid, now.Unix()-p.TimeOffset)
			}
			return a.Peek(uuid)
		},
	}
}

// liveEnv is the node as live.php sees it for a refresh: the producer, the
// playlists, the viewer's record and the agent's spool. Read only.
func (s *Server) liveEnv(p *Policy, now time.Time) LiveEnv {
	return LiveEnv{
		StreamAlive: func(stream int, tokenPID json.RawMessage) bool {
			pid, _ := jsonInt(tokenPID)
			if b, err := os.ReadFile(p.Paths.Streams + strconv.Itoa(stream) + "_.pid"); err == nil && len(b) <= 1<<20 {
				pid = phpIntval(b)
			}
			return isStreamAlive(pid, stream)
		},
		OnDisk:   func(stream int) bool { return exists(p.Paths.Streams + strconv.Itoa(stream) + "_.m3u8") },
		Playlist: func(stream int) string { return s.node.FedPlaylist(strconv.Itoa(stream)) },
		Fed:      func(stream int) bool { return s.node.Fed(strconv.Itoa(stream)) },
		Record: func(uuid string) (map[string]json.RawMessage, bool, bool) {
			if p.Paths.AgentSock == "" {
				return nil, false, false
			}
			return s.agentFor(p.Paths.AgentSock).Get(uuid)
		},
		AgentAlive: func() bool { return agentAlive(p.Paths.Flows, now) },
	}
}

// agentFor is the client of the agent at sock, kept while the policy names it.
func (s *Server) agentFor(sock string) *AgentConns {
	s.agentMu.Lock()
	defer s.agentMu.Unlock()
	if s.agent == nil || s.agentSock != sock {
		s.agent, s.agentSock = NewAgentConns(sock), sock
	}
	return s.agent
}

func (s *Server) shadow(r *http.Request) {
	now := s.now()
	j := s.judge(r, now, false)
	kind, v := j.kind, j.verdict
	s.count(kind+" "+string(v.Action)+" "+v.Reason, s.now().Sub(now))
	s.book.Gateway(r.Header.Get("X-XC-Request-ID"), kind, v)
	// No token in the log: it is a credential.
	dlog.Logf("gateway", "shadow %s: %s (%s) stream=%d seq=%d ip=%s", kind, v.Action, v.Reason, v.Stream, v.Seq, r.Header.Get("X-XC-Client-IP"))
}

// serves is whether the node's mode lets the gateway answer this kind.
func serves(mode, kind string) bool {
	switch mode {
	case "segments":
		return kind == "segment" || kind == "key"
	case "segments+playlist":
		return kind == "segment" || kind == "key" || kind == "live"
	}
	return false
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	var j judged
	var took time.Duration // the decision's time, not the serving's: what the gateway adds
	defer func() { s.count(j.kind+" "+string(j.verdict.Action)+" "+j.verdict.Reason, took) }()
	kind, _ := ParseURI(r.Header.Get("X-XC-Original-URI"))
	if cur := s.policy.Get(now); cur == nil || !serves(cur.Mode, kind) {
		// No policy, or the node's mode moved off serving this and nginx has
		// not been reloaded yet: nothing judged (nor touched), PHP answers.
		j = judged{kind: kind, verdict: verdict(PHP, "mode")}
	} else {
		j = s.judge(r, now, true)
	}
	took = s.now().Sub(now)
	if j.kind == "" {
		j.kind = "other"
	}
	p, v := j.policy, j.verdict
	if v.Action == Serve && j.kind == "live" {
		why := s.serveLive(w, r, p, j.refresh, now)
		if why == "" {
			return
		}
		j.verdict = verdict(PHP, why)
		v = j.verdict
	}
	switch v.Action {
	case PHP:
		w.Header().Set("X-Accel-Redirect", phpLocation(j.kind))
		return
	case Redirect:
		policyHeaders(w, p)
		http.Redirect(w, r, v.Location, http.StatusFound)
		return
	case Deny:
		policyHeaders(w, p)
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(notFoundPage))
		return
	}
	policyHeaders(w, p)
	if v.Reason == "archive" {
		// segment.php's hand-over to fanout's file server (handOverFile), in-process.
		s.node.ServeFilePart(w, r, v.Path, v.Offset, "video/mp2t")
		return
	}
	if j.kind == "key" {
		// key.php echoes the file as it is, nothing when it is missing.
		key, _ := os.ReadFile(filepath.Join(p.Paths.Streams, strconv.Itoa(v.Stream)+"_.key"))
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(key)
		return
	}
	// segment.php's X-Accel-Redirect to /xc_fanout_hls/<id>_<seq>, without the hop:
	// fanout's own /hls/<id>/<seq>.ts, with the viewer for a pending overlay.
	in := r.Clone(r.Context())
	in.URL = &url.URL{Path: fmt.Sprintf("/hls/%d/%d.ts", v.Stream, v.Seq), RawQuery: url.Values{"c": {v.UUID}, "vc": {v.Codec}}.Encode()}
	in.RequestURI = in.URL.RequestURI()
	s.segments.ServeHTTP(w, in)
}

// phpLocation is nginx's PHP handler for a kind (GatewayNginxConfig).
func phpLocation(kind string) string {
	switch kind {
	case "key":
		return "@gw_key_php"
	case "live":
		return "@gw_live_php"
	}
	return "@gw_segment_php"
}

// serveLive answers a playlist refresh or a TS reconnect as live.php does
// once its connection is found: the record refreshed in the agent, the line's
// limit spooled for MAIN, then the viewer's marker touched and the playlist
// tokenized, or the stream served from fanout's ring. "" when served, else
// why PHP answers it after all (it then does the same, as before).
func (s *Server) serveLive(w http.ResponseWriter, r *http.Request, p *Policy, ref *Refresh, now time.Time) string {
	ip := r.Header.Get("X-XC-Client-IP")
	rec := make(map[string]json.RawMessage, len(ref.Record)+4)
	for k, v := range ref.Record {
		rec[k] = v
	}
	if ref.TS {
		// live.php's TS arm: the connection is fanout's (pid 0), its server unchanged.
		rec["pid"] = json.RawMessage("0")
	} else {
		rec["server_id"] = ref.ServerID
		rec["proxy_id"] = json.RawMessage("null")
	}
	rec["hls_last_read"] = json.RawMessage(strconv.FormatInt(now.Unix()-p.TimeOffset, 10))
	rec["hls_end"] = json.RawMessage("0")
	if !s.agentFor(p.Paths.AgentSock).Put(ref.UUID, rec) {
		return "agent-put"
	}
	if ref.Limit {
		if err := spoolAppend(p.Paths.Spool, "p0", "conn.limit", connLimit(ref, ip), now); err != nil {
			return "spool"
		}
	}
	var body string
	if !ref.TS {
		var err error
		if body, err = Tokenize(p, ref, ip, now); err != nil {
			return "tokenize"
		}
	}
	if !ref.TS {
		// A TS viewer has no marker: live.php's hand-off to fanout leaves none.
		marker := filepath.Join(p.Paths.Cons, ref.UUID)
		if f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			f.Close()
			_ = os.Chtimes(marker, now, now)
		}
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	if p.Headers.Server != "" {
		h.Set("Server", p.Headers.Server)
	}
	if p.Headers.Protection {
		h.Set("X-XSS-Protection", "0")
		h.Set("X-Content-Type-Options", "nosniff")
	}
	if p.Headers.AltsvcPort > 0 {
		h.Set("Alt-Svc", altsvc(p.Headers.AltsvcPort))
	}
	if ref.NoBuffer || ref.TS {
		h.Set("X-Accel-Buffering", "no")
	}
	if ref.TS {
		// live.php's X-Accel-Redirect to /xc_fanout/<id>, without the hop:
		// fanout's own /live/<id>, which counts the viewer under its uuid
		// (fanout_sync, the agent) and sends the overlay due to it.
		in := r.Clone(r.Context())
		in.URL = &url.URL{Path: "/live/" + strconv.Itoa(ref.Stream), RawQuery: url.Values{"c": {ref.UUID}, "prebuffer": {strconv.FormatInt(ref.Prebuffer, 10)}, "vc": {ref.Codec}}.Encode()}
		in.RequestURI = in.URL.RequestURI()
		h.Set("Content-Type", "video/mp2t")
		s.segments.ServeHTTP(w, in)
		return ""
	}
	h.Set("Content-Type", "application/x-mpegurl")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("Cache-Control", "no-store, no-cache, must-revalidate")
	// The daemon's playlist with base64url tokens: the address (nginx's
	// $remote_addr, not the client's header) is only ever sealed inside them.
	// nosemgrep: go.net.xss.no-direct-write-to-responsewriter-taint.no-direct-write-to-responsewriter-taint
	_, _ = w.Write([]byte(body))
	return ""
}

// policyHeaders are the headers segment.php and key.php send.
func policyHeaders(w http.ResponseWriter, p *Policy) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("X-Content-Type-Options", "nosniff")
	if p.Headers.Server != "" {
		h.Set("Server", p.Headers.Server)
	}
	if p.Headers.Protection {
		h.Set("X-XSS-Protection", "0")
	}
	if port := p.Headers.AltsvcPort; port > 0 {
		h.Set("Alt-Svc", altsvc(port))
	}
}

// altsvc is the panel's Alt-Svc header for the node's HTTPS port.
func altsvc(port int) string {
	q := strconv.Itoa(port)
	return `h3-29=":` + q + `"; ma=2592000,h3-T051=":` + q + `"; ma=2592000,h3-Q050=":` + q + `"; ma=2592000,h3-Q046=":` + q + `"; ma=2592000,h3-Q043=":` + q + `"; ma=2592000,quic=":` + q + `"; ma=2592000; v="46,43"`
}

// notFoundPage is PHP's 404 body (ErrorResponder::render404).
const notFoundPage = "<html>\r\n<head><title>404 Not Found</title></head>\r\n<body>\r\n<center><h1>404 Not Found</h1></center>\r\n<hr><center>nginx</center>\r\n</body>\r\n</html>\r\n" +
	"<!-- a padding to disable MSIE and Chrome friendly error page -->\r\n<!-- a padding to disable MSIE and Chrome friendly error page -->\r\n<!-- a padding to disable MSIE and Chrome friendly error page -->\r\n" +
	"<!-- a padding to disable MSIE and Chrome friendly error page -->\r\n<!-- a padding to disable MSIE and Chrome friendly error page -->\r\n<!-- a padding to disable MSIE and Chrome friendly error page -->"

// ParseURI splits a viewer's request URI into its kind ("segment" for
// /hls/<token>, "key" for /key/<token>, "live" for /auth/<token>) and its
// token, as nginx's server-level rewrites read it; "" for anything else.
func ParseURI(uri string) (kind, token string) {
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		uri = uri[:i]
	}
	for prefix, k := range map[string]string{"/hls/": "segment", "/key/": "key", "/auth/": "live"} {
		if rest, ok := strings.CutPrefix(uri, prefix); ok && !strings.Contains(rest, "/") {
			return k, rest
		}
	}
	return "", ""
}

func (s *Server) count(key string, took time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[key]++
	b := s.took[key]
	if b == nil {
		b = new([len(latencyBuckets) + 1]uint64)
		s.took[key] = b
	}
	i := 0
	for i < len(latencyBuckets) && took > latencyBuckets[i] {
		i++
	}
	b[i]++
}

// Stats is each verdict's count ("<kind> <action> <reason>") since the start.
func (s *Server) Stats() map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]uint64, len(s.counts))
	for k, n := range s.counts {
		out[k] = n
	}
	return out
}

// latency is each verdict's times, counted under the first bound (µs) they fit ("inf" past the last).
func (s *Server) latency() map[string]map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]uint64, len(s.took))
	for k, b := range s.took {
		m := map[string]uint64{}
		for i, n := range b {
			if n == 0 {
				continue
			}
			label := "inf"
			if i < len(latencyBuckets) {
				label = strconv.FormatInt(latencyBuckets[i].Microseconds(), 10)
			}
			m[label] = n
		}
		out[k] = m
	}
	return out
}
