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
	"github.com/Vateron-Media/XC_VM_Fanout/internal/server"
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
	// HandOverFile serves local files back to back to a viewer counted under
	// uuid (pid 0), its range honoured and the panel's throttle applied, as the
	// panel's handOverFile does; false (nothing written) when a part is not
	// under a file root, so the caller can still hand the request to PHP.
	HandOverFile(w http.ResponseWriter, r *http.Request, id, uuid string, parts []server.FilePart, contentType string, limitPerc int, rate int64, rng string) bool
	FedPlaylist(id string) string
	Fed(id string) bool
	// Registered reports whether the node holds a pull config for the stream
	// (it can be started on demand), as opposed to unknown or externally fed.
	Registered(id string) bool
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
	case "/stream/segment", "/stream/key", "/stream/live", "/stream/thumb", "/stream/subtitle", "/stream/vod", "/stream/timeshift":
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
	case "thumb":
		j.verdict = JudgeThumb(p, token, now.Unix(), s.env(p, now, touch))
	case "subtitle":
		j.verdict = JudgeSubtitle(p, token, now.Unix(), s.env(p, now, touch))
	case "vod":
		j.verdict, j.refresh = JudgeVOD(p, token, ip, r.Header.Get("User-Agent"), now.Unix(), s.liveEnv(p, now))
	case "timeshift":
		j.verdict, j.refresh = JudgeTimeshift(p, token, ip, r.Header.Get("User-Agent"), now.Unix(), s.liveEnv(p, now))
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
		Mtime: func(path string) (int64, bool) {
			fi, err := os.Stat(path)
			if err != nil {
				return 0, false
			}
			return fi.ModTime().Unix(), true
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
		OnDisk:     func(stream int) bool { return exists(p.Paths.Streams + strconv.Itoa(stream) + "_.m3u8") },
		Playlist:   func(stream int) string { return s.node.FedPlaylist(strconv.Itoa(stream)) },
		Fed:        func(stream int) bool { return s.node.Fed(strconv.Itoa(stream)) },
		Registered: func(stream int) bool { return s.node.Registered(strconv.Itoa(stream)) },
		Record: func(uuid string) (map[string]json.RawMessage, bool, bool) {
			if p.Paths.AgentSock == "" {
				return nil, false, false
			}
			return s.agentFor(p.Paths.AgentSock).Get(uuid)
		},
		AgentAlive: func() bool { return agentAlive(p.Paths.Flows, now) },
		File:       exists,
		FileSize: func(path string) (int64, bool) {
			fi, err := os.Stat(path)
			if err != nil || !fi.Mode().IsRegular() {
				return 0, false
			}
			return fi.Size(), true
		},
		Offset: func(path string) int64 {
			b, err := os.ReadFile(path + ".offset")
			if err != nil || len(b) > 32 {
				return 0
			}
			return phpIntval(b)
		},
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
	if j.refresh != nil && j.refresh.New && j.refresh.Admission != "" {
		// A new viewer the agent has to admit: its answer is known only by
		// asking, which serving does and judging does not. Not compared.
		v = verdict(PHP, "admission")
	}
	s.count(kind+" "+string(v.Action)+" "+v.Reason, s.now().Sub(now))
	s.book.Gateway(r.Header.Get("X-XC-Request-ID"), kind, v)
	// No token in the log: it is a credential.
	dlog.Logf("gateway", "shadow %s: %s (%s) stream=%d seq=%d ip=%s", kind, v.Action, v.Reason, v.Stream, v.Seq, r.Header.Get("X-XC-Client-IP"))
}

// serves is whether the node's mode lets the gateway answer this kind. The
// modes are an additive ladder: each rung serves its own kind and everything
// the rung below it does.
func serves(mode, kind string) bool {
	switch mode {
	case "segments+playlist+thumb+subtitle+vod+timeshift":
		if kind == "timeshift" {
			return true
		}
		fallthrough
	case "segments+playlist+thumb+subtitle+vod":
		if kind == "vod" {
			return true
		}
		fallthrough
	case "segments+playlist+thumb+subtitle":
		if kind == "subtitle" {
			return true
		}
		fallthrough
	case "segments+playlist+thumb":
		if kind == "thumb" {
			return true
		}
		fallthrough
	case "segments+playlist":
		if kind == "live" {
			return true
		}
		fallthrough
	case "segments":
		return kind == "segment" || kind == "key"
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
	if v.Action == Serve && j.kind == "thumb" {
		why := s.serveThumb(w, p, v, now)
		if why == "" {
			return
		}
		j.verdict = verdict(PHP, why)
		v = j.verdict
	}
	if v.Action == Serve && j.kind == "subtitle" {
		why := s.serveSubtitle(w, p, v)
		if why == "" {
			return
		}
		j.verdict = verdict(PHP, why)
		v = j.verdict
	}
	if v.Action == Serve && j.kind == "vod" {
		why := s.serveVOD(w, r, p, j.refresh, now)
		if why == "" {
			return
		}
		j.verdict = verdict(PHP, why)
		v = j.verdict
	}
	if v.Action == Serve && j.kind == "timeshift" {
		why := s.serveTimeshift(w, r, p, j.refresh, now)
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
	case "thumb":
		return "@gw_thumb_php"
	case "subtitle":
		return "@gw_subtitle_php"
	case "vod":
		return "@gw_vod_php"
	case "timeshift":
		return "@gw_timeshift_php"
	}
	return "@gw_segment_php"
}

// serveThumb answers thumb.php's happy path: the stream's thumbnail with the
// headers thumb.php sends. A node with send_unique_header set never reaches
// here (JudgeThumb hands those to PHP, which writes the per-response cookie),
// so no cookie is written. "" when served; else why PHP answers after all (the
// file went away between the judgement and here).
func (s *Server) serveThumb(w http.ResponseWriter, p *Policy, v Verdict, now time.Time) string {
	fi, err := os.Stat(v.Path)
	if err != nil {
		return "stale"
	}
	body, err := os.ReadFile(v.Path)
	if err != nil {
		return "stale"
	}
	// thumb.php's headers: unlike segment.php's policyHeaders, it sets
	// X-Content-Type-Options only with the protection headers, not always.
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
	age := now.Unix() - fi.ModTime().Unix()
	if age < 0 {
		age = 0
	}
	h.Set("Age", strconv.FormatInt(age, 10))
	h.Set("Content-Type", "image/jpg")
	_, _ = w.Write(body)
	return ""
}

// serveSubtitle answers subtitle.php's happy path: the VOD subtitle file, as
// WebVTT when the token asked, with the headers subtitle.php sends. Like
// serveThumb, a node with send_unique_header set never reaches here. "" when
// served; else why PHP answers after all (the file went away since the judgement).
func (s *Server) serveSubtitle(w http.ResponseWriter, p *Policy, v Verdict) string {
	body, err := os.ReadFile(v.Path)
	if err != nil {
		return "stale"
	}
	ext := "srt"
	if v.WebVTT {
		body = convertVTT(body)
		ext = "vtt"
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
	name := strconv.Itoa(v.Stream) + "_" + strconv.FormatInt(v.Seq, 10) + "." + ext
	h.Set("Content-Description", "File Transfer")
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Disposition", `attachment; filename="`+name+`"`)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
	return ""
}

// serveLive answers a playlist refresh or a TS viewer as live.php does: the
// record refreshed in the agent (a new TS viewer's created, admitted by the
// agent), the line's limit spooled for MAIN, then the viewer's marker touched
// and the playlist tokenized, or the stream served from fanout's ring. "" when served, else
// why PHP answers it after all (it then does the same, as before).
func (s *Server) serveLive(w http.ResponseWriter, r *http.Request, p *Policy, ref *Refresh, now time.Time) string {
	ip := r.Header.Get("X-XC-Client-IP")
	if ref.New {
		// live.php's createLive: the record stored, the viewer of a line with a
		// limit admitted by the agent. One it refuses is PHP's to answer
		// (StreamAuth::refuseAdmission): it asks again and is refused the same.
		stored, refused := s.agentFor(p.Paths.AgentSock).Register(ref.UUID, ref.Record, ref.Admission)
		if refused {
			return "refused"
		}
		if !stored {
			return "agent-put"
		}
	} else {
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
	}
	if ref.Limit {
		if err := spoolAppend(p.Paths.Spool, "p0", "conn.limit", connLimit(ref, ip), now); err != nil {
			return "spool"
		}
	}
	if ref.Warmup && !ref.TS {
		// serve-only HLS: the stream is registered but idle. Route to fanout's own
		// HLS handler so its touch() starts the puller; it answers "no segments
		// yet" (404) and the player retries while the puller warms up.
		in := r.Clone(r.Context())
		in.URL = &url.URL{Path: "/hls/" + strconv.Itoa(ref.Stream) + "/index.m3u8"}
		in.RequestURI = in.URL.RequestURI()
		s.segments.ServeHTTP(w, in)
		return ""
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

// serveVOD answers vod.php's fanout-daemon happy path: the connection recorded
// (or refreshed) in the agent with pid 0 and admitted, the limit spooled for
// MAIN, the viewer's marker touched, then the movie file handed to the daemon
// (node.HandOverFile) with vod.php's content type, throttle and range. A node
// with send_unique_header, a direct proxy, a legacy node or a connection a PHP
// worker feeds never reaches here (JudgeVOD hands those to PHP). "" when served,
// else why PHP answers it after all.
func (s *Server) serveVOD(w http.ResponseWriter, r *http.Request, p *Policy, ref *Refresh, now time.Time) string {
	if why := s.serveFileConn(w, r, p, ref, now, json.RawMessage("0")); why != "" {
		return why
	}
	if !s.node.HandOverFile(w, r, strconv.Itoa(ref.Stream), ref.UUID, []server.FilePart{{Path: ref.Path, Length: -1}}, ref.ContentType, ref.LimitPerc, ref.Rate, ref.Range) {
		return "handover"
	}
	return ""
}

// serveTimeshift answers timeshift.php's two arms. The file arm (default): the
// connection as serveFileConn records it, then the archive minutes (ref.Parts,
// the first from its .offset) handed to the daemon as one video/mp2t response.
// The m3u8 arm: the same connection (container "hls", pid untouched) and a
// VOD-type playlist of catch-up segment tokens minted here. A proxy, a
// worker-kill and a legacy node never reach here (JudgeTimeshift → PHP).
func (s *Server) serveTimeshift(w http.ResponseWriter, r *http.Request, p *Policy, ref *Refresh, now time.Time) string {
	m3u8 := ref.ContentType == "application/x-mpegurl"
	refreshPID := json.RawMessage("0")
	if m3u8 {
		refreshPID = nil // a playlist viewer keeps its pid (null), as timeshift.php does
	}
	if why := s.serveFileConn(w, r, p, ref, now, refreshPID); why != "" {
		return why
	}
	if m3u8 {
		body, err := timeshiftPlaylist(p, ref, r.Header.Get("X-XC-Client-IP"))
		if err != nil {
			return "mint"
		}
		h := w.Header()
		h.Set("Content-Type", "application/x-mpegurl")
		h.Set("Content-Length", strconv.Itoa(len(body)))
		// Fixed HLS tags and /hls/<token> lines; each token is a sealed (AES-GCM,
		// base64url) blob minted by mintOwn, so no request data reaches the body
		// as text, and it is served as application/x-mpegurl, not HTML (as serveLive's).
		// nosemgrep: go.net.xss.no-direct-write-to-responsewriter-taint.no-direct-write-to-responsewriter-taint
		_, _ = w.Write([]byte(body))
		return ""
	}
	if !s.node.HandOverFile(w, r, strconv.Itoa(ref.Stream), ref.UUID, ref.Parts, ref.ContentType, ref.LimitPerc, ref.Rate, ref.Range) {
		return "handover"
	}
	return ""
}

// serveFileConn records (or refreshes) a file viewer's connection in the agent
// as vod.php/timeshift.php do, spools its line limit, touches its marker and
// writes the policy's stream headers — the shared lead of serveVOD and
// serveTimeshift, before the bytes. refreshPID is the pid a refresh (updateLive)
// sets — the daemon's "0" for a file serve, nil for the m3u8 playlist (which
// leaves the viewer's pid as it is). A node with send_unique_header never
// reaches here. "" when ready to serve, else why PHP answers it after all.
func (s *Server) serveFileConn(w http.ResponseWriter, r *http.Request, p *Policy, ref *Refresh, now time.Time, refreshPID json.RawMessage) string {
	ip := r.Header.Get("X-XC-Client-IP")
	if ref.New {
		// openRecord: stored, a limited line admitted by the agent.
		stored, refused := s.agentFor(p.Paths.AgentSock).Register(ref.UUID, ref.Record, ref.Admission)
		if refused {
			return "refused"
		}
		if !stored {
			return "agent-put"
		}
	} else {
		// updateLive.
		rec := make(map[string]json.RawMessage, len(ref.Record)+4)
		for k, v := range ref.Record {
			rec[k] = v
		}
		if refreshPID != nil {
			rec["pid"] = refreshPID
		}
		rec["server_id"] = ref.ServerID
		rec["proxy_id"] = json.RawMessage("null")
		rec["hls_last_read"] = json.RawMessage(strconv.FormatInt(now.Unix()-p.TimeOffset, 10))
		rec["hls_end"] = json.RawMessage("0")
		if !s.agentFor(p.Paths.AgentSock).Put(ref.UUID, rec) {
			return "agent-put"
		}
	}
	if ref.Limit {
		if err := spoolAppend(p.Paths.Spool, "p0", "conn.limit", connLimit(ref, ip), now); err != nil {
			return "spool"
		}
	}
	// The viewer's marker (CONS_TMP_PATH/<uuid>).
	marker := filepath.Join(p.Paths.Cons, ref.UUID)
	if f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		f.Close()
		_ = os.Chtimes(marker, now, now)
	}
	// sendStreamHeaders, as thumb/subtitle send them (protection gates both
	// headers), plus X-Accel-Buffering with use_buffer off.
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
	if !p.Live.UseBuffer {
		h.Set("X-Accel-Buffering", "no")
	}
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
// /hls/<token>, "key" for /key/<token>, "live" for /auth/<token>, "thumb" for
// /thauth/<token>, "subtitle" for /subauth/<token>, "vod" for /vauth/<token>,
// "timeshift" for /tsauth/<token>) and its token, as nginx's server-level
// rewrites read it; "" for anything else.
func ParseURI(uri string) (kind, token string) {
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		uri = uri[:i]
	}
	for prefix, k := range map[string]string{"/hls/": "segment", "/key/": "key", "/auth/": "live", "/thauth/": "thumb", "/subauth/": "subtitle", "/vauth/": "vod", "/tsauth/": "timeshift"} {
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
