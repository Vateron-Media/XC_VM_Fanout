package clusteragent

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The loopback relay proxy (ADR 0004, Phase 8; plan, section 7, "Local
// socket and data plane"): with its DATAPLANE flow on, a node's encoders and
// fanout pull what they relay, and read what other servers hold, from here
// instead of from a URL carrying the stream secret.
//
//	GET http://127.0.0.1:31290/relay/<k>/<stream id>.ts[?prebuffer=1]
//	    -> GET <parent>/admin/live?stream=<id>&extension=ts[&prebuffer=1]
//	       X-XCVM-Relay       the stream's relay ticket (tickets.go)
//	       X-XCVM-Relay-Auth  this node's signature over the ticket, GET, the target, MAIN's time and a fresh nonce
//	GET http://127.0.0.1:31290/xfile/<k>/<ref>[.<ext>]  (Range as the reader sends it)
//	    -> GET <owner>/xfile?o=<offset>&n=<FileChunk>, chunk by chunk
//	       X-XCVM-File        the file ticket naming ref
//	       X-XCVM-File-Auth   the same proof over the file ticket
//	       <- X-XCVM-File-Digest, checked before a byte of the chunk is passed on
//
// `k` is the loopback key (relay.key beside the state, 0600): the node's PHP
// puts it in the URLs it builds, and a request without it is refused, so a
// local user who reads an encoder's command line holds a key that unlocks
// this listener only, and no ticket. The URLs never name a ticket, so a
// refresh reaches the next connect with no encoder restart.
//
// Where the parent or owner is comes from the servers section the agent
// stores (replica/servers.json): its private address when both it and this
// node have one, else its public one, on its HTTP broadcast port, as the
// panel's legacy URLs chose. Its key, for a file's digest, comes from the
// section's signed node list, or is the panel's when the owner is MAIN.
//
// Relay bytes pass unchecked (plan, D11): a relay is authenticated at
// connect, not framed. A file chunk whose digest does not verify, is not the
// one asked for, or does not match its bytes ends the read with nothing of it
// passed on.

// RelayProxyAddr is where the loopback relay proxy listens.
const RelayProxyAddr = "127.0.0.1:31290"

// RelayKeyFile is the loopback key's name beside the state (the panel's
// DataPlane::KEY_FILE).
const RelayKeyFile = "relay.key"

// RelayUpstreamTimeout bounds a connect and the wait for the upstream's
// headers; a relay's body then streams for as long as it lasts.
var RelayUpstreamTimeout = 15 * time.Second

// RelayProxy serves the loopback listener.
type RelayProxy struct {
	a      *Agent
	key    string
	client *http.Client
	// servers overrides where a server id is reached (tests); nil reads
	// the replica's servers section.
	servers func() (*serverRoutes, error)

	mu      sync.Mutex
	routes  *serverRoutes
	routeAt time.Time
	modAt   time.Time
}

// serverRoutes is what the proxy needs of the servers section.
type serverRoutes struct {
	self    int64
	byID    map[int64]serverRoute
	nodes   map[int64]routeNode
	mainSid int64
}

type serverRoute struct {
	ip, private string
	port        int64
}

type routeNode struct {
	pub   []byte
	gen   int64
	state string
}

// LoadRelayKey reads the loopback key beside the state, making one the first
// time (32 random bytes, base64url).
func LoadRelayKey(dir string) (string, error) {
	path := filepath.Join(dir, RelayKeyFile)
	if b, err := os.ReadFile(path); err == nil {
		if k := strings.TrimSpace(string(b)); len(k) >= 22 && len(k) <= 64 && validKey(k) {
			return k, nil
		}
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	k := base64.RawURLEncoding.EncodeToString(raw)
	if err := writeFileMode(path, []byte(k+"\n"), 0o600); err != nil {
		return "", err
	}
	return k, nil
}

func validKey(k string) bool {
	for i := 0; i < len(k); i++ {
		c := k[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// NewRelayProxy is the proxy for this agent, with the loopback key k.
func (a *Agent) NewRelayProxy(k string) *RelayProxy {
	dialer := &net.Dialer{Timeout: RelayUpstreamTimeout, KeepAlive: 30 * time.Second}
	return &RelayProxy{a: a, key: k, client: &http.Client{
		Transport: &http.Transport{
			Proxy: nil, DialContext: dialer.DialContext, ResponseHeaderTimeout: RelayUpstreamTimeout,
			MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second, DisableCompression: true,
		},
		// A redirect would carry the ticket to wherever it points.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// ServeRelayProxy listens on addr until ctx ends.
func (a *Agent) ServeRelayProxy(ctx context.Context, addr, keyDir string) error {
	k, err := LoadRelayKey(keyDir)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: a.NewRelayProxy(k), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	if err := srv.Serve(l); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (p *RelayProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(parts) != 3 || subtle.ConstantTimeCompare([]byte(parts[1]), []byte(p.key)) != 1 || p.key == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	switch parts[0] {
	case "relay":
		name, ok := strings.CutSuffix(parts[2], ".ts")
		id, err := strconv.ParseInt(name, 10, 64)
		if !ok || err != nil || id < 1 || strconv.FormatInt(id, 10) != name {
			http.NotFound(w, r)
			return
		}
		p.relay(w, r, id, r.URL.Query().Get("prebuffer") == "1")
	case "xfile":
		ref, _, _ := strings.Cut(parts[2], ".")
		if len(ref) != 32 || strings.Trim(ref, "0123456789abcdef") != "" {
			http.NotFound(w, r)
			return
		}
		p.xfile(w, r, ref)
	default:
		http.NotFound(w, r)
	}
}

// relay pulls a stream from its parent with the stream's relay ticket.
func (p *RelayProxy) relay(w http.ResponseWriter, r *http.Request, id int64, prebuffer bool) {
	a := p.a
	if a.flows.Load()&FlowDataplane == 0 {
		http.Error(w, "the data plane is off", http.StatusServiceUnavailable)
		return
	}
	wire := a.tickets().relay(id)
	t, ok := a.verifiedTicket("rly", wire)
	if wire == "" || !ok {
		http.Error(w, "no relay ticket for the stream", http.StatusNotFound)
		return
	}
	parent, _ := t.Int("parent_sid")
	base, err := p.base(parent)
	if err != nil {
		a.logf("cluster: relay: stream %d: %v", id, err)
		http.Error(w, "parent unknown", http.StatusBadGateway)
		return
	}
	target := "/admin/live?stream=" + strconv.FormatInt(id, 10) + "&extension=ts"
	if prebuffer {
		target += "&prebuffer=1"
	}
	resp, err := p.upstream(r.Context(), base, target, "X-XCVM-Relay", "X-XCVM-Relay-Auth", wire)
	if err != nil {
		a.logf("cluster: relay: stream %d from server %d: %v", id, parent, err)
		http.Error(w, "upstream", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		a.logf("cluster: relay: stream %d: server %d answered %d", id, parent, resp.StatusCode)
		http.Error(w, "upstream refused", http.StatusBadGateway)
		return
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(http.StatusOK)
	flushCopy(w, resp.Body)
}

// upstream sends one signed GET to base+target.
func (p *RelayProxy) upstream(ctx context.Context, base, target, ticketHeader, authHeader, wire string) (*http.Response, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	auth, err := cc.RelayAuthHeader(p.a.Client.State.SignKey(), wire, http.MethodGet, target, p.a.Client.MainNowMs(), nonce)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(ticketHeader, wire)
	req.Header.Set(authHeader, auth)
	req.Header.Set("User-Agent", "xc_agent/relay")
	return p.client.Do(req)
}

// xfile reads a file another server owns, chunk by chunk, each checked
// against its owner's digest before it is passed on.
func (p *RelayProxy) xfile(w http.ResponseWriter, r *http.Request, ref string) {
	a := p.a
	if a.flows.Load()&FlowDataplane == 0 {
		http.Error(w, "the data plane is off", http.StatusServiceUnavailable)
		return
	}
	wire := a.tickets().file(ref)
	t, ok := a.verifiedTicket("fil", wire)
	if wire == "" || !ok {
		http.Error(w, "no file ticket", http.StatusNotFound)
		return
	}
	owner, _ := t.Int("owner_sid")
	base, err := p.base(owner)
	if err != nil {
		http.Error(w, "owner unknown", http.StatusBadGateway)
		return
	}
	panelPub, nodePub, err := p.ownerKey(owner)
	if err != nil {
		http.Error(w, "owner unknown", http.StatusBadGateway)
		return
	}
	rng, hasRange, ok := parseRange(r.Header.Get("Range"))
	if !ok {
		http.Error(w, "range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	c := &chunkReader{p: p, ctx: r.Context(), base: base, wire: wire, tid: t.Tid, owner: owner, panelPub: panelPub, nodePub: nodePub}
	// The chunk holding the start tells the file's size; a suffix range
	// needs the size first.
	first := int64(0)
	if !rng.suffix {
		first = rng.start - rng.start%cc.FileChunk
	}
	body, err := c.fetch(first)
	if err != nil {
		a.logf("cluster: xfile: %s from server %d: %v", ref, owner, err)
		http.Error(w, "upstream", http.StatusBadGateway)
		return
	}
	total := c.total
	start, end := int64(0), total-1
	if hasRange {
		if rng.suffix {
			start = max(0, total-rng.length)
		} else {
			start = rng.start
			if rng.end >= 0 && rng.end < end {
				end = rng.end
			}
		}
		if start >= total || start > end {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
			http.Error(w, "range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if at := start - start%cc.FileChunk; at != first {
			if body, err = c.fetch(at); err != nil {
				a.logf("cluster: xfile: %s from server %d: %v", ref, owner, err)
				http.Error(w, "upstream", http.StatusBadGateway)
				return
			}
			first = at
		}
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(max(0, end-start+1), 10))
	if hasRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	at := first
	for pos := start; pos <= end; {
		lo := pos - at
		hi := min(int64(len(body)), end-at+1)
		if lo < 0 || lo >= hi {
			break
		}
		if _, err := w.Write(body[lo:hi]); err != nil {
			return
		}
		pos = at + hi
		if pos > end {
			break
		}
		at += int64(len(body))
		if body, err = c.fetch(at); err != nil {
			// The headers are gone: the reader sees a short body.
			a.logf("cluster: xfile: %s from server %d at %d: %v", ref, owner, at, err)
			panic(http.ErrAbortHandler)
		}
	}
}

// chunkReader fetches and checks the chunks of one file.
type chunkReader struct {
	p                 *RelayProxy
	ctx               context.Context
	base, wire, tid   string
	owner             int64
	panelPub, nodePub []byte
	total             int64 // -1 until the first chunk
	seen              bool
}

// fetch reads the chunk at off and checks it; nothing of a chunk that does
// not verify is returned.
func (c *chunkReader) fetch(off int64) ([]byte, error) {
	target := "/xfile?o=" + strconv.FormatInt(off, 10) + "&n=" + strconv.Itoa(cc.FileChunk)
	resp, err := c.p.upstream(c.ctx, c.base, target, "X-XCVM-File", "X-XCVM-File-Auth", c.wire)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the owner answered %d", resp.StatusCode)
	}
	d, ok := cc.VerifyFileDigest(resp.Header.Get("X-XCVM-File-Digest"), c.tid, c.panelPub, c.nodePub)
	if !ok || d.OwnerSid != c.owner || d.Offset == nil || d.Total == nil {
		return nil, errors.New("the chunk's digest does not verify")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, cc.FileChunk+1))
	if err != nil {
		return nil, err
	}
	if !d.ChunkMatches(off, body) {
		return nil, errors.New("the chunk does not match its digest")
	}
	if c.seen && *d.Total != c.total {
		return nil, errors.New("the file changed while it was read")
	}
	// Every chunk but the last is whole: a short one in the middle would
	// splice two files together.
	if want := min(int64(cc.FileChunk), *d.Total-off); int64(len(body)) != max(0, want) {
		return nil, errors.New("a short chunk")
	}
	c.total, c.seen = *d.Total, true
	return body, nil
}

// byteRange is one Range header's single range.
type byteRange struct {
	start, end int64 // end -1: to the end
	suffix     bool
	length     int64
}

// parseRange reads "bytes=a-b", "bytes=a-" or "bytes=-n"; several ranges are
// refused (ok false), no header is the whole file.
func parseRange(h string) (byteRange, bool, bool) {
	if h == "" {
		return byteRange{end: -1}, false, true
	}
	spec, ok := strings.CutPrefix(strings.TrimSpace(h), "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return byteRange{}, true, false
	}
	a, b, ok := strings.Cut(spec, "-")
	if !ok {
		return byteRange{}, true, false
	}
	if a == "" {
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n <= 0 {
			return byteRange{}, true, false
		}
		return byteRange{suffix: true, length: n, end: -1}, true, true
	}
	s, err := strconv.ParseInt(a, 10, 64)
	if err != nil || s < 0 {
		return byteRange{}, true, false
	}
	e := int64(-1)
	if b != "" {
		if e, err = strconv.ParseInt(b, 10, 64); err != nil || e < s {
			return byteRange{}, true, false
		}
	}
	return byteRange{start: s, end: e}, true, true
}

// flushCopy copies a relay's body, flushing as it goes.
func flushCopy(w http.ResponseWriter, body io.Reader) {
	f, _ := w.(http.Flusher)
	buf := make([]byte, 64<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if f != nil {
				f.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

// base is where a server is reached: http://<address>:<http_broadcast_port>.
func (p *RelayProxy) base(sid int64) (string, error) {
	rt, err := p.serverRoutes()
	if err != nil {
		return "", err
	}
	s, ok := rt.byID[sid]
	if !ok || s.port <= 0 {
		return "", fmt.Errorf("server %d is not in the servers section", sid)
	}
	host := s.ip
	if self, ok := rt.byID[rt.self]; ok && self.private != "" && s.private != "" {
		host = s.private
	}
	if host == "" {
		return "", fmt.Errorf("server %d has no address", sid)
	}
	return "http://" + net.JoinHostPort(host, strconv.FormatInt(s.port, 10)), nil
}

// ownerKey is the key a file's digest verifies under: the panel's for MAIN,
// else the owner node's while the node list has it active.
func (p *RelayProxy) ownerKey(sid int64) (panelPub, nodePub []byte, err error) {
	rt, err := p.serverRoutes()
	if err != nil {
		return nil, nil, err
	}
	if sid == rt.mainSid {
		return p.a.Client.State.PanelSignPub, nil, nil
	}
	n, ok := rt.nodes[sid]
	if !ok || n.state != "active" {
		return nil, nil, fmt.Errorf("server %d is not an active node", sid)
	}
	return nil, n.pub, nil
}

// serverRoutes reads replica/servers.json, again whenever it changed.
func (p *RelayProxy) serverRoutes() (*serverRoutes, error) {
	if p.servers != nil {
		return p.servers()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	name := filepath.Join(p.a.ReplicaDir, "servers.json")
	fi, err := os.Stat(name)
	if err != nil {
		return nil, errors.New("no servers section")
	}
	if p.routes != nil && fi.ModTime().Equal(p.modAt) && time.Since(p.routeAt) < time.Minute {
		return p.routes, nil
	}
	b, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	rt, err := parseServerRoutes(b, p.a.Client.State.ServerID)
	if err != nil {
		return nil, err
	}
	p.routes, p.modAt, p.routeAt = rt, fi.ModTime(), time.Now()
	return rt, nil
}

// parseServerRoutes reads the servers section as the agent stored it.
func parseServerRoutes(b []byte, self int64) (*serverRoutes, error) {
	var doc struct {
		Data struct {
			Servers []struct {
				ID        int64   `json:"id"`
				IsMain    int64   `json:"is_main"`
				ServerIP  *string `json:"server_ip"`
				PrivateIP *string `json:"private_ip"`
				HTTPPort  int64   `json:"http_broadcast_port"`
			} `json:"servers"`
			Nodes []struct {
				Sid   int64  `json:"sid"`
				Gen   int64  `json:"gen"`
				State string `json:"state"`
				EdPub []byte `json:"ed_pub"`
			} `json:"nodes"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, errors.New("the servers section does not read")
	}
	rt := &serverRoutes{self: self, byID: map[int64]serverRoute{}, nodes: map[int64]routeNode{}}
	for _, s := range doc.Data.Servers {
		r := serverRoute{port: s.HTTPPort}
		if s.ServerIP != nil {
			r.ip = strings.TrimSpace(*s.ServerIP)
		}
		if s.PrivateIP != nil {
			r.private = strings.TrimSpace(*s.PrivateIP)
		}
		rt.byID[s.ID] = r
		if s.IsMain == 1 && rt.mainSid == 0 {
			rt.mainSid = s.ID
		}
	}
	for _, n := range doc.Data.Nodes {
		if len(n.EdPub) == 32 {
			rt.nodes[n.Sid] = routeNode{pub: n.EdPub, gen: n.Gen, state: n.State}
		}
	}
	return rt, nil
}
