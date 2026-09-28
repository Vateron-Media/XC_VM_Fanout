package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// relayFixture is a node (server 3, gen 1) with its DATAPLANE flow on, its
// tickets minted by the fake MAIN's panel key, and a proxy whose servers
// are the test's own listeners.
type relayFixture struct {
	t      *testing.T
	a      *Agent
	panel  ed25519.PrivateKey
	proxy  *RelayProxy
	routes *serverRoutes
	now    int64
}

func newRelayFixture(t *testing.T) *relayFixture {
	t.Helper()
	f, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: filepath.Join(t.TempDir(), "replica")}
	a.flows.Store(FlowStreams | 16 | FlowDataplane)
	now := time.Now().Unix()
	a.Client.setMainTime(now * 1000)
	fx := &relayFixture{t: t, a: a, panel: f.panel, now: now,
		routes: &serverRoutes{self: 3, byID: map[int64]serverRoute{3: {ip: "127.0.0.1", port: 1}}, nodes: map[int64]routeNode{}}}
	fx.proxy = a.NewRelayProxy("k0123456789abcdefghijklm")
	fx.proxy.servers = func() (*serverRoutes, error) { return fx.routes, nil }
	return fx
}

// ticket mints a ticket as MAIN's TicketService does.
func (fx *relayFixture) ticket(tag, tid string, fields map[string]any) string {
	typ := map[string]string{"rly": "xcvm-relay", "fil": "xcvm-file"}[tag]
	doc := map[string]any{"v": 1, "typ": typ, "tid": tid, "iat": fx.now - 60, "exp": fx.now + 3600}
	for k, v := range fields {
		doc[k] = v
	}
	b, _ := json.Marshal(doc) // Go sorts map keys, as the panel does
	return cc.JoinSigned(b, ed25519.Sign(fx.panel, cc.PanelSigInput(tag, b)))
}

// route points a server id at a test listener.
func (fx *relayFixture) route(sid int64, srv *httptest.Server) {
	u, _ := url.Parse(srv.URL)
	host, port, _ := net.SplitHostPort(u.Host)
	p, _ := strconv.ParseInt(port, 10, 64)
	fx.routes.byID[sid] = serverRoute{ip: host, port: p}
}

func (fx *relayFixture) get(path string, header ...string) *http.Response {
	fx.t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	rec := httptest.NewRecorder()
	func() {
		defer func() {
			if v := recover(); v != nil && v != http.ErrAbortHandler {
				panic(v)
			}
		}()
		fx.proxy.ServeHTTP(rec, req)
	}()
	return rec.Result()
}

// parent is a fake parent that checks relay requests as the panel's
// RelayGuard does, with its own nonce window.
type parent struct {
	panelPub, childPub ed25519.PublicKey
	nonces             *nonceCache
	stream             int64
	seen               []http.Header
}

func (p *parent) check(h http.Header, target string) bool {
	t, ok := cc.VerifyTicket(p.panelPub, "rly", h.Get("X-XCVM-Relay"), time.Now().Unix())
	if !ok {
		return false
	}
	if s, _ := t.Int("stream_id"); s != p.stream {
		return false
	}
	_, nonce, ok := cc.VerifyRelayAuth(p.childPub, h.Get("X-XCVM-Relay-Auth"), h.Get("X-XCVM-Relay"), http.MethodGet, target, time.Now().UnixMilli())
	return ok && p.nonces.fresh(hex.EncodeToString(nonce))
}

func TestRelayProxySignsEachConnectAndAParentRefusesAReplay(t *testing.T) {
	fx := newRelayFixture(t)
	par := &parent{panelPub: fx.panel.Public().(ed25519.PublicKey), childPub: fx.a.Client.State.SignKey().Public().(ed25519.PublicKey), nonces: newNonceCache(), stream: 77}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		par.seen = append(par.seen, r.Header.Clone())
		if r.URL.Path != "/admin/live" || !par.check(r.Header, r.URL.RequestURI()) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/mp2t")
		io.WriteString(w, "TS-BYTES")
	}))
	defer srv.Close()
	fx.route(1, srv)
	wire := fx.ticket("rly", "r1-3-77", map[string]any{"child_sid": 3, "child_gen": 1, "parent_sid": 1, "stream_id": 77})
	if err := fx.a.tickets().setStreams(map[int64]streamTickets{77: {Relay: wire}}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		res := fx.get("/relay/k0123456789abcdefghijklm/77.ts")
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode != 200 || string(body) != "TS-BYTES" {
			t.Fatalf("connect %d: %d %q", i, res.StatusCode, body)
		}
	}
	// Each connect carried a fresh proof, and no secret: the ticket and the
	// node key's signature only.
	if len(par.seen) != 2 || par.seen[0].Get("X-XCVM-Relay-Auth") == par.seen[1].Get("X-XCVM-Relay-Auth") {
		t.Fatal("two connects carried the same proof")
	}
	// A sniffer replaying the headers it copied is refused: the nonce is spent.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/admin/live?stream=77&extension=ts", nil)
	req.Header = par.seen[0]
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != http.StatusNotFound {
		t.Fatalf("a replayed relay auth was accepted: %v %v", res, err)
	}
	// The same headers for another stream are refused too (the target is signed).
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/admin/live?stream=78&extension=ts", nil)
	req.Header = par.seen[1]
	if res, _ := http.DefaultClient.Do(req); res.StatusCode != http.StatusNotFound {
		t.Fatal("a proof moved to another stream was accepted")
	}
}

func TestRelayProxyRefusesWithoutKeyTicketOrFlow(t *testing.T) {
	fx := newRelayFixture(t)
	fx.a.tickets().setStreams(map[int64]streamTickets{77: {Relay: fx.ticket("rly", "r1-3-77", map[string]any{"child_sid": 3, "child_gen": 1, "parent_sid": 1, "stream_id": 77})}})
	if res := fx.get("/relay/wrong-key-0123456789abc/77.ts"); res.StatusCode != http.StatusForbidden {
		t.Fatalf("a wrong loopback key: %d", res.StatusCode)
	}
	if res := fx.get("/relay/k0123456789abcdefghijklm/78.ts"); res.StatusCode != http.StatusNotFound {
		t.Fatalf("a stream without a ticket: %d", res.StatusCode)
	}
	fx.a.flows.Store(FlowStreams | 16)
	if res := fx.get("/relay/k0123456789abcdefghijklm/77.ts"); res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("DATAPLANE off: %d", res.StatusCode)
	}
}

// A ticket is kept only when it verifies for this node: another node's, one
// of an older generation (the node re-enrolled, or a revoked node's), one for
// another stream, an expired one or one from another panel is dropped.
func TestTicketsAreKeptOnlyForThisNodeAndGeneration(t *testing.T) {
	fx := newRelayFixture(t)
	good := fx.ticket("rly", "r1-3-77", map[string]any{"child_sid": 3, "child_gen": 1, "parent_sid": 1, "stream_id": 77})
	_, stranger, _ := ed25519.GenerateKey(nil)
	for name, wire := range map[string]string{
		"another node":   fx.ticket("rly", "r1-9-77", map[string]any{"child_sid": 9, "child_gen": 1, "parent_sid": 1, "stream_id": 77}),
		"old generation": fx.ticket("rly", "r1-3-77", map[string]any{"child_sid": 3, "child_gen": 2, "parent_sid": 1, "stream_id": 77}),
		"another stream": fx.ticket("rly", "r1-3-78", map[string]any{"child_sid": 3, "child_gen": 1, "parent_sid": 1, "stream_id": 78}),
		"another panel": func() string {
			b, _, _ := cc.SplitSigned(good, cc.TicketMaxWire)
			return cc.JoinSigned(b, ed25519.Sign(stranger, cc.PanelSigInput("rly", b)))
		}(),
		"a file ticket": fx.ticket("fil", "f1-3-0123456789abcdef0123456789abcdef", map[string]any{"fetcher_sid": 3, "fetcher_gen": 1, "owner_sid": 1, "ref": "0123456789abcdef0123456789abcdef", "file": "n.x"}),
	} {
		raw, _ := json.Marshal(map[string]any{"relay": wire, "files": nil})
		got, ok := fx.a.parseTickets(77, raw)
		if !ok || got.Relay != "" {
			t.Errorf("%s: kept", name)
		}
	}
	raw, _ := json.Marshal(map[string]any{"relay": good, "files": nil})
	if got, _ := fx.a.parseTickets(77, raw); got.Relay != good {
		t.Fatal("the node's own ticket was dropped")
	}
}

// fakeOwner is a fake file owner: it serves /xfile in chunks, each with a digest
// signed by its node key, as the panel's FileTicketServer does.
type fakeOwner struct {
	t      *testing.T
	key    ed25519.PrivateKey
	data   []byte
	tamper func(off int64, b []byte) []byte
	asks   []int64
}

func (o *fakeOwner) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/xfile" || r.Header.Get("X-XCVM-File") == "" || r.Header.Get("X-XCVM-File-Auth") == "" {
		http.NotFound(w, r)
		return
	}
	t, _, _ := cc.SplitSigned(r.Header.Get("X-XCVM-File"), cc.TicketMaxWire)
	var doc struct {
		Tid string `json:"tid"`
	}
	json.Unmarshal(t, &doc)
	off, _ := strconv.ParseInt(r.URL.Query().Get("o"), 10, 64)
	n, _ := strconv.ParseInt(r.URL.Query().Get("n"), 10, 64)
	o.asks = append(o.asks, off)
	total := int64(len(o.data))
	end := min(total, off+n)
	chunk := append([]byte{}, o.data[off:end]...)
	sum := sha256.Sum256(chunk)
	d, _ := cc.FileDigestDoc(doc.Tid, 5, int64(len(chunk)), hex.EncodeToString(sum[:]), time.Now().Unix(), &off, &total)
	if o.tamper != nil {
		chunk = o.tamper(off, chunk)
	}
	w.Header().Set("X-XCVM-File-Digest", cc.JoinSigned(d, cc.SignNode(o.key, "digest", d)))
	w.Write(chunk)
}

func xfileFixture(t *testing.T, size int) (*relayFixture, *fakeOwner, string) {
	fx := newRelayFixture(t)
	_, key, _ := ed25519.GenerateKey(nil)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + i/cc.FileChunk)
	}
	o := &fakeOwner{t: t, key: key, data: data}
	srv := httptest.NewServer(o)
	t.Cleanup(srv.Close)
	fx.route(5, srv)
	fx.routes.nodes[5] = routeNode{pub: key.Public().(ed25519.PublicKey), gen: 1, state: "active"}
	ref := "0123456789abcdef0123456789abcdef"
	wire := fx.ticket("fil", "f1-3-"+ref, map[string]any{"fetcher_sid": 3, "fetcher_gen": 1, "owner_sid": 5, "ref": ref, "file": "n.opaque"})
	fx.a.tickets().setStreams(map[int64]streamTickets{100: {Files: map[string]string{ref: wire}}})
	return fx, o, "/xfile/k0123456789abcdefghijklm/" + ref + ".mkv"
}

func TestXfileReadsAWholeFileChunkByChunkAndARange(t *testing.T) {
	fx, o, path := xfileFixture(t, 2*cc.FileChunk+1000)
	res := fx.get(path)
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !bytes.Equal(body, o.data) || res.Header.Get("Content-Length") != strconv.Itoa(len(o.data)) {
		t.Fatalf("whole file: %d, %d bytes", res.StatusCode, len(body))
	}
	if fmt.Sprint(o.asks) != fmt.Sprint([]int64{0, cc.FileChunk, 2 * cc.FileChunk}) {
		t.Fatalf("chunks asked: %v", o.asks)
	}
	// A reader seeking (ffmpeg looks for an mp4's index at the end).
	o.asks = nil
	start := int64(cc.FileChunk - 10)
	res = fx.get(path, "Range", fmt.Sprintf("bytes=%d-%d", start, start+19))
	body, _ = io.ReadAll(res.Body)
	if res.StatusCode != 206 || !bytes.Equal(body, o.data[start:start+20]) || res.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start, start+19, len(o.data)) {
		t.Fatalf("range: %d %q", res.StatusCode, res.Header.Get("Content-Range"))
	}
	res = fx.get(path, "Range", "bytes=-100")
	body, _ = io.ReadAll(res.Body)
	if res.StatusCode != 206 || !bytes.Equal(body, o.data[len(o.data)-100:]) {
		t.Fatalf("suffix range: %d", res.StatusCode)
	}
	if res := fx.get(path, "Range", fmt.Sprintf("bytes=%d-", len(o.data))); res.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("past the end: %d", res.StatusCode)
	}
}

// A MITM altering a chunk (or an owner serving what its digest does not say)
// is refused before any byte of that chunk reaches the reader.
func TestXfileRefusesATamperedBody(t *testing.T) {
	fx, o, path := xfileFixture(t, 2*cc.FileChunk+1000)
	o.tamper = func(off int64, b []byte) []byte {
		if off == cc.FileChunk {
			b[17] ^= 1
		}
		return b
	}
	res := fx.get(path)
	body, _ := io.ReadAll(res.Body)
	if len(body) != cc.FileChunk || !bytes.Equal(body, o.data[:cc.FileChunk]) {
		t.Fatalf("%d bytes passed on, the tampered chunk among them", len(body))
	}
	// Tampered in the first chunk: nothing at all, and no 200.
	o.tamper = func(off int64, b []byte) []byte {
		if off == 0 {
			b[0] ^= 1
		}
		return b
	}
	if res := fx.get(path); res.StatusCode != http.StatusBadGateway {
		t.Fatalf("a tampered first chunk: %d", res.StatusCode)
	}
	// A chunk moved to another offset carries its own offset in its digest.
	o.tamper = nil
	fx2, o2, path2 := xfileFixture(t, cc.FileChunk+10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		q.Set("o", "0") // always the first chunk, correctly signed for offset 0
		r.URL.RawQuery = q.Encode()
		o2.ServeHTTP(w, r)
	}))
	defer srv.Close()
	fx2.route(5, srv)
	res = fx2.get(path2, "Range", fmt.Sprintf("bytes=%d-", cc.FileChunk))
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("a chunk served for another offset: %d", res.StatusCode)
	}
}

// A digest signed by another key than the owner's (a node the list does not
// have active, or anyone else) is refused.
func TestXfileRefusesADigestFromAnotherKeyOrAnInactiveOwner(t *testing.T) {
	fx, _, path := xfileFixture(t, 1000)
	_, other, _ := ed25519.GenerateKey(nil)
	fx.routes.nodes[5] = routeNode{pub: other.Public().(ed25519.PublicKey), gen: 1, state: "active"}
	if res := fx.get(path); res.StatusCode != http.StatusBadGateway {
		t.Fatalf("another key: %d", res.StatusCode)
	}
	fx.routes.nodes[5] = routeNode{pub: other.Public().(ed25519.PublicKey), gen: 1, state: "revoked"}
	if res := fx.get(path); res.StatusCode != http.StatusBadGateway {
		t.Fatalf("a revoked owner: %d", res.StatusCode)
	}
}

// The delta carries the epoch's tickets apart from the records: a refresh
// moves no cursor, rewrites no record file and pages until MAIN has no more.
func TestTicketRefreshIsKeptApartFromTheRecords(t *testing.T) {
	fx := newRelayFixture(t)
	a := fx.a
	if ask := a.ticketsAsk(); ask == nil || ask.Epoch != 0 || ask.From != 0 {
		t.Fatalf("a node holding no tickets asks from 0: %+v", ask)
	}
	epoch := fx.now / TicketEpoch
	relay := func(id int64) json.RawMessage {
		raw, _ := json.Marshal(map[string]any{"relay": fx.ticket("rly", "r1-3-"+strconv.FormatInt(id, 10), map[string]any{"child_sid": 3, "child_gen": 1, "parent_sid": 1, "stream_id": id}), "files": nil})
		return raw
	}
	next := int64(200)
	more, err := a.ticketsTake(&ticketsReply{Epoch: epoch, Streams: map[string]json.RawMessage{"77": relay(77)}, Next: &next})
	if err != nil || !more {
		t.Fatalf("a first page: more %v, %v", more, err)
	}
	if ask := a.ticketsAsk(); ask == nil || ask.From != 200 {
		t.Fatalf("goes on from next: %+v", ask)
	}
	if more, _ := a.ticketsTake(&ticketsReply{Epoch: epoch, Streams: map[string]json.RawMessage{"300": relay(300)}}); more {
		t.Fatal("the last page said more")
	}
	if ask := a.ticketsAsk(); ask != nil {
		t.Fatalf("this epoch's are held: %+v", ask)
	}
	if fmt.Sprint(a.tickets().held()) != "[77 300]" {
		t.Fatalf("held %v", a.tickets().held())
	}
	fi, err := os.Stat(filepath.Join(a.ReplicaDir, "tickets.json"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("tickets.json: %v", err)
	}
	if _, err := os.Stat(streamsDir(a.ReplicaDir)); !os.IsNotExist(err) {
		t.Fatal("a ticket refresh wrote a record file")
	}
	// Reloaded from disk as it was.
	b := &Agent{Client: a.Client, ReplicaDir: a.ReplicaDir}
	b.flows.Store(FlowDataplane)
	if b.tickets().relay(300) == "" || b.ticketsAsk() != nil {
		t.Fatal("the tickets did not survive a restart")
	}
	// The flow off forgets the epoch, so turning it on again refreshes all.
	b.flows.Store(FlowStreams)
	if b.ticketsAsk() != nil {
		t.Fatal("asked with DATAPLANE off")
	}
	b.flows.Store(FlowDataplane)
	if ask := b.ticketsAsk(); ask == nil || ask.Epoch != 0 {
		t.Fatalf("after the flow went off and on: %+v", ask)
	}
	// Without a licence MAIN keeps the node's epoch: nothing is marked held.
	if more, _ := b.ticketsTake(&ticketsReply{Epoch: 0, Streams: map[string]json.RawMessage{}, Withheld: true}); more || b.ticketsAsk() == nil {
		t.Fatal("a withheld refresh was taken as done")
	}
	ids := b.tickets().held()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 2 || !strings.HasPrefix(b.tickets().relay(77), "ey") {
		t.Fatalf("held %v", ids)
	}
}

func TestLoadRelayKeyIsMadeOnceAndKept(t *testing.T) {
	dir := t.TempDir()
	k, err := LoadRelayKey(dir)
	if err != nil || len(k) < 22 || !validKey(k) {
		t.Fatalf("%q %v", k, err)
	}
	fi, _ := os.Stat(filepath.Join(dir, RelayKeyStore))
	if fi == nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("the kept key: %v", fi)
	}
	if _, err := os.Stat(filepath.Join(dir, RelayKeyFile)); !os.IsNotExist(err) {
		t.Fatal("the key was published before any port was held")
	}
	if k2, _ := LoadRelayKey(dir); k2 != k {
		t.Fatal("the key changed across restarts: every encoder's URL would break")
	}
	// An older agent's relay.key is taken over, so its encoders' URLs survive the upgrade.
	old := t.TempDir()
	os.WriteFile(filepath.Join(old, RelayKeyFile), []byte("olderAgentsKey0123456789ab\n"), 0o600)
	if k, _ := LoadRelayKey(old); k != "olderAgentsKey0123456789ab" {
		t.Fatalf("an older agent's key was not kept: %q", k)
	}
}

// The port is an unprivileged one: relay.key is published only while the
// proxy holds it. A port held by someone else is retried (and logged) until
// it frees, and the key is withdrawn when the proxy lets go.
func TestServeRelayProxyPublishesTheKeyOnlyWhileItHoldsThePort(t *testing.T) {
	defer func(a, b time.Duration) { RelayBindRetryMin, RelayBindRetryMax = a, b }(RelayBindRetryMin, RelayBindRetryMax)
	RelayBindRetryMin, RelayBindRetryMax = 10*time.Millisecond, 40*time.Millisecond
	dir := t.TempDir()
	published := func() bool { _, err := os.Stat(filepath.Join(dir, RelayKeyFile)); return err == nil }
	// A relay.key a crash (or an older agent) left behind: its key is kept,
	// so the encoders' URLs survive, but the file is withdrawn at once.
	os.WriteFile(filepath.Join(dir, RelayKeyFile), []byte("leftBehindKey0123456789abc\n"), 0o600)

	squatter, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := squatter.Addr().String()
	var logs []string
	var mu sync.Mutex
	a := &Agent{Logf: func(f string, v ...any) { mu.Lock(); logs = append(logs, fmt.Sprintf(f, v...)); mu.Unlock() }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.ServeRelayProxy(ctx, addr, dir) }()

	waitFor(t, "a bind failure logged", func() bool { mu.Lock(); defer mu.Unlock(); return len(logs) > 0 })
	time.Sleep(100 * time.Millisecond)
	if published() {
		t.Fatal("relay.key published while another process holds the port")
	}
	mu.Lock()
	if !strings.Contains(logs[0], "cannot listen on "+addr) {
		t.Fatalf("log %q", logs[0])
	}
	mu.Unlock()

	squatter.Close()
	waitFor(t, "relay.key published once the port is ours", published)
	k, _ := os.ReadFile(filepath.Join(dir, RelayKeyFile))
	kept, _ := os.ReadFile(filepath.Join(dir, RelayKeyStore))
	if string(k) != string(kept) || strings.TrimSpace(string(k)) != "leftBehindKey0123456789abc" {
		t.Fatalf("published %q, kept %q", k, kept)
	}
	res, err := http.Get("http://" + addr + "/relay/" + strings.TrimSpace(string(k)) + "/77.ts")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("the proxy answered %d (the flow is off: 503)", res.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the proxy did not stop")
	}
	if published() {
		t.Fatal("relay.key left behind after the proxy let go of the port")
	}
	if _, err := os.Stat(filepath.Join(dir, RelayKeyStore)); err != nil {
		t.Fatal("the kept key went with it: the next start would break every encoder's URL")
	}
}

// fileTicket mints a file ticket for ref with its own tid and exp.
func (fx *relayFixture) fileTicket(ref, tid string, exp int64) string {
	doc := map[string]any{"v": 1, "typ": "xcvm-file", "tid": tid, "iat": fx.now - 60, "exp": exp,
		"fetcher_sid": 3, "fetcher_gen": 1, "owner_sid": 5, "ref": ref, "file": "n.opaque"}
	b, _ := json.Marshal(doc)
	return cc.JoinSigned(b, ed25519.Sign(fx.panel, cc.PanelSigInput("fil", b)))
}

// Each chunk is asked for with the ticket the store holds at that moment: a
// refresh mid-transfer is used from the next chunk, and a ticket gone (the
// stream dropped, the node revoked) ends the read there.
func TestXfileRereadsTheTicketForEachChunk(t *testing.T) {
	fx, o, path := xfileFixture(t, 3*cc.FileChunk)
	ref := "0123456789abcdef0123456789abcdef"
	fresh := fx.fileTicket(ref, "f2-3-"+ref, fx.now+7200)
	var tids []string
	inner := o.tamper
	o.tamper = func(off int64, b []byte) []byte {
		if off == 0 {
			fx.a.tickets().setStreams(map[int64]streamTickets{100: {Files: map[string]string{ref: fresh}}})
		}
		if inner != nil {
			return inner(off, b)
		}
		return b
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		doc, _, _ := cc.SplitSigned(r.Header.Get("X-XCVM-File"), cc.TicketMaxWire)
		var d struct {
			Tid string `json:"tid"`
		}
		json.Unmarshal(doc, &d)
		tids = append(tids, d.Tid)
		o.ServeHTTP(w, r)
	}))
	defer srv.Close()
	fx.route(5, srv)
	res := fx.get(path)
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !bytes.Equal(body, o.data) {
		t.Fatalf("%d, %d bytes", res.StatusCode, len(body))
	}
	if fmt.Sprint(tids) != fmt.Sprint([]string{"f1-3-" + ref, "f2-3-" + ref, "f2-3-" + ref}) {
		t.Fatalf("tickets used per chunk: %v", tids)
	}

	// The ticket dropped after the first chunk: the read ends short.
	tids = nil
	fx.a.tickets().setStreams(map[int64]streamTickets{100: {Files: map[string]string{ref: fresh}}})
	o.tamper = func(off int64, b []byte) []byte {
		if off == 0 {
			fx.a.tickets().drop([]int64{100})
		}
		return b
	}
	res = fx.get(path)
	body, _ = io.ReadAll(res.Body)
	if len(body) != cc.FileChunk || len(tids) != 1 {
		t.Fatalf("read on without a ticket: %d bytes, %d chunks asked", len(body), len(tids))
	}
}

// The owner's nginx answers 429 past its /xfile rate (503 when PHP is
// busy): the chunk is asked for again after a short wait, a bounded number
// of times.
func TestXfileRetriesABusyOwner(t *testing.T) {
	defer func(n int, a, b time.Duration) { ChunkRetries, ChunkRetryMin, ChunkRetryMax = n, a, b }(ChunkRetries, ChunkRetryMin, ChunkRetryMax)
	ChunkRetries, ChunkRetryMin, ChunkRetryMax = 3, 5*time.Millisecond, 20*time.Millisecond
	fx, o, path := xfileFixture(t, 2*cc.FileChunk+10)
	busy := map[int64]int{0: 1, cc.FileChunk: 2} // refusals left per offset
	status := http.StatusTooManyRequests
	asks := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asks++
		off, _ := strconv.ParseInt(r.URL.Query().Get("o"), 10, 64)
		if busy[off] > 0 {
			busy[off]--
			http.Error(w, "busy", status)
			return
		}
		o.ServeHTTP(w, r)
	}))
	defer srv.Close()
	fx.route(5, srv)
	res := fx.get(path)
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !bytes.Equal(body, o.data) || asks != 6 {
		t.Fatalf("%d, %d bytes, %d asks", res.StatusCode, len(body), asks)
	}
	// Busy for longer than the tries allow: a 502, after exactly ChunkRetries asks.
	asks, status = 0, http.StatusServiceUnavailable
	busy = map[int64]int{0: 10}
	if res := fx.get(path); res.StatusCode != http.StatusBadGateway || asks != 3 {
		t.Fatalf("%d after %d asks", res.StatusCode, asks)
	}
	// Any other refusal is not retried.
	asks = 0
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { asks++; http.NotFound(w, r) })
	if res := fx.get(path); res.StatusCode != http.StatusBadGateway || asks != 1 {
		t.Fatalf("a 404: %d after %d asks", res.StatusCode, asks)
	}
}

// Two streams reading the same file each hold a ticket for its ref: the one
// that expires last is used, whichever was stored last.
func TestTheFileIndexPicksTheLatestTicketPerRef(t *testing.T) {
	fx := newRelayFixture(t)
	ref := "0123456789abcdef0123456789abcdef"
	late := fx.fileTicket(ref, "late", fx.now+7200)
	early := fx.fileTicket(ref, "early", fx.now+600)
	for i := 0; i < 20; i++ { // map order varies: every walk must agree
		s := fx.a.tickets()
		s.setStreams(map[int64]streamTickets{10: {Files: map[string]string{ref: late}}})
		s.setStreams(map[int64]streamTickets{11: {Files: map[string]string{ref: early}}, 12: {Files: map[string]string{ref: early}}})
		if got := s.file(ref); got != late {
			t.Fatalf("walk %d: the earlier-expiring ticket was indexed", i)
		}
		s.setStreams(map[int64]streamTickets{10: {Files: map[string]string{ref: early}}})
		if got := s.file(ref); got != early {
			t.Fatal("the only remaining ticket was not indexed")
		}
		s.drop([]int64{10, 11, 12})
	}
	if ticketExp("not a ticket") != 0 || ticketExp(late) != fx.now+7200 {
		t.Fatal("ticketExp")
	}
}
