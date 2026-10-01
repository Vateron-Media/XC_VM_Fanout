package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// artefactMain is MAIN's artefact op in miniature (ArtefactGrants::serve):
// it serves chunks of each grant's bytes, cut at the end, records every
// request and ack, and answers the scripted refusals first.
type artefactMain struct {
	*fakeMain
	mu     sync.Mutex
	files  map[string][]byte // by cmd_id
	sums   map[string]string // the grant's sha256, by cmd_id
	reqs   []map[string]any  // artefact requests
	acks   []ackRec
	hellos [][]string // the features of each hello
	// script answers the next requests, one each, before MAIN serves.
	script []func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any)
	gate   chan struct{} // when set, a chunk is served only once it closes
}

type ackRec struct {
	ID     string
	OK     bool
	Result string
}

func (m *artefactMain) answer(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
	req := m.opened(r, reqCtx)
	switch {
	case strings.HasSuffix(r.URL.Path, "/ack"):
		m.mu.Lock()
		m.acks = append(m.acks, ackRec{ID: req["cmd_id"].(string), OK: req["ok"].(bool), Result: req["result"].(string)})
		m.mu.Unlock()
		m.box(w, reqCtx, map[string]any{"ok": true})
	case strings.HasSuffix(r.URL.Path, "/hello"):
		var fs []string
		for _, f := range req["features"].([]any) {
			fs = append(fs, f.(string))
		}
		m.mu.Lock()
		m.hellos = append(m.hellos, fs)
		m.mu.Unlock()
		m.box(w, reqCtx, map[string]any{"state": "active", "flows": FlowCommands})
	case strings.HasSuffix(r.URL.Path, "/artefact"):
		m.mu.Lock()
		m.reqs = append(m.reqs, req)
		var next func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any)
		if len(m.script) > 0 {
			next, m.script = m.script[0], m.script[1:]
		}
		gate := m.gate
		m.mu.Unlock()
		if next != nil {
			next(w, reqCtx, nonce, req)
			return
		}
		if gate != nil {
			<-gate
		}
		chunk, status, reason := m.chunk(req)
		if chunk == nil {
			m.refuse(w, status, nonce, reason, nil)
			return
		}
		m.box(w, reqCtx, chunk)
	default:
		http.NotFound(w, r)
	}
}

// chunk is what MAIN serves for req, or the refusal.
func (m *artefactMain) chunk(req map[string]any) (map[string]any, int, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, _ := req["grant"].(string)
	off, _ := req["offset"].(float64)
	n, _ := req["length"].(float64)
	data, ok := m.files[id]
	switch {
	case !ok:
		return nil, 403, "GRANT_INVALID"
	case int(off) >= len(data) || n > 4<<20:
		return nil, 416, "BAD_RANGE"
	}
	end := min(len(data), int(off)+int(n))
	return map[string]any{"grant": id, "offset": int(off), "length": end - int(off), "size": len(data), "sha256": m.sums[id],
		"data": base64.StdEncoding.EncodeToString(data[int(off):end]), "eof": end == len(data), "main_time_ms": time.Now().UnixMilli()}, 0, ""
}

func (m *artefactMain) requests() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]map[string]any(nil), m.reqs...)
}

func (m *artefactMain) acked() []ackRec {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ackRec(nil), m.acks...)
}

func (m *artefactMain) ackOf(t *testing.T, cmdID string) ackRec {
	t.Helper()
	var got ackRec
	waitFor(t, "the ack of "+cmdID, func() bool {
		for _, a := range m.acked() {
			if a.ID == cmdID {
				got = a
				return true
			}
		}
		return false
	})
	return got
}

// offsets are the (offset, length) pairs the agent asked for.
func (m *artefactMain) offsets() [][2]int {
	var out [][2]int
	for _, r := range m.requests() {
		out = append(out, [2]int{int(r["offset"].(float64)), int(r["length"].(float64))})
	}
	return out
}

func cmdIDFor(seq uint64) string { return fmt.Sprintf("%032x", seq) }

// grant is MAIN's grant for data under id and name.
func grantFor(id, name string, data []byte) map[string]any {
	sum := sha256.Sum256(data)
	return map[string]any{"id": id, "name": name, "size": len(data), "sha256": hex.EncodeToString(sum[:]), "mtime": 1800000000, "ctime": 1800000000, "exp": time.Now().Unix() + 3600}
}

// command signs a command for this node as MAIN's CommandBus does: the
// action of a node.root or node.rpc is lifted out of args to the top level.
// A grant in args is served from data.
func (m *artefactMain) command(seq uint64, typ string, args map[string]any, data []byte) WireCommand {
	id := cmdIDFor(seq)
	d := map[string]any{"v": 1, "type": typ, "exp": time.Now().Unix() + 3600, "iat": time.Now().Unix(), "cmd_id": id, "seq": seq,
		"node_uuid": m.uuid, "gen": 1, "dedupe_key": nil}
	if action, ok := args["action"]; ok && (typ == "node.root" || typ == "node.rpc") {
		rest := map[string]any{}
		for k, v := range args {
			if k != "action" {
				rest[k] = v
			}
		}
		d["action"], args = action, rest
	}
	d["args"] = args
	doc, _ := json.Marshal(d)
	if g, ok := args["artefact"].(map[string]any); ok && data != nil {
		m.mu.Lock()
		m.files[id], m.sums[id] = data, g["sha256"].(string)
		m.mu.Unlock()
	}
	return WireCommand{Doc: string(doc), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("cmd", doc))), Seq: seq}
}

// execLog stands for cluster:exec: it records what ran, in order, and what
// the agent's download held when it ran.
type execLog struct {
	mu      sync.Mutex
	ran     []*Command
	files   map[string][]byte
	outcome func(cmd *Command) (bool, []byte)
	dir     string
}

func (e *execLog) run(_ context.Context, cmd *Command, _ WireCommand) (bool, []byte) {
	e.mu.Lock()
	e.ran = append(e.ran, cmd)
	if b, err := os.ReadFile(filepath.Join(e.dir, cmd.CmdID)); err == nil {
		e.files[cmd.CmdID] = b
	}
	out := e.outcome
	e.mu.Unlock()
	if out != nil {
		return out(cmd)
	}
	return true, []byte(`{"ran":"` + cmd.Type + `"}`)
}

// order is the seqs that ran, in order.
func (e *execLog) order() []uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []uint64
	for _, c := range e.ran {
		out = append(out, c.Seq)
	}
	return out
}

func (e *execLog) file(cmdID string) ([]byte, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.files[cmdID]
	return b, ok
}

func (l *logSink) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func (m *artefactMain) lastHello() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hellos[len(m.hellos)-1]
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// newArtefactAgent is an agent whose PHP runs artefact.fetch, against the
// fake MAIN, with its downloads in a fresh artefacts directory. Its worker
// is not running yet (start).
func newArtefactAgent(t *testing.T) (*artefactMain, *Agent, *execLog, *logSink) {
	f, st := newFake(t)
	m := &artefactMain{fakeMain: f, files: map[string][]byte{}, sums: map[string]string{}}
	f.answer = m.answer
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	dir := t.TempDir()
	st.path = filepath.Join(dir, "agent.json")
	logs := &logSink{}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: logs.logf(t), ArtefactDir: filepath.Join(dir, "artefacts")}
	a.artefactOn.Store(true)
	ex := &execLog{files: map[string][]byte{}, dir: a.ArtefactDir}
	a.run = ex.run
	shortArtefactWaits(t)
	return m, a, ex, logs
}

func shortArtefactWaits(t *testing.T) {
	oldChunk, oldMin, oldRetry, oldPause, oldIdle := ArtefactChunk, ArtefactChunkMin, ArtefactRetryMin, ArtefactPause, ArtefactIdle
	ArtefactRetryMin, ArtefactPause, ArtefactIdle = 10*time.Millisecond, 50*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() {
		ArtefactChunk, ArtefactChunkMin, ArtefactRetryMin, ArtefactPause, ArtefactIdle = oldChunk, oldMin, oldRetry, oldPause, oldIdle
	})
}

// start runs the agent's artefact worker until the test ends.
func start(t *testing.T, a *Agent) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.RunArtefacts(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func payload(n int) []byte {
	b := bytes.Repeat([]byte("S3cretVideoBytes"), n/16+1)[:n]
	for i := range b {
		b[i] ^= byte(i * 7)
	}
	return b
}

func TestArtefactGrantShape(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{"id": "offair/not_on_air", "name": "off-air.ts", "size": 10, "sha256": strings.Repeat("ab", 32), "mtime": 1, "ctime": 1, "exp": 1800000000, "extra": "ignored"}
	}
	doc := func(cmdID string, grant any) string {
		b, _ := json.Marshal(map[string]any{"cmd_id": cmdID, "args": map[string]any{"artefact": grant}})
		return string(b)
	}
	id := cmdIDFor(1)
	for _, ok := range []string{"offair/connected", "offair/expiring", "module/radio-2/1.0.3", "module/a/V_1-2", "agent/amd64", "agent/arm64", "agent/armv7", "agent/386", "fanout/amd64", "fanout/arm64", "core/php8.1", "core/php8.4"} {
		g := valid()
		g["id"] = ok
		if _, _, err := parseGrant(id, doc(id, g)); err != nil {
			t.Errorf("%s refused: %v", ok, err)
		}
	}
	g, gid, err := parseGrant(id, doc(id, valid()))
	if err != nil || gid != "offair/not_on_air" || *g != (Grant{ID: "offair/not_on_air", Name: "off-air.ts", Size: 10, SHA256: strings.Repeat("ab", 32), Exp: 1800000000}) {
		t.Fatalf("valid grant: %+v %q %v", g, gid, err)
	}
	cases := map[string]func(g map[string]any){
		"an unknown off-air name":  func(g map[string]any) { g["id"] = "offair/custom" },
		"an unknown kind":          func(g map[string]any) { g["id"] = "binary/xc_fanout" },
		"a traversing id":          func(g map[string]any) { g["id"] = "offair/../../etc/passwd" },
		"a module without version": func(g map[string]any) { g["id"] = "module/radio" },
		"a module in capitals":     func(g map[string]any) { g["id"] = "module/Radio/1.0" },
		"a version with ..":        func(g map[string]any) { g["id"] = "module/radio/1..0" },
		"a longer module id":       func(g map[string]any) { g["id"] = "module/radio/1.0/x" },
		"an unknown arch":          func(g map[string]any) { g["id"] = "agent/mips" },
		"a fanout of no arch":      func(g map[string]any) { g["id"] = "fanout/mips" },
		"a core of no PHP":         func(g map[string]any) { g["id"] = "core/php" },
		"a core past its group":    func(g map[string]any) { g["id"] = "core/php8.1/x" },
		"a core group traversing":  func(g map[string]any) { g["id"] = "core/../x" },
		"no id":                    func(g map[string]any) { delete(g, "id") },
		"a numeric id":             func(g map[string]any) { g["id"] = 7 },
		"a dot file":               func(g map[string]any) { g["name"] = ".hidden.ts" },
		"a path":                   func(g map[string]any) { g["name"] = "video/off.ts" },
		"a name ending in newline": func(g map[string]any) { g["name"] = "off.ts\n" },
		"a name too long":          func(g map[string]any) { g["name"] = strings.Repeat("a", 129) },
		"no name":                  func(g map[string]any) { delete(g, "name") },
		"size 0":                   func(g map[string]any) { g["size"] = 0 },
		"a negative size":          func(g map[string]any) { g["size"] = -1 },
		"a fractional size":        func(g map[string]any) { g["size"] = 1.5 },
		"a size as a string":       func(g map[string]any) { g["size"] = "10" },
		"no size":                  func(g map[string]any) { delete(g, "size") },
		"an uppercase sha256":      func(g map[string]any) { g["sha256"] = strings.Repeat("AB", 32) },
		"a short sha256":           func(g map[string]any) { g["sha256"] = strings.Repeat("a", 63) },
		"no sha256":                func(g map[string]any) { delete(g, "sha256") },
		"no exp":                   func(g map[string]any) { delete(g, "exp") },
		"a fractional exp":         func(g map[string]any) { g["exp"] = 1.5 },
		"an exp as a string":       func(g map[string]any) { g["exp"] = "1800000000" },
	}
	for name, change := range cases {
		g := valid()
		change(g)
		if _, _, err := parseGrant(id, doc(id, g)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for name, d := range map[string]string{
		"a grant that is not an object": doc(id, "offair/not_on_air"),
		"a null grant":                  doc(id, nil),
		"a cmd_id that is not 32 hex":   doc("../../x", valid()),
	} {
		cmdID := id
		if strings.Contains(d, "../../x") {
			cmdID = "../../x"
		}
		if _, _, err := parseGrant(cmdID, d); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, c := range []struct {
		cmd  Command
		want bool
	}{
		{Command{Type: "artefact.fetch"}, true},
		{Command{Type: "node.root", Action: "agent_binary", Args: map[string]any{"artefact": map[string]any{}}}, true},
		{Command{Type: "node.root", Action: "install_module", Args: map[string]any{}}, false},
		{Command{Type: "node.rpc", Args: map[string]any{"artefact": map[string]any{}}}, false},
	} {
		if carriesGrant(&c.cmd) != c.want {
			t.Errorf("carriesGrant(%s %v) != %v", c.cmd.Type, c.cmd.Args, c.want)
		}
	}
}

func TestArtefactMalformedGrantIsRefusedNotRun(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	start(t, a)
	ctx := context.Background()
	g := grantFor("offair/not_on_air", ".hidden.ts", payload(10))
	a.handleCommand(ctx, m.command(1, "artefact.fetch", map[string]any{"artefact": g}, nil), a.run)
	if got := m.ackOf(t, cmdIDFor(1)); got.OK || got.Result != "artefact refused: offair/not_on_air: a malformed grant" {
		t.Fatalf("ack %+v", got)
	}
	g = grantFor("offair/not_on_air", "off.ts", payload(10))
	delete(g, "id")
	a.handleCommand(ctx, m.command(2, "node.root", map[string]any{"action": "agent_binary", "arch": "amd64", "artefact": g}, nil), a.run)
	if got := m.ackOf(t, cmdIDFor(2)); got.OK || got.Result != "artefact refused: ?: a malformed grant" {
		t.Fatalf("ack %+v", got)
	}
	if len(ex.order()) != 0 || len(m.requests()) != 0 || a.Client.State.CmdSeq != 2 {
		t.Fatalf("ran %v, fetched %d, high-water %d", ex.order(), len(m.requests()), a.Client.State.CmdSeq)
	}
}

func TestArtefactFetchedInChunksAndHandedOn(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	ArtefactChunk = 1000
	data := payload(3500)
	w := m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/not_on_air", "off.ts", data)}, data)
	a.handleCommand(context.Background(), w, a.run)
	// Kept, with its doc and sig, and the high-water past it, before any byte.
	back, err := loadRaw(a.Client.State.path)
	if err != nil || len(back.HeldCmds) != 1 || back.HeldCmds[0].Doc != w.Doc || back.HeldCmds[0].Sig != w.Sig || back.CmdSeq != 1 {
		t.Fatalf("state.json: %+v %v", back, err)
	}
	start(t, a)
	if got := m.ackOf(t, cmdIDFor(1)); !got.OK || got.Result != `{"ran":"artefact.fetch"}` {
		t.Fatalf("ack %+v", got)
	}
	if want := [][2]int{{0, 1000}, {1000, 1000}, {2000, 1000}, {3000, 500}}; !slices.Equal(m.offsets(), want) {
		t.Fatalf("asked for %v, want %v", m.offsets(), want)
	}
	for _, r := range m.requests() {
		if r["grant"] != cmdIDFor(1) {
			t.Fatalf("request %v", r)
		}
	}
	if got, ok := ex.file(cmdIDFor(1)); !ok || !bytes.Equal(got, data) {
		t.Fatal("cluster:exec did not find the checked download")
	}
	// Gone once cluster:exec returned: the download and the kept command.
	waitFor(t, "the command to be forgotten", func() bool { return len(a.Client.State.held()) == 0 })
	if left, _ := os.ReadDir(a.ArtefactDir); len(left) != 0 {
		t.Fatalf("left behind: %v", left)
	}
	if fi, _ := os.Stat(a.ArtefactDir); fi.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode %v", fi.Mode().Perm())
	}
}

func TestArtefactDownloadModes(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	data := payload(100)
	g := grantFor("agent/amd64", "xc_agent-linux-amd64", data)
	a.handleCommand(context.Background(), m.command(1, "node.root", map[string]any{"action": "agent_binary", "arch": "amd64", "version": "1.5.0", "artefact": g}, data), a.run)
	var mode os.FileMode
	ex.outcome = func(cmd *Command) (bool, []byte) {
		fi, _ := os.Stat(filepath.Join(a.ArtefactDir, cmd.CmdID))
		mode = fi.Mode().Perm()
		return true, []byte("xc_agent installed; it restarts in 10 s")
	}
	start(t, a)
	if got := m.ackOf(t, cmdIDFor(1)); !got.OK || got.Result != "xc_agent installed; it restarts in 10 s" {
		t.Fatalf("ack %+v", got)
	}
	if mode != 0o600 {
		t.Fatalf("download mode %v", mode)
	}
	// A node.root's download is root's from the hand-over: the agent leaves it.
	if b, err := os.ReadFile(filepath.Join(a.ArtefactDir, cmdIDFor(1))); err != nil || !bytes.Equal(b, data) {
		t.Fatalf("root's download: %v", err)
	}
}

func TestArtefactResumesItsPart(t *testing.T) {
	data := payload(3000)
	for _, c := range []struct {
		name  string
		part  []byte
		first [][2]int
	}{
		{"a shorter part goes on from its size", data[:1500], [][2]int{{1500, 1000}, {2500, 500}}},
		{"a whole part goes straight to the check", data, nil},
		{"a longer part starts over", append(append([]byte{}, data...), 'x'), [][2]int{{0, 1000}, {1000, 1000}, {2000, 1000}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, a, ex, _ := newArtefactAgent(t)
			ArtefactChunk = 1000
			os.MkdirAll(a.ArtefactDir, 0o700)
			os.WriteFile(filepath.Join(a.ArtefactDir, "."+cmdIDFor(1)+".part"), c.part, 0o600)
			a.handleCommand(context.Background(), m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/banned", "banned.mp4", data)}, data), a.run)
			start(t, a)
			if got := m.ackOf(t, cmdIDFor(1)); !got.OK {
				t.Fatalf("ack %+v", got)
			}
			if !slices.Equal(m.offsets(), c.first) {
				t.Fatalf("asked for %v, want %v", m.offsets(), c.first)
			}
			if got, _ := ex.file(cmdIDFor(1)); !bytes.Equal(got, data) {
				t.Fatal("the download is not the grant's bytes")
			}
		})
	}
}

// The agent's own hash refusal (ArtefactHashRefusalTest's, in Go): bytes
// that are not the grant's are deleted, never handed to cluster:exec, and
// the ack says so in the words MAIN audits.
func TestArtefactHashRefusal(t *testing.T) {
	m, a, ex, logs := newArtefactAgent(t)
	ArtefactChunk = 1000
	data := payload(2500)
	g := grantFor("offair/not_on_air", "off.ts", append([]byte("tampered"), data[8:]...))
	a.handleCommand(context.Background(), m.command(1, "artefact.fetch", map[string]any{"artefact": g}, data), a.run)
	start(t, a)
	got := m.ackOf(t, cmdIDFor(1))
	if got.OK || got.Result != "artefact refused: offair/not_on_air (off.ts): sha256 mismatch" {
		t.Fatalf("ack %+v", got)
	}
	if len(ex.order()) != 0 {
		t.Fatal("cluster:exec ran a download that is not the grant's")
	}
	if left, _ := os.ReadDir(a.ArtefactDir); len(left) != 0 {
		t.Fatalf("left behind: %v", left)
	}
	if !logs.has("artefact offair/not_on_air: sha256 mismatch") {
		t.Fatalf("not logged:\n%s", logs.all())
	}
	waitFor(t, "the command to be forgotten", func() bool { return len(a.Client.State.held()) == 0 })
}

func TestArtefactRefusalsAndRetries(t *testing.T) {
	refuse := func(status int, reason string, extra map[string]any) func(m *artefactMain) func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any) {
		return func(m *artefactMain) func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any) {
			return func(w http.ResponseWriter, _, nonce []byte, _ map[string]any) {
				m.refuse(w, status, nonce, reason, extra)
			}
		}
	}
	forged := func(change func(c map[string]any)) func(m *artefactMain) func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any) {
		return func(m *artefactMain) func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any) {
			return func(w http.ResponseWriter, reqCtx, _ []byte, req map[string]any) {
				c, _, _ := m.chunk(req)
				change(c)
				m.box(w, reqCtx, c)
			}
		}
	}
	unsigned := func(status int) func(m *artefactMain) func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any) {
		return func(*artefactMain) func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any) {
			return func(w http.ResponseWriter, _, _ []byte, _ map[string]any) { w.WriteHeader(status) }
		}
	}
	type answer = func(m *artefactMain) func(w http.ResponseWriter, reqCtx, nonce []byte, req map[string]any)
	for _, c := range []struct {
		name   string
		first  []answer
		result string // "" for a download that goes on to be handed over
		busy   int64
	}{
		{"503 RATE_LIMITED on the bulk lane", []answer{refuse(503, "RATE_LIMITED", map[string]any{"op": "artefact", "lane": "bulk", "retry_after_ms": 10})}, "", 1},
		{"503 STARTING", []answer{refuse(503, "STARTING", map[string]any{"retry_after_ms": 10})}, "", 0},
		{"401 REPLAY with retry_after_ms, twice", []answer{refuse(401, "REPLAY", map[string]any{"retry_after_ms": 10}), refuse(401, "REPLAY", map[string]any{"retry_after_ms": 10})}, "", 0},
		{"503 DB", []answer{refuse(503, "DB", nil), refuse(503, "DB", nil)}, "", 0},
		{"an unsigned 502 and 429", []answer{unsigned(502), unsigned(429), unsigned(504)}, "", 0},
		{"403 GRANT_INVALID", []answer{refuse(403, "GRANT_INVALID", nil)}, "artefact offair/expired: GRANT_INVALID", 0},
		{"409 ARTEFACT_CHANGED", []answer{refuse(409, "ARTEFACT_CHANGED", nil)}, "artefact offair/expired: ARTEFACT_CHANGED", 0},
		{"416 BAD_RANGE", []answer{refuse(416, "BAD_RANGE", map[string]any{"size": 1, "max_chunk": 4194304})}, "artefact offair/expired: BAD_RANGE", 0},
		{"400 BAD_REQUEST", []answer{refuse(400, "BAD_REQUEST", nil)}, "artefact offair/expired: BAD_REQUEST", 0},
		{"404 UNKNOWN_OP (MAIN rolled back)", []answer{refuse(404, "UNKNOWN_OP", nil)}, "artefact offair/expired: UNKNOWN_OP", 0},
		{"a chunk of another grant", []answer{forged(func(c map[string]any) { c["grant"] = cmdIDFor(9) })}, "artefact offair/expired: bad reply", 0},
		{"a chunk at another offset", []answer{forged(func(c map[string]any) { c["offset"] = 1 })}, "artefact offair/expired: bad reply", 0},
		{"data shorter than its length", []answer{forged(func(c map[string]any) { c["length"] = c["length"].(int) + 1 })}, "artefact offair/expired: bad reply", 0},
		{"a length above the one asked", []answer{forged(func(c map[string]any) {
			c["length"], c["data"] = 1001, base64.StdEncoding.EncodeToString(make([]byte, 1001))
		})}, "artefact offair/expired: bad reply", 0},
		{"an empty chunk", []answer{forged(func(c map[string]any) { c["length"], c["data"] = 0, "" })}, "artefact offair/expired: bad reply", 0},
		{"data that is not base64", []answer{forged(func(c map[string]any) { c["data"] = "!!!" })}, "artefact offair/expired: bad reply", 0},
		{"another size", []answer{forged(func(c map[string]any) { c["size"] = 1 })}, "artefact offair/expired: bad reply", 0},
		{"another sha256", []answer{forged(func(c map[string]any) { c["sha256"] = strings.Repeat("0", 64) })}, "artefact offair/expired: bad reply", 0},
		{"eof before the end", []answer{forged(func(c map[string]any) { c["eof"] = true })}, "artefact offair/expired: bad reply", 0},
		{"no eof", []answer{forged(func(c map[string]any) { delete(c, "eof") })}, "artefact offair/expired: bad reply", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			m, a, ex, _ := newArtefactAgent(t)
			ArtefactChunk = 1000
			data := payload(1500)
			for _, f := range c.first {
				m.script = append(m.script, f(m))
			}
			a.handleCommand(context.Background(), m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/expired", "expired.ts", data)}, data), a.run)
			start(t, a)
			got := m.ackOf(t, cmdIDFor(1))
			if c.result == "" {
				if !got.OK || len(ex.order()) != 1 {
					t.Fatalf("ack %+v, ran %v", got, ex.order())
				}
				// The same chunk again, then on: no request skipped or doubled.
				offs := m.offsets()
				var want [][2]int
				for i := 0; i <= len(c.first); i++ {
					want = append(want, [2]int{0, 1000})
				}
				if want = append(want, [2]int{1000, 500}); !slices.Equal(offs, want) {
					t.Fatalf("asked for %v, want %v", offs, want)
				}
				if file, _ := ex.file(cmdIDFor(1)); !bytes.Equal(file, data) {
					t.Fatal("not the grant's bytes")
				}
			} else if got.OK || got.Result != c.result || len(ex.order()) != 0 || len(m.requests()) != 1 {
				t.Fatalf("ack %+v, ran %v, %d request(s)", got, ex.order(), len(m.requests()))
			}
			if a.BusyRefusals() != c.busy {
				t.Fatalf("busy refusals %d, want %d", a.BusyRefusals(), c.busy)
			}
			waitFor(t, "a clean directory", func() bool {
				left, _ := os.ReadDir(a.ArtefactDir)
				return len(left) == 0
			})
		})
	}
}

func TestArtefactExpiredGrant(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	data := payload(10)
	g := grantFor("offair/connected", "connected.ts", data)
	g["exp"] = time.Now().Unix() - 1 // the grant's own exp, on MAIN's clock
	a.handleCommand(context.Background(), m.command(1, "artefact.fetch", map[string]any{"artefact": g}, data), a.run)
	start(t, a)
	if got := m.ackOf(t, cmdIDFor(1)); got.OK || got.Result != "artefact offair/connected: expired" {
		t.Fatalf("ack %+v", got)
	}
	if len(m.requests()) != 0 || len(ex.order()) != 0 {
		t.Fatal("fetched or ran an expired grant")
	}
}

// A quarantined node (409 NOT_ACTIVE) keeps its part and resumes once MAIN
// serves it again, without acking anything meanwhile.
func TestArtefactPausedWhileNotActive(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	ArtefactChunk = 1000
	data := payload(2500)
	notActive := func(w http.ResponseWriter, _, nonce []byte, _ map[string]any) {
		m.refuse(w, 409, nonce, "NOT_ACTIVE", nil)
	}
	serve := func(w http.ResponseWriter, reqCtx, _ []byte, req map[string]any) {
		c, _, _ := m.chunk(req)
		m.box(w, reqCtx, c)
	}
	m.script = append(m.script, serve, notActive, notActive, notActive)
	a.handleCommand(context.Background(), m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/not_on_air", "off.ts", data)}, data), a.run)
	start(t, a)
	waitFor(t, "the pauses", func() bool { return len(m.requests()) >= 4 })
	if len(m.acked()) != 0 {
		t.Fatalf("acked while paused: %+v", m.acked())
	}
	if got := m.ackOf(t, cmdIDFor(1)); !got.OK {
		t.Fatalf("ack %+v", got)
	}
	if want := [][2]int{{0, 1000}, {1000, 1000}, {1000, 1000}, {1000, 1000}, {1000, 1000}, {2000, 500}}; !slices.Equal(m.offsets(), want) {
		t.Fatalf("asked for %v, want %v", m.offsets(), want)
	}
	if file, _ := ex.file(cmdIDFor(1)); !bytes.Equal(file, data) {
		t.Fatal("not the grant's bytes")
	}
}

// A timeout asks for the same offset again with the length halved, never
// below ArtefactChunkMin, and keeps the smaller length.
func TestArtefactTimeoutHalvesTheChunk(t *testing.T) {
	m, a, _, _ := newArtefactAgent(t)
	ArtefactChunk, ArtefactChunkMin = 4096, 1024
	a.Client.BulkHTTP.Timeout = 150 * time.Millisecond
	data := payload(5000)
	slow := func(w http.ResponseWriter, reqCtx, _ []byte, req map[string]any) {
		time.Sleep(400 * time.Millisecond)
	}
	m.script = append(m.script, slow, slow, slow)
	a.handleCommand(context.Background(), m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/not_on_air", "off.ts", data)}, data), a.run)
	start(t, a)
	if got := m.ackOf(t, cmdIDFor(1)); !got.OK {
		t.Fatalf("ack %+v", got)
	}
	want := [][2]int{{0, 4096}, {0, 2048}, {0, 1024}, {0, 1024}, {1024, 1024}, {2048, 1024}, {3072, 1024}, {4096, 904}}
	if !slices.Equal(m.offsets(), want) {
		t.Fatalf("asked for %v, want %v", m.offsets(), want)
	}
}

// A download runs beside the command loop: a kill queued behind it runs at
// once, and the redelivered grant is not fetched twice.
func TestArtefactDownloadNeverHoldsUpCommands(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	m.gate = make(chan struct{})
	data := payload(100)
	fetch := m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/not_on_air", "off.ts", data)}, data)
	ctx := context.Background()
	a.handleCommand(ctx, fetch, a.run)
	start(t, a)
	waitFor(t, "the download to start", func() bool { return len(m.requests()) == 1 })
	a.handleCommand(ctx, m.command(2, "conn.kill_worker", map[string]any{"pid": 42}, nil), a.run)
	if got := m.ackOf(t, cmdIDFor(2)); !got.OK || !slices.Equal(ex.order(), []uint64{2}) {
		t.Fatalf("the kill waited behind the download: ack %+v, ran %v", got, ex.order())
	}
	if a.Client.State.CmdSeq != 2 {
		t.Fatalf("high-water %d", a.Client.State.CmdSeq)
	}
	a.handleCommand(ctx, fetch, a.run) // MAIN hands it out again
	back, _ := loadRaw(a.Client.State.path)
	if len(back.HeldCmds) != 1 || back.HeldCmds[0].CmdID != cmdIDFor(1) {
		t.Fatalf("kept %+v", back.HeldCmds)
	}
	close(m.gate)
	if got := m.ackOf(t, cmdIDFor(1)); !got.OK {
		t.Fatalf("ack %+v", got)
	}
	if !slices.Equal(ex.order(), []uint64{2, 1}) || len(m.requests()) != 1 {
		t.Fatalf("ran %v, %d request(s)", ex.order(), len(m.requests()))
	}
}

// Root commands reach cluster:exec one at a time in seq order: one behind a
// downloading node.root waits, kept with it; every other type goes on.
func TestRootCommandsGoInSeqOrder(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	m.gate = make(chan struct{})
	ctx := context.Background()
	bin := payload(300)
	a.handleCommand(ctx, m.command(1, "node.root", map[string]any{"action": "agent_binary", "arch": "amd64", "version": "1.5.0", "artefact": grantFor("agent/amd64", "xc_agent-linux-amd64", bin)}, bin), a.run)
	a.handleCommand(ctx, m.command(2, "node.root", map[string]any{"action": "reload_nginx"}, nil), a.run)
	a.handleCommand(ctx, m.command(3, "node.rpc", map[string]any{"action": "get_pids"}, nil), a.run)
	vid := payload(50)
	a.handleCommand(ctx, m.command(4, "artefact.fetch", map[string]any{"artefact": grantFor("offair/banned", "banned.ts", vid)}, vid), a.run)
	a.handleCommand(ctx, m.command(5, "node.root", map[string]any{"action": "delete_module", "name": "radio"}, nil), a.run)
	if !slices.Equal(ex.order(), []uint64{3}) || a.Client.State.CmdSeq != 5 {
		t.Fatalf("ran %v, high-water %d", ex.order(), a.Client.State.CmdSeq)
	}
	back, _ := loadRaw(a.Client.State.path)
	var kept []uint64
	for _, h := range back.HeldCmds {
		kept = append(kept, h.Seq)
	}
	if !slices.Equal(kept, []uint64{1, 2, 4, 5}) {
		t.Fatalf("kept %v", kept)
	}
	// A restart: another agent takes the kept commands from state.json.
	st, err := LoadState(a.Client.State.path)
	if err != nil {
		t.Fatal(err)
	}
	b := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf, ArtefactDir: a.ArtefactDir, run: ex.run}
	b.artefactOn.Store(true)
	start(t, b)
	waitFor(t, "the first download", func() bool { return len(m.requests()) == 1 })
	close(m.gate)
	for _, seq := range []uint64{1, 2, 4, 5} {
		if got := m.ackOf(t, cmdIDFor(seq)); !got.OK {
			t.Fatalf("ack of %d: %+v", seq, got)
		}
	}
	if order := ex.order(); !slices.Equal(order, []uint64{3, 1, 2, 4, 5}) {
		t.Fatalf("ran %v", order)
	}
}

// When a node.root's download fails, it is acked failed and the root
// commands behind it are handed over.
func TestRootCommandsGoOnAfterAFailedDownload(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	m.script = append(m.script, func(w http.ResponseWriter, _, nonce []byte, _ map[string]any) {
		m.refuse(w, 409, nonce, "ARTEFACT_CHANGED", nil)
	})
	ctx := context.Background()
	zip := payload(64)
	a.handleCommand(ctx, m.command(1, "node.root", map[string]any{"action": "install_module", "source": "local", "name": "radio", "version": "1.0", "artefact": grantFor("module/radio/1.0", "radio_1.0.zip", zip)}, zip), a.run)
	a.handleCommand(ctx, m.command(2, "node.root", map[string]any{"action": "reload_nginx"}, nil), a.run)
	start(t, a)
	if got := m.ackOf(t, cmdIDFor(1)); got.OK || got.Result != "artefact module/radio/1.0: ARTEFACT_CHANGED" {
		t.Fatalf("ack %+v", got)
	}
	if got := m.ackOf(t, cmdIDFor(2)); !got.OK || !slices.Equal(ex.order(), []uint64{2}) {
		t.Fatalf("ack %+v, ran %v", got, ex.order())
	}
}

func TestArtefactFilesAreCleanedUp(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	// An artefact.fetch's download goes once cluster:exec returned, even
	// when PHP refused the command before reading it.
	ex.outcome = func(*Command) (bool, []byte) {
		return false, []byte("cluster:exec: exit status 2: cluster:exec: unknown command type")
	}
	data := payload(20)
	a.handleCommand(context.Background(), m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/not_on_air", "off.ts", data)}, data), a.run)
	start(t, a)
	if got := m.ackOf(t, cmdIDFor(1)); got.OK || got.Result != "cluster:exec: exit status 2: cluster:exec: unknown command type" {
		t.Fatalf("ack %+v", got)
	}
	if _, ok := ex.file(cmdIDFor(1)); !ok {
		t.Fatal("cluster:exec ran without the download")
	}
	waitFor(t, "the download to go", func() bool {
		_, err := os.Stat(filepath.Join(a.ArtefactDir, cmdIDFor(1)))
		return os.IsNotExist(err)
	})
}

// The sweep: a root's download is left to root for ArtefactKeep, a part no
// kept command fetches goes.
func TestArtefactSweep(t *testing.T) {
	_, a, _, _ := newArtefactAgent(t)
	dir := a.ArtefactDir
	os.MkdirAll(dir, 0o700)
	write := func(name string, age time.Duration) {
		p := filepath.Join(dir, name)
		os.WriteFile(p, []byte("x"), 0o600)
		os.Chtimes(p, time.Now().Add(-age), time.Now().Add(-age))
	}
	write(cmdIDFor(7), 26*time.Hour)
	write(cmdIDFor(8), time.Hour)
	write("."+cmdIDFor(9)+".part", time.Minute)
	a.Client.State.hold(HeldCmd{CmdID: cmdIDFor(10), Seq: 10, Type: "artefact.fetch", Grant: &Grant{ID: "offair/banned"}})
	write("."+cmdIDFor(10)+".part", 30*time.Hour)
	a.sweepArtefacts()
	var left []string
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		left = append(left, e.Name())
	}
	if want := []string{"." + cmdIDFor(10) + ".part", cmdIDFor(8)}; !slices.Equal(left, want) {
		t.Fatalf("left %v, want %v", left, want)
	}
}

// Without the feature (the node's PHP does not run artefact.fetch), a
// command with a grant goes to PHP as today, which refuses it.
func TestArtefactFeatureFollowsThePHP(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	a.Exec = ex.run
	var types []string
	var typesErr error
	asked := 0
	a.Types = func(context.Context) ([]string, error) {
		asked++
		return types, typesErr
	}
	ctx := context.Background()
	hello := func() []string {
		t.Helper()
		if _, err := a.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return m.lastHello()
	}
	a.Client.State.Enrolled = true
	typesErr = fmt.Errorf("exit status 2: cluster:exec: bad signature")
	if f := hello(); slices.Contains(f, FeatureArtefact) {
		t.Fatalf("said %v for a PHP that predates --types", f)
	}
	types, typesErr = []string{"node.rpc", "node.root"}, nil
	if f := hello(); slices.Contains(f, FeatureArtefact) {
		t.Fatalf("said %v for a PHP without artefact.fetch", f)
	}
	data := payload(10)
	a.handleCommand(ctx, m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/not_on_air", "off.ts", data)}, data), a.run)
	if !slices.Equal(ex.order(), []uint64{1}) || len(a.Client.State.held()) != 0 {
		t.Fatalf("a grant without the feature did not go to PHP as today: ran %v", ex.order())
	}
	types = []string{"node.rpc", "node.root", "artefact.fetch"}
	if f := hello(); !slices.Contains(f, FeatureArtefact) {
		t.Fatalf("said %v once the PHP runs artefact.fetch", f)
	}
	if asked != 3 {
		t.Fatalf("asked the PHP %d times for 3 hellos", asked)
	}
	a.ArtefactDir = ""
	if f := hello(); slices.Contains(f, FeatureArtefact) {
		t.Fatalf("said %v without an artefacts directory", f)
	}
}

// typed_starts follows the node's PHP as artefact does: said at hello only
// once its cluster:exec --types lists stream.start, and whether or not the
// agent keeps an artefacts directory.
func TestTypedStartsFollowThePHP(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	a.Exec = ex.run
	types := []string{"node.rpc", "stream.stop"}
	a.Types = func(context.Context) ([]string, error) { return types, nil }
	ctx := context.Background()
	hello := func() []string {
		t.Helper()
		if _, err := a.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return m.lastHello()
	}
	a.Client.State.Enrolled = true
	if f := hello(); slices.Contains(f, FeatureTypedStarts) {
		t.Fatalf("said %v for a PHP without stream.start", f)
	}
	types = []string{"node.rpc", "stream.stop", "stream.start", "vod.start"}
	a.ArtefactDir = ""
	if f := hello(); !slices.Contains(f, FeatureTypedStarts) {
		t.Fatalf("said %v once the PHP runs stream.start", f)
	}
}

// The fanout daemon and xcvm_core follow the agent's path only to a node
// whose PHP installs them: artefact_binaries, beside artefact, while its
// cluster:exec --types lists both root installs.
func TestArtefactBinariesFollowThePHP(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	a.Exec = ex.run
	types := []string{"node.rpc", TypeArtefactFetch}
	a.Types = func(context.Context) ([]string, error) { return types, nil }
	ctx := context.Background()
	hello := func() []string {
		t.Helper()
		if _, err := a.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return m.lastHello()
	}
	a.Client.State.Enrolled = true
	if f := hello(); !slices.Contains(f, FeatureArtefact) || slices.Contains(f, FeatureArtefactBinaries) {
		t.Fatalf("an older PHP: artefacts only, said %v", f)
	}
	types = append(types, "root:fanout_binary")
	if f := hello(); slices.Contains(f, FeatureArtefactBinaries) {
		t.Fatalf("one of the two root installs is not enough, said %v", f)
	}
	types = append(types, "root:xcvm_core")
	if f := hello(); !slices.Contains(f, FeatureArtefactBinaries) {
		t.Fatalf("the PHP installs both, said %v", f)
	}
	types = []string{"node.rpc", "root:fanout_binary", "root:xcvm_core"}
	if f := hello(); slices.Contains(f, FeatureArtefactBinaries) {
		t.Fatalf("never without artefacts, said %v", f)
	}
}

func TestTypesViaPHP(t *testing.T) {
	dir := t.TempDir()
	script := func(name, body string) string {
		p := filepath.Join(dir, name)
		// $1 is the console; stdin must be closed (empty).
		os.WriteFile(p, []byte("#!/bin/sh\n[ \"$2 $3\" = \"cluster:exec --types\" ] || exit 9\n[ -z \"$(cat)\" ] || exit 8\n"+body+"\n"), 0o755)
		return p
	}
	ctx := context.Background()
	got, err := TypesViaPHP(script("new", `echo '["node.rpc","artefact.fetch"]'`), "console.php", 5*time.Second)(ctx)
	if err != nil || !slices.Equal(got, []string{"node.rpc", "artefact.fetch"}) {
		t.Fatalf("%v %v", got, err)
	}
	if _, err := TypesViaPHP(script("old", `echo 'cluster:exec: bad signature' >&2; exit 2`), "console.php", 5*time.Second)(ctx); err == nil || !strings.Contains(err.Error(), "bad signature") {
		t.Fatalf("an old PHP: %v", err)
	}
	if _, err := TypesViaPHP(script("junk", `echo 'OK'`), "console.php", 5*time.Second)(ctx); err == nil {
		t.Fatal("accepted junk")
	}
}

func TestArtefactNeverLogged(t *testing.T) {
	m, a, _, logs := newArtefactAgent(t)
	ArtefactChunk = 1000
	data := payload(2500)
	other := slices.Clone(data)
	slices.Reverse(other)
	ctx := context.Background()
	// Fetched and handed on; refused for its SHA-256; a bad reply.
	a.handleCommand(ctx, m.command(1, "artefact.fetch", map[string]any{"artefact": grantFor("offair/not_on_air", "off.ts", data)}, data), a.run)
	a.handleCommand(ctx, m.command(2, "artefact.fetch", map[string]any{"artefact": grantFor("offair/banned", "banned.ts", other)}, data), a.run)
	a.handleCommand(ctx, m.command(3, "artefact.fetch", map[string]any{"artefact": grantFor("offair/expired", "expired.ts", data)}, data), a.run)
	m.script = append(m.script, nil, nil, nil, nil, nil, nil, func(w http.ResponseWriter, reqCtx, _ []byte, req map[string]any) {
		c, _, _ := m.chunk(req)
		c["offset"] = 7
		m.box(w, reqCtx, c)
	})
	start(t, a)
	if !m.ackOf(t, cmdIDFor(1)).OK || m.ackOf(t, cmdIDFor(2)).OK || m.ackOf(t, cmdIDFor(3)).Result != "artefact offair/expired: bad reply" {
		t.Fatalf("acks %+v", m.acked())
	}
	all := logs.all()
	for _, leak := range []string{"S3cretVideoBytes", base64.StdEncoding.EncodeToString(data[:12]), hex.EncodeToString(data[:12]), string(data[:12])} {
		if strings.Contains(all, leak) {
			t.Fatalf("logged %q:\n%s", leak, all)
		}
	}
}
