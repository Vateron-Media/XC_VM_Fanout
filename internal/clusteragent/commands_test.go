package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// TestInteropCommands runs the command channel against MAIN's real PHP: a
// command queued on MAIN reaches the agent through the commands long-poll,
// passes the agent's checks, runs, and its ack becomes the command's result
// on MAIN. A node without the COMMANDS flow holds no poll. Opt-in, as the
// other interop tests.
func TestInteropCommands(t *testing.T) {
	panel := os.Getenv("XCVM_PANEL_DIR")
	if panel == "" {
		t.Skip("XCVM_PANEL_DIR not set")
	}
	php, err := exec.LookPath("php")
	if err != nil {
		t.Skip("php not found")
	}
	dir := t.TempDir()
	harness, _ := filepath.Abs("testdata/panel")
	port := freePort(t)
	env := append(os.Environ(), "XCVM_PANEL_DIR="+panel, "XCVM_INTEROP_DB="+filepath.Join(dir, "main.sqlite"), fmt.Sprintf("XCVM_INTEROP_PORT=%d", port))
	runPHP := func(script string, args ...string) string {
		cmd := exec.Command(php, append([]string{filepath.Join(harness, script)}, args...)...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", script, args, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	// Enrol by code (the shortest path to an active node here).
	code := runPHP("enrol_code.php")
	srv := exec.Command(php, "-S", fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(harness, "router.php"))
	srv.Env = env
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Process.Kill()
	waitPort(t, port)
	old := EnrolPoll
	EnrolPoll = 100 * time.Millisecond
	defer func() { EnrolPoll = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	statePath := filepath.Join(dir, "agent.json")
	sasCh, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- EnrolByCode(ctx, statePath, code, "xc_agent/interop", false, func(s string) { sasCh <- s })
	}()
	runPHP("approve.php", <-sasCh)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st, _ := LoadState(statePath)
	c := NewClient(st, "xc_agent/interop")
	a := &Agent{Client: c, Version: "0.0.0-interop", Logf: t.Logf}
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Flow off: the lane stays idle and holds no poll.
	oldIdle, oldWait := CommandsIdle, CommandsWait
	CommandsIdle, CommandsWait = 50*time.Millisecond, 500*time.Millisecond
	defer func() { CommandsIdle, CommandsWait = oldIdle, oldWait }()
	ran := make(chan *Command, 4)
	run := func(_ context.Context, cmd *Command, _ WireCommand) (bool, []byte) {
		ran <- cmd
		return true, []byte(`{"result":true,"pids":[1,2]}`)
	}
	lctx, stopLane := context.WithCancel(ctx)
	defer stopLane()
	go a.RunCommands(lctx, run)

	runPHP("command.php", "flows", "2")
	// The agent learns its flows from a reply.
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	id := runPHP("command.php", "enqueue", "node.rpc", `{"action":"get_pids"}`)
	select {
	case cmd := <-ran:
		if cmd.CmdID != id || cmd.Type != "node.rpc" || cmd.Action != "get_pids" || len(cmd.Args) != 0 || cmd.Seq != 1 {
			t.Fatalf("ran %+v", cmd)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the command did not reach the agent")
	}
	var res string
	for i := 0; i < 100; i++ {
		if res = runPHP("command.php", "result", id); res != "pending" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	var out []any
	if json.Unmarshal([]byte(res), &out) != nil || out[0] != true || out[1] != `{"result":true,"pids":[1,2]}` {
		t.Fatalf("result on MAIN: %s", res)
	}
	if st.CmdSeq != 1 {
		t.Fatalf("high-water %d", st.CmdSeq)
	}
	back, _ := LoadState(statePath)
	if back.CmdSeq != 1 {
		t.Fatal("the high-water was not persisted")
	}
}

func TestAgentChecksEveryCommand(t *testing.T) {
	f, st := newFake(t)
	c := NewClient(st, "t")
	_, stranger, _ := ed25519.GenerateKey(nil)
	now := time.Now().Unix()
	wire := func(key ed25519.PrivateKey, tag string, over map[string]any) WireCommand {
		doc := map[string]any{"v": 1, "type": "node.rpc", "exp": now + 600, "iat": now, "cmd_id": strings.Repeat("a", 32), "seq": 1,
			"node_uuid": f.uuid, "gen": 1, "dedupe_key": nil, "action": "get_pids", "args": map[string]any{}}
		for k, v := range over {
			doc[k] = v
		}
		b, _ := json.Marshal(doc)
		return WireCommand{Doc: string(b), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, cc.PanelSigInput(tag, b)))}
	}
	if _, err := c.verifyCommand(wire(f.panel, "cmd", nil)); err != nil {
		t.Fatalf("genuine: %v", err)
	}
	cases := map[string]WireCommand{
		"another key":        wire(stranger, "cmd", nil),
		"another tag":        wire(f.panel, "den", nil),
		"another node":       wire(f.panel, "cmd", map[string]any{"node_uuid": "11111111-1111-4111-a111-111111111111"}),
		"another generation": wire(f.panel, "cmd", map[string]any{"gen": 2}),
		"expired":            wire(f.panel, "cmd", map[string]any{"exp": now - 1}),
	}
	for name, w := range cases {
		if _, err := c.verifyCommand(w); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	st.CmdSeq = 1
	if _, err := c.verifyCommand(wire(f.panel, "cmd", nil)); err == nil {
		t.Error("a seq at the high-water was accepted (replay)")
	}
}

// token.rotate_now is the one command the agent runs itself: the token it
// rotates is the agent's, and the node's PHP would refuse the type ("unknown
// command type"). An operator's rotation must therefore never reach the
// executor, and must still be acked.
func TestRotateNowIsTheAgentsOwn(t *testing.T) {
	f, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	now := time.Now().Unix()
	wire := func(typ string, seq uint64) WireCommand {
		doc := map[string]any{"v": 1, "type": typ, "exp": now + 600, "iat": now, "cmd_id": strings.Repeat("b", 32), "seq": seq,
			"node_uuid": f.uuid, "gen": 1, "dedupe_key": nil, "args": map[string]any{}}
		b, _ := json.Marshal(doc)
		return WireCommand{Doc: string(b), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("cmd", b))), Seq: seq}
	}
	ran := 0
	run := func(context.Context, *Command, WireCommand) (bool, []byte) {
		ran++
		return true, []byte("php ran it")
	}

	a.handleCommand(context.Background(), wire(TypeRotateNow, 1), run)
	if ran != 0 {
		t.Fatalf("the rotation reached the executor %d time(s)", ran)
	}
	if st.CmdSeq != 1 {
		t.Fatalf("high-water %d, want 1 (a redelivery must not rotate twice)", st.CmdSeq)
	}

	// Every other type still goes to the node's PHP.
	a.handleCommand(context.Background(), wire("node.rpc", 2), run)
	if ran != 1 {
		t.Fatalf("node.rpc reached the executor %d time(s), want 1", ran)
	}
}

// An operator's token.rotate_now refreshes before its ack, which says how it
// went: MAIN's refusal here, rather than a "rotating" that promised nothing.
// It asks whatever backoff a failed scheduled refresh left.
func TestRotateNowAcksTheRefreshsOutcome(t *testing.T) {
	f, st := newFake(t)
	var mu sync.Mutex
	var acks []map[string]any
	refreshes := 0
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/token_refresh"):
			mu.Lock()
			refreshes++
			mu.Unlock()
			f.refuse(w, 403, nonce, "LICENCE_INVALID", nil)
		case strings.HasSuffix(r.URL.Path, "/ack"):
			req := f.opened(r, reqCtx)
			mu.Lock()
			acks = append(acks, req)
			mu.Unlock()
			f.box(w, reqCtx, map[string]any{"ok": true})
		default:
			f.box(w, reqCtx, map[string]any{"state": "active", "mode": 1})
		}
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs, st.Enrolled = []string{srv.URL + "/cluster/v1/"}, true
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	a.refreshNotBefore.Store(time.Now().Add(time.Hour).UnixNano()) // a failed refresh's backoff
	now := time.Now().Unix()
	doc, _ := json.Marshal(map[string]any{"v": 1, "type": TypeRotateNow, "exp": now + 600, "iat": now, "cmd_id": strings.Repeat("c", 32), "seq": 1,
		"node_uuid": f.uuid, "gen": 1, "dedupe_key": nil, "args": map[string]any{}})
	w := WireCommand{Doc: string(doc), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("cmd", doc))), Seq: 1}

	a.handleCommand(context.Background(), w, func(context.Context, *Command, WireCommand) (bool, []byte) { return true, nil })

	mu.Lock()
	defer mu.Unlock()
	if refreshes != 1 {
		t.Fatalf("%d refresh(es) asked, want 1 despite the backoff", refreshes)
	}
	if len(acks) != 1 || acks[0]["ok"] != false || !strings.Contains(acks[0]["result"].(string), "LICENCE_INVALID") {
		t.Fatalf("acks %v, want one failure naming MAIN's refusal", acks)
	}
}

func TestRootReadyFollowsRootsPin(t *testing.T) {
	_, st := newFake(t)
	old := RootPinDir
	RootPinDir = t.TempDir()
	defer func() { RootPinDir = old }()
	if RootReady(st) {
		t.Fatal("ready without a pin")
	}
	os.WriteFile(filepath.Join(RootPinDir, "main_sign.pub"), []byte(hex.EncodeToString(st.PanelSignPub)+"\n"), 0o644)
	os.WriteFile(filepath.Join(RootPinDir, "node"), []byte(st.NodeUUID+"\n"), 0o644)
	if !RootReady(st) {
		t.Fatal("not ready with a matching pin")
	}
	os.WriteFile(filepath.Join(RootPinDir, "main_sign.pub"), []byte(strings.Repeat("00", 32)), 0o644)
	if RootReady(st) {
		t.Fatal("ready with another key pinned")
	}
}

// A command refused before it runs is acked by its cmd_id, but its seq,
// read from a document that failed its checks, never raises the
// high-water: one entry claiming a seq near 2^64 (a forged signature, a
// genuine command for another generation or node) would otherwise make the
// agent skip every later command for good. MAIN takes a refused command out
// of the queue on its ack, so it is not handed out again.
func TestARefusedCommandLeavesTheHighWater(t *testing.T) {
	m, a, ex, _ := newArtefactAgent(t)
	a.artefactOn.Store(false)
	st := a.Client.State
	_, stranger, _ := ed25519.GenerateKey(nil)
	now := time.Now().Unix()
	wire := func(key ed25519.PrivateKey, id string, seq uint64, gen int) WireCommand {
		doc, _ := json.Marshal(map[string]any{"v": 1, "type": "conn.drop", "exp": now + 600, "iat": now, "cmd_id": id, "seq": seq,
			"node_uuid": m.uuid, "gen": gen, "dedupe_key": nil, "args": map[string]any{"uuid": "v1"}})
		return WireCommand{Doc: string(doc), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, cc.PanelSigInput("cmd", doc))), Seq: seq}
	}
	ctx := context.Background()
	for _, c := range []struct {
		name string
		w    WireCommand
	}{
		{"a forged signature", wire(stranger, strings.Repeat("e", 32), math.MaxUint64-1, 1)},
		{"another generation", wire(m.panel, strings.Repeat("f", 32), math.MaxUint64/2, 2)},
	} {
		a.handleCommand(ctx, c.w, ex.run)
		var probe Command
		json.Unmarshal([]byte(c.w.Doc), &probe)
		if ack := m.ackOf(t, probe.CmdID); ack.OK || !strings.HasPrefix(ack.Result, "refused: ") {
			t.Fatalf("%s: acked %+v", c.name, ack)
		}
		st.mu.Lock()
		high := st.CmdSeq
		st.mu.Unlock()
		if high != 0 {
			t.Fatalf("%s: the high-water went to %d", c.name, high)
		}
	}
	a.handleCommand(ctx, m.command(1, "node.rpc", map[string]any{"action": "get_pids"}, nil), ex.run)
	if ack := m.ackOf(t, cmdIDFor(1)); !ack.OK {
		t.Fatalf("the next genuine command: %+v", ack)
	}
	if len(ex.ran) != 1 || ex.ran[0].Action != "get_pids" || st.CmdSeq != 1 {
		t.Fatalf("ran %d, high-water %d", len(ex.ran), st.CmdSeq)
	}
	back, _ := LoadState(st.path)
	if back.CmdSeq != 1 {
		t.Fatalf("persisted high-water %d", back.CmdSeq)
	}
}

// A refused command MAIN cannot take off its queue (no cmd_id to ack it by,
// or an ack MAIN will not take) is handed out again on every poll, at once,
// and no longer moves the high-water past itself. The agent must neither run
// it nor spin against MAIN on it: it is skipped once seen, the polls that
// bring nothing else pause, and a genuine command queued behind it still
// runs.
func TestAStuckRefusalIsNotPolledInATightLoop(t *testing.T) {
	f, st := newFake(t)
	_, stranger, _ := ed25519.GenerateKey(nil)
	now := time.Now().Unix()
	sign := func(key ed25519.PrivateKey, doc map[string]any) WireCommand {
		b, _ := json.Marshal(doc)
		seq, _ := doc["seq"].(uint64)
		return WireCommand{Doc: string(b), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, cc.PanelSigInput("cmd", b))), Seq: seq}
	}
	body := func(id string, seq uint64, node string) map[string]any {
		d := map[string]any{"v": 1, "type": "node.rpc", "action": "get_pids", "exp": now + 3600, "iat": now, "seq": seq,
			"node_uuid": node, "gen": 1, "dedupe_key": nil, "args": map[string]any{}}
		if id != "" {
			d["cmd_id"] = id
		}
		return d
	}
	stray := strings.Repeat("d", 32) // a cmd_id MAIN does not know: BAD_REQUEST on its ack
	genuine := strings.Repeat("c", 32)
	var mu sync.Mutex
	rows := []WireCommand{
		sign(stranger, body("", math.MaxUint64-1, f.uuid)),                                   // forged, no cmd_id
		sign(f.panel, body(stray, math.MaxUint64/2, "11111111-1111-4111-a111-111111111111")), // another node's
	}
	polls, acks := 0, map[string]int{}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		req := f.opened(r, reqCtx)
		mu.Lock()
		defer mu.Unlock()
		switch op {
		case "commands":
			polls++
			after, _ := req["after_seq"].(float64)
			var out []WireCommand
			for _, w := range rows {
				if w.Seq > uint64(after) {
					out = append(out, w)
				}
			}
			f.box(w, reqCtx, map[string]any{"commands": out})
		case "ack":
			id, _ := req["cmd_id"].(string)
			acks[id]++
			if id != genuine {
				f.refuse(w, 400, nonce, "BAD_REQUEST", nil)
				return
			}
			f.box(w, reqCtx, map[string]any{"ok": true})
		default:
			f.box(w, reqCtx, map[string]any{"state": "active", "flows": FlowCommands})
		}
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	a.flows.Store(FlowCommands)
	oldStuck := CommandsStuck
	CommandsStuck = 300 * time.Millisecond
	defer func() { CommandsStuck = oldStuck }()
	var ran []string
	run := func(_ context.Context, cmd *Command, _ WireCommand) (bool, []byte) {
		mu.Lock()
		defer mu.Unlock()
		ran = append(ran, cmd.CmdID)
		return true, []byte("ran")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.RunCommands(ctx, run); close(done) }()
	defer func() { cancel(); <-done }()
	count := func() (int, int) {
		mu.Lock()
		defer mu.Unlock()
		return polls, acks[stray]
	}

	// The stray refusal's ack is tried once: MAIN's BAD_REQUEST is final.
	for i := 0; i < 1000; i++ {
		if _, n := count(); n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	p0, n0 := count()
	if n0 != 1 {
		t.Fatalf("the stray refusal was acked %d time(s), want 1", n0)
	}
	time.Sleep(time.Second)
	p1, n1 := count()
	if p1-p0 > 8 {
		t.Fatalf("%d polls in a second on a queue of refusals: a tight loop", p1-p0)
	}
	if n1 != n0 {
		t.Fatalf("the stray refusal was acked again (%d)", n1-n0)
	}

	// A genuine command queued behind them is still picked up and run.
	mu.Lock()
	rows = append(rows, sign(f.panel, body(genuine, 1, f.uuid)))
	mu.Unlock()
	for i := 0; i < 300; i++ {
		mu.Lock()
		got := acks[genuine]
		mu.Unlock()
		if got > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if acks[genuine] != 1 || len(ran) != 1 || ran[0] != genuine {
		t.Fatalf("genuine: acked %d, ran %v", acks[genuine], ran)
	}
	st.mu.Lock()
	high := st.CmdSeq
	st.mu.Unlock()
	if high != 1 {
		t.Fatalf("high-water %d, want 1", high)
	}
}

// A refusal whose ack failed only for now (MAIN answering 503 through all
// of one ack's tries) is acked again on a later delivery, and leaves the
// queue once MAIN takes it; the commands behind it keep flowing meanwhile.
func TestARefusalWhoseAckFailedForNowIsAckedAgain(t *testing.T) {
	f, st := newFake(t)
	now := time.Now().Unix()
	body := func(id string, seq uint64, node string) WireCommand {
		b, _ := json.Marshal(map[string]any{"v": 1, "type": "node.rpc", "action": "get_pids", "exp": now + 3600, "iat": now,
			"seq": seq, "cmd_id": id, "node_uuid": node, "gen": 1, "dedupe_key": nil, "args": map[string]any{}})
		return WireCommand{Doc: string(b), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("cmd", b))), Seq: seq}
	}
	refused := strings.Repeat("e", 32) // another node's: refused, but a row MAIN holds
	genuine := strings.Repeat("c", 32)
	var mu sync.Mutex
	rows := map[string]WireCommand{refused: body(refused, math.MaxUint64/2, "11111111-1111-4111-a111-111111111111")}
	acks, failing := map[string]int{}, 3 // one ack's three tries fail
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		req := f.opened(r, reqCtx)
		mu.Lock()
		defer mu.Unlock()
		switch op {
		case "commands":
			after, _ := req["after_seq"].(float64)
			var out []WireCommand
			for _, w := range rows {
				if w.Seq > uint64(after) {
					out = append(out, w)
				}
			}
			f.box(w, reqCtx, map[string]any{"commands": out})
		case "ack":
			id, _ := req["cmd_id"].(string)
			acks[id]++
			if id == refused && failing > 0 {
				failing--
				f.refuse(w, 503, nonce, "DB", nil)
				return
			}
			delete(rows, id) // acked: off the queue
			f.box(w, reqCtx, map[string]any{"ok": true})
		default:
			f.box(w, reqCtx, map[string]any{"state": "active", "flows": FlowCommands})
		}
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	a.flows.Store(FlowCommands)
	oldStuck, oldRetry := CommandsStuck, RefusalAckRetry
	CommandsStuck, RefusalAckRetry = 200*time.Millisecond, 300*time.Millisecond
	defer func() { CommandsStuck, RefusalAckRetry = oldStuck, oldRetry }()
	var ran []string
	run := func(_ context.Context, cmd *Command, _ WireCommand) (bool, []byte) {
		mu.Lock()
		defer mu.Unlock()
		ran = append(ran, cmd.CmdID)
		return true, []byte("ran")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.RunCommands(ctx, run); close(done) }()
	defer func() { cancel(); <-done }()

	// Its first ack (three tries) fails; a genuine command queued meanwhile
	// runs while the refusal waits for its next try.
	for i := 0; i < 1000; i++ {
		mu.Lock()
		n := acks[refused]
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	rows[genuine] = body(genuine, 1, f.uuid)
	mu.Unlock()
	for i := 0; i < 1000; i++ {
		mu.Lock()
		_, left := rows[refused]
		_, gleft := rows[genuine]
		mu.Unlock()
		if !left && !gleft {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	_, left := rows[refused]
	n := acks[refused]
	g := acks[genuine]
	gotRan := append([]string(nil), ran...)
	mu.Unlock()
	if left || n != 4 {
		t.Fatalf("refusal: still queued %v, acked %d time(s), want off the queue after 4", left, n)
	}
	if g != 1 || len(gotRan) != 1 || gotRan[0] != genuine {
		t.Fatalf("genuine: acked %d, ran %v", g, gotRan)
	}
	// Once taken it is not acked again, and the high-water is the verified
	// command's alone.
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	n2 := acks[refused]
	mu.Unlock()
	if n2 != n {
		t.Fatalf("the refusal was acked again after MAIN took it (%d)", n2-n)
	}
	st.mu.Lock()
	high := st.CmdSeq
	st.mu.Unlock()
	if high != 1 {
		t.Fatalf("high-water %d, want 1", high)
	}
}

// commandRegistry is xcvm_core's command registry, the table cluster_sign
// classes by, as the extension generates it (a byte-identical copy; the
// clustercrypto vectors test pins its digest).
type commandRegistry struct {
	Envelope  []string `json:"envelope"`
	ActionKey string   `json:"action_key"`
	Types     map[string]struct {
		Class  string   `json:"class"`
		Action bool     `json:"action"`
		Args   []string `json:"args"`
	} `json:"types"`
}

func loadCommandRegistry(t *testing.T) commandRegistry {
	t.Helper()
	b, err := os.ReadFile("../clustercrypto/testdata/cluster_commands.json")
	if err != nil {
		t.Fatal(err)
	}
	var reg commandRegistry
	if err := json.Unmarshal(b, &reg); err != nil || len(reg.Types) == 0 {
		t.Fatalf("cluster_commands.json: %v", err)
	}
	return reg
}

// The agent's restrictive set (what a quarantined node still runs) is the
// extension's: every type its registry classes R, and no other.
func TestRestrictiveIsTheRegistrys(t *testing.T) {
	reg := loadCommandRegistry(t)
	for typ, e := range reg.Types {
		if (e.Class == "R") != Restrictive[typ] {
			t.Errorf("%s: class %s, Restrictive %v", typ, e.Class, Restrictive[typ])
		}
	}
	for typ := range Restrictive {
		if _, ok := reg.Types[typ]; !ok {
			t.Errorf("%s is not a type the extension signs", typ)
		}
	}
}

// Every type the extension signs is one the agent runs itself or hands to
// the node's PHP (cluster:exec) unchanged, and acks: none is dropped. The
// types the agent dispatches on by name are all in the registry, and the
// action MAIN signs at the top level reaches the executor.
func TestEveryRegistryTypeIsHandledOrForwarded(t *testing.T) {
	reg := loadCommandRegistry(t)
	for _, typ := range []string{TypeRotateNow, TypeArtefactFetch, "node.root", "conn.close", "conn.drop", "config.changed", TypeFence, TypeUnfence, TypeQuarantine, TypeResync, TypePolicy} {
		if _, ok := reg.Types[typ]; !ok {
			t.Errorf("the agent dispatches on %q, which the extension does not sign", typ)
		}
	}
	if reg.ActionKey != "action" || !reg.Types["node.rpc"].Action || !reg.Types["node.root"].Action {
		t.Fatalf("the action key moved: %+v", reg)
	}

	m, a, ex, _ := newArtefactAgent(t)
	a.artefactOn.Store(false)
	// No fanout socket, connection registry or replica here: all but the
	// agent's own (token.rotate_now, TestRotateNowIsTheAgentsOwn, and the
	// control commands, TestTheAgentsControlCommandsNeverReachPHP) go to PHP.
	run := a.localExec(ex.run)
	own := map[string]bool{TypeRotateNow: true, TypeFence: true, TypeUnfence: true, TypeQuarantine: true, TypeResync: true, TypePolicy: true}
	types := make([]string, 0, len(reg.Types))
	for typ := range reg.Types {
		if !own[typ] {
			types = append(types, typ)
		}
	}
	slices.Sort(types)
	for i, typ := range types {
		seq := uint64(i + 1)
		args := map[string]any{}
		if slices.Contains(reg.Types[typ].Args, "uuid") {
			args["uuid"] = "v1"
		}
		if reg.Types[typ].Action {
			args["action"] = "x"
		}
		a.handleCommand(context.Background(), m.command(seq, typ, args, nil), run)
		if ack := m.ackOf(t, cmdIDFor(seq)); !ack.OK {
			t.Errorf("%s: acked %+v", typ, ack)
		}
	}
	if len(ex.ran) != len(types) {
		t.Fatalf("%d of %d types reached cluster:exec", len(ex.ran), len(types))
	}
	for i, cmd := range ex.ran {
		if cmd.Type != types[i] || reg.Types[cmd.Type].Action != (cmd.Action == "x") {
			t.Errorf("ran %s (action %q), want %s", cmd.Type, cmd.Action, types[i])
		}
	}
}
