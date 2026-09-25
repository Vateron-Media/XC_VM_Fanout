package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
		if cmd.CmdID != id || cmd.Type != "node.rpc" || cmd.Args["action"] != "get_pids" || cmd.Seq != 1 {
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
			"node_uuid": f.uuid, "gen": 1, "dedupe_key": nil, "args": map[string]any{"action": "get_pids"}}
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
