package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// On a PHP-free node (NodeRootGo), an essential node.root action runs in Go and
// never reaches the PHP executor; a deferred one is dropped; both are acked
// (the high-water advances). With NodeRootGo off, every node.root goes to PHP.
func TestNodeRootGoRoutesEssentialToGo(t *testing.T) {
	f, st := newFake(t)
	ranShell := make(chan string, 4)
	old, oldDelay := nodeRootRun, nodeRootStopDelay
	nodeRootRun = func(_ context.Context, name string, args ...string) error {
		ranShell <- name + " " + strings.Join(args, " ")
		return nil
	}
	nodeRootStopDelay = 0
	defer func() { nodeRootRun, nodeRootStopDelay = old, oldDelay }()

	now := time.Now().Unix()
	wire := func(action string, seq uint64) WireCommand {
		doc := map[string]any{"v": 1, "type": "node.root", "action": action, "exp": now + 600, "iat": now, "cmd_id": fmt.Sprintf("%032x", seq), "seq": seq,
			"node_uuid": f.uuid, "gen": 1, "dedupe_key": nil, "args": map[string]any{}}
		b, _ := json.Marshal(doc)
		return WireCommand{Doc: string(b), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("cmd", b))), Seq: seq}
	}
	phpRan := 0
	php := func(context.Context, *Command, WireCommand) (bool, []byte) { phpRan++; return true, []byte("php") }

	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, NodeRootGo: true}
	a.handleCommand(context.Background(), wire("restart_services", 1), a.localExec(php)) // essential (self-stopping)
	if phpRan != 0 {
		t.Fatalf("essential node.root reached PHP %d time(s)", phpRan)
	}
	select {
	case got := <-ranShell:
		if !strings.Contains(got, "xc_vm") {
			t.Fatalf("restart_services ran %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart_services did not run in Go")
	}

	a.handleCommand(context.Background(), wire("rotate_db", 2), a.localExec(php)) // deferred to P5: dropped
	if phpRan != 0 {
		t.Fatalf("deferred node.root reached PHP %d time(s)", phpRan)
	}
	if st.CmdSeq != 2 {
		t.Fatalf("high-water %d, want 2 (both acked)", st.CmdSeq)
	}

	// NodeRootGo off: node.root is handed to PHP as today.
	b := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	b.handleCommand(context.Background(), wire("restart_services", 3), b.localExec(php))
	if phpRan != 1 {
		t.Fatalf("with NodeRootGo off, node.root must reach PHP; phpRan=%d", phpRan)
	}
}

func TestNodeRootClassification(t *testing.T) {
	for _, a := range []string{"reboot", "restart_services", "stop_services"} {
		if !nodeRootEssential[a] {
			t.Errorf("%s must be essential (ported in B1)", a)
		}
		if !nodeRootSelfStopping[a] {
			t.Errorf("%s stops the agent and must be self-stopping", a)
		}
	}
	for _, a := range []string{"rotate_db", "install_module", "update", "set_port"} {
		if nodeRootEssential[a] {
			t.Errorf("%s is deferred to P5, must not be essential", a)
		}
	}
}
