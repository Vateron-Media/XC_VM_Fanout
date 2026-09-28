package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// fenceAgent is an agent with a fence file, a signals directory and a
// registry holding two open viewers and one ended one.
func fenceAgent(t *testing.T) (*fakeMain, *Agent, string) {
	t.Helper()
	f, st := newFake(t)
	dir := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, FenceFile: filepath.Join(dir, "fence.json"), SignalsDir: filepath.Join(dir, "signals")}
	if err := os.Mkdir(a.SignalsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	a.Registry = NewRegistry(filepath.Join(dir, "registry.snap"), func([]map[string]any) error { return nil }, t.Logf)
	a.Registry.Seed([]map[string]any{
		{"uuid": "v1", "user_id": 1, "stream_id": 5},
		{"uuid": "v2", "user_id": 2, "stream_id": 5},
		{"uuid": "v3", "user_id": 3, "stream_id": 5, "hls_end": 1},
	}, true)
	return f, a, dir
}

func controlCmd(typ string, args map[string]any) *Command {
	return &Command{V: 1, Type: typ, CmdID: strings.Repeat("c", 32), Args: args}
}

func readFence(t *testing.T, a *Agent) map[string]any {
	t.Helper()
	b, err := os.ReadFile(a.FenceFile)
	if err != nil {
		return nil
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// An operator's fence: draining at once, fenced after its drain, and then one
// drop signal per open viewer — never one for a viewer already gone, never a
// second for the same viewer — until node.unfence lifts it all.
func TestAFenceDrainsThenDropsTheViewersItHolds(t *testing.T) {
	_, a, _ := fenceAgent(t)
	handled, ok, _ := a.controlExec(context.Background(), controlCmd(TypeFence, map[string]any{"reason": "admin", "drain_min": float64(10)}))
	if !handled || !ok {
		t.Fatalf("fence: handled %v ok %v", handled, ok)
	}
	doc := readFence(t, a)
	if doc["state"] != FenceDraining || doc["reason"] != "admin" {
		t.Fatalf("fence file %v, want draining", doc)
	}
	if entries, _ := os.ReadDir(a.SignalsDir); len(entries) != 0 {
		t.Fatalf("%d viewer(s) dropped during the drain", len(entries))
	}

	since := a.Client.State.Fence.SinceMs
	if a.fenceTick(time.UnixMilli(since+10*60*1000)) != FenceFenced {
		t.Fatal("not fenced once the drain is over")
	}
	for _, uuid := range []string{"v1", "v2"} {
		b, err := os.ReadFile(filepath.Join(a.SignalsDir, uuid))
		if err != nil {
			t.Fatalf("%s: no drop signal: %v", uuid, err)
		}
		var sig map[string]string
		json.Unmarshal(b, &sig)
		if sig["type"] != "drop" {
			t.Fatalf("%s: signal %v", uuid, sig)
		}
	}
	if _, err := os.Stat(filepath.Join(a.SignalsDir, "v3")); err == nil {
		t.Fatal("an ended viewer was signalled")
	}
	// The worker took its signal; another tick writes no second one.
	os.Remove(filepath.Join(a.SignalsDir, "v1"))
	a.fenceTick(time.UnixMilli(since + 11*60*1000))
	if _, err := os.Stat(filepath.Join(a.SignalsDir, "v1")); err == nil {
		t.Fatal("a viewer was dropped twice")
	}

	// A redelivered fence keeps the drain's start.
	a.controlExec(context.Background(), controlCmd(TypeFence, map[string]any{"reason": "admin", "drain_min": float64(10)}))
	if a.Client.State.Fence.SinceMs != since {
		t.Fatal("a second fence restarted the drain")
	}

	if _, ok, _ := a.controlExec(context.Background(), controlCmd(TypeUnfence, map[string]any{})); !ok {
		t.Fatal("unfence refused")
	}
	if a.Client.State.Fence != nil || readFence(t, a) != nil {
		t.Fatal("the fence outlived node.unfence")
	}
	back, err := loadRaw(a.Client.State.path)
	if err != nil || back.Fence != nil {
		t.Fatalf("the lifted fence is still in the state file: %v", err)
	}
}

// The fence survives a restart with its start, so a restart never gives a
// fenced node a fresh drain.
func TestAFenceIsKeptAcrossARestart(t *testing.T) {
	_, a, _ := fenceAgent(t)
	a.controlExec(context.Background(), controlCmd(TypeFence, map[string]any{"reason": "admin", "drain_min": float64(0)}))
	back, err := loadRaw(a.Client.State.path)
	if err != nil || back.Fence == nil || back.Fence.Reason != "admin" || back.Fence.SinceMs != a.Client.State.Fence.SinceMs {
		t.Fatalf("fence not persisted: %+v %v", back.Fence, err)
	}
	if readFence(t, a)["state"] != FenceFenced {
		t.Fatal("a drain of 0 is not fenced at once")
	}
}

// A licence fence is taken only from a LICENCE_INVALID (the session is
// refused); handed out by the long-poll it is acked and not taken, and one
// held ends once MAIN accepts the session again.
func TestALicenceFenceFollowsTheSession(t *testing.T) {
	_, a, _ := fenceAgent(t)
	lic := controlCmd(TypeFence, map[string]any{"reason": LicenceFence, "drain_min": float64(10)})
	if _, ok, res := a.controlExec(context.Background(), lic); !ok || a.Client.State.Fence != nil {
		t.Fatalf("a licence fence was taken with the session accepted: %s", res)
	}
	a.fenced.Store(true)
	a.controlExec(context.Background(), lic)
	if a.Client.State.Fence == nil {
		t.Fatal("a licence fence from a refused session was not taken")
	}
	a.fenced.Store(false)
	a.licenceBack()
	if a.Client.State.Fence != nil || readFence(t, a) != nil {
		t.Fatal("the licence fence outlived the licence's return")
	}

	// An operator's fence does not end with the session.
	a.controlExec(context.Background(), controlCmd(TypeFence, map[string]any{"reason": "admin", "drain_min": float64(10)}))
	a.licenceBack()
	if a.Client.State.Fence == nil {
		t.Fatal("an operator's fence ended with the session")
	}
}

func TestAFenceWithBadArgumentsIsRefused(t *testing.T) {
	_, a, _ := fenceAgent(t)
	for name, args := range map[string]map[string]any{
		"no reason":    {"drain_min": float64(1)},
		"odd reason":   {"reason": "../x", "drain_min": float64(1)},
		"long drain":   {"reason": "admin", "drain_min": float64(61)},
		"minus drain":  {"reason": "admin", "drain_min": float64(-1)},
		"string drain": {"reason": "admin", "drain_min": "x"},
	} {
		_, ok, _ := a.controlExec(context.Background(), controlCmd(TypeFence, args))
		if name == "string drain" {
			// intOf reads a non-number as 0, as PHP's intval() would: a drain of 0.
			a.liftFence("test")
			continue
		}
		if ok || a.Client.State.Fence != nil {
			t.Errorf("%s: taken", name)
		}
	}
}

// A quarantine lets only restrictive commands run and stops the replica,
// until a reply says the node is active again.
func TestAQuarantineRunsOnlyRestrictiveCommands(t *testing.T) {
	_, a, _ := fenceAgent(t)
	a.controlExec(context.Background(), controlCmd(TypeQuarantine, map[string]any{"reason": "admin"}))
	if !a.quarantined() {
		t.Fatal("not quarantined")
	}
	if handled, ok, _ := a.controlExec(context.Background(), controlCmd("node.rpc", nil)); !handled || ok {
		t.Fatal("a granting command ran on a quarantined node")
	}
	if handled, _, _ := a.controlExec(context.Background(), controlCmd("conn.drop", nil)); handled {
		t.Fatal("a restrictive command was stopped by the quarantine")
	}
	a.ReplicaDir = t.TempDir()
	if applied, err := a.syncReplica(context.Background()); applied || err != nil {
		t.Fatalf("a quarantined node synced its replica: %v %v", applied, err)
	}
	a.followState("quarantined")
	if !a.quarantined() {
		t.Fatal("the quarantine ended without MAIN trusting the node")
	}
	a.followState("active")
	if a.quarantined() {
		t.Fatal("the quarantine outlived MAIN's trust")
	}
}

func TestResyncAsksForWhatItNames(t *testing.T) {
	_, a, _ := fenceAgent(t)
	a.ReplicaDir = t.TempDir()
	_, ok, res := a.controlExec(context.Background(), controlCmd(TypeResync, map[string]any{"sections": []any{"config", "streams", "nonsense"}}))
	if !ok {
		t.Fatal(string(res))
	}
	if !a.replicaResync.Load() || !a.streamsResyncWanted.Load() {
		t.Fatal("a resync did not mark the replica and the streams")
	}
	var out struct {
		Sections []string `json:"sections"`
	}
	json.Unmarshal(res, &out)
	if strings.Join(out.Sections, ",") != "config,streams" {
		t.Fatalf("resync did %v", out.Sections)
	}
}

// The agent's control commands never reach the node's PHP, which would
// answer "unknown command type", and each is acked.
func TestTheAgentsControlCommandsNeverReachPHP(t *testing.T) {
	f, a, _ := fenceAgent(t)
	now := time.Now().Unix()
	ran := 0
	run := a.localExec(func(context.Context, *Command, WireCommand) (bool, []byte) {
		ran++
		return true, nil
	})
	for i, typ := range []string{TypeFence, TypeUnfence, TypeResync, TypeQuarantine} {
		args := map[string]any{"reason": "admin", "drain_min": 1, "sections": []string{}}
		doc, _ := json.Marshal(map[string]any{"v": 1, "type": typ, "exp": now + 600, "iat": now, "cmd_id": strings.Repeat("d", 32), "seq": i + 1,
			"node_uuid": f.uuid, "gen": 1, "dedupe_key": nil, "args": args})
		w := WireCommand{Doc: string(doc), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("cmd", doc))), Seq: uint64(i + 1)}
		cmd, err := a.Client.verifyCommand(w)
		if err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		if ok, res := run(context.Background(), cmd, w); !ok {
			t.Fatalf("%s: %s", typ, res)
		}
	}
	if ran != 0 {
		t.Fatalf("%d control command(s) reached cluster:exec", ran)
	}
}

// MAIN's real CommandBus signs the fence, the unfence and the quarantine
// (the extension's registry holds their arguments), and the agent runs each
// itself: the fence file follows, and the node's PHP is never asked.
func TestInteropFence(t *testing.T) {
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
	a := &Agent{Client: NewClient(st, "xc_agent/interop"), Version: "0.0.0-interop", Logf: t.Logf, FenceFile: filepath.Join(dir, "fence.json")}
	runPHP("command.php", "flows", "2")
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	oldIdle, oldWait := CommandsIdle, CommandsWait
	CommandsIdle, CommandsWait = 50*time.Millisecond, 500*time.Millisecond
	defer func() { CommandsIdle, CommandsWait = oldIdle, oldWait }()
	var php2 atomic.Int32
	run := a.localExec(func(context.Context, *Command, WireCommand) (bool, []byte) {
		php2.Add(1)
		return false, []byte("cluster:exec: unknown command type")
	})
	lctx, stopLane := context.WithCancel(ctx)
	defer stopLane()
	go a.RunCommands(lctx, run)

	await := func(id string) []any {
		t.Helper()
		for i := 0; i < 200; i++ {
			if res := runPHP("command.php", "result", id); res != "pending" {
				var out []any
				json.Unmarshal([]byte(res), &out)
				return out
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("command %s never acked", id)
		return nil
	}
	if out := await(runPHP("command.php", "enqueue", TypeFence, `{"reason":"admin","drain_min":0}`)); out[0] != true {
		t.Fatalf("fence acked %v", out)
	}
	if doc := readFence(t, a); doc["state"] != FenceFenced || doc["reason"] != "admin" {
		t.Fatalf("fence file %v", doc)
	}
	if out := await(runPHP("command.php", "enqueue", TypeUnfence, `{}`)); out[0] != true {
		t.Fatalf("unfence acked %v", out)
	}
	if readFence(t, a) != nil || a.Client.State.Fence != nil {
		t.Fatal("the fence outlived node.unfence")
	}
	if out := await(runPHP("command.php", "enqueue", TypeQuarantine, `{"reason":"admin"}`)); out[0] != true || !a.quarantined() {
		t.Fatalf("quarantine acked %v", out)
	}
	if n := php2.Load(); n != 0 {
		t.Fatalf("%d control command(s) reached cluster:exec", n)
	}
}
