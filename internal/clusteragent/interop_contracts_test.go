package clusteragent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// interopNode enrols an agent by code against MAIN's real PHP ClusterApi
// (XCVM_PANEL_DIR, as TestInteropWithPanel) and returns it with a runner for
// the harness scripts. The agent has a spool, a registry and a local socket,
// none of them running yet.
func interopNode(t *testing.T) (*Agent, func(script string, args ...string) string, context.Context) {
	t.Helper()
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
	t.Cleanup(func() { srv.Process.Kill() })
	waitPort(t, port)
	old := EnrolPoll
	EnrolPoll = 100 * time.Millisecond
	t.Cleanup(func() { EnrolPoll = old })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
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
	sockDir, _ := os.MkdirTemp("", "xc")
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	spool := filepath.Join(dir, "spool")
	a := &Agent{Client: NewClient(st, "xc_agent/interop"), Version: "0.0.0-interop", Logf: t.Logf,
		FlowsFile: filepath.Join(dir, "flows.json"), SpoolDir: spool, SocketPath: filepath.Join(sockDir, "a.sock")}
	a.Registry = NewRegistry(filepath.Join(dir, "registry.snap"), func(ev []map[string]any) error { return spoolP0(spool, ev) }, t.Logf)
	a.Registry.Admit = a.admit
	return a, runPHP, ctx
}

func serveSocket(t *testing.T, ctx context.Context, a *Agent) {
	t.Helper()
	go a.ServeSocket(ctx, a.SocketPath)
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(a.SocketPath); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the local socket never came up")
}

// TestInteropAdmission: the node's PHP registers limited viewers with the
// X-XCVM-Admission header it builds (AgentConnections::admission), the agent
// asks MAIN's real conn_admit, and PHP reads the agent's answer.
func TestInteropAdmission(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	runPHP("admission.php", "lines")
	runPHP("events.php", "flows", "64") // CONNECTIONS
	r, err := a.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.OfflineAdmission != OfflineLocal || a.OfflineAdmission() != OfflineLocal {
		t.Fatalf("hello carried offline_admission %q", r.OfflineAdmission)
	}
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	serveSocket(t, lctx, a)

	var out struct {
		Header bool `json:"header"`
		Result any  `json:"result"`
	}
	// A valid line: MAIN admits, the viewer is stored.
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "70", "okviewer")), &out)
	if !out.Header || out.Result != true || a.Registry.Get("okviewer") == nil {
		t.Fatalf("valid line: %+v, stored %v", out, a.Registry.Get("okviewer"))
	}
	// An expired line: MAIN refuses, PHP reads the reason, nothing is stored.
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "71", "expviewer")), &out)
	if !out.Header || out.Result != "EXPIRED" || a.Registry.Get("expviewer") != nil {
		t.Fatalf("expired line: %+v", out)
	}
	// An unknown line likewise.
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "79", "noline")), &out)
	if out.Result != "UNKNOWN_LINE" {
		t.Fatalf("unknown line: %+v", out)
	}
	// CONNECTIONS off on MAIN while the agent still thinks it is on: a signed
	// FLOW_OFF, which admits.
	runPHP("events.php", "flows", "0")
	json.Unmarshal([]byte(runPHP("admission.php", "node", a.FlowsFile, a.SocketPath, "71", "flowoff")), &out)
	if out.Result != true {
		t.Fatalf("FLOW_OFF must admit: %+v", out)
	}
}

// TestInteropP2Touches: with CONNECTIONS on, MAIN lists conn.touch in
// p2_types; a touch goes on P2 and, without a cluster bus, lands in MAIN's
// store; the P0 upsert of the open carried the viewer there first.
func TestInteropP2Touches(t *testing.T) {
	a, runPHP, ctx := interopNode(t)
	runPHP("events.php", "flows", "64")
	a.Registry.P2 = a.p2Touch.Load
	r, err := a.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !a.p2Wanted(r) || !a.p2Touch.Load() {
		t.Fatalf("hello's p2_types %v did not turn P2 on", r.P2Types)
	}
	oldLanes := Lanes
	Lanes = []Lane{{Name: "p0", Interval: 50 * time.Millisecond}}
	defer func() { Lanes = oldLanes }()
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go a.RunEvents(lctx, Lanes[0])
	a.Registry.Put("hlsv", map[string]any{"user_id": 7, "stream_id": 100, "server_id": 7, "user_ip": "10.0.0.9", "user_agent": "VLC", "container": "hls", "pid": nil, "date_start": 1800000000, "hls_last_read": 1800000000, "hls_end": 0})
	for i := 0; i < 100 && runPHP("touch.php", "hlsv") != "1800000000"; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if got := runPHP("touch.php", "hlsv"); got != "1800000000" {
		t.Fatalf("the open never reached MAIN: %s", got)
	}
	a.Registry.Touch("hlsv", 1800000042)
	if err := a.sendTouches(ctx); err != nil {
		t.Fatal(err)
	}
	if got := runPHP("touch.php", "hlsv"); got != "1800000042" {
		t.Fatalf("MAIN's store after the P2 touch: %s", got)
	}
}
