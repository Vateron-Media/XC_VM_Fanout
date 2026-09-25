package clusteragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type sink struct {
	events []map[string]any
	fail   bool
}

func (s *sink) emit(ev []map[string]any) error {
	if s.fail {
		return errors.New("spool full")
	}
	s.events = append(s.events, ev...)
	return nil
}

func (s *sink) types() string {
	var out []string
	for _, e := range s.events {
		out = append(out, e["type"].(string))
	}
	return strings.Join(out, ",")
}

func newTestRegistry(t *testing.T) (*Registry, *sink, *time.Time) {
	s := &sink{}
	now := time.Unix(1800000000, 0)
	r := NewRegistry(filepath.Join(t.TempDir(), "registry.snap"), s.emit, t.Logf)
	r.now = func() time.Time { return now }
	return r, s, &now
}

func rec(user int, ip string, start int, lastRead int) map[string]any {
	return map[string]any{"user_id": user, "stream_id": 100, "server_id": 5, "user_ip": ip, "container": "hls", "user_agent": "VLC", "date_start": start, "hls_last_read": lastRead, "hls_end": 0, "identity": user}
}

func TestRegistryMirrorsChangesAndThrottlesTouches(t *testing.T) {
	r, s, now := newTestRegistry(t)
	r.Put("a", rec(7, "10.0.0.1", 100, 100))
	r.Put("a", rec(7, "10.0.0.1", 100, 102)) // only hls_last_read: held back
	if s.types() != "conn.upsert" {
		t.Fatalf("events %s", s.types())
	}
	c := rec(7, "10.0.0.1", 100, 103)
	c["pid"] = 55 // a real change goes at once
	r.Put("a", c)
	*now = now.Add(TouchEvery)
	r.Touch("a", 200) // the throttle has run out
	if s.types() != "conn.upsert,conn.upsert,conn.upsert" {
		t.Fatalf("events %s", s.types())
	}
	if got := r.Get("a"); got["hls_last_read"] != 200 || got["pid"] != 55 {
		t.Fatalf("record %v", got)
	}
	if rr, _ := r.Touch("missing", 1); rr != nil {
		t.Fatal("touched a missing connection")
	}
	r.Delete("a")
	if r.Get("a") != nil || !strings.HasSuffix(s.types(), "conn.remove") {
		t.Fatalf("after delete: %v %s", r.Get("a"), s.types())
	}
}

func TestRegistryFindOldestAndMainsCloses(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	r.Put("b", rec(7, "10.0.0.2", 200, 200))
	r.Put("a", rec(7, "10.0.0.1", 100, 100))
	r.Put("c", rec(8, "10.0.0.3", 50, 50))
	if got := r.Oldest(7); got["user_ip"] != "10.0.0.1" {
		t.Fatalf("oldest %v", got)
	}
	if got := r.Find(map[string]any{"user_id": 8, "container": "hls"}); got["uuid"] != "c" {
		t.Fatalf("find %v", got)
	}
	if r.Find(map[string]any{"user_id": 9}) != nil {
		t.Fatal("found a line that has none")
	}
	n := len(s.events)
	r.Close("a", false) // MAIN ended it (an HLS kick): no event back
	if got := r.Get("a"); got["hls_end"] != 1 || len(s.events) != n {
		t.Fatalf("ended %v, events %d→%d", got, n, len(s.events))
	}
	if got := r.Oldest(7); got["uuid"] != "b" {
		t.Fatalf("an ended connection is not the line's accepted IP: %v", got)
	}
	r.Close("b", true)
	if r.Get("b") != nil {
		t.Fatal("not removed")
	}
}

func TestRegistrySurvivesARestartAndAFullSpool(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	r.Put("a", rec(7, "10.0.0.1", 100, 100))
	if err := r.Save(); err != nil {
		t.Fatal(err)
	}
	back := NewRegistry(r.snap, s.emit, t.Logf)
	if got := back.Get("a"); got == nil || got["user_ip"] != "10.0.0.1" {
		t.Fatalf("after restart: %v", got)
	}
	s.fail = true
	if err := back.Put("z", rec(9, "x", 1, 1)); err == nil || back.Get("z") != nil {
		t.Fatal("a write the spool refused was kept")
	}
	if err := back.Delete("a"); err == nil || back.Get("a") == nil {
		t.Fatal("a delete the spool refused was applied")
	}
}

func TestRegistryOverTheSocket(t *testing.T) {
	r, _, _ := newTestRegistry(t)
	a := &Agent{Registry: r, Logf: t.Logf}
	h := a.socketHandler()
	do := func(method, path, body string) (int, map[string]any) {
		rw := httptest.NewRecorder()
		h.ServeHTTP(rw, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
		var out map[string]any
		json.Unmarshal(rw.Body.Bytes(), &out)
		return rw.Code, out
	}
	if code, out := do("PUT", "/v1/conn/u1", `{"user_id":7,"user_ip":"1.2.3.4","date_start":5,"hls_end":0,"hls_last_read":5}`); code != 200 || out["uuid"] != "u1" {
		t.Fatalf("put %d %v", code, out)
	}
	if code, out := do("GET", "/v1/conn/u1", ""); code != 200 || out["user_ip"] != "1.2.3.4" {
		t.Fatalf("get %d %v", code, out)
	}
	if code, out := do("POST", "/v1/conn/u1/touch", `{"hls_last_read":9}`); code != 200 || out["hls_last_read"] != float64(9) {
		t.Fatalf("touch %d %v", code, out)
	}
	if code, out := do("POST", "/v1/conn/oldest", `{"user_id":7}`); code != 200 || out["uuid"] != "u1" {
		t.Fatalf("oldest %d %v", code, out)
	}
	if code, _ := do("POST", "/v1/conn/find", `{"match":{"user_id":8}}`); code != http.StatusNotFound {
		t.Fatalf("find none: %d", code)
	}
	if code, _ := do("PUT", "/v1/conn/../etc", `{}`); code != http.StatusBadRequest {
		t.Fatalf("bad uuid: %d", code)
	}
	if code, _ := do("DELETE", "/v1/conn/u1", ""); code != http.StatusNoContent {
		t.Fatalf("delete %d", code)
	}
	if code, _ := do("GET", "/v1/conn/u1", ""); code != http.StatusNotFound {
		t.Fatalf("gone: %d", code)
	}
}

func TestConnCloseCommandAppliesMainsClose(t *testing.T) {
	r, _, _ := newTestRegistry(t)
	r.Put("u1", rec(7, "1.2.3.4", 1, 1))
	r.Put("u2", rec(7, "1.2.3.4", 1, 1))
	a := &Agent{Registry: r, Logf: t.Logf}
	run := a.localExec(func(_ context.Context, cmd *Command, _ WireCommand) (bool, []byte) { return false, []byte("php") })
	ctx := context.Background()
	if ok, _ := run(ctx, &Command{Type: "conn.close", Args: map[string]any{"uuid": "u1", "remove": false}}, WireCommand{}); !ok || r.Get("u1")["hls_end"] != 1 {
		t.Fatalf("end: %v", r.Get("u1"))
	}
	if ok, _ := run(ctx, &Command{Type: "conn.close", Args: map[string]any{"uuid": "u2", "remove": true}}, WireCommand{}); !ok || r.Get("u2") != nil {
		t.Fatal("remove")
	}
	if ok, res := run(ctx, &Command{Type: "conn.close", Args: map[string]any{"uuid": "a/b"}}, WireCommand{}); ok || string(res) != "refused: bad uuid" {
		t.Fatalf("bad uuid: %v %s", ok, res)
	}
}

// TestInteropConnections records a viewer the way the stream endpoints do on
// a CONNECTIONS node — the panel's real ConnectionTracker seam, through this
// agent's socket — and checks MAIN's lines_live follows through the P0 lane,
// then that a close MAIN decides reaches the registry as a command. Opt-in,
// as the other interop tests.
func TestInteropConnections(t *testing.T) {
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
	sockDir, _ := os.MkdirTemp("", "xc")
	defer os.RemoveAll(sockDir)
	spool := filepath.Join(dir, "spool")
	a := &Agent{Client: NewClient(st, "xc_agent/interop"), Version: "0.0.0-interop", Logf: t.Logf,
		FlowsFile: filepath.Join(dir, "flows.json"), SpoolDir: spool, SocketPath: filepath.Join(sockDir, "a.sock")}
	a.Registry = NewRegistry(filepath.Join(dir, "registry.snap"), func(ev []map[string]any) error { return spoolP0(spool, ev) }, t.Logf)
	runPHP("events.php", "flows", "74") // COMMANDS | STREAMS | CONNECTIONS
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	oldLanes, oldWait, oldIdle := Lanes, CommandsWait, CommandsIdle
	Lanes = []Lane{{Name: "p0", Interval: 50 * time.Millisecond}}
	CommandsWait, CommandsIdle = 500*time.Millisecond, 50*time.Millisecond
	defer func() { Lanes, CommandsWait, CommandsIdle = oldLanes, oldWait, oldIdle }()
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go a.ServeSocket(lctx, a.SocketPath)
	go a.RunEvents(lctx, Lanes[0])
	go a.RunCommands(lctx, a.localExec(func(context.Context, *Command, WireCommand) (bool, []byte) { return false, []byte("no php here") }))
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(a.SocketPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	var node map[string]any
	json.Unmarshal([]byte(runPHP("conn.php", "node", a.FlowsFile, spool, a.SocketPath)), &node)
	if node["open"] != true || node["found_ip"] != "10.0.0.9" || node["updated"] != true || node["beat"] != float64(1800000100) || node["accepted"] != "10.0.0.9" {
		t.Fatalf("node side: %v", node)
	}
	var rows []map[string]any
	for i := 0; i < 100; i++ {
		json.Unmarshal([]byte(runPHP("conn.php", "main")), &rows)
		if len(rows) == 1 && fmt.Sprint(rows[0]["pid"]) == "4242" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["server_id"]) != "7" || fmt.Sprint(rows[0]["pid"]) != "4242" || fmt.Sprint(rows[0]["hls_end"]) != "0" {
		t.Fatalf("MAIN's lines_live: %v", rows)
	}

	runPHP("conn.php", "close", "0") // MAIN ended it (an HLS kick)
	for i := 0; i < 100 && fmt.Sprint(a.Registry.Get("v1")["hls_end"]) != "1"; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if fmt.Sprint(a.Registry.Get("v1")["hls_end"]) != "1" {
		t.Fatalf("the registry did not follow MAIN's close: %v", a.Registry.Get("v1"))
	}
}
