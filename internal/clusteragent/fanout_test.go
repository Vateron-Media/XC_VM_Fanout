package clusteragent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A fanout control socket answering /events: a reset snapshot first, then
// one transition, then nothing.
func fakeFanout(t *testing.T, stream string) (string, *atomic.Int32) {
	dir, _ := os.MkdirTemp("", "xf")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		q := r.URL.Query()
		switch {
		case q.Get("boot") != "b1":
			w.Write([]byte(`{"boot":"b1","seq":4,"reset":true,"events":[{"seq":4,"type":"monitor","stream":"` + stream + `","state":{"supervised":true,"running":true,"confirmed":false,"pid":90,"daemon_pid":900,"source":"http://alice:pw@origin/live/alice/pw/12.ts","last_error":"http://alice:pw@x failed"}}]}`))
		case q.Get("since") == "4":
			w.Write([]byte(`{"boot":"b1","seq":5,"events":[{"seq":5,"type":"monitor","stream":"` + stream + `","state":{"supervised":true,"running":true,"confirmed":true,"pid":90,"daemon_pid":900,"uptime_ms":3000}},{"seq":5,"type":"connection","stream":"x"}]}`))
		default:
			time.Sleep(50 * time.Millisecond)
			w.Write([]byte(`{"boot":"b1","seq":5,"events":[]}`))
		}
		_ = n
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return sock, &calls
}

func TestAgentSpoolsTheFanoutsTransitionsWhileStreamsIsOn(t *testing.T) {
	sock, calls := fakeFanout(t, "12")
	dir := t.TempDir()
	a := &Agent{Logf: t.Logf, SpoolDir: filepath.Join(dir, "spool"), FanoutCtl: sock, FlowsFile: filepath.Join(dir, "flows.json")}
	oldIdle := FanoutIdle
	FanoutIdle = 20 * time.Millisecond
	defer func() { FanoutIdle = oldIdle }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.RunFanoutEvents(ctx)

	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("followed the feed with STREAMS off")
	}
	a.flows.Store(FlowStreams)
	var evs []json.RawMessage
	for i := 0; i < 100 && len(evs) < 2; i++ {
		time.Sleep(20 * time.Millisecond)
		files, _ := laneFor(a, "p0").spooled()
		evs = nil
		for _, f := range files {
			e, _, _ := readSpoolFile(filepath.Join(a.SpoolDir, "p0", f.Name()))
			evs = append(evs, e...)
		}
	}
	if len(evs) != 2 {
		t.Fatalf("spooled %d events", len(evs))
	}
	all := string(evs[0]) + string(evs[1])
	if strings.Contains(all, "pw") || strings.Contains(all, "last_error") || !strings.Contains(all, `"type":"stream.monitor"`) || !strings.Contains(all, `"stream_id":12`) {
		t.Fatalf("events %s", all)
	}
	if !a.fanoutLive.Load() {
		t.Fatal("not marked followed")
	}
	a.publish(&Reply{State: "active", Mode: 1, Flows: FlowStreams})
	if b, _ := os.ReadFile(a.FlowsFile); !strings.Contains(string(b), `"features":["fanout_events"]`) {
		t.Fatalf("flows.json %s", b)
	}
}

// TestInteropFanoutEvents follows a (fake) fanout's monitor feed into MAIN's
// real PHP: the snapshot and the confirming transition reach the node's
// streams_servers row through the P0 lane, derived as the reconcile would.
func TestInteropFanoutEvents(t *testing.T) {
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
	sock, _ := fakeFanout(t, "100")
	a := &Agent{Client: NewClient(st, "xc_agent/interop"), Version: "0.0.0-interop", Logf: t.Logf,
		FlowsFile: filepath.Join(dir, "flows.json"), SpoolDir: filepath.Join(dir, "spool"), FanoutCtl: sock}
	runPHP("events.php", "flows", "8") // STREAMS
	if _, err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	oldLanes := Lanes
	Lanes = []Lane{{Name: "p0", Interval: 50 * time.Millisecond}}
	defer func() { Lanes = oldLanes }()
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	go a.RunFanoutEvents(lctx)
	go a.RunEvents(lctx, Lanes[0])
	var got struct {
		Pid    int    `json:"pid"`
		Status int    `json:"status"`
		Source string `json:"source"`
		P0     int    `json:"p0"`
	}
	for i := 0; i < 100; i++ {
		json.Unmarshal([]byte(runPHP("events.php", "state")), &got)
		if got.P0 >= 2 && got.Status == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got.Pid != 90 || got.Status != 0 || strings.Contains(got.Source, "pw") || got.Source == "" {
		t.Fatalf("MAIN holds %+v", got)
	}
}

func TestAgentDropsDaemonViewersItself(t *testing.T) {
	dir, _ := os.MkdirTemp("", "xd")
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var dropped []string
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uuid := strings.TrimPrefix(r.URL.Path, "/connections/")
		if r.Method != http.MethodDelete {
			http.Error(w, "no", 405)
			return
		}
		dropped = append(dropped, uuid)
		if uuid == "live1" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	})}
	go srv.Serve(l)
	defer srv.Close()

	var fellBack []string
	next := func(_ context.Context, cmd *Command, _ WireCommand) (bool, []byte) {
		fellBack = append(fellBack, cmd.Type)
		return true, []byte("php")
	}
	a := &Agent{Logf: t.Logf, FanoutCtl: sock}
	run := a.localExec(next)
	ctx := context.Background()
	cases := []struct {
		cmd    *Command
		ok     bool
		result string
	}{
		{&Command{Type: "conn.drop", Args: map[string]any{"uuid": "live1"}}, true, `{"result":true}`},
		{&Command{Type: "conn.drop", Args: map[string]any{"uuid": "gone"}}, true, `{"result":false}`},
		{&Command{Type: "conn.drop", Args: map[string]any{"uuid": "../../x"}}, false, "refused: bad uuid"},
		{&Command{Type: "node.rpc", Args: map[string]any{"action": "get_pids"}}, true, "php"},
	}
	for _, c := range cases {
		ok, res := run(ctx, c.cmd, WireCommand{})
		if ok != c.ok || string(res) != c.result {
			t.Errorf("%s %v: %v %s", c.cmd.Type, c.cmd.Args, ok, res)
		}
	}
	if strings.Join(dropped, ",") != "live1,gone" || strings.Join(fellBack, ",") != "node.rpc" {
		t.Fatalf("dropped %v, fell back %v", dropped, fellBack)
	}

	// No fanout to reach: the node's PHP gets it.
	a.FanoutCtl = filepath.Join(dir, "missing.sock")
	if ok, res := a.localExec(next)(ctx, &Command{Type: "conn.drop", Args: map[string]any{"uuid": "live1"}}, WireCommand{}); !ok || string(res) != "php" {
		t.Fatalf("fallback: %v %s", ok, res)
	}
}

// fakeConnections answers GET /connections with the uuids still connected.
func fakeConnections(t *testing.T, live ...string) string {
	dir, _ := os.MkdirTemp("", "xf")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(live)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/connections" {
			http.NotFound(w, r)
			return
		}
		w.Write(b)
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return sock
}

func TestAgentEndsDaemonViewersTheFanoutReportsGone(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	ts := func(pid int) map[string]any {
		c := rec(7, "10.0.0.1", 100, 100)
		c["container"], c["pid"] = "ts", pid
		return c
	}
	r.Put("gone", ts(0))
	r.Put("back", ts(0)) // reconnected with the same uuid since
	r.Put("hls", rec(7, "10.0.0.2", 100, 100))
	r.Put("php", ts(55)) // a PHP worker serves it
	s.events = nil
	a := &Agent{Logf: t.Logf, Registry: r, FanoutCtl: fakeConnections(t, "back")}
	out := &fanoutReply{Events: []fanoutEvent{
		{Type: "conn_close", Stream: "1", UUID: "gone"},
		{Type: "conn_close", Stream: "1", UUID: "back"},
		{Type: "conn_close", Stream: "1", UUID: "hls"},
		{Type: "conn_close", Stream: "1", UUID: "php"},
		{Type: "conn_close", Stream: "1", UUID: "nobody"},
		{Type: "conn_close", Stream: "1", UUID: "bad uuid!"},
	}}

	if err := a.spoolFanout(out); err != nil || len(s.events) != 0 {
		t.Fatalf("CONNECTIONS off: err %v, events %s", err, s.types())
	}
	a.flows.Store(FlowStreams | FlowCommands | FlowConnections)
	if err := a.spoolFanout(out); err != nil {
		t.Fatal(err)
	}
	if s.types() != "conn.close" || s.events[0]["d"].(map[string]any)["uuid"] != "gone" {
		t.Fatalf("events %s %v", s.types(), s.events)
	}
	if r.Get("gone") != nil || r.Get("back") == nil || r.Get("hls") == nil || r.Get("php") == nil {
		t.Fatal("wrong records ended")
	}

	// A full spool: nothing ends, and the same feed comes again.
	r.Put("gone", ts(0))
	s.events, s.fail = nil, true
	if err := a.spoolFanout(out); err == nil || r.Get("gone") == nil {
		t.Fatalf("err %v", err)
	}
	// A fanout that does not answer: nothing ends here (fanout_sync still reconciles).
	s.fail = false
	a.FanoutCtl = filepath.Join(t.TempDir(), "none.sock")
	if err := a.spoolFanout(out); err != nil || r.Get("gone") == nil || len(s.events) != 0 {
		t.Fatalf("err %v, events %s", err, s.types())
	}
}
