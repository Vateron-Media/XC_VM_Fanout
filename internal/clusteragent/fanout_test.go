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
