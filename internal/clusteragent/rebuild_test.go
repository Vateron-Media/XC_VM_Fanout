package clusteragent

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeConnectionsDetail answers GET /connections?detail=1 with viewer objects.
func fakeConnectionsDetail(t *testing.T, live ...string) string {
	dir, _ := os.MkdirTemp("", "xf")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "c.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/connections" || r.URL.Query().Get("detail") != "1" {
			http.NotFound(w, r)
			return
		}
		var out []map[string]any
		for _, u := range live {
			out = append(out, map[string]any{"uuid": u, "stream_id": "100", "since_ms": 1800000000000, "refs": 1})
		}
		json.NewEncoder(w).Encode(out)
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return sock
}

func tsRec(pid int) map[string]any {
	c := rec(7, "10.0.0.1", 100, 100)
	c["container"], c["pid"] = "ts", pid
	return c
}

func TestRebuildAfterARestartEndsWhatLeftWhileTheAgentWasDown(t *testing.T) {
	for name, fanout := range map[string]func(*testing.T, ...string) string{"detail": fakeConnectionsDetail, "bare (older daemon)": fakeConnections} {
		t.Run(name, func(t *testing.T) {
			r, s, _ := newTestRegistry(t)
			r.Seed([]map[string]any{
				withUUID("left", tsRec(0)),  // the fanout no longer serves it
				withUUID("still", tsRec(0)), // still connected
				withUUID("hls", rec(7, "10.0.0.2", 100, 100)),
				withUUID("vod-gone", tsRec(4_000_001)),
				withUUID("vod-live", tsRec(4_000_002)),
			}, true)
			a := &Agent{Logf: t.Logf, Registry: r, FanoutCtl: fanout(t, "still", "newcomer")}
			a.flows.Store(FlowConnections)
			old := ProcDir
			ProcDir = t.TempDir()
			defer func() { ProcDir = old }()
			os.MkdirAll(filepath.Join(ProcDir, "1"), 0o755)
			os.MkdirAll(filepath.Join(ProcDir, "4000002"), 0o755)
			os.WriteFile(filepath.Join(ProcDir, "4000002", "comm"), []byte("php-fpm\n"), 0o644)
			a.RebuildRegistry(context.Background())
			for _, u := range []string{"left", "vod-gone"} {
				if r.Get(u) != nil {
					t.Errorf("%s was not ended", u)
				}
			}
			for _, u := range []string{"still", "hls", "vod-live"} {
				if r.Get(u) == nil {
					t.Errorf("%s was ended", u)
				}
			}
			closed := map[string]bool{}
			for _, e := range s.events {
				if e["type"] != "conn.close" {
					t.Fatalf("event %v", e)
				}
				closed[e["d"].(map[string]any)["uuid"].(string)] = true
			}
			if len(closed) != 2 || !closed["left"] || !closed["vod-gone"] {
				t.Fatalf("closes sent for %v", closed)
			}
		})
	}
}

func TestRebuildLeavesWorkersAloneWithoutAReadableProc(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	r.Seed([]map[string]any{withUUID("vod", tsRec(4_000_001))}, true)
	old := ProcDir
	ProcDir = t.TempDir() // no /proc/1: hidepid, or no procfs
	defer func() { ProcDir = old }()
	a := &Agent{Logf: t.Logf, Registry: r}
	a.flows.Store(FlowConnections)
	a.RebuildRegistry(context.Background())
	if r.Get("vod") == nil || len(s.events) != 0 {
		t.Fatal("ended a viewer whose worker it could not see")
	}
	// A pid reused by something that is not PHP is gone.
	os.MkdirAll(filepath.Join(ProcDir, "1"), 0o755)
	os.MkdirAll(filepath.Join(ProcDir, "4000001"), 0o755)
	os.WriteFile(filepath.Join(ProcDir, "4000001", "comm"), []byte("bash\n"), 0o644)
	if on, ok := workerAlive(4000001); on || !ok {
		t.Fatalf("a reused pid: alive %v, known %v", on, ok)
	}
	if on, ok := workerAlive(int64(os.Getpid())); !ok || on {
		t.Fatalf("pid %d is not in the fake /proc: alive %v known %v", os.Getpid(), on, ok)
	}
}

func TestRebuildWaitsForAFanoutThatIsNotUpYet(t *testing.T) {
	r, s, _ := newTestRegistry(t)
	r.Seed([]map[string]any{withUUID("left", tsRec(0))}, true)
	dir, _ := os.MkdirTemp("", "xf")
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "late.sock")
	oldTries, oldRetry := RebuildTries, RebuildRetry
	RebuildTries, RebuildRetry = 50, 20*time.Millisecond
	defer func() { RebuildTries, RebuildRetry = oldTries, oldRetry }()
	a := &Agent{Logf: t.Logf, Registry: r, FanoutCtl: sock}
	a.flows.Store(FlowConnections)
	go func() {
		time.Sleep(100 * time.Millisecond)
		l, _ := net.Listen("unix", sock)
		http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`[]`)) }))
	}()
	a.RebuildRegistry(context.Background())
	if r.Get("left") != nil || len(s.events) != 1 {
		t.Fatal("the rebuild gave up before the fanout came up")
	}
}

func withUUID(uuid string, c map[string]any) map[string]any {
	c["uuid"] = uuid
	return c
}
