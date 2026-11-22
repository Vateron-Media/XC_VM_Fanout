package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatusBeforeMainAnswered(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Version: "1.2.3"}
	doc := a.Status(time.Now())
	if doc["v"] != StatusVersion || doc["version"] != "1.2.3" || doc["server_id"] != int64(3) {
		t.Fatalf("identity: %v", doc)
	}
	if doc["main_skew_ms"] != int64(0) || doc["main_time_seen_ms"] != int64(0) || doc["last_heartbeat_ms"] != int64(0) {
		t.Errorf("nothing observed yet, got skew %v seen %v beat %v", doc["main_skew_ms"], doc["main_time_seen_ms"], doc["last_heartbeat_ms"])
	}
	if doc["lease"] != nil || doc["fenced"] != false || doc["state"] != "" {
		t.Errorf("lease %v fenced %v state %v", doc["lease"], doc["fenced"], doc["state"])
	}
	tok, ok := doc["token"].(map[string]any)
	if !ok || tok["epoch"] != uint64(1) || tok["exp"].(int64) <= time.Now().Unix() {
		t.Fatalf("token: %v", doc["token"])
	}
	b, _ := json.Marshal(doc)
	if strings.Contains(string(b), `"token":"`) || strings.Contains(string(b), "seed") || strings.Contains(string(b), "eph_sk") {
		t.Fatalf("status leaks secret material: %s", b)
	}
	if lanes := doc["lanes"].([]LaneStatus); len(lanes) != 0 {
		t.Errorf("no spool, got lanes %v", lanes)
	}
	if doc["relay"] != nil {
		t.Errorf("no relay proxy, got relay %v", doc["relay"])
	}
}

func TestStatusReportsClockLeaseAndLanes(t *testing.T) {
	_, st := newFake(t)
	spool := t.TempDir()
	a := &Agent{Client: NewClient(st, "xc_agent/test"), SpoolDir: spool}
	a.Client.setMainTime(time.Now().UnixMilli() + 5000)
	a.lastBeatMs.Store(time.Now().UnixMilli())
	a.fenced.Store(true)
	a.state.Store("active")
	a.mode.Store(2)
	a.cursorP0.Store(42)
	st.Lease = &Lease{Gen: 1, Iat: 100, Exp: 200, ServerID: 3}
	st.LeaseRefused = "older than the lease this node already holds"
	a.RelayAddr, a.RelayKeyDir = RelayProxyAddr, t.TempDir()
	a.relayBind.down(time.UnixMilli(1_800_000_000_000), 4, errors.New("listen tcp 127.0.0.1:31290: bind: address already in use"))

	p0 := filepath.Join(spool, "p0")
	os.MkdirAll(p0, 0o755)
	os.WriteFile(filepath.Join(p0, "1-1-a.ndjson"), []byte("{}\n{}\n"), 0o644)
	os.WriteFile(filepath.Join(p0, "2-1-a.ndjson"), []byte("{}\n"), 0o644)
	os.WriteFile(filepath.Join(p0, ".partial.ndjson"), []byte("{}\n"), 0o644)
	old := time.Now().Add(-time.Minute)
	os.Chtimes(filepath.Join(p0, "1-1-a.ndjson"), old, old)
	os.WriteFile(filepath.Join(spool, "p0.inflight"), []byte(`{"first":42,"count":2,"files":["1-1-a.ndjson"]}`), 0o644)

	doc := a.Status(time.Now())
	if skew := doc["main_skew_ms"].(int64); skew < 4900 || skew > 5100 {
		t.Errorf("skew %d, want ~5000", skew)
	}
	if doc["fenced"] != true || doc["state"] != "active" || doc["mode"] != int64(2) || doc["last_heartbeat_ms"].(int64) == 0 {
		t.Errorf("fenced %v state %v mode %v beat %v", doc["fenced"], doc["state"], doc["mode"], doc["last_heartbeat_ms"])
	}
	l := doc["lease"].(map[string]any)
	if l["exp"] != int64(200) || l["gen"] != uint64(1) || doc["lease_refused"] == "" {
		t.Errorf("lease %v refused %q", l, doc["lease_refused"])
	}
	lanes := doc["lanes"].([]LaneStatus)
	if len(lanes) != len(Lanes) || lanes[0].Name != "p0" {
		t.Fatalf("lanes %v", lanes)
	}
	if p := lanes[0]; p.Files != 2 || p.Bytes != 9 || p.Inflight != 2 || p.Cursor != 41 || p.OldestMs != old.UnixMilli() {
		t.Errorf("p0 %+v", p)
	}
	if p := lanes[1]; p.Files != 0 || p.OldestMs != 0 || p.Cursor != -1 {
		t.Errorf("p1 %+v", p)
	}
	if r := doc["relay"].(map[string]any); r["bound"] != false || r["since_ms"] != int64(1_800_000_000_000) || r["failures"] != 4 || !strings.Contains(r["error"].(string), "address already in use") {
		t.Errorf("relay %v", r)
	}
}

func TestSocketServesStatus(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	sock := filepath.Join(t.TempDir(), "agent.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.ServeSocket(ctx, sock)
	var err error
	for i := 0; i < 50; i++ {
		var c net.Conn
		if c, err = net.Dial("unix", sock); err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	res, err := hc.Get("http://agent/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	var doc map[string]any
	if res.StatusCode != 200 || json.Unmarshal(b, &doc) != nil || doc["v"] != float64(StatusVersion) || doc["server_id"] != float64(3) {
		t.Fatalf("GET /v1/status: %d %s", res.StatusCode, b)
	}
	res, err = hc.Post("http://agent/v1/status", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /v1/status: %d", res.StatusCode)
	}
}
