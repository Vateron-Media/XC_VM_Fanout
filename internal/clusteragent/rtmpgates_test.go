package clusteragent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// writeBlocklist writes a blocklist.json in the on-disk {seq,etag,data} shape
// the agent's materialise() produces (replica.go), so the gate readers see what
// MAIN ships: data.ip ([]string) and data.rtmp ([]row).
func writeBlocklist(t *testing.T, dir string, data map[string]any) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"seq": 1, "etag": "e", "data": data})
	if err := os.WriteFile(filepath.Join(dir, "blocklist.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestAllowedRTMPReadsBlocklist(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir()}
	writeBlocklist(t, a.ReplicaDir, map[string]any{
		"ip":   []string{},
		"rtmp": []map[string]any{{"id": 1, "ip": "203.0.113.5", "password": "", "push": 1, "pull": 0}},
	})
	e, ok := a.allowedRTMP("203.0.113.5")
	if !ok || e.Push != 1 || e.Pull != 0 || e.Password != "" {
		t.Fatalf("entry = %+v ok=%v", e, ok)
	}
	if _, ok := a.allowedRTMP("198.51.100.1"); ok {
		t.Fatal("unlisted ip must not match")
	}
}

func TestAllowedRTMPMissingFileSafe(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir()} // no blocklist.json
	if _, ok := a.allowedRTMP("203.0.113.5"); ok {
		t.Fatal("missing blocklist.json must yield empty allowlist, not match")
	}
}

func TestSecretMatches(t *testing.T) {
	if secretMatches("", "anything") {
		t.Fatal("empty known must never match")
	}
	if !secretMatches("s3cret", "s3cret") || secretMatches("s3cret", "nope") {
		t.Fatal("non-empty known is a constant-time equal")
	}
}

// writeStreamRecord writes a per-stream record at streams/<id>.json in the
// on-disk {etag,ver,data} envelope checkStream produces, with data carried
// through verbatim (so data.server survives, as MAIN signs it).
func writeStreamRecord(t *testing.T, dir, id string, data map[string]any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "streams"), 0o750); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"etag": "e", "ver": 1, "data": data})
	if err := os.WriteFile(filepath.Join(dir, "streams", id+".json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestStreamOnDemand(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir()}
	writeStreamRecord(t, a.ReplicaDir, "200", map[string]any{"server": map[string]any{"on_demand": 1}})
	writeStreamRecord(t, a.ReplicaDir, "201", map[string]any{"server": map[string]any{"on_demand": 0}})
	writeStreamRecord(t, a.ReplicaDir, "202", map[string]any{"server": nil})
	if !a.streamOnDemand("200") {
		t.Fatal("on_demand=1 must be true")
	}
	for _, id := range []string{"201", "202", "999"} { // 999 = missing file
		if a.streamOnDemand(id) {
			t.Fatalf("%s must be not-on-demand", id)
		}
	}
}

func floodSettings(t *testing.T, dir string, kv map[string]any) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"data": kv})
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeServers(t *testing.T, dir string, servers []map[string]any) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"data": map[string]any{"servers": servers}})
	if err := os.WriteFile(filepath.Join(dir, "servers.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertP0BlockIP(t *testing.T, spoolDir, ip, reason string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(spoolDir, "p0"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("no p0 spool: %v", err)
	}
	for _, e := range entries {
		b, _ := os.ReadFile(filepath.Join(spoolDir, "p0", e.Name()))
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			var ev struct {
				Type string `json:"type"`
				D    struct {
					IP, Reason string
				} `json:"d"`
			}
			if json.Unmarshal([]byte(line), &ev) == nil && ev.Type == "security.block_ip" && ev.D.IP == ip && ev.D.Reason == reason {
				return
			}
		}
	}
	t.Fatalf("no security.block_ip{ip:%s, reason:%s} in p0", ip, reason)
}

func assertNoP0(t *testing.T, spoolDir string) {
	t.Helper()
	if entries, err := os.ReadDir(filepath.Join(spoolDir, "p0")); err == nil && len(entries) > 0 {
		t.Fatalf("expected no p0 spool, found %d", len(entries))
	}
}

func TestFloodTickBansAtLimit(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir(), FloodDir: t.TempDir(), SpoolDir: t.TempDir()}
	floodSettings(t, a.ReplicaDir, map[string]any{"flood_limit": 3, "flood_seconds": 100})
	ip := "203.0.113.9"
	for i := 0; i < 4; i++ { // first=0, then 1,2,3 -> blocks on the 4th (requests>=3)
		a.floodTick(ip)
	}
	if _, err := os.Stat(filepath.Join(a.FloodDir, "block_"+ip)); err != nil {
		t.Fatalf("expected block marker: %v", err)
	}
	assertP0BlockIP(t, a.SpoolDir, ip, "FLOOD ATTACK")
}

func TestFloodTickOffWhenLimitZero(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir(), FloodDir: t.TempDir(), SpoolDir: t.TempDir()}
	floodSettings(t, a.ReplicaDir, map[string]any{"flood_limit": 0})
	for i := 0; i < 50; i++ {
		a.floodTick("203.0.113.9")
	}
	if _, err := os.Stat(filepath.Join(a.FloodDir, "block_203.0.113.9")); err == nil {
		t.Fatal("flood_limit=0 must never ban")
	}
}

func TestFloodTickExclusions(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir(), FloodDir: t.TempDir(), SpoolDir: t.TempDir()}
	floodSettings(t, a.ReplicaDir, map[string]any{"flood_limit": 1, "flood_seconds": 100, "flood_ips_exclude": "5.5.5.5"})
	writeServers(t, a.ReplicaDir, []map[string]any{
		{"id": 3, "server_ip": "9.9.9.9", "private_ip": "", "whitelist_ips": `["7.7.7.7"]`, "domain_name": ""},
		{"id": 4, "server_ip": "8.8.8.8", "private_ip": "", "whitelist_ips": nil, "domain_name": ""}, // null whitelist must be skipped, not crash
	})
	// "not-an-ip" exercises the net.ParseIP guard (must return before writing a block_ marker)
	for _, ip := range []string{"127.0.0.1", "9.9.9.9", "7.7.7.7", "5.5.5.5", "not-an-ip"} {
		for i := 0; i < 5; i++ {
			a.floodTick(ip)
		}
		if _, err := os.Stat(filepath.Join(a.FloodDir, "block_"+ip)); err == nil {
			t.Fatalf("%s must be excluded from banning", ip)
		}
	}
}

func TestFloodTickAlreadyBlockedSkipsSpool(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir(), FloodDir: t.TempDir(), SpoolDir: t.TempDir()}
	floodSettings(t, a.ReplicaDir, map[string]any{"flood_limit": 1, "flood_seconds": 100})
	writeBlocklist(t, a.ReplicaDir, map[string]any{"ip": []string{"203.0.113.9"}})
	for i := 0; i < 3; i++ {
		a.floodTick("203.0.113.9")
	}
	if _, err := os.Stat(filepath.Join(a.FloodDir, "block_203.0.113.9")); err != nil {
		t.Fatal("marker still touched for an already-blocked IP")
	}
	assertNoP0(t, a.SpoolDir) // getBlockedIPs() guard: no P0 for an already-listed IP
}

func TestFloodTickWindowExpiryResets(t *testing.T) {
	// A counter outside the window resets to requests:0 and does NOT block, even from a high count.
	a := &Agent{ReplicaDir: t.TempDir(), FloodDir: t.TempDir(), SpoolDir: t.TempDir()}
	floodSettings(t, a.ReplicaDir, map[string]any{"flood_limit": 2, "flood_seconds": 100})
	ip := "203.0.113.9"
	b, _ := json.Marshal(map[string]any{"requests": 5, "last_request": time.Now().Unix() - 1000}) // expired
	if err := os.WriteFile(filepath.Join(a.FloodDir, ip), b, 0o600); err != nil {
		t.Fatal(err)
	}
	a.floodTick(ip)
	if _, err := os.Stat(filepath.Join(a.FloodDir, "block_"+ip)); err == nil {
		t.Fatal("an expired window must reset, not ban")
	}
	raw, _ := os.ReadFile(filepath.Join(a.FloodDir, ip))
	var row struct {
		Requests int64 `json:"requests"`
	}
	_ = json.Unmarshal(raw, &row)
	if row.Requests != 0 {
		t.Fatalf("expired window must reset requests to 0, got %d", row.Requests)
	}
}

func TestFloodTickConcurrent(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir(), FloodDir: t.TempDir(), SpoolDir: t.TempDir()}
	floodSettings(t, a.ReplicaDir, map[string]any{"flood_limit": 1000000, "flood_seconds": 100})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); a.floodTick("203.0.113.9") }()
	}
	wg.Wait() // must not panic or leave an unparseable file
	if b, err := os.ReadFile(filepath.Join(a.FloodDir, "203.0.113.9")); err == nil {
		var row struct{ Requests, LastRequest int64 }
		if json.Unmarshal(b, &row) != nil {
			t.Fatal("counter file must stay valid JSON under concurrency")
		}
	}
}
