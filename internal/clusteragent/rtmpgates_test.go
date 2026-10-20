package clusteragent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
