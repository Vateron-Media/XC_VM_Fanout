package clusteragent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/gateway"
)

// On a serve-only node the agent is the sole writer of tmp/gateway/policy.json:
// it unwraps the MAIN-sealed `policy` section ({data, etag}) and re-stamps
// written_at to the node's clock so the gateway's 600 s freshness never lapses
// (MAIN ships the section only on change). The gateway must be able to parse
// what the agent writes.
func TestWriteGatewayPolicyReStampsAndUnwraps(t *testing.T) {
	dir := t.TempDir()
	gw := filepath.Join(t.TempDir(), "policy.json")
	// The stored whole section as storeWhole writes it: {data: <policy>, etag}.
	// written_at 0 = stale; the agent must re-stamp it to now.
	section := `{"data":{"v":1,"server_id":2,"written_at":0,"mode":"segments+playlist",` +
		`"keys":{"viewer":[{"hex":"abcd","until":null}],"shared":[],"context":[]},"serve_until":0},"etag":"x"}`
	if err := os.WriteFile(filepath.Join(dir, "policy.json"), []byte(section), 0o640); err != nil {
		t.Fatal(err)
	}
	a := &Agent{ReplicaDir: dir, GatewayPolicyFile: gw}
	now := int64(1_000_000)
	if err := a.writeGatewayPolicy(now); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(gw)
	if err != nil {
		t.Fatal(err)
	}
	p, err := gateway.ParsePolicy(b)
	if err != nil {
		t.Fatalf("gateway cannot parse the written policy: %v", err)
	}
	if p.ServerID != 2 || p.Mode != "segments+playlist" {
		t.Fatalf("policy fields lost in the unwrap: %+v", p)
	}
	if p.WrittenAt != now {
		t.Fatalf("written_at = %d, want %d (re-stamped to the node clock)", p.WrittenAt, now)
	}
	if !p.Fresh(now) {
		t.Fatal("written policy is not Fresh at now")
	}
	if fi, _ := os.Stat(gw); fi == nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("policy file mode = %v, want 0600 (carries viewer-key material)", fi)
	}
}

// No section stored yet (MAIN has not served it) is a quiet no-op — no error
// the per-tick caller would log, and no policy file written.
func TestWriteGatewayPolicyNoSection(t *testing.T) {
	a := &Agent{ReplicaDir: t.TempDir(), GatewayPolicyFile: filepath.Join(t.TempDir(), "policy.json")}
	if err := a.writeGatewayPolicy(1); err != nil {
		t.Fatalf("a missing section must be a quiet no-op, got %v", err)
	}
	if _, err := os.Stat(a.GatewayPolicyFile); !os.IsNotExist(err) {
		t.Fatal("no policy file should be written when there is no section")
	}
}
