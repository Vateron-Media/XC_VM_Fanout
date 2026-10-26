package clusteragent

import (
	"path/filepath"
	"testing"
)

// A serve-only node reports its own daemon status in local.fanout so MAIN's
// HeartbeatService writes a fresh servers.watchdog_data.fanout (ReleaseCanary
// reads running; capacity reads connections) instead of the stale value a dead
// PHP watchdog last left. Regression for XC_VM #478.
func TestFanoutStatusAndTelemetry(t *testing.T) {
	// A reachable daemon with two live viewers.
	sock := fakeConnections(t, "a", "b")
	a := &Agent{FanoutCtl: sock}
	st := a.FanoutStatus()
	if st["socket"] != true || st["running"] != true {
		t.Fatalf("reachable daemon: socket=%v running=%v, want true/true", st["socket"], st["running"])
	}
	if c, ok := st["connections"].(int); !ok || c != 2 {
		t.Fatalf("connections = %v (%T), want int 2", st["connections"], st["connections"])
	}
	if st["memory"] != nil {
		t.Fatalf("memory must be nil (PHP-only), got %v", st["memory"])
	}

	// An unreachable daemon (socket file absent).
	down := &Agent{FanoutCtl: filepath.Join(t.TempDir(), "none.sock")}
	d := down.FanoutStatus()
	if d["socket"] != false || d["running"] != false || d["connections"] != nil {
		t.Fatalf("down daemon: %v, want socket/running false, connections nil", d)
	}

	// The wrapper merges local.fanout and preserves any other local fields.
	base := func() map[string]any {
		return map[string]any{"cpu": 12.0, "local": map[string]any{"requests_per_second": 5}}
	}
	out := a.TelemetryWithFanout(base)()
	local, _ := out["local"].(map[string]any)
	if local == nil || local["requests_per_second"] != 5 {
		t.Fatalf("existing local fields must be preserved: %v", out["local"])
	}
	f, _ := local["fanout"].(map[string]any)
	if f == nil || f["running"] != true {
		t.Fatalf("local.fanout not populated: %v", local["fanout"])
	}

	// A sample with no local at all gets one created.
	out2 := a.TelemetryWithFanout(func() map[string]any { return map[string]any{} })()
	l2, _ := out2["local"].(map[string]any)
	if l2 == nil || l2["fanout"] == nil {
		t.Fatalf("local created with fanout: %v", out2["local"])
	}
}
