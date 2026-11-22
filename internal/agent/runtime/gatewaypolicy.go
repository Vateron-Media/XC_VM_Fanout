package clusteragent

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// writeGatewayPolicy writes tmp/gateway/policy.json from the MAIN-sealed `policy`
// replica section (stored by storeWhole as {data, etag}). MAIN builds the policy
// per node — its viewer keys, mode, redirects, every scalar — and ships it only
// on change; the agent re-stamps written_at to the node's clock each tick so the
// gateway's 600 s freshness never lapses, and overlays serve_until, the one field
// MAIN cannot set on the node's host clock, from the node's own lease fence (now
// once the lease's drain is over — stop serving — else 0). Everything else is
// MAIN's verbatim. Returns an error (no file written) when no section is stored
// yet; a no-op when no gateway-policy path is configured (not a serve-only node).
func (a *Agent) writeGatewayPolicy(now int64) error {
	if a.GatewayPolicyFile == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(a.ReplicaDir, "policy.json"))
	if errors.Is(err, os.ErrNotExist) {
		// No policy section yet (MAIN has not served it, or this is a fresh
		// node): a quiet no-op, not an error the per-tick caller logs.
		return nil
	}
	if err != nil {
		return err
	}
	var wrap struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(b, &wrap); err != nil || wrap.Data == nil {
		return errors.New("clusteragent: policy section unreadable")
	}
	wrap.Data["written_at"] = now
	su := int64(0)
	if fenced, _ := a.leaseFenced(); fenced {
		su = now
	}
	wrap.Data["serve_until"] = su
	out, err := json.Marshal(wrap.Data)
	if err != nil {
		return err
	}
	// 0600: the policy carries the node's viewer-key material, matching PHP's
	// tmp/gateway/policy.json.
	return writeFileMode(a.GatewayPolicyFile, out, 0o600)
}
