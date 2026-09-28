package clusteragent

import (
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// The start-up recheck (verifyBlocklist) and materialise take the stored
// blocklist with one set of checks (openBlocklist): whatever one refuses, so
// does the other.
func TestStoredBlocklistOneCheckSet(t *testing.T) {
	m, a := newReplicaMain(t)
	dir := a.ReplicaDir
	if err := os.MkdirAll(filepath.Join(dir, "blocklist.d"), 0o750); err != nil {
		t.Fatal(err)
	}
	etag := hex.EncodeToString(make([]byte, 32))
	other := hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	write := func(name, b64 string) {
		t.Helper()
		b, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o640); err != nil {
			t.Fatal(err)
		}
	}
	repWith := func(section, node string, set map[string]any) string {
		doc := map[string]any{
			"v": 1, "section": section, "node": node, "gen": 1, "etag": etag, "seq": 5, "data": map[string]any{"ip": []string{"203.0.113.1"}},
		}
		for k, v := range set {
			doc[k] = v
		}
		return m.record(t, m.panel, "rep", doc)
	}
	rep := func(section, node string) string { return repWith(section, node, nil) }
	both := func(held string) (error, error) {
		return a.verifyBlocklist(dir, held), a.materialise(dir, ReplicaState{BlocklistEtag: held})
	}

	write("blocklist.rep", rep("blocklist", m.uuid))
	write("blocklist.d/0000000000000000006.blk", m.delta(t, m.panel, 6)["delta"].(string))
	if v, mat := both(etag); v != nil || mat != nil {
		t.Fatalf("the stored blocklist refused: verify %v, materialise %v", v, mat)
	}

	cases := []struct {
		name  string
		setup func()
		held  string
	}{
		{"another ETag than the one held", func() {}, other},
		{"another section's record", func() { write("blocklist.rep", rep("settings", m.uuid)) }, etag},
		{"another node's record", func() { write("blocklist.rep", rep("blocklist", "11111111-1111-4111-8111-111111111111")) }, etag},
		{"a gen that is not an integer", func() {
			write("blocklist.rep", repWith("blocklist", m.uuid, map[string]any{"gen": "1"}))
		}, etag},
		{"data that is not an object", func() {
			write("blocklist.rep", repWith("blocklist", m.uuid, map[string]any{"data": []string{"203.0.113.1"}}))
		}, etag},
		{"an ip list that is not strings", func() {
			write("blocklist.rep", repWith("blocklist", m.uuid, map[string]any{"data": map[string]any{"ip": []int{1}}}))
		}, etag},
		{"a delta not above the section", func() {
			write("blocklist.d/0000000000000000004.blk", m.delta(t, m.panel, 4)["delta"].(string))
		}, etag},
	}
	for _, c := range cases {
		write("blocklist.rep", rep("blocklist", m.uuid))
		os.Remove(filepath.Join(dir, "blocklist.d", "0000000000000000004.blk"))
		c.setup()
		if v, mat := both(c.held); v == nil || mat == nil {
			t.Errorf("%s: verify %v, materialise %v; both must refuse", c.name, v, mat)
		}
	}
}
