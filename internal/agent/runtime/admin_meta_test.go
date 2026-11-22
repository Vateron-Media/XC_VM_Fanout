package clusteragent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStreamMetaParsesAdminFields(t *testing.T) {
	dir := t.TempDir()
	sd := streamsDir(dir)
	if err := os.MkdirAll(sd, 0o755); err != nil {
		t.Fatal(err)
	}
	js := `{"data":{"stream":{"target_container":"mkv","vframes_server_id":5,"direct_source":0},` +
		`"server":{"on_demand":1},"type":{"live":1}}}`
	if err := os.WriteFile(filepath.Join(sd, "42.json"), []byte(js), 0o644); err != nil {
		t.Fatal(err)
	}
	a := &Agent{ReplicaDir: dir, Logf: t.Logf}

	m, ok := a.streamMeta("42")
	if !ok {
		t.Fatal("streamMeta ok=false for a present record")
	}
	if m.TargetContainer != "mkv" || m.VframesServerID != 5 || m.DirectSource != 0 || m.TypeLive != 1 || m.OnDemand != 1 {
		t.Fatalf("streamMeta = %+v", m)
	}

	// A VOD-style record with no type block: live flag 0, container present.
	js2 := `{"data":{"stream":{"target_container":"mp4","vframes_server_id":0,"direct_source":1},"server":{"on_demand":0}}}`
	if err := os.WriteFile(filepath.Join(sd, "7.json"), []byte(js2), 0o644); err != nil {
		t.Fatal(err)
	}
	m2, ok := a.streamMeta("7")
	if !ok || m2.TargetContainer != "mp4" || m2.DirectSource != 1 || m2.TypeLive != 0 {
		t.Fatalf("streamMeta(7) ok=%v = %+v", ok, m2)
	}

	if _, ok := a.streamMeta("999"); ok {
		t.Fatal("a missing record must be ok=false")
	}
}
