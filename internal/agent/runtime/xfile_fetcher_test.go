package clusteragent

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func writeServersJSON(t *testing.T, dir string, sid, gen int64, state string, edPub []byte) {
	t.Helper()
	doc := `{"data":{"servers":[],"nodes":[{"sid":` + strconv.FormatInt(sid, 10) +
		`,"gen":` + strconv.FormatInt(gen, 10) + `,"state":"` + state + `","ed_pub":"` +
		base64.StdEncoding.EncodeToString(edPub) + `","box_pub":""}]}}`
	if err := os.WriteFile(filepath.Join(dir, "servers.json"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The owner of /xfile must accept a fetcher's proof only when that fetcher is
// active at exactly the generation the ticket names (the panel's
// RelayGuard::child): a re-enrolled node at a new gen, an inactive node, or an
// unknown sid is refused.
func TestFetcherSignPubGenMatch(t *testing.T) {
	_, st := newFake(t) // ServerID = 3
	dir := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: dir}
	pub := bytes.Repeat([]byte{2}, 32)

	writeServersJSON(t, dir, 5, 3, "active", pub)
	if got, ok := a.fetcherSignPub(5, 3); !ok || !bytes.Equal(got, pub) {
		t.Fatal("active + gen match must resolve to the node pub")
	}
	if _, ok := a.fetcherSignPub(5, 4); ok {
		t.Fatal("gen mismatch must refuse")
	}
	if _, ok := a.fetcherSignPub(6, 3); ok {
		t.Fatal("unknown sid must refuse")
	}

	// A node re-enrolled or disabled: once the file changes and the ~5s cache
	// expires, the owner refuses it.
	writeServersJSON(t, dir, 5, 3, "disabled", pub)
	a.xfileRouteAt = time.Time{} // expire the cache so the rewrite is read
	if _, ok := a.fetcherSignPub(5, 3); ok {
		t.Fatal("inactive node must refuse")
	}
}

// A missing servers.json is a refusal, not a panic.
func TestFetcherSignPubNoServersFile(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: t.TempDir()}
	if _, ok := a.fetcherSignPub(5, 3); ok {
		t.Fatal("no servers.json must refuse")
	}
}
