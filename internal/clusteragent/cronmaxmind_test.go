package clusteragent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func makeTarGz(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

func writeSecret(t *testing.T, dir, key, val string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"data": map[string]any{key: val}})
	if err := os.WriteFile(filepath.Join(dir, "secrets.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func maxmindRig(t *testing.T) (*Agent, *int) {
	t.Helper()
	a := newDrainAgent(t)
	dir := t.TempDir()
	origDir, origURL := maxmindDirPath, maxmindBaseURL
	maxmindDirPath = dir
	t.Cleanup(func() { maxmindDirPath, maxmindBaseURL = origDir, origURL })
	hits := new(int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*hits++
		if r.Header.Get("If-Modified-Since") != "" && r.URL.Query().Get("notmod") == "1" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Write(makeTarGz(t, "GeoLite2-Country_20260101/GeoLite2-Country.mmdb", []byte("MMDBDATA")))
	}))
	t.Cleanup(srv.Close)
	maxmindBaseURL = srv.URL
	return a, hits
}

func TestMaxmindDownloadsWhenMissing(t *testing.T) {
	a, hits := maxmindRig(t)
	floodSettings(t, a.ReplicaDir, map[string]any{"maxmind_account_id": "42", "maxmind_editions": `["GeoLite2-Country"]`})
	writeSecret(t, a.ReplicaDir, "maxmind_license_key", "lic")
	if err := a.updateMaxmind(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(maxmindDirPath, "GeoLite2-Country.mmdb"))
	if err != nil || string(b) != "MMDBDATA" {
		t.Fatalf("mmdb not extracted: %v %q", err, b)
	}
	if *hits != 1 {
		t.Fatalf("want 1 download, got %d", *hits)
	}
}

func TestMaxmindSkipsWithoutCreds(t *testing.T) {
	a, hits := maxmindRig(t)
	floodSettings(t, a.ReplicaDir, map[string]any{}) // no creds
	if err := a.updateMaxmind(); err != nil {
		t.Fatal(err)
	}
	if *hits != 0 {
		t.Fatal("no creds must make no request")
	}
}

func TestMaxmindThrottlesWhenFresh(t *testing.T) {
	a, hits := maxmindRig(t)
	floodSettings(t, a.ReplicaDir, map[string]any{"maxmind_account_id": "42", "maxmind_editions": `["GeoLite2-Country"]`})
	writeSecret(t, a.ReplicaDir, "maxmind_license_key", "lic")
	// a fresh primary already present -> not due, no request
	if err := os.WriteFile(filepath.Join(maxmindDirPath, "GeoLite2-Country.mmdb"), []byte("OLD"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := a.updateMaxmind(); err != nil {
		t.Fatal(err)
	}
	if *hits != 0 {
		t.Fatalf("a fresh primary must not be re-downloaded, got %d requests", *hits)
	}
}

func TestOwnedCronsMaxmindConditional(t *testing.T) {
	a := newDrainAgent(t)
	floodSettings(t, a.ReplicaDir, map[string]any{})
	has := func(list []string, s string) bool {
		for _, x := range list {
			if x == s {
				return true
			}
		}
		return false
	}
	if has(a.ownedCrons(), "maxmind") {
		t.Fatal("no creds: maxmind must not be owned")
	}
	floodSettings(t, a.ReplicaDir, map[string]any{"maxmind_account_id": "42", "maxmind_editions": `["GeoLite2-Country"]`})
	writeSecret(t, a.ReplicaDir, "maxmind_license_key", "lic")
	if !has(a.ownedCrons(), "maxmind") {
		t.Fatal("with creds: maxmind must be owned")
	}
	for _, c := range []string{"streams_logs", "lines_logs", "activity", "errors", "tmp"} {
		if !has(a.ownedCrons(), c) {
			t.Fatalf("%s must always be owned", c)
		}
	}
}
