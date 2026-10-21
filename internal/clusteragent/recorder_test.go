package clusteragent

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// recWithLoopback adds a 32-byte loopback key file to a liveRig so the lb1-
// admission arm is live, and returns the key.
func recWithLoopback(t *testing.T, rg *liveRig) []byte {
	t.Helper()
	key := bytes.Repeat([]byte{9}, 32)
	p := filepath.Join(rg.dir, "loopback.key")
	if err := os.WriteFile(p, key, 0o600); err != nil {
		t.Fatal(err)
	}
	rg.a.LoopbackKeyFile = p
	return key
}

// recSetArchivePath rewrites the rig's policy to point archive/vod/streams at
// fresh temp dirs (newLiveRig leaves paths empty), returning (archiveDir, vodDir).
func recSetArchivePath(t *testing.T, rg *liveRig) (string, string) {
	t.Helper()
	streams, vod, arch := t.TempDir(), t.TempDir(), t.TempDir()
	policy := fmt.Sprintf(`{"ip_subnet_match":false,"keys":{"viewer":[{"hex":%q,"until":null}],"shared":[],"context":[{"hex":%q,"until":null}],"accept_legacy_cbc":false},"paths":{"streams":%q,"vod":%q,"archive":%q},"default_timezone":"UTC"}`,
		hex.EncodeToString(rg.vkey), hex.EncodeToString(rg.vctx), streams+string(os.PathSeparator), vod+string(os.PathSeparator), arch+string(os.PathSeparator))
	if err := os.WriteFile(rg.a.GatewayPolicyFile, []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
	return arch, vod
}

func TestCaptureLiveWritesFanoutBody(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true, liveBody: []byte("LIVE-TS-DATA")}
	rg := newLiveRig(t, lf)
	recWithLoopback(t, rg)
	var buf bytes.Buffer
	now := time.Now().Unix()
	st := rg.a.captureRecording(context.Background(), recording{ID: 7, StreamID: 42, Start: now, End: now + 1}, &buf)
	if st != http.StatusOK || buf.String() != "LIVE-TS-DATA" {
		t.Fatalf("live capture: status=%d body=%q", st, buf.String())
	}
	if strings.Contains(lf.lastLiveQuery(), "c=") {
		t.Fatalf("recorder capture must not carry a viewer uuid: %q", lf.lastLiveQuery())
	}
}

func TestCaptureCatchupConcatenatesArchive(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true}
	rg := newLiveRig(t, lf)
	recWithLoopback(t, rg)
	arch, _ := recSetArchivePath(t, rg)
	// archive minute files for stream 42 over a 3-minute span starting at a round minute.
	start := (time.Now().Unix() / 60) * 60
	dir := filepath.Join(arch, "42")
	os.MkdirAll(dir, 0o755)
	for i, c := range []string{"AAAA", "BBBB", "CCCC"} {
		name := time.Unix(start+int64(i)*60, 0).UTC().Format("2006-01-02:15-04") + ".ts"
		os.WriteFile(filepath.Join(dir, name), []byte(c), 0o644)
	}
	var buf bytes.Buffer
	st := rg.a.captureRecording(context.Background(), recording{ID: 7, StreamID: 42, Start: start, End: start + 180, Archive: true}, &buf)
	if st != http.StatusOK || buf.String() != "AAAABBBBCCCC" {
		t.Fatalf("catch-up capture: status=%d body=%q, want AAAABBBBCCCC", st, buf.String())
	}
}

func TestCaptureBadKeyRefused(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true, liveBody: []byte("X")}
	rg := newLiveRig(t, lf)
	// no loopback key file set → the lb1- arm is off → 404, nothing captured.
	var buf bytes.Buffer
	now := time.Now().Unix()
	st := rg.a.captureRecording(context.Background(), recording{ID: 7, StreamID: 42, Start: now, End: now + 1}, &buf)
	if st != http.StatusNotFound || buf.Len() != 0 {
		t.Fatalf("no key: status=%d body=%q, want 404 + empty", st, buf.String())
	}
}

func TestIssueLoopbackTokenRoundTrips(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	now := time.Now().Unix()
	tok := issueLoopbackToken(key, 42, now+300)
	if !verifyLoopbackToken(key, 42, tok, now) {
		t.Fatalf("minted token %q not accepted by verifyLoopbackToken", tok)
	}
	if verifyLoopbackToken(key, 43, tok, now) {
		t.Fatal("token accepted for the wrong stream")
	}
	if verifyLoopbackToken(key, 42, issueLoopbackToken(key, 42, now-1), now) {
		t.Fatal("expired token accepted")
	}
	if issueLoopbackToken(bytes.Repeat([]byte{7}, 16), 42, now+300) != "" {
		t.Fatal("a non-32-byte key must mint an empty token")
	}
}

func TestRecordSink(t *testing.T) {
	var buf bytes.Buffer
	s := newRecordSink(&buf)
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("TSDATA"))
	}).ServeHTTP(s, httptest.NewRequest(http.MethodGet, "/x", nil))
	if s.status != http.StatusOK || buf.String() != "TSDATA" {
		t.Fatalf("200 body: status=%d body=%q", s.status, buf.String())
	}

	var buf2 bytes.Buffer
	s2 := newRecordSink(&buf2)
	http.NotFound(s2, httptest.NewRequest(http.MethodGet, "/x", nil))
	if s2.status != http.StatusNotFound || buf2.Len() != 0 {
		t.Fatalf("404 handler: status=%d body=%q, want 404 + discarded body", s2.status, buf2.String())
	}
}
