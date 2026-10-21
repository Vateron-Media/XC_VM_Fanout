package clusteragent

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// spooledRecStates reads the recording.state statuses the recorder spooled, in
// chronological order (p0 files are named by a monotonic clock).
func spooledRecStates(t *testing.T, spoolDir string) []int {
	t.Helper()
	ents, _ := os.ReadDir(filepath.Join(spoolDir, "p0"))
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name() < ents[j].Name() })
	var out []int
	for _, e := range ents {
		b, _ := os.ReadFile(filepath.Join(spoolDir, "p0", e.Name()))
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			var ev struct {
				Type string `json:"type"`
				D    struct {
					Status int `json:"status"`
				} `json:"d"`
			}
			if json.Unmarshal([]byte(line), &ev) == nil && ev.Type == "recording.state" {
				out = append(out, ev.D.Status)
			}
		}
	}
	return out
}

// recFakeExternals swaps the MAIN recording_complete call and the ffmpeg runner
// for fakes, restoring them when the test ends.
func recFakeExternals(t *testing.T, vodID int64, vodErr error, run func(ctx context.Context, name string, args ...string) error) {
	t.Helper()
	oldC, oldR := recorderComplete, recorderRun
	t.Cleanup(func() { recorderComplete, recorderRun = oldC, oldR })
	recorderComplete = func(a *Agent, ctx context.Context, recID int64, icon string) (int64, error) { return vodID, vodErr }
	recorderRun = run
}

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

func TestFinalizeCreatesVod(t *testing.T) {
	rg := newLiveRig(t, &liveFanout{id: "42"})
	_, vod := recSetArchivePath(t, rg)
	var gotArgs []string
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error {
		gotArgs = append([]string{name}, args...)
		return os.WriteFile(args[len(args)-1], []byte("MP4"), 0o644) // fake ffmpeg writes the output
	})
	ts := filepath.Join(t.TempDir(), "rec.ts")
	os.WriteFile(ts, []byte("TSDATA"), 0o644)
	vodID, err := rg.a.finalizeRecording(context.Background(), recording{ID: 3, StreamID: 42}, ts)
	if err != nil || vodID != 7 {
		t.Fatalf("finalize: vodID=%d err=%v", vodID, err)
	}
	if _, err := os.Stat(filepath.Join(vod, "7.mp4")); err != nil {
		t.Fatalf("mp4 not at vod/7.mp4: %v", err)
	}
	joined := strings.Join(gotArgs, " ")
	if !strings.Contains(joined, "-c:v copy") || !strings.Contains(joined, "-c:a copy") {
		t.Fatalf("ffmpeg args missing copy codecs: %v", gotArgs)
	}
}

func TestFinalizeVodZeroFails(t *testing.T) {
	rg := newLiveRig(t, &liveFanout{id: "42"})
	recSetArchivePath(t, rg)
	ran := false
	recFakeExternals(t, 0, nil, func(ctx context.Context, name string, args ...string) error { ran = true; return nil })
	ts := filepath.Join(t.TempDir(), "rec.ts")
	os.WriteFile(ts, []byte("X"), 0o644)
	if _, err := rg.a.finalizeRecording(context.Background(), recording{ID: 3, StreamID: 42}, ts); err == nil {
		t.Fatal("vod id 0 should fail")
	}
	if ran {
		t.Fatal("ffmpeg must not run when MAIN made no VOD")
	}
}

func TestFinalizeFfmpegFails(t *testing.T) {
	rg := newLiveRig(t, &liveFanout{id: "42"})
	recSetArchivePath(t, rg)
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error { return errors.New("ffmpeg boom") })
	ts := filepath.Join(t.TempDir(), "rec.ts")
	os.WriteFile(ts, []byte("X"), 0o644)
	if _, err := rg.a.finalizeRecording(context.Background(), recording{ID: 3, StreamID: 42}, ts); err == nil {
		t.Fatal("ffmpeg failure should propagate")
	}
}

func TestRunRecordingLiveSuccess(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true, liveBody: []byte("LIVE-TS")}
	rg := newLiveRig(t, lf)
	recWithLoopback(t, rg)
	_, vod := recSetArchivePath(t, rg)
	rg.a.SpoolDir = t.TempDir()
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error {
		return os.WriteFile(args[len(args)-1], []byte("MP4"), 0o644)
	})
	now := time.Now().Unix()
	rg.a.runRecording(context.Background(), recording{ID: 3, StreamID: 42, Start: now - 10, End: now + 1})
	if got := spooledRecStates(t, rg.a.SpoolDir); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("state events = %v, want [1 2]", got)
	}
	if _, err := os.Stat(filepath.Join(vod, "7.mp4")); err != nil {
		t.Fatalf("mp4 not created: %v", err)
	}
}

// Parity: the mp4 is built from exactly the bytes the viewer /admin/live would
// serve — ffmpeg's input .ts equals the fanout live body.
func TestRunRecordingParityFfmpegInput(t *testing.T) {
	body := []byte("PARITY-LIVE-BYTES")
	rg := newLiveRig(t, &liveFanout{id: "42", running: true, hasData: true, liveBody: body})
	recWithLoopback(t, rg)
	recSetArchivePath(t, rg)
	rg.a.SpoolDir = t.TempDir()
	var ffIn []byte
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error {
		ffIn, _ = os.ReadFile(args[1]) // args: -i <ts> -c:v copy -c:a copy <out>
		return os.WriteFile(args[len(args)-1], []byte("MP4"), 0o644)
	})
	now := time.Now().Unix()
	rg.a.runRecording(context.Background(), recording{ID: 3, StreamID: 42, Start: now - 10, End: now + 1})
	if !bytes.Equal(ffIn, body) {
		t.Fatalf("ffmpeg input = %q, want the fanout body %q", ffIn, body)
	}
}

func TestRunRecordingNotAiring(t *testing.T) {
	rg := newLiveRig(t, &liveFanout{id: "42"})
	recWithLoopback(t, rg)
	recSetArchivePath(t, rg)
	rg.a.SpoolDir = t.TempDir()
	ran := false
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error { ran = true; return nil })
	now := time.Now().Unix()
	rg.a.runRecording(context.Background(), recording{ID: 3, StreamID: 42, Start: now - 3600, End: now - 1800}) // window passed, not archive
	if got := spooledRecStates(t, rg.a.SpoolDir); !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("state events = %v, want [3]", got)
	}
	if ran {
		t.Fatal("ffmpeg must not run for a non-airing recording")
	}
}

func TestRunRecordingEmptyCapture(t *testing.T) {
	rg := newLiveRig(t, &liveFanout{id: "42", running: true, hasData: true}) // no liveBody → empty capture
	recWithLoopback(t, rg)
	recSetArchivePath(t, rg)
	rg.a.SpoolDir = t.TempDir()
	ran := false
	recFakeExternals(t, 7, nil, func(ctx context.Context, name string, args ...string) error { ran = true; return nil })
	now := time.Now().Unix()
	rg.a.runRecording(context.Background(), recording{ID: 3, StreamID: 42, Start: now - 10, End: now + 1})
	if got := spooledRecStates(t, rg.a.SpoolDir); !reflect.DeepEqual(got, []int{1, 3}) {
		t.Fatalf("state events = %v, want [1 3]", got)
	}
	if ran {
		t.Fatal("ffmpeg must not run for an empty capture")
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
