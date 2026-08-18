package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// ctlRequest issues one control-surface request and returns the status code.
func ctlRequest(t *testing.T, base, method, path, body string) int {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, base+path, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestControlRegisterAndUnregister(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	// Register a source. With no viewer attached (refs==0) the puller must NOT
	// start — registration only stores config.
	if code := ctlRequest(t, ts.URL, http.MethodPut, "/streams/7", `{"urls":["http://src/1.ts"],"ua":"VLC"}`); code != http.StatusNoContent {
		t.Fatalf("PUT register: status %d, want 204", code)
	}
	st := mgr.Get("7")
	if st == nil {
		t.Fatal("stream 7 must exist after register")
	}
	st.mu.Lock()
	cfgSet, running := st.cfg != nil, st.running
	st.mu.Unlock()
	if !cfgSet {
		t.Fatal("register must store the pull config")
	}
	if running {
		t.Fatal("puller must not start without a viewer")
	}

	// Unregister removes the stream.
	if code := ctlRequest(t, ts.URL, http.MethodDelete, "/streams/7", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE unregister: status %d, want 204", code)
	}
	if mgr.Get("7") != nil {
		t.Fatal("stream 7 must be gone after unregister")
	}
}

func TestControlRejectsBadRequests(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"empty urls", http.MethodPut, "/streams/7", `{"urls":[]}`, http.StatusBadRequest},
		{"malformed json", http.MethodPut, "/streams/7", `{not-json`, http.StatusBadRequest},
		{"method not allowed", http.MethodPatch, "/streams/7", "", http.StatusMethodNotAllowed},
		{"missing id", http.MethodPut, "/streams/", `{"urls":["http://x"]}`, http.StatusNotFound},
	}
	for _, c := range cases {
		if code := ctlRequest(t, ts.URL, c.method, c.path, c.body); code != c.want {
			t.Errorf("%s: status %d, want %d", c.name, code, c.want)
		}
	}
}

// TestIdleStopRespectsRefsAndAccess exercises the idle-stop decision directly
// (no real puller): a running control-managed stream stops only when it has no
// live viewers AND no HLS access within the grace window.
func TestIdleStopRespectsRefsAndAccess(t *testing.T) {
	st := &Stream{grace: 100 * time.Millisecond, cfg: &puller.Source{URLs: []string{"x"}}}
	_, cancel := context.WithCancel(context.Background())
	st.running = true
	st.cancel = cancel

	// Recent access → must NOT stop.
	st.lastAccess.Store(time.Now().UnixNano())
	st.mu.Lock()
	st.idleStopLocked(time.Now())
	st.mu.Unlock()
	if !st.running {
		t.Fatal("stopped despite recent access")
	}

	// Live viewer present with stale access → must NOT stop.
	st.refs = 1
	st.lastAccess.Store(time.Now().Add(-time.Second).UnixNano())
	st.mu.Lock()
	st.idleStopLocked(time.Now())
	st.mu.Unlock()
	if !st.running {
		t.Fatal("stopped despite a live viewer")
	}

	// No viewers, stale access → stop.
	st.refs = 0
	st.mu.Lock()
	st.idleStopLocked(time.Now())
	st.mu.Unlock()
	if st.running {
		t.Fatal("did not stop when idle past grace")
	}
}

func TestControlStatusEndpoint(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	if code := ctlRequest(t, ts.URL, http.MethodGet, "/streams/nope", ""); code != http.StatusNotFound {
		t.Fatalf("GET unknown stream: %d, want 404", code)
	}

	// Register (no viewer) → running=false, has_data=false.
	ctlRequest(t, ts.URL, http.MethodPut, "/streams/7", `{"urls":["http://x/1.ts"]}`)
	var s0 streamStatus
	getJSON(t, ts.URL+"/streams/7", &s0)
	if s0.Running || s0.Refs != 0 || s0.HasData || s0.SinceDataMs != -1 {
		t.Fatalf("fresh status = %+v, want stopped/no-data", s0)
	}

	// Feed one packet → has_data flips true with a fresh SinceDataMs.
	mgr.Get("7").Publish(tsfixture.Fill(0x101))
	var s1 streamStatus
	getJSON(t, ts.URL+"/streams/7", &s1)
	if !s1.HasData || s1.SinceDataMs < 0 {
		t.Fatalf("after publish status = %+v, want has_data", s1)
	}
}

// TestIngestPushFeedsLiveAndHLS covers the non-proxy path: a producer connects
// to the per-stream ingest socket and pushes mpegts; the daemon fans it out to
// /live and segments it for /hls, with no puller involved.
func TestIngestPushFeedsLiveAndHLS(t *testing.T) {
	dir := t.TempDir()
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.SetIngestDir(dir)

	sock, err := mgr.RegisterIngest("9", 0)
	if err != nil {
		t.Fatalf("RegisterIngest: %v", err)
	}

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial ingest socket: %v", err)
	}
	var feed []byte
	feed = append(feed, tsfixture.PAT(0x100)...)
	feed = append(feed, tsfixture.PMT(0x100, 0x101)...)
	for _, sec := range []int64{0, 2, 4} {
		feed = append(feed, tsfixture.Keyframe(0x101, sec*90000)...)
	}
	if _, err := conn.Write(feed); err != nil {
		t.Fatalf("write to ingest: %v", err)
	}

	// Wait for the accept+Copy goroutines to publish.
	st := mgr.Get("9")
	if st == nil {
		t.Fatal("stream 9 missing after RegisterIngest")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !st.status().HasData {
		time.Sleep(10 * time.Millisecond)
	}
	if !st.status().HasData {
		t.Fatal("ingest connection did not feed the stream")
	}

	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()
	body, ct, code := get(t, ts.URL+"/hls/9/index.m3u8")
	if code != 200 || !strings.Contains(ct, "mpegurl") || !strings.Contains(string(body), "#EXTM3U") {
		t.Fatalf("bad m3u8 after ingest: status=%d ct=%q\n%s", code, ct, body)
	}

	conn.Close()
	mgr.Unregister("9")
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatalf("ingest socket not cleaned up on Unregister: %v", err)
	}
}

// TestProbeReportsNoDataForDeadSource: probing a registered stream whose source
// can't produce returns has_data=false within the wait window (off-air signal).
func TestProbeReportsNoDataForDeadSource(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	// Source refuses fast (nothing on 127.0.0.1:1) → puller never yields data.
	ctlRequest(t, ts.URL, http.MethodPut, "/streams/7", `{"urls":["http://127.0.0.1:1/none.ts"]}`)

	var st streamStatus
	getJSON(t, ts.URL+"/probe/7?wait=150", &st)
	if st.HasData {
		t.Fatal("probe reported has_data for a dead source")
	}

	ctlRequest(t, ts.URL, http.MethodDelete, "/streams/7", "") // stop the puller goroutine
}

func TestConnectionTrackingAndReconcileList(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	st := mgr.GetOrCreate("5")
	st.addConn("uuidA")
	st.addConn("uuidB")
	st.addConn("uuidA") // refcount 2

	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	var got []string
	getJSON(t, ts.URL+"/connections", &got)
	set := map[string]bool{}
	for _, u := range got {
		set[u] = true
	}
	if len(set) != 2 || !set["uuidA"] || !set["uuidB"] {
		t.Fatalf("/connections = %v, want {uuidA,uuidB}", got)
	}

	st.removeConn("uuidA") // 2 → 1, still present
	got = nil
	getJSON(t, ts.URL+"/connections", &got)
	if len(got) != 2 {
		t.Fatalf("after one removeConn = %v, want both still present (refcount)", got)
	}

	st.removeConn("uuidA") // → gone
	st.removeConn("uuidB")
	got = nil
	getJSON(t, ts.URL+"/connections", &got)
	if len(got) != 0 {
		t.Fatalf("after all removals = %v, want empty", got)
	}
}

// TestRatesEndpoint verifies the per-viewer delivery-rate telemetry (P4): the
// daemon reports each active uuid's average KB/s since attach, and drops it on
// disconnect. fanout_sync turns this into lines_live.divergence.
func TestRatesEndpoint(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	st := mgr.GetOrCreate("9")
	cs := st.addConn("uuidR")
	// 200 KB delivered over 2 s → 100 KB/s.
	cs.since = time.Now().Add(-2 * time.Second)
	cs.bytes.Store(200 * 1024)

	ts := httptest.NewServer(mgr.ControlHandler())
	defer ts.Close()

	// A connected viewer that has received no bytes yet must still appear as 0
	// (a stall is the divergence signal we want to surface, not hide).
	stalled := st.addConn("uuidStalled")
	stalled.since = time.Now().Add(-2 * time.Second)

	var got map[string]int
	getJSON(t, ts.URL+"/rates", &got)
	if kbps, ok := got["uuidR"]; !ok || kbps < 90 || kbps > 110 {
		t.Fatalf("/rates[uuidR] = %v (ok=%v), want ~100 KB/s", got["uuidR"], ok)
	}
	if kbps, ok := got["uuidStalled"]; !ok || kbps != 0 {
		t.Fatalf("/rates[uuidStalled] = %v (ok=%v), want 0 KB/s present", got["uuidStalled"], ok)
	}
	st.removeConn("uuidStalled")

	st.removeConn("uuidR") // → gone
	got = nil
	getJSON(t, ts.URL+"/rates", &got)
	if len(got) != 0 {
		t.Fatalf("after disconnect /rates = %v, want empty", got)
	}
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

// feedStream primes a stream with PAT/PMT and three keyframes, yielding two
// finalized HLS segments and a non-empty live join snapshot.
func feedStream(st *Stream) {
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	for _, sec := range []int64{0, 2, 4} {
		st.Publish(tsfixture.Keyframe(0x101, sec*90000))
	}
}

func get(t *testing.T, url string) ([]byte, string, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.Header.Get("Content-Type"), resp.StatusCode
}

func TestServeHLSPlaylistAndSegment(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	feedStream(mgr.GetOrCreate("5"))
	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	body, ct, code := get(t, ts.URL+"/hls/5/index.m3u8")
	if code != 200 || !strings.Contains(ct, "mpegurl") || !strings.Contains(string(body), "#EXTM3U") {
		t.Fatalf("bad m3u8: status=%d ct=%q\n%s", code, ct, body)
	}

	body, ct, code = get(t, ts.URL+"/hls/5/0.ts")
	if code != 200 || !strings.Contains(ct, "video/mp2t") {
		t.Fatalf("bad segment: status=%d ct=%q", code, ct)
	}
	if len(body) == 0 || body[0] != 0x47 {
		t.Fatal("segment must be a TS starting with 0x47")
	}
}

func TestServe404s(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	feedStream(mgr.GetOrCreate("5"))
	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	for _, u := range []string{"/hls/5/999.ts", "/hls/999/index.m3u8", "/live/999"} {
		if _, _, code := get(t, ts.URL+u); code != 404 {
			t.Fatalf("%s status %d, want 404", u, code)
		}
	}
}

func TestServeLiveStreamsSnapshotThenTail(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	st := mgr.GetOrCreate("5")
	feedStream(st)
	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/live/5", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("live request: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "video/mp2t") {
		t.Fatalf("live content-type %q", ct)
	}

	// Join snapshot = PAT + PMT + current GOP (3 packets) must arrive immediately.
	snap := make([]byte, 3*tsfixture.P)
	if _, err := io.ReadFull(resp.Body, snap); err != nil {
		t.Fatalf("reading live snapshot: %v", err)
	}
	if snap[0] != 0x47 {
		t.Fatal("live snapshot must start with a TS sync byte")
	}

	// A subsequent publish must reach the already-connected client (live tail).
	st.Publish(tsfixture.Fill(0x101))
	tail := make([]byte, tsfixture.P)
	if _, err := io.ReadFull(resp.Body, tail); err != nil {
		t.Fatalf("reading live tail: %v", err)
	}
	if tail[0] != 0x47 {
		t.Fatal("live tail must be TS-aligned")
	}
}
