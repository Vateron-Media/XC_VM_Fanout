// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package gateway

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// recordingHandler stands in for fanout's own HLS/live handler behind the
// gateway: it records the re-dispatched URI and answers 200, so a test can tell
// a served warmup (reached the handler) from a deferred/503 one (did not).
type recordingHandler struct {
	uri  string
	hits int
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.uri, h.hits = r.URL.RequestURI(), h.hits+1
	w.Header().Set("Content-Type", "application/x-mpegurl")
	_, _ = w.Write([]byte("#EXTM3U\n"))
}

// warmupPolicy writes a serve-capable live policy (agent store, admission) for
// the given mode and serve_only flag, with a fresh agent that admits. It returns
// the temp dir so a caller can start a producer (pid + _.m3u8) — the legacy
// StreamAlive/OnDisk gates a live request clears before the warmup branch.
func warmupPolicy(t *testing.T, mode string, serveOnly bool) (string, string, *fakeAgent) {
	t.Helper()
	dir := t.TempDir()
	for _, d := range []string{"cons", "streams", "signals", "spool"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	flows := filepath.Join(dir, "flows.json")
	_ = os.WriteFile(flows, []byte("{}"), 0o644)
	a := &fakeAgent{records: map[string]map[string]any{}}
	sock := startAgent(t, a)
	path := writeServePolicy(t, dir, mode, func(doc map[string]any) {
		doc["serve_only"] = serveOnly
		doc["conn_store"] = "agent"
		doc["live"] = map[string]any{"use_buffer": true, "create_expiration": 5, "admission": true}
		paths := doc["paths"].(map[string]any)
		paths["agent_sock"], paths["streams"], paths["signals"], paths["spool"], paths["flows"] = sock, dir+"/streams/", dir+"/signals/", dir+"/spool", flows
		paths["cons"] = dir + "/cons"
	})
	return dir, path, a
}

// warmupAsk makes a freshly-minted HLS live request (stream 12) as nginx hands it.
func warmupAsk(s *Server) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/stream/live?token=x", nil)
	req.Header.Set("X-XC-Original-URI", "/auth/"+firstHLSToken(func(m map[string]any) { m["activity_start"] = time.Now().Unix() }))
	req.Header.Set("X-XC-Client-IP", "198.51.100.7")
	req.Header.Set("User-Agent", testUA)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

// A serve-only LB serving a registered-but-idle HLS stream: the warmup branch
// starts it via EnsureRunning and, once ready, serves the TOKENIZED playlist
// in-process (as the non-warmup path does) rather than proxying the daemon's
// raw playlist (relative segment names → 404 at the player, XC_VM_Fanout#128).
func TestServeOnlyWarmupStartsAndServes(t *testing.T) {
	dir, path, _ := warmupPolicy(t, "segments+playlist", true)
	startProducer(t, filepath.Join(dir, "streams"))
	seg := &recordingHandler{}
	// The stream is idle at judge time (empty playlist → warmup), and its first
	// segment has finalized (one relative "166.ts" line) by the time serveLive
	// polls for readiness.
	calls := 0
	node := &fakeFiles{
		serveable:     map[string]bool{"12": true},
		ensureRunning: func(string) bool { return true },
		fedPlaylist: func(string) string {
			calls++
			if calls <= 1 {
				return "" // judge sees an idle stream
			}
			return "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:5\n#EXTINF:4.0,\n166.ts\n"
		},
	}
	s := NewServer(path, seg, node)

	rec := warmupAsk(s)

	if rec.Code != 200 {
		t.Fatalf("served warmup: code %d; want 200", rec.Code)
	}
	if rec.Header().Get("X-Accel-Redirect") != "" {
		t.Fatalf("served warmup must not defer to PHP: %q", rec.Header().Get("X-Accel-Redirect"))
	}
	if node.ensureRunningIDs == nil || node.ensureRunningIDs[0] != "12" {
		t.Fatalf("EnsureRunning called with %v; want [12]", node.ensureRunningIDs)
	}
	assertTokenizedPlaylist(t, rec, seg)
}

// assertTokenizedPlaylist fails unless the response is a tokenized HLS playlist
// served in-process: an x-mpegurl body with /hls/<token> lines and no relative
// "<seq>.ts" name, and no re-dispatch to the daemon's raw HLS handler.
func assertTokenizedPlaylist(t *testing.T, rec *httptest.ResponseRecorder, seg *recordingHandler) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-mpegurl" {
		t.Fatalf("Content-Type %q; want application/x-mpegurl", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "/hls/") {
		t.Fatalf("playlist not tokenized (no /hls/ lines): %q", body)
	}
	if strings.Contains(body, "166.ts") {
		t.Fatalf("playlist still carries the relative segment name: %q", body)
	}
	if seg.hits != 0 {
		t.Fatalf("a tokenized warmup must not proxy the daemon's raw playlist (seg hits %d)", seg.hits)
	}
}

// A genuinely COLD serve-only stream — no producer at all (no <id>_.pid, no
// <id>_.m3u8), the on-demand case a viewer's first request hits — must still
// start in-process via the warmup branch (EnsureRunning) instead of deferring to
// a PHP backend that does not exist. This is the on-demand cold-start the legacy
// StreamAlive/OnDisk gates used to preempt (→ 502 on a PHP-free node).
func TestServeOnlyColdStartStartsAndServes(t *testing.T) {
	_, path, _ := warmupPolicy(t, "segments+playlist", true)
	// Deliberately NO startProducer: the stream is down (StreamAlive/OnDisk false).
	seg := &recordingHandler{}
	calls := 0
	node := &fakeFiles{
		serveable:     map[string]bool{"12": true},
		ensureRunning: func(string) bool { return true },
		fedPlaylist: func(string) string {
			calls++
			if calls <= 1 {
				return "" // idle at judge time → warmup
			}
			return "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:5\n#EXTINF:4.0,\n166.ts\n" // ready once started
		},
	}
	s := NewServer(path, seg, node)

	rec := warmupAsk(s)

	if rec.Code != 200 {
		t.Fatalf("cold serve-only stream served: code %d; want 200", rec.Code)
	}
	if rec.Header().Get("X-Accel-Redirect") != "" {
		t.Fatalf("cold serve-only start must not defer to PHP: %q", rec.Header().Get("X-Accel-Redirect"))
	}
	if node.ensureRunningIDs == nil || node.ensureRunningIDs[0] != "12" {
		t.Fatalf("EnsureRunning called with %v; want [12]", node.ensureRunningIDs)
	}
	assertTokenizedPlaylist(t, rec, seg)
}

// A COLD stream on a node that is NOT serve-only still defers to PHP (the backend
// is there to start it): the cold-start relaxation is serve-only only.
func TestColdStartNonServeOnlyDefers(t *testing.T) {
	_, path, _ := warmupPolicy(t, "segments+playlist", false)
	seg := &recordingHandler{}
	node := &fakeFiles{serveable: map[string]bool{"12": true}, ensureRunning: func(string) bool { return true }}
	s := NewServer(path, seg, node)

	rec := warmupAsk(s)

	if rec.Header().Get("X-Accel-Redirect") != "@gw_live_php" {
		t.Fatalf("a non-serve-only cold stream defers to PHP: X-Accel-Redirect %q", rec.Header().Get("X-Accel-Redirect"))
	}
	if node.ensureRunningIDs != nil {
		t.Fatalf("EnsureRunning must not run off the serve-only path: %v", node.ensureRunningIDs)
	}
}

// withFastWarmupPlaylist lowers the HLS readiness-wait tunables so the
// never-ready case is a fraction of a second. Process-global vars, so tests
// using it must not call t.Parallel().
func withFastWarmupPlaylist(t *testing.T) {
	t.Helper()
	oldWait, oldPoll := warmupPlaylistWait, warmupPlaylistPoll
	warmupPlaylistWait, warmupPlaylistPoll = 150*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { warmupPlaylistWait, warmupPlaylistPoll = oldWait, oldPoll })
}

// EnsureRunning started the stream, but its first HLS segment never finalizes
// within the bound: the gateway asks the client to retry (503) rather than pass
// the HLS handler's "no segments yet" 404 on, uniform with the start timeout.
func TestServeOnlyWarmupPlaylistNeverReadyReturns503(t *testing.T) {
	withFastWarmupPlaylist(t)
	dir, path, _ := warmupPolicy(t, "segments+playlist", true)
	startProducer(t, filepath.Join(dir, "streams"))
	seg := &recordingHandler{}
	node := &fakeFiles{
		serveable:     map[string]bool{"12": true},
		ensureRunning: func(string) bool { return true },
		fedPlaylist:   func(string) string { return "" }, // on air but no segment ever cut
	}
	s := NewServer(path, seg, node)

	rec := warmupAsk(s)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("playlist never ready: code %d; want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatalf("503 must carry Retry-After")
	}
	if node.ensureRunningIDs == nil || node.ensureRunningIDs[0] != "12" {
		t.Fatalf("EnsureRunning called with %v; want [12]", node.ensureRunningIDs)
	}
	if seg.hits != 0 {
		t.Fatalf("a never-ready playlist must not re-dispatch to the HLS handler (hits %d)", seg.hits)
	}
}

// The stream never becomes ready within the bound: EnsureRunning returns false,
// so the gateway asks the client to retry (503 Retry-After) rather than defer or
// hang, and never re-dispatches to the (empty) HLS handler.
func TestServeOnlyWarmupTimeoutReturns503(t *testing.T) {
	dir, path, _ := warmupPolicy(t, "segments+playlist", true)
	startProducer(t, filepath.Join(dir, "streams"))
	seg := &recordingHandler{}
	node := &fakeFiles{serveable: map[string]bool{"12": true}, ensureRunning: func(string) bool { return false }}
	s := NewServer(path, seg, node)

	rec := warmupAsk(s)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("timed-out warmup: code %d; want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatalf("503 must carry Retry-After")
	}
	if rec.Header().Get("X-Accel-Redirect") != "" {
		t.Fatalf("a 503 must not also defer to PHP: %q", rec.Header().Get("X-Accel-Redirect"))
	}
	if node.ensureRunningIDs == nil || node.ensureRunningIDs[0] != "12" {
		t.Fatalf("EnsureRunning called with %v; want [12]", node.ensureRunningIDs)
	}
	if seg.hits != 0 {
		t.Fatalf("a timed-out warmup must not re-dispatch to the HLS handler (hits %d)", seg.hits)
	}
}

// Not serve-only (a node with a PHP backend): an idle stream still defers to PHP,
// and the warmup branch — with EnsureRunning — is never taken.
func TestPhpBackendNotRunningStillDefers(t *testing.T) {
	dir, path, _ := warmupPolicy(t, "segments+playlist", false)
	startProducer(t, filepath.Join(dir, "streams"))
	seg := &recordingHandler{}
	node := &fakeFiles{serveable: map[string]bool{"12": true}, ensureRunning: func(string) bool { return true }}
	s := NewServer(path, seg, node)

	rec := warmupAsk(s)

	if rec.Header().Get("X-Accel-Redirect") != "@gw_live_php" {
		t.Fatalf("a non-serve-only idle stream defers to PHP: X-Accel-Redirect %q", rec.Header().Get("X-Accel-Redirect"))
	}
	if node.ensureRunningIDs != nil {
		t.Fatalf("EnsureRunning must not be called off the serve-only warmup path: %v", node.ensureRunningIDs)
	}
	if seg.hits != 0 {
		t.Fatalf("a deferred request must not re-dispatch in-process (hits %d)", seg.hits)
	}
}

// Shadow mode judges without serving: a /stream request on a shadow-mode node
// defers (serves() is false for a shadow policy), so nothing is started.
func TestShadowNeverCallsEnsureRunning(t *testing.T) {
	_, path, _ := warmupPolicy(t, "shadow", true)
	seg := &recordingHandler{}
	node := &fakeFiles{serveable: map[string]bool{"12": true}, ensureRunning: func(string) bool { return true }}
	s := NewServer(path, seg, node)

	rec := warmupAsk(s)

	if rec.Header().Get("X-Accel-Redirect") == "" {
		t.Fatalf("a shadow-mode /stream request defers to PHP; got no X-Accel-Redirect (code %d)", rec.Code)
	}
	if node.ensureRunningIDs != nil {
		t.Fatalf("shadow must never start a stream: EnsureRunning %v", node.ensureRunningIDs)
	}
	if seg.hits != 0 {
		t.Fatalf("shadow must not serve in-process (hits %d)", seg.hits)
	}
}
