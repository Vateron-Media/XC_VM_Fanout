// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package clusteragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/server"
)

// fanoutStub is a unix-socket HTTP server standing in for the fanout's control
// socket, recording the one request it is sent and answering a fixed status.
type fanoutStub struct {
	sock         string
	mu           sync.Mutex
	method, path string
	body         []byte
	calls        int
}

func newFanoutStub(t *testing.T, status int) *fanoutStub {
	t.Helper()
	st := &fanoutStub{sock: filepath.Join(t.TempDir(), "ctl.sock")}
	ln, err := net.Listen("unix", st.sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		st.mu.Lock()
		st.method, st.path, st.body = r.Method, r.URL.Path, b
		st.calls++
		st.mu.Unlock()
		w.WriteHeader(status)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return st
}

func (s *fanoutStub) agent() *Agent { return &Agent{FanoutCtl: s.sock} }

func (s *fanoutStub) seen() (string, string, []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.method, s.path, s.body
}

func (s *fanoutStub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// The runtime directive is the cross-repo contract MAIN emits
// (StreamRuntimeDirective) and the agent applies. testdata/runtime_directive_
// proxy.json is the canonical shape, shared with the XC_VM side's fixture.
func TestParseRuntimeDirectiveProxy(t *testing.T) {
	data, err := os.ReadFile("testdata/runtime_directive_proxy.json")
	if err != nil {
		t.Fatal(err)
	}
	d, ok := parseRuntimeDirective(data)
	if !ok {
		t.Fatal("want ok for a record carrying a runtime directive")
	}
	if d.Kind != "proxy" {
		t.Fatalf("kind = %q, want proxy", d.Kind)
	}
	if d.Source == nil || len(d.Source.URLs) != 2 || d.Source.URLs[0] != "http://a/1.ts" {
		t.Fatalf("source = %+v", d.Source)
	}
	if d.Source.UA != "MyPlayer/1.0" || d.Source.Proxy != "1.2.3.4:8080" || d.Source.Cookie != "sid=xyz" {
		t.Fatalf("source fields = %+v", d.Source)
	}
}

func TestParseRuntimeDirectiveNoRuntime(t *testing.T) {
	if _, ok := parseRuntimeDirective([]byte(`{"etag":"x","ver":1,"data":{"stream":{"id":1}}}`)); ok {
		t.Fatal("a record with no runtime directive must report ok=false")
	}
}

func TestParseRuntimeDirectiveOther(t *testing.T) {
	d, ok := parseRuntimeDirective([]byte(`{"data":{"runtime":{"kind":"other","source":null}}}`))
	if !ok {
		t.Fatal("want ok")
	}
	if d.Kind != "other" || d.Source != nil {
		t.Fatalf("got kind=%q source=%+v", d.Kind, d.Source)
	}
}

func TestRegisterBodyFromProxyDirective(t *testing.T) {
	d := runtimeDirective{Kind: "proxy", Source: &directiveSource{
		URLs: []string{"http://a/1"}, UA: "UA/1", Proxy: "p:1", Cookie: "c=1"}}
	b, ok := d.registerBody("/opt/ffmpeg")
	if !ok {
		t.Fatal("want ok for a proxy directive with urls")
	}
	if len(b.URLs) != 1 || b.URLs[0] != "http://a/1" || b.UA != "UA/1" || b.Proxy != "p:1" || b.Cookie != "c=1" {
		t.Fatalf("body = %+v", b)
	}
	// The node-local ffmpeg path is the agent's to fill (the directive omits it).
	if b.Ffmpeg != "/opt/ffmpeg" {
		t.Fatalf("ffmpeg = %q, want the node path", b.Ffmpeg)
	}
}

func TestRegisterBodyEmptyURLsNotOK(t *testing.T) {
	d := runtimeDirective{Kind: "proxy", Source: &directiveSource{URLs: []string{""}}}
	if _, ok := d.registerBody("/opt/ffmpeg"); ok {
		t.Fatal("a proxy directive with no usable url must not register")
	}
}

func TestRegisterBodyNonProxyNotOK(t *testing.T) {
	if _, ok := (runtimeDirective{Kind: "other"}).registerBody("/opt/ffmpeg"); ok {
		t.Fatal("a non-proxy directive must not register")
	}
}

func TestRegisterStreamPUTsBody(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	err := st.agent().registerStream(context.Background(), 42,
		streamRegisterBody{URLs: []string{"http://a/1"}, UA: "UA/1", Ffmpeg: "/f"})
	if err != nil {
		t.Fatal(err)
	}
	m, p, body := st.seen()
	if m != http.MethodPut || p != "/streams/42" {
		t.Fatalf("got %s %s", m, p)
	}
	var b streamRegisterBody
	if err := json.Unmarshal(body, &b); err != nil || len(b.URLs) != 1 || b.URLs[0] != "http://a/1" {
		t.Fatalf("body not sent: %s", body)
	}
}

func TestRegisterStream503IsTransient(t *testing.T) {
	st := newFanoutStub(t, http.StatusServiceUnavailable)
	err := st.agent().registerStream(context.Background(), 42, streamRegisterBody{URLs: []string{"http://a/1"}})
	if !errors.Is(err, errFanoutTransient) {
		t.Fatalf("want errFanoutTransient, got %v", err)
	}
}

func TestUnregisterStreamDELETEs(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	if err := st.agent().unregisterStream(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if m, p, _ := st.seen(); m != http.MethodDelete || p != "/streams/42" {
		t.Fatalf("got %s %s", m, p)
	}
}

func TestRegisterStreamNoSocketIsNoop(t *testing.T) {
	a := &Agent{FanoutCtl: ""}
	if err := a.registerStream(context.Background(), 42, streamRegisterBody{URLs: []string{"http://a/1"}}); err != nil {
		t.Fatalf("empty FanoutCtl must no-op, got %v", err)
	}
	if err := a.unregisterStream(context.Background(), 42); err != nil {
		t.Fatalf("empty FanoutCtl must no-op, got %v", err)
	}
}

func TestSeedFanoutRecordProxyRegisters(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	a := st.agent()
	a.SeedFanout, a.FfmpegBin = true, "/f"
	data, _ := os.ReadFile("testdata/runtime_directive_proxy.json")

	a.seedFanoutRecord(42, data)

	if m, p, _ := st.seen(); m != http.MethodPut || p != "/streams/42" {
		t.Fatalf("got %s %s, want PUT /streams/42", m, p)
	}
}

func TestSeedFanoutRecordOtherUnregisters(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	a := st.agent()
	a.SeedFanout = true

	a.seedFanoutRecord(42, []byte(`{"data":{"runtime":{"kind":"other"}}}`))

	if m, p, _ := st.seen(); m != http.MethodDelete || p != "/streams/42" {
		t.Fatalf("got %s %s, want DELETE /streams/42 (clear a non-proxy)", m, p)
	}
}

func TestSeedFanoutRecordDisabledMakesNoCall(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	a := st.agent() // SeedFanout stays false
	data, _ := os.ReadFile("testdata/runtime_directive_proxy.json")

	a.seedFanoutRecord(42, data)

	if m, _, _ := st.seen(); m != "" {
		t.Fatalf("SeedFanout off must make no call, saw %s", m)
	}
}

func TestSeedFanoutRecordTransientIsSwallowed(t *testing.T) {
	st := newFanoutStub(t, http.StatusServiceUnavailable)
	a := st.agent()
	a.SeedFanout, a.FfmpegBin = true, "/f"
	data, _ := os.ReadFile("testdata/runtime_directive_proxy.json")

	// A 503 must not panic or propagate — it is logged by id and retried next sync.
	a.seedFanoutRecord(42, data)

	if m, _, _ := st.seen(); m != http.MethodPut {
		t.Fatalf("want a PUT attempt, saw %s", m)
	}
}

func TestUnseedFanoutRecordDELETEs(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	a := st.agent()
	a.SeedFanout = true

	a.unseedFanoutRecord(42)

	if m, p, _ := st.seen(); m != http.MethodDelete || p != "/streams/42" {
		t.Fatalf("got %s %s, want DELETE /streams/42", m, p)
	}
}

func TestSeedHeldProxiesRegistersOnlyProxies(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	dir := t.TempDir()
	sd := filepath.Join(dir, "streams")
	if err := os.MkdirAll(sd, 0o700); err != nil {
		t.Fatal(err)
	}
	proxy, _ := os.ReadFile("testdata/runtime_directive_proxy.json")
	if err := os.WriteFile(filepath.Join(sd, "7.json"), proxy, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sd, "8.json"), []byte(`{"data":{"runtime":{"kind":"other"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a := st.agent()
	a.SeedFanout, a.FfmpegBin, a.ReplicaDir = true, "/f", dir

	a.seedHeldProxies(context.Background())

	if n := st.count(); n != 1 {
		t.Fatalf("made %d calls, want 1 (only the proxy stream)", n)
	}
	if m, p, _ := st.seen(); m != http.MethodPut || p != "/streams/7" {
		t.Fatalf("got %s %s, want PUT /streams/7", m, p)
	}
}

// reconcileFanout registers held proxies the fanout lacks and clears fanout
// registrations that are no longer held proxies, leaving matches alone.
func TestReconcileFanoutRegistersMissingClearsStale(t *testing.T) {
	var mu sync.Mutex
	puts, dels := map[string]bool{}, map[string]bool{}
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/streams":
			_ = json.NewEncoder(w).Encode(map[string][]string{"streams": {"7", "9"}}) // fanout holds 7, 9
		case r.Method == http.MethodPut:
			mu.Lock()
			puts[strings.TrimPrefix(r.URL.Path, "/streams/")] = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete:
			mu.Lock()
			dels[strings.TrimPrefix(r.URL.Path, "/streams/")] = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	dir := t.TempDir()
	sd := filepath.Join(dir, "streams")
	if err := os.MkdirAll(sd, 0o700); err != nil {
		t.Fatal(err)
	}
	proxy, _ := os.ReadFile("testdata/runtime_directive_proxy.json")
	_ = os.WriteFile(filepath.Join(sd, "7.json"), proxy, 0o600) // held proxy 7 (already in fanout)
	_ = os.WriteFile(filepath.Join(sd, "8.json"), proxy, 0o600) // held proxy 8 (missing from fanout)

	a := &Agent{FanoutCtl: sock, SeedFanout: true, FfmpegBin: "/f", ReplicaDir: dir}
	a.reconcileFanout(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if !puts["8"] {
		t.Error("held proxy 8 missing from the fanout should be registered")
	}
	if puts["7"] {
		t.Error("held proxy 7 already registered should not be re-PUT")
	}
	if !dels["9"] {
		t.Error("fanout id 9 that is no longer a held proxy should be unregistered")
	}
	if dels["7"] {
		t.Error("held proxy 7 must never be unregistered")
	}
}

func TestParseTranscodeDirective(t *testing.T) {
	data, err := os.ReadFile("testdata/runtime_directive_transcode.json")
	if err != nil {
		t.Fatal(err)
	}
	d, ok := parseRuntimeDirective(data)
	if !ok {
		t.Fatal("want ok")
	}
	if d.Kind != "transcode" {
		t.Fatalf("kind = %q, want transcode", d.Kind)
	}
	spec, ok := d.transcodeSpec()
	if !ok {
		t.Fatal("want a transcode spec")
	}
	if !bytes.Contains(spec, []byte("{ingest_sock}")) {
		t.Fatalf("spec missing its placeholders: %s", spec)
	}
}

func TestResolveSpec(t *testing.T) {
	data, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	d, _ := parseRuntimeDirective(data)
	spec, _ := d.transcodeSpec()
	n := agentNode{streams: "/s/", logs: "/l/", serverID: "3", ffmpegCpu: "/ff", ffmpegGpu: "/ffg", ffprobe: "/fp", binary: "/bin"}

	out, err := resolveSpec(spec, n, "/i/7.sock", "aabb", "ccdd")
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, tok := range []string{"{ingest_sock}", "{streams}", "{ffmpeg_cpu}", "{key}", "{iv}", "{server_id}", "{logs}"} {
		if strings.Contains(s, tok) {
			t.Fatalf("unresolved token %s in %s", tok, s)
		}
	}
	if !strings.Contains(s, "unix:/i/7.sock") || !strings.Contains(s, "/s/7_.pid") || !strings.Contains(s, "aabb") {
		t.Fatalf("values not substituted: %s", s)
	}
}

func TestResolveSpecResidualTokenErrors(t *testing.T) {
	if _, err := resolveSpec(json.RawMessage(`{"cmd":"{bogus}"}`), agentNode{}, "", "", ""); err == nil {
		t.Fatal("an unresolved/unknown token must error (incomplete node config)")
	}
}

func TestResolveSpecNoEnc(t *testing.T) {
	out, err := resolveSpec(json.RawMessage(`{"cmd":"{ffmpeg_cpu} -i x {streams}1_.m3u8"}`), agentNode{ffmpegCpu: "/ff", streams: "/s/"}, "/i", "", "")
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if strings.Contains(s, "{ffmpeg_cpu}") || strings.Contains(s, "{streams}") || residualTokenTest.MatchString(s) {
		t.Fatalf("unexpected residual token: %s", s)
	}
	if !strings.Contains(s, "/ff -i x /s/1_.m3u8") {
		t.Fatalf("values not substituted: %s", s)
	}
}

var residualTokenTest = regexp.MustCompile(`\{[a-z_]+\}`)

func TestSuperviseStreamResolvesAndHandsOff(t *testing.T) {
	var mu sync.Mutex
	var monitorBody []byte
	var ingestHit, monitorHit bool
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/ingest/"):
			mu.Lock()
			ingestHit = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]string{"socket": "/i/7.sock"})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/monitor/"):
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			monitorBody, monitorHit = b, true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	a := &Agent{FanoutCtl: sock, node: agentNode{streams: "/s/", logs: "/l/", serverID: "3", ffmpegCpu: "/ff", ffmpegGpu: "/ffg", ffprobe: "/fp", binary: "/bin"}}
	data, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	d, _ := parseRuntimeDirective(data)
	spec, _ := d.transcodeSpec()

	if err := a.superviseStream(context.Background(), 7, spec); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !ingestHit || !monitorHit {
		t.Fatalf("want ingest + monitor PUTs (ingest=%v monitor=%v)", ingestHit, monitorHit)
	}
	if residualTokenTest.Match(monitorBody) {
		t.Fatalf("unresolved token in the monitor body: %s", monitorBody)
	}
	if !bytes.Contains(monitorBody, []byte("unix:/i/7.sock")) {
		t.Fatalf("ingest socket not substituted into the command: %s", monitorBody)
	}
}

func TestSuperviseStreamResidualTokenDoesNotHandOff(t *testing.T) {
	var monitorHit bool
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	ln, _ := net.Listen("unix", sock)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/ingest/") {
			_ = json.NewEncoder(w).Encode(map[string]string{"socket": "/i/7.sock"})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/monitor/") {
			monitorHit = true
			w.WriteHeader(http.StatusNoContent)
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	// node missing ffmpegCpu → the spec's {ffmpeg_cpu} can't resolve → no /monitor PUT.
	a := &Agent{FanoutCtl: sock, node: agentNode{streams: "/s/"}}
	data, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	d, _ := parseRuntimeDirective(data)
	spec, _ := d.transcodeSpec()
	if err := a.superviseStream(context.Background(), 7, spec); err == nil {
		t.Fatal("want an error when a placeholder can't resolve")
	}
	if monitorHit {
		t.Fatal("must not hand off to the supervisor with an unresolved spec")
	}
}

func TestUnsuperviseStreamDELETEsMonitor(t *testing.T) {
	st := newFanoutStub(t, http.StatusNoContent)
	if err := st.agent().unsuperviseStream(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	if m, p, _ := st.seen(); m != http.MethodDelete || p != "/monitor/7" {
		t.Fatalf("got %s %s, want DELETE /monitor/7", m, p)
	}
}

// routingStub records every control call and answers /ingest/ with a socket, so
// the transcode routing (ingest + monitor + streams) can be asserted together.
type routeRec struct {
	method, path string
	body         []byte
}

type routingStub struct {
	sock string
	mu   sync.Mutex
	recs []routeRec
}

func newRoutingStub(t *testing.T) *routingStub {
	t.Helper()
	st := &routingStub{sock: filepath.Join(t.TempDir(), "ctl.sock")}
	ln, err := net.Listen("unix", st.sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		st.mu.Lock()
		st.recs = append(st.recs, routeRec{r.Method, r.URL.Path, b})
		st.mu.Unlock()
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/ingest/") {
			_ = json.NewEncoder(w).Encode(map[string]string{"socket": "/i/7.sock"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return st
}

func (s *routingStub) count(method, prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.recs {
		if r.method == method && strings.HasPrefix(r.path, prefix) {
			n++
		}
	}
	return n
}

func (s *routingStub) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.recs)
}

func (s *routingStub) agent() *Agent {
	return &Agent{
		FanoutCtl: s.sock, SeedFanout: true, FfmpegBin: "/opt/ffmpeg",
		node: agentNode{streams: "/s/", logs: "/l/", serverID: "3", ffmpegCpu: "/ff", ffmpegGpu: "/ffg", ffprobe: "/fp", binary: "/bin"},
	}
}

func TestSeedFanoutRecordTranscode(t *testing.T) {
	st := newRoutingStub(t)
	rec, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	st.agent().seedFanoutRecord(7, rec)
	if n := st.count(http.MethodPut, "/ingest/"); n != 1 {
		t.Fatalf("want 1 ingest PUT, got %d", n)
	}
	if n := st.count(http.MethodPut, "/monitor/"); n != 1 {
		t.Fatalf("want 1 monitor PUT, got %d", n)
	}
}

func TestSeedFanoutRecordTranscodeDedup(t *testing.T) {
	st := newRoutingStub(t)
	a := st.agent()
	rec, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	a.seedFanoutRecord(7, rec)
	a.seedFanoutRecord(7, rec) // same spec on a resync must NOT restart the encoder
	if n := st.count(http.MethodPut, "/monitor/"); n != 1 {
		t.Fatalf("want 1 monitor PUT after a duplicate seed (dedup), got %d", n)
	}
}

func TestSeedFanoutRecordFlipTranscodeToProxy(t *testing.T) {
	st := newRoutingStub(t)
	a := st.agent()
	tr, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	px, _ := os.ReadFile("testdata/runtime_directive_proxy.json")
	a.seedFanoutRecord(7, tr)
	a.seedFanoutRecord(7, px) // reconfigured to a proxy: release the encoder, register the puller
	if n := st.count(http.MethodDelete, "/monitor/"); n != 1 {
		t.Fatalf("flip to proxy must DELETE /monitor/, got %d", n)
	}
	if n := st.count(http.MethodPut, "/streams/"); n != 1 {
		t.Fatalf("flip to proxy must register the puller, got %d", n)
	}
}

func TestSeedFanoutRecordTranscodeSeedOff(t *testing.T) {
	st := newRoutingStub(t)
	a := st.agent()
	a.SeedFanout = false
	rec, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	a.seedFanoutRecord(7, rec)
	if n := st.total(); n != 0 {
		t.Fatalf("SeedFanout off must make no calls, got %d", n)
	}
}

func TestSeedHeldReSupervisesTranscodes(t *testing.T) {
	st := newRoutingStub(t)
	dir := t.TempDir()
	sd := filepath.Join(dir, "streams")
	if err := os.MkdirAll(sd, 0o700); err != nil {
		t.Fatal(err)
	}
	tr, _ := os.ReadFile("testdata/runtime_directive_transcode.json")
	if err := os.WriteFile(filepath.Join(sd, "7.json"), tr, 0o600); err != nil {
		t.Fatal(err)
	}
	a := st.agent()
	a.ReplicaDir = dir
	a.seedHeldProxies(context.Background()) // the fanout-restart re-seed
	if n := st.count(http.MethodPut, "/monitor/"); n != 1 {
		t.Fatalf("a held transcode must be re-supervised on a fanout restart, got %d monitor PUTs", n)
	}
}

func TestResolveSpecIgnoresFfmpegExpansion(t *testing.T) {
	// ffmpeg's %{...} expansion (e.g. a drawtext timestamp overlay) must not read
	// as an unresolved token — else the transcode fails closed and never serves.
	out, err := resolveSpec(json.RawMessage(`{"cmd":"{ffmpeg_cpu} -vf drawtext=text='%{localtime}' {streams}1_.m3u8"}`),
		agentNode{ffmpegCpu: "/ff", streams: "/s/"}, "/i", "", "")
	if err != nil {
		t.Fatalf("ffmpeg %%{...} expansion must not trip the residual check: %v", err)
	}
	if !strings.Contains(string(out), "%{localtime}") {
		t.Fatalf("the expansion should be preserved: %s", out)
	}
}

// Phase 4b end to end: MAIN's run/stop of an LB stream reaches a REAL fanout
// Manager entirely through the Go agent — no fake, no PHP. A MAIN proxy
// assignment (seedFanoutRecord → PUT /streams/<id>) registers the stream on the
// Manager; the matching unassign (unseedFanoutRecord → DELETE /streams/<id>)
// unregisters it. This also pins the agent↔Manager /streams body contract that
// the fanoutStub tests cannot see.
func TestSeedThenUnseedDrivesRealManagerRegisterUnregister(t *testing.T) {
	m := server.NewManager(1<<20, 0, 2, 6, time.Second)
	t.Cleanup(func() { m.UnregisterAll() })
	sock := filepath.Join(t.TempDir(), "ctl.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: m.ControlHandler()}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	a := &Agent{FanoutCtl: sock}
	a.SeedFanout, a.FfmpegBin = true, "/f"
	data, err := os.ReadFile("testdata/runtime_directive_proxy.json")
	if err != nil {
		t.Fatal(err)
	}

	a.seedFanoutRecord(7, data) // MAIN assigns a proxy stream
	if m.Get("7") == nil {
		t.Fatal("a MAIN assignment must register the stream on the Manager (run)")
	}

	a.unseedFanoutRecord(7) // MAIN unassigns
	if m.Get("7") != nil {
		t.Fatal("a MAIN unassign must unregister the stream on the Manager (stop)")
	}
}
