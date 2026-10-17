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
