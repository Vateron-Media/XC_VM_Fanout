package gateway

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testNow  = int64(1800000000)
	testUUID = "a3f1c2d4e5b60718293a4b5c6d7e8f90"
)

var (
	testSecret  = []byte("gw-shared-secret")
	testContext = []byte("test-openssl-extra")
	testNonce   = []byte("0123456789ab")
)

func testPolicy() *Policy {
	p := &Policy{V: 1, ServerID: 3, WrittenAt: testNow, Mode: "shadow", RestrictSameIP: true}
	p.Keys = Keys{Shared: []Key{{Value: testSecret}}, Context: []Key{{Value: testContext}}}
	p.Redirect = map[string][]string{"5": {"http://lb5.example:8080"}}
	return p
}

func tok(plain string) string { return Seal([]byte(plain), testSecret, testContext, testNonce) }

func liveTok(server, segment, ip string) string {
	return tok("user1/pass1/" + ip + "/12/" + segment + "/" + testUUID + "/" + server + "/h264/0")
}

// envOf is a node whose markers, files and agent answer as given.
func envOf(cons, file bool, ended, heard bool) Env {
	return Env{
		Cons:  func(string) bool { return cons },
		File:  func(string) bool { return file },
		Heard: func(string) (bool, bool) { return ended, heard },
	}
}

var (
	consAll  = envOf(true, true, false, true)
	consNone = envOf(false, true, false, true)
)

func TestJudgeSegmentLive(t *testing.T) {
	p := testPolicy()
	cases := []struct {
		name   string
		token  string
		ip     string
		cons   Env
		policy func(*Policy)
		want   Action
		reason string
	}{
		{"a live daemon segment", liveTok("3", "12_d345.ts", "198.51.100.7"), "198.51.100.7", consAll, nil, Serve, "daemon"},
		{"another address", liveTok("3", "12_d345.ts", "198.51.100.7"), "198.51.100.8", consAll, nil, Deny, "ip"},
		{"another address, same /24 with ip_subnet_match", liveTok("3", "12_d345.ts", "198.51.100.7"), "198.51.100.8", consAll, func(p *Policy) { p.IPSubnetMatch = true }, Serve, "daemon"},
		{"another address, restrict_same_ip off", liveTok("3", "12_d345.ts", "198.51.100.7"), "203.0.113.1", consAll, func(p *Policy) { p.RestrictSameIP = false }, Serve, "daemon"},
		{"no connection marker", liveTok("3", "12_d345.ts", "198.51.100.7"), "198.51.100.7", consNone, nil, Deny, "connection"},
		{"an on-disk segment while fanout delivers", liveTok("3", "12_345.ts", "198.51.100.7"), "198.51.100.7", consAll, nil, Deny, "not-daemon"},
		{"another stream's daemon segment", liveTok("3", "13_d345.ts", "198.51.100.7"), "198.51.100.7", consAll, nil, Deny, "not-daemon"},
		{"another server's token", liveTok("5", "12_d345.ts", "198.51.100.7"), "198.51.100.7", consAll, nil, Redirect, "owner"},
		{"a server the policy does not know", liveTok("9", "12_d345.ts", "198.51.100.7"), "198.51.100.7", consAll, nil, PHP, "server-unknown"},
		{"a server id PHP reads loosely", liveTok("03x", "12_d345.ts", "198.51.100.7"), "198.51.100.7", consAll, nil, PHP, "server-id"},
		{"a catch-up token of eight fields", tok("TS/u/p/198.51.100.7/3600/2026-10-08:12-00/x/3"), "198.51.100.7", consAll, nil, Deny, "fields"},
		{"five fields", tok("a/b/c/d/e"), "198.51.100.7", consAll, nil, Deny, "fields"},
		{"six fields, no server", tok("u/p/198.51.100.7/12/12_d1.ts/" + testUUID), "198.51.100.7", consAll, nil, PHP, "fields"},
		{"not a connection id", tok("u/p/198.51.100.7/12/12_d1.ts/..xx/3/h264/0"), "198.51.100.7", consAll, nil, PHP, "uuid"},
		{"a token this node does not open", Seal([]byte("x"), []byte("other"), testContext, testNonce), "198.51.100.7", consAll, nil, Deny, "token"},
		{"a token PHP may read otherwise", liveTok("3", "12_d345.ts", "198.51.100.7") + "=", "198.51.100.7", consAll, nil, PHP, "token-form"},
		{"past the lease", liveTok("3", "12_d345.ts", "198.51.100.7"), "198.51.100.7", consAll, func(p *Policy) { p.ServeUntil = testNow - 1 }, PHP, "lease"},
	}
	for _, c := range cases {
		q := *p
		if c.policy != nil {
			c.policy(&q)
		}
		v := JudgeSegment(&q, c.token, c.ip, testNow, c.cons)
		if v.Action != c.want || v.Reason != c.reason {
			t.Errorf("%s: %s (%s); want %s (%s)", c.name, v.Action, v.Reason, c.want, c.reason)
		}
	}
	v := JudgeSegment(p, liveTok("3", "12_d345.ts", "198.51.100.7"), "198.51.100.7", testNow, consAll)
	if v.Stream != 12 || v.Seq != 345 || v.UUID != testUUID || v.Codec != "h264" {
		t.Fatalf("served segment: %+v", v)
	}
	r := JudgeSegment(p, liveTok("5", "12_d345.ts", "198.51.100.7"), "198.51.100.7", testNow, consAll)
	if r.Location != "http://lb5.example:8080/hls/"+liveTok("5", "12_d345.ts", "198.51.100.7") {
		t.Fatalf("redirect: %s", r.Location)
	}
	if JudgeSegment(nil, "x", "", testNow, consAll).Action != PHP {
		t.Fatal("no policy: PHP")
	}
}

func archiveTok(segment, ip string) string {
	return tok("TS/u/p/" + ip + "/3600/2026-10-08:12-00/" + segment + "/" + testUUID + "/3")
}

func TestJudgeSegmentCatchUp(t *testing.T) {
	ip := "198.51.100.7"
	agent := func(p *Policy) { p.ConnStore = "agent"; p.Paths.Archive = "/archive/" }
	cases := []struct {
		name   string
		token  string
		ip     string
		env    Env
		policy func(*Policy)
		want   Action
		reason string
	}{
		{"a recorded minute", archiveTok("12_2026-10-08:12-00.ts_0", ip), ip, consAll, agent, Serve, "archive"},
		{"the session ended", archiveTok("12_2026-10-08:12-00.ts_0", ip), ip, envOf(true, true, true, true), agent, Deny, "ended"},
		{"the agent does not answer", archiveTok("12_2026-10-08:12-00.ts_0", ip), ip, envOf(true, true, false, false), agent, PHP, "agent"},
		{"viewers kept in MAIN's store", archiveTok("12_2026-10-08:12-00.ts_0", ip), ip, consAll, func(p *Policy) { p.ConnStore = "php" }, PHP, "conn-store"},
		{"no such minute", archiveTok("12_2026-10-08:12-00.ts_0", ip), ip, envOf(true, false, false, true), agent, Deny, "archive-file"},
		{"not a minute's name", archiveTok("12_2026-10-08 12-00.ts_0", ip), ip, consAll, agent, Deny, "archive-name"},
		{"a path in the minute", archiveTok("12_../../etc/passwd_0", ip), ip, consAll, agent, Deny, "fields"},
		{"no connection marker", archiveTok("12_2026-10-08:12-00.ts_0", ip), ip, consNone, agent, Deny, "connection"},
		{"another address", archiveTok("12_2026-10-08:12-00.ts_0", ip), "203.0.113.9", consAll, agent, Deny, "ip"},
		{"an offset PHP reads with intval", archiveTok("12_2026-10-08:12-00.ts_9x", ip), ip, consAll, agent, PHP, "offset"},
		{"no minute at all", archiveTok("12", ip), ip, consAll, agent, PHP, "archive-fields"},
	}
	for _, c := range cases {
		q := *testPolicy()
		c.policy(&q)
		v := JudgeSegment(&q, c.token, c.ip, testNow, c.env)
		if v.Action != c.want || v.Reason != c.reason {
			t.Errorf("%s: %s (%s); want %s (%s)", c.name, v.Action, v.Reason, c.want, c.reason)
		}
	}
	q := *testPolicy()
	agent(&q)
	v := JudgeSegment(&q, archiveTok("12_2026-10-08:12-00.ts_188", ip), ip, testNow, consAll)
	if v.Path != "/archive/12/2026-10-08:12-00.ts" || v.Offset != 188 || v.Stream != 12 {
		t.Fatalf("served minute: %+v", v)
	}
	v = JudgeSegment(&q, archiveTok("12_2026-10-08:12-00.ts", ip), ip, testNow, consAll)
	if v.Action != Serve || v.Offset != 0 {
		t.Fatalf("no offset is the whole minute: %+v", v)
	}
}

func TestJudgeKey(t *testing.T) {
	p := testPolicy()
	if v := JudgeKey(p, tok("198.51.100.7/12"), "198.51.100.7", testNow); v.Action != Serve || v.Stream != 12 {
		t.Fatalf("key: %+v", v)
	}
	if v := JudgeKey(p, tok("198.51.100.7/12"), "198.51.100.9", testNow); v.Action != Deny || v.Reason != "ip" {
		t.Fatalf("another address: %+v", v)
	}
	if v := JudgeKey(p, tok("198.51.100.7"), "198.51.100.7", testNow); v.Action != Deny || v.Reason != "fields" {
		t.Fatalf("one field: %+v", v)
	}
	if v := JudgeKey(p, tok("198.51.100.7/12x"), "198.51.100.7", testNow); v.Action != PHP {
		t.Fatalf("a stream id PHP reads with intval: %+v", v)
	}
}

func TestParseURI(t *testing.T) {
	for uri, want := range map[string][2]string{
		"/hls/abc":     {"segment", "abc"},
		"/key/abc?x=1": {"key", "abc"},
		"/hls/abc/def": {"", ""},
		"/auth/abc":    {"live", "abc"},
		"/r1/hls/abc":  {"", ""},
		"":             {"", ""},
	} {
		if k, tk := ParseURI(uri); k != want[0] || tk != want[1] {
			t.Errorf("%q: %q %q; want %v", uri, k, tk, want)
		}
	}
}

func writePolicy(t *testing.T, path string, writtenAt int64, v int) {
	t.Helper()
	doc := map[string]any{"v": v, "server_id": 3, "written_at": writtenAt, "mode": "shadow",
		"keys":  map[string]any{"viewer": []any{}, "shared": []any{map[string]any{"hex": "6b", "until": nil}}, "context": []any{map[string]any{"hex": "63", "until": nil}}, "accept_legacy_cbc": false},
		"paths": map[string]any{"cons": filepath.Dir(path) + "/cons/"}}
	b, _ := json.Marshal(doc)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	// A distinct mtime for each write, as the reader keys on it.
	_ = os.Chtimes(path, time.Unix(writtenAt, 0), time.Unix(writtenAt, 0))
}

func TestPolicyFileIsActedOnOnlyWhileFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	f := NewPolicyFile(path)
	at := time.Unix(testNow, 0)
	if f.Get(at) != nil {
		t.Fatal("no file: nil")
	}
	writePolicy(t, path, testNow, 1)
	if p := f.Get(at.Add(time.Second)); p == nil || p.ServerID != 3 || string(p.Keys.Shared[0].Value) != "k" {
		t.Fatalf("fresh policy: %+v", p)
	}
	writePolicy(t, path, testNow+2, 2)
	if f.Get(at.Add(3*time.Second)) != nil {
		t.Fatal("another version: nil")
	}
	writePolicy(t, path, testNow+4, 1)
	if f.Get(at.Add(5*time.Second)) == nil {
		t.Fatal("rewritten: read again")
	}
	if f.Get(at.Add((StaleAfter+10)*time.Second)) != nil {
		t.Fatal("stale: nil")
	}
}

func TestShadowCountsVerdicts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	writePolicy(t, path, time.Now().Unix(), 1)
	s := NewServer(path, http.NotFoundHandler(), nil)
	for _, uri := range []string{"/hls/" + Seal([]byte("x"), []byte("k"), []byte("c"), testNonce), "/key/zz=", "/play/x"} {
		req := httptest.NewRequest(http.MethodPost, "/shadow", nil)
		req.Header.Set("X-XC-Original-URI", uri)
		req.Header.Set("X-XC-Client-IP", "198.51.100.7")
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s: status %d", uri, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	body := rec.Body.String()
	for _, want := range []string{`"segment deny fields":1`, `"key php token-form":1`, `"other php uri":1`} {
		if !strings.Contains(body, want) {
			t.Errorf("stats %s: missing %s", body, want)
		}
	}
}

// Viewer-facing input: no token, URI or plaintext may panic the gateway.
func FuzzRequest(f *testing.F) {
	f.Add("")
	f.Add("/hls/AAAA")
	f.Add("/key/" + tok("198.51.100.7/12"))
	f.Add("/hls/" + liveTok("3", "12_d345.ts", "198.51.100.7"))
	p := testPolicy()
	f.Fuzz(func(t *testing.T, uri string) {
		kind, token := ParseURI(uri)
		_ = kind
		JudgeSegment(p, token, "198.51.100.7", testNow, consAll)
		JudgeKey(p, token, "198.51.100.7", testNow)
		p.Keys.Read(token, true, testNow)
	})
}

// What a token carries, once it opens: the field parsing.
func FuzzPlaintext(f *testing.F) {
	f.Add("u/p/198.51.100.7/12/12_d345.ts/" + testUUID + "/3/h264/0")
	f.Add("TS/u/p/1.2.3.4/3600/x/12_x.ts_9/" + testUUID + "/3")
	f.Add("1.2.3.4/12")
	f.Add("//////////")
	p := testPolicy()
	f.Fuzz(func(t *testing.T, plain string) {
		token := tok(plain)
		JudgeSegment(p, token, "198.51.100.7", testNow, consAll)
		JudgeKey(p, token, "198.51.100.7", testNow)
	})
}

// A policy for serving: the test keys, server 3, paths under dir.
func writeServePolicy(t *testing.T, dir, mode string, edit func(map[string]any)) string {
	t.Helper()
	doc := map[string]any{"v": 1, "server_id": 3, "written_at": time.Now().Unix(), "mode": mode,
		"keys": map[string]any{"viewer": []any{}, "shared": []any{map[string]any{"hex": hexOf(testSecret), "until": nil}},
			"context": []any{map[string]any{"hex": hexOf(testContext), "until": nil}}, "accept_legacy_cbc": false},
		"restrict_same_ip": true, "headers": map[string]any{"server": "", "protection": false, "altsvc_port": 0},
		"paths":    map[string]any{"cons": dir + "/cons", "streams": dir + "/streams", "archive": dir + "/archive/"},
		"redirect": map[string]any{"5": []any{"http://lb5.example:8080"}}}
	if edit != nil {
		edit(doc)
	}
	path := filepath.Join(dir, "policy.json")
	b, _ := json.Marshal(doc)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func hexOf(b []byte) string { return hex.EncodeToString(b) }

func serveReq(s *Server, uri, ip string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/stream/segment?token=x", nil)
	req.Header.Set("X-XC-Original-URI", uri)
	req.Header.Set("X-XC-Client-IP", ip)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestServeAnswersAsPHPDoes(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"cons", "streams"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	_ = os.WriteFile(filepath.Join(dir, "cons", testUUID), nil, 0o644)
	_ = os.WriteFile(filepath.Join(dir, "streams", "12_.key"), []byte("0123456789abcdef"), 0o644)
	var asked string
	daemon := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = r.URL.RequestURI()
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write([]byte("TS"))
	})
	s := NewServer(writeServePolicy(t, dir, "segments", nil), daemon, nil)
	ip := "198.51.100.7"

	rec := serveReq(s, "/hls/"+liveTok("3", "12_d345.ts", ip), ip)
	if rec.Code != 200 || rec.Body.String() != "TS" || asked != "/hls/12/345.ts?c="+testUUID+"&vc=h264" {
		t.Fatalf("daemon segment: %d %q, asked %q", rec.Code, rec.Body.String(), asked)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("headers: %v", rec.Header())
	}

	rec = serveReq(s, "/hls/"+liveTok("3", "12_d345.ts", ip), "203.0.113.9")
	if rec.Code != 404 || !strings.Contains(rec.Body.String(), "<h1>404 Not Found</h1>") || rec.Header().Get("X-Accel-Redirect") != "" {
		t.Fatalf("refused: %d %q", rec.Code, rec.Body.String())
	}

	tok5 := liveTok("5", "12_d345.ts", ip)
	if rec = serveReq(s, "/hls/"+tok5, ip); rec.Code != 302 || rec.Header().Get("Location") != "http://lb5.example:8080/hls/"+tok5 {
		t.Fatalf("another server's token: %d %s", rec.Code, rec.Header().Get("Location"))
	}

	// A recorded minute on a node whose viewers are in MAIN's store: PHP hears it.
	_ = os.MkdirAll(filepath.Join(dir, "archive", "12"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, "archive", "12", "2026-10-08:12-00.ts"), []byte("x"), 0o644)
	archive := tok("TS/u/p/" + ip + "/3600/2026-10-08:12-00/12_2026-10-08:12-00.ts_0/" + testUUID + "/3")
	if rec = serveReq(s, "/hls/"+archive, ip); rec.Header().Get("X-Accel-Redirect") != "@gw_segment_php" || rec.Body.Len() != 0 {
		t.Fatalf("catch-up: PHP's: %d %v", rec.Code, rec.Header())
	}
	if rec = serveReq(s, "/key/zz=", ip); rec.Header().Get("X-Accel-Redirect") != "@gw_key_php" {
		t.Fatalf("an unsure key token: PHP's key handler: %v", rec.Header())
	}

	rec = serveReq(s, "/key/"+tok(ip+"/12"), ip)
	if rec.Code != 200 || rec.Body.String() != "0123456789abcdef" || rec.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("key: %d %q %v", rec.Code, rec.Body.String(), rec.Header())
	}

	// In shadow nothing is served, whatever nginx passes.
	shadow := NewServer(writeServePolicy(t, dir, "shadow", nil), daemon, nil)
	if rec = serveReq(shadow, "/hls/"+liveTok("3", "12_d345.ts", ip), ip); rec.Header().Get("X-Accel-Redirect") != "@gw_segment_php" {
		t.Fatalf("shadow mode, a request passed to serve: %v", rec.Header())
	}
	if !strings.Contains(fmt.Sprint(shadow.Stats()), "segment php mode") {
		t.Fatalf("stats: %v", shadow.Stats())
	}
}

func TestServeSendsThePolicysHeaders(t *testing.T) {
	dir := t.TempDir()
	path := writeServePolicy(t, dir, "segments", func(doc map[string]any) {
		doc["headers"] = map[string]any{"server": "XCVM", "protection": true, "altsvc_port": 8443}
	})
	s := NewServer(path, http.NotFoundHandler(), nil)
	rec := serveReq(s, "/hls/"+tok("a/b/c/d/e"), "198.51.100.7")
	h := rec.Header()
	if rec.Code != 404 || h.Get("Server") != "XCVM" || h.Get("X-XSS-Protection") != "0" || !strings.HasPrefix(h.Get("Alt-Svc"), `h3-29=":8443"; ma=2592000,`) || !strings.HasSuffix(h.Get("Alt-Svc"), `quic=":8443"; ma=2592000; v="46,43"`) {
		t.Fatalf("headers: %d %v", rec.Code, h)
	}
}

func TestBootstrapRefusesAsPHPDoes(t *testing.T) {
	p := testPolicy()
	p.Paths.Flood = "/flood/"
	blocked := func(path string) bool { return path == "/flood/block_198.51.100.66" }
	if got := BootstrapRefuses(p, "198.51.100.66", "lb.example", blocked); got != "blocked" {
		t.Fatalf("a blocked address: %q", got)
	}
	if got := BootstrapRefuses(p, "198.51.100.7", "anything.example", blocked); got != "" {
		t.Fatalf("no host check: %q", got)
	}
	p.VerifyHost = true
	if got := BootstrapRefuses(p, "198.51.100.7", "anything.example", blocked); got != "" {
		t.Fatalf("no list yet: hosts pass: %q", got)
	}
	p.AllowedDomains = []string{"lb.example", "tv.example"}
	for host, want := range map[string]string{"lb.example": "", "tv.example:8080": "", " lb.example ": "", "203.0.113.4:80": "", "xc_vm": "", "evil.example": "host", "[2001:db8::1]:80": "host"} {
		if got := BootstrapRefuses(p, "198.51.100.7", host, blocked); got != want {
			t.Errorf("host %q: %q; want %q", host, got, want)
		}
	}
}
