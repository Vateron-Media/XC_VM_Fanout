package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// tsStart is three minutes before testNow, so a 3-minute scan covers exactly
// the minutes ts, ts+60, ts+120 (all at or before "now").
const tsStart = testNow - 180

func tsToken(edit func(map[string]any)) string {
	t := map[string]any{
		"username": "user1", "password": "pass1", "stream": 12, "extension": "ts",
		"start": strconv.FormatInt(tsStart, 10), "duration": 3, "redirect_id": 3, "originator_id": nil,
		"user_info": map[string]any{"id": 41, "max_connections": 2, "is_restreamer": 0, "con_isp_name": "Example ISP"},
		"uuid":      testUUID, "activity_start": testNow - 2, "country_code": "PT",
	}
	if edit != nil {
		edit(t)
	}
	b, _ := json.Marshal(t)
	return tok(string(b))
}

// tsSizes is the archive with the three minute files present (each 1,000,000 bytes).
func tsSizes(archive string) map[string]int64 {
	m := map[string]int64{}
	for i := int64(0); i < 3; i++ {
		m[archive+"12/"+gmdateMinute(tsStart+i*60)+".ts"] = 1_000_000
	}
	return m
}

func tsEnv() *liveEnvState {
	st := goodLiveEnv()
	st.recUUID = testUUID
	st.rec = map[string]any{"uuid": testUUID, "identity": 41, "user_id": 41, "server_id": 3, "stream_id": 12, "container": "ts", "user_ip": "198.51.100.7", "hls_end": 0, "pid": 0}
	st.sizes = tsSizes("/archive/")
	st.offset = 1000
	return st
}

func tsPolicyTS() *Policy {
	p := livePolicy()
	p.Paths.Archive, p.VodLimitPerc, p.VodBitratePlus = "/archive/", 50, 0
	return p
}

func TestJudgeTimeshift(t *testing.T) {
	ip := "198.51.100.7"

	// First request: the three archive minutes queued, the first from its
	// offset, a new connection (container = the extension, pid 0) admitted.
	p := tsPolicyTS()
	p.Live.Admission = true
	st := tsEnv()
	st.rec = nil
	v, r := JudgeTimeshift(p, tsToken(nil), ip, testUA, testNow, st.env())
	if v.Action != Serve || v.Reason != "timeshift-new" || r == nil || !r.New || r.UUID != testUUID || r.ContentType != "video/mp2t" {
		t.Fatalf("new timeshift: %s (%s) %+v", v.Action, v.Reason, r)
	}
	if string(r.Record["container"]) != `"ts"` || string(r.Record["pid"]) != "0" || string(r.Record["external_device"]) != `""` || string(r.Record["on_demand"]) != "0" {
		t.Fatalf("record: %v", r.Record)
	}
	if len(r.Parts) != 3 || r.Parts[0].Offset != 1000 || r.Parts[0].Length != 999000 || r.Parts[1].Offset != 0 || r.Parts[1].Length != 1_000_000 {
		t.Fatalf("parts: %+v", r.Parts)
	}
	if r.Parts[0].Path != "/archive/12/"+gmdateMinute(tsStart)+".ts" {
		t.Fatalf("first part path: %s", r.Parts[0].Path)
	}
	// rSize = 3,000,000 - 1000; bitrate*125 = rSize*0.008/180*125 = 16661 (plus 0%).
	if r.LimitPerc != 50 || r.Rate != 16661 {
		t.Fatalf("throttle: limit=%d rate=%d", r.LimitPerc, r.Rate)
	}

	// A known viewer (pid 0): a refresh, no admission.
	v, r = JudgeTimeshift(p, tsToken(nil), ip, testUA, testNow, tsEnv().env())
	if v.Action != Serve || v.Reason != "timeshift" || r == nil || r.New || r.Admission != "" {
		t.Fatalf("refresh: %s (%s) %+v", v.Action, v.Reason, r)
	}

	// A restreamer is never throttled; the limit_perc still travels.
	_, r = JudgeTimeshift(p, tsToken(func(m map[string]any) { m["user_info"].(map[string]any)["is_restreamer"] = 1 }), ip, testUA, testNow, tsEnv().env())
	if r.LimitPerc != 50 || r.Rate != 0 {
		t.Fatalf("restreamer: limit=%d rate=%d", r.LimitPerc, r.Rate)
	}

	// A date-form start parsed in the node's timezone (UTC here).
	utcStart := time.Unix(tsStart, 0).UTC().Format("2006-01-02:15-04")
	if v, _ := JudgeTimeshift(p, tsToken(func(m map[string]any) { m["start"] = utcStart }), ip, testUA, testNow, func() *liveEnvState { s := tsEnv(); s.rec = nil; return s }().env()); v.Action != Serve {
		t.Fatalf("date-form start: %s", v.Reason)
	}

	// Everything the gateway does not own is PHP's.
	cases := []struct {
		name   string
		token  string
		policy func(*Policy)
		env    func(*liveEnvState)
		reason string
	}{
		{"the m3u8 playlist arm", tsToken(func(m map[string]any) { m["extension"] = "m3u8" }), nil, nil, "extension"},
		{"behind an originator", tsToken(func(m map[string]any) { m["originator_id"] = 9 }), nil, nil, "proxy"},
		{"send_unique_header", tsToken(nil), func(p *Policy) { p.Headers.UniqueHeader = true }, nil, "unique-header"},
		{"the first minute is gone", tsToken(nil), nil, func(s *liveEnvState) { s.sizes = map[string]int64{} }, "missing"},
		{"no timestamp form", tsToken(func(m map[string]any) { m["start"] = "notadate" }), nil, nil, "timestamp"},
		{"an unloadable timezone", tsToken(func(m map[string]any) { m["start"] = "2026-10-16:14-00" }), func(p *Policy) { p.DefaultTimezone = "No/Such" }, func(s *liveEnvState) { s.rec = nil }, "timezone"},
		{"a PHP worker feeds it", tsToken(nil), nil, func(s *liveEnvState) { s.rec["pid"] = 777 }, "worker"},
		{"another address", tsToken(nil), nil, func(s *liveEnvState) { s.rec["user_ip"] = "203.0.113.9" }, "ip"},
		{"zero duration", tsToken(func(m map[string]any) { m["duration"] = 0 }), nil, nil, "timestamp"},
	}
	for _, c := range cases {
		p := tsPolicyTS()
		if c.policy != nil {
			c.policy(p)
		}
		st := tsEnv()
		if c.env != nil {
			c.env(st)
		}
		if v, _ := JudgeTimeshift(p, c.token, ip, testUA, testNow, st.env()); v.Action != PHP || v.Reason != c.reason {
			t.Errorf("%s: %s (%s); want PHP (%s)", c.name, v.Action, v.Reason, c.reason)
		}
	}
}

// The timeshift serve path end to end: the connection created (container ts,
// pid 0), the archive minutes handed to the daemon as one video/mp2t response.
func TestServeTimeshift(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"cons", "spool", "archive/12"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	// The scan's file_exists/filesize are real os.Stat in the server's liveEnv.
	for i := int64(0); i < 3; i++ {
		_ = os.WriteFile(filepath.Join(dir, "archive/12", gmdateMinute(tsStart+i*60)+".ts"), make([]byte, 1000), 0o644)
	}
	_ = os.WriteFile(filepath.Join(dir, "archive/12", gmdateMinute(tsStart)+".ts.offset"), []byte("100"), 0o644)
	flows := filepath.Join(dir, "flows.json")
	_ = os.WriteFile(flows, []byte("{}"), 0o644)
	a := &fakeAgent{records: map[string]map[string]any{}}
	sock := startAgent(t, a)
	path := writeServePolicy(t, dir, "segments+playlist+thumb+subtitle+vod+timeshift", func(doc map[string]any) {
		doc["conn_store"] = "agent"
		doc["vod_limit_perc"] = 50
		doc["live"] = map[string]any{"use_buffer": true, "create_expiration": 5, "admission": true}
		paths := doc["paths"].(map[string]any)
		paths["agent_sock"], paths["spool"], paths["flows"], paths["archive"] = sock, dir+"/spool", flows, dir+"/archive/"
		paths["cons"] = dir + "/cons"
	})
	node := &fakeFiles{}
	s := NewServer(path, http.NotFoundHandler(), node)
	// Minted now: the judge reads the start against the real clock. Use a start
	// three minutes ago so the scan covers the three minutes written above.
	nowStart := time.Now().Add(-3 * time.Minute).Unix()
	for i := int64(0); i < 3; i++ {
		_ = os.WriteFile(filepath.Join(dir, "archive/12", gmdateMinute(nowStart+i*60)+".ts"), make([]byte, 1000), 0o644)
	}
	_ = os.WriteFile(filepath.Join(dir, "archive/12", gmdateMinute(nowStart)+".ts.offset"), []byte("100"), 0o644)
	req := httptest.NewRequest(http.MethodGet, "/stream/timeshift?token=x", nil)
	req.Header.Set("X-XC-Original-URI", "/tsauth/"+tsToken(func(m map[string]any) {
		m["start"] = strconv.FormatInt(nowStart, 10)
		m["activity_start"] = time.Now().Unix()
	}))
	req.Header.Set("X-XC-Client-IP", "198.51.100.7")
	req.Header.Set("User-Agent", testUA)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != 200 || rec.Body.String() != "MOVIE" || rec.Header().Get("Content-Type") != "video/mp2t" {
		t.Fatalf("timeshift: %d %q %v; stats %v", rec.Code, rec.Body.String(), rec.Header(), s.Stats())
	}
	if node.hoID != "12" || node.hoType != "video/mp2t" || len(node.hoParts) != 3 || node.hoParts[0].Offset != 100 || node.hoParts[0].Length != 900 {
		t.Fatalf("handed to the daemon: %+v", node.hoParts)
	}
	got := a.records[testUUID]
	if got == nil || got["container"] != "ts" || got["pid"] != float64(0) || got["user_id"] != float64(41) {
		t.Fatalf("the record created: %v", got)
	}
	if s.Stats()["timeshift serve timeshift-new"] != 1 {
		t.Fatalf("stats: %v", s.Stats())
	}

	// A mode that does not serve timeshift hands it to @gw_timeshift_php.
	off := writeServePolicy(t, dir, "segments+playlist+thumb+subtitle+vod", func(doc map[string]any) {
		doc["conn_store"] = "agent"
		doc["paths"].(map[string]any)["archive"] = dir + "/archive/"
	})
	rec = serveReq(NewServer(off, http.NotFoundHandler(), node), "/tsauth/"+tsToken(nil), "198.51.100.7")
	if rec.Header().Get("X-Accel-Redirect") != "@gw_timeshift_php" {
		t.Fatalf("mode off timeshift: want @gw_timeshift_php, got %v", rec.Header())
	}
}
