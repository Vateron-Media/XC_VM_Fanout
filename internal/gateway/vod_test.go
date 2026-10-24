package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// vodToken is a VOD link as StreamAuthMiddleware mints it: the live token's
// shape plus the file extension, the target container and (optionally) a Range.
func vodToken(edit func(map[string]any)) string {
	t := map[string]any{
		"username": "user1", "password": "pass1", "stream_id": 12, "extension": "mp4", "type": "movie", "video_codec": "h264",
		"channel_info": map[string]any{"redirect_id": 3, "originator_id": nil, "on_demand": 0, "proxy": false, "target_container": "mp4", "bitrate": 0},
		"user_info":    map[string]any{"id": 41, "max_connections": 2, "is_restreamer": 0, "con_isp_name": "Example ISP"},
		"uuid":         testUUID, "activity_start": testNow - 2, "country_code": "PT", "external_device": "",
	}
	if edit != nil {
		edit(t)
	}
	b, _ := json.Marshal(t)
	return tok(string(b))
}

// vodEnv is a VOD viewer's open connection: the token's own uuid, the daemon's (pid 0).
func vodEnv() *liveEnvState {
	st := goodLiveEnv()
	st.recUUID = testUUID
	st.rec = map[string]any{"uuid": testUUID, "identity": 41, "user_id": 41, "server_id": 3, "stream_id": 12, "container": "VOD", "user_ip": "198.51.100.7", "hls_end": 0, "pid": 0}
	return st
}

func vodPolicy() *Policy {
	p := livePolicy() // conn_store agent, use_buffer on
	p.Paths.Vod = "/vod/"
	return p
}

func TestJudgeVOD(t *testing.T) {
	ip := "198.51.100.7"

	// First request: the movie exists, a new connection created under the
	// token's uuid (container VOD, pid 0) and admitted; served by the daemon.
	p := vodPolicy()
	p.Live.Admission = true
	st := vodEnv()
	st.rec = nil
	v, r := JudgeVOD(p, vodToken(nil), ip, testUA, testNow, st.env())
	if v.Action != Serve || v.Reason != "vod-new" || r == nil || !r.New || r.UUID != testUUID || r.Path != "/vod/12.mp4" || r.ContentType != "video/mp4" {
		t.Fatalf("new VOD: %s (%s) %+v", v.Action, v.Reason, r)
	}
	if string(r.Record["container"]) != `"VOD"` || string(r.Record["pid"]) != "0" || r.Record["adm_uuid"] != nil {
		t.Fatalf("the record: %v", r.Record)
	}
	// vod.php hardcodes these, whatever the token/channel carry.
	stNew := vodEnv()
	stNew.rec = nil
	v2, r2 := JudgeVOD(p, vodToken(func(m map[string]any) {
		m["channel_info"].(map[string]any)["on_demand"] = 1
		delete(m, "external_device")
	}), ip, testUA, testNow, stNew.env())
	if v2.Action != Serve || string(r2.Record["external_device"]) != `""` || string(r2.Record["on_demand"]) != "0" {
		t.Fatalf("hardcoded record fields: %s %s", r2.Record["external_device"], r2.Record["on_demand"])
	}
	if r.Admission != `{"ip":"198.51.100.7","line_id":41,"max_connections":2,"stream_id":12,"ua":"`+testUA+`"}` {
		t.Fatalf("admission: %q", r.Admission)
	}

	// A known viewer's request (pid 0): a refresh, no admission.
	v, r = JudgeVOD(p, vodToken(nil), ip, testUA, testNow, vodEnv().env())
	if v.Action != Serve || v.Reason != "vod" || r == nil || r.New || r.Admission != "" {
		t.Fatalf("VOD refresh: %s (%s) %+v", v.Action, v.Reason, r)
	}

	// A Range the token carried is passed to the daemon.
	_, r = JudgeVOD(p, vodToken(func(m map[string]any) { m["http_range"] = "bytes=100-" }), ip, testUA, testNow, vodEnv().env())
	if r.Range != "bytes=100-" {
		t.Fatalf("token range: %q", r.Range)
	}

	// Everything the gateway does not own is PHP's.
	cases := []struct {
		name   string
		token  string
		policy func(*Policy)
		env    func(*liveEnvState)
		reason string
	}{
		{"a direct-proxy movie", vodToken(func(m map[string]any) { m["channel_info"].(map[string]any)["proxy"] = 1 }), nil, nil, "proxy"},
		{"behind an originator", vodToken(func(m map[string]any) { m["channel_info"].(map[string]any)["originator_id"] = 9 }), nil, nil, "proxy"},
		{"viewers in MAIN's store", vodToken(nil), func(p *Policy) { p.ConnStore = "php" }, nil, "conn-store"},
		{"the movie is gone", vodToken(nil), nil, func(s *liveEnvState) { s.fileMissing = true }, "missing"},
		{"no extension", vodToken(func(m map[string]any) { delete(m, "extension") }), nil, nil, "extension"},
		{"an odd extension", vodToken(func(m map[string]any) { m["extension"] = "../x" }), nil, nil, "extension"},
		{"a PHP worker feeds it", vodToken(nil), nil, func(s *liveEnvState) { s.rec["pid"] = 777 }, "worker"},
		{"another address", vodToken(nil), nil, func(s *liveEnvState) { s.rec["user_ip"] = "203.0.113.9" }, "ip"},
		{"past its 60s window", vodToken(func(m map[string]any) { m["activity_start"] = testNow - 61 }), nil, func(s *liveEnvState) { s.rec = nil }, "expired"},
		{"a limit and no agent spool", vodToken(nil), nil, func(s *liveEnvState) { s.rec, s.agentAlive = nil, false }, "spool"},
	}
	for _, c := range cases {
		p := vodPolicy()
		if c.policy != nil {
			c.policy(p)
		}
		st := vodEnv()
		if c.env != nil {
			c.env(st)
		}
		if v, _ := JudgeVOD(p, c.token, ip, testUA, testNow, st.env()); v.Action != PHP || v.Reason != c.reason {
			t.Errorf("%s: %s (%s); want PHP (%s)", c.name, v.Action, v.Reason, c.reason)
		}
	}

	// The throttle (vod.php): past vod_limit_perc of the response, at the
	// movie's bitrate (kbps * 125) plus vod_bitrate_plus percent.
	p = vodPolicy()
	p.VodLimitPerc, p.VodBitratePlus = 80, 20
	_, r = JudgeVOD(p, vodToken(func(m map[string]any) { m["channel_info"].(map[string]any)["bitrate"] = 1000 }), ip, testUA, testNow, vodEnv().env())
	if r.LimitPerc != 80 || r.Rate != 150000 { // 1000*125 = 125000, +20% = 150000
		t.Fatalf("throttle: limit=%d rate=%d", r.LimitPerc, r.Rate)
	}
	// A restreamer is never throttled; the limit_perc still travels.
	_, r = JudgeVOD(p, vodToken(func(m map[string]any) {
		m["channel_info"].(map[string]any)["bitrate"] = 1000
		m["user_info"].(map[string]any)["is_restreamer"] = 1
	}), ip, testUA, testNow, vodEnv().env())
	if r.LimitPerc != 80 || r.Rate != 0 {
		t.Fatalf("restreamer throttle: limit=%d rate=%d", r.LimitPerc, r.Rate)
	}
}

// The VOD serve path end to end: the connection created in the agent (container
// VOD, pid 0), the limit spooled, the marker touched, the movie handed to the
// daemon with its content type / range / throttle; a second request is a refresh.
func TestServeVOD(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"cons", "spool", "vod"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	// The judge's file_exists is a real os.Stat; the fake daemon serves "MOVIE".
	_ = os.WriteFile(filepath.Join(dir, "vod", "12.mp4"), []byte("the movie bytes"), 0o644)
	flows := filepath.Join(dir, "flows.json")
	_ = os.WriteFile(flows, []byte("{}"), 0o644)
	a := &fakeAgent{records: map[string]map[string]any{}}
	sock := startAgent(t, a)
	path := writeServePolicy(t, dir, "segments+playlist+thumb+subtitle+vod", func(doc map[string]any) {
		withUniqueCookie(doc)
		doc["conn_store"] = "agent"
		doc["vod_limit_perc"], doc["vod_bitrate_plus"] = 80, 20
		doc["live"] = map[string]any{"use_buffer": true, "create_expiration": 5, "admission": true}
		paths := doc["paths"].(map[string]any)
		paths["agent_sock"], paths["spool"], paths["flows"], paths["vod"] = sock, dir+"/spool", flows, dir+"/vod/"
		paths["cons"] = dir + "/cons"
	})
	node := &fakeFiles{}
	s := NewServer(path, http.NotFoundHandler(), node)
	ask := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/stream/vod?token=x", nil)
		req.Header.Set("X-XC-Original-URI", "/vauth/"+vodToken(func(m map[string]any) {
			m["activity_start"] = time.Now().Unix()
			m["channel_info"].(map[string]any)["bitrate"] = 1000
		}))
		req.Header.Set("X-XC-Client-IP", "198.51.100.7")
		req.Header.Set("X-XC-Host", "tv.example.com:8080")
		req.Header.Set("User-Agent", testUA)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		return rec
	}

	rec := ask()
	wantCookies(t, rec, []string{"xcuniq="})
	if !strings.Contains(rec.Header().Get("Set-Cookie"), "; Domain=.tv.example.com;") {
		t.Fatalf("vod cookie domain: %v", rec.Header()["Set-Cookie"])
	}
	if rec.Code != 200 || rec.Body.String() != "MOVIE" || rec.Header().Get("Content-Type") != "video/mp4" {
		t.Fatalf("first VOD request: %d %q %v; stats %v", rec.Code, rec.Body.String(), rec.Header(), s.Stats())
	}
	if node.hoID != "12" || node.hoUUID != testUUID || len(node.hoParts) != 1 || node.hoParts[0].Path != dir+"/vod/12.mp4" || node.hoType != "video/mp4" || node.hoLimitPerc != 80 || node.hoRate != 150000 {
		t.Fatalf("handed to the daemon: %+v", node)
	}
	got := a.records[testUUID]
	if got == nil || got["container"] != "VOD" || got["pid"] != float64(0) || got["user_id"] != float64(41) {
		t.Fatalf("the record created in the agent: %v", got)
	}
	if len(a.admissions) == 0 {
		t.Fatalf("no admission asked: %v", a.admissions)
	}
	spooled, _ := filepath.Glob(filepath.Join(dir, "spool", "p0", "*.ndjson"))
	if len(spooled) != 1 || !strings.Contains(string(mustRead(t, spooled[0])), `"uuid":"`+testUUID+`"`) {
		t.Fatalf("conn.limit spooled for MAIN: %v", spooled)
	}
	if _, err := os.Stat(filepath.Join(dir, "cons", testUUID)); err != nil {
		t.Fatalf("the viewer's marker touched: %v", err)
	}
	if s.Stats()["vod serve vod-new"] != 1 {
		t.Fatalf("stats: %v", s.Stats())
	}

	// The same link again: the viewer is known, a refresh, no new admission.
	n := len(a.admissions)
	if rec := ask(); rec.Code != 200 || s.Stats()["vod serve vod"] != 1 || len(a.admissions) != n {
		t.Fatalf("the refresh: %d stats %v admissions %d want %d", rec.Code, s.Stats(), len(a.admissions), n)
	}

	// A mode that does not serve VOD hands it to @gw_vod_php.
	off := writeServePolicy(t, dir, "segments+playlist+thumb+subtitle", func(doc map[string]any) {
		doc["conn_store"] = "agent"
		doc["paths"].(map[string]any)["vod"] = dir + "/vod/"
	})
	rec = serveReq(NewServer(off, http.NotFoundHandler(), node), "/vauth/"+vodToken(nil), "198.51.100.7")
	if rec.Header().Get("X-Accel-Redirect") != "@gw_vod_php" {
		t.Fatalf("mode off VOD: want @gw_vod_php, got %v", rec.Header())
	}
}
