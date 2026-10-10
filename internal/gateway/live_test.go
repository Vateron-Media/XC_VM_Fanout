package gateway

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The panel's tests/Support/gateway_live_vectors.json (GatewayLiveVectorsTest).
type liveVectors struct {
	Context  string `json:"context"`
	ServerID int    `json:"server_id"`
	HLSKeys  []struct {
		HmacID     json.RawMessage `json:"hmac_id"`
		Identifier *string         `json:"identifier"`
		UserID     json.RawMessage `json:"user_id"`
		StreamID   int             `json:"stream_id"`
		IP         string          `json:"ip"`
		UserAgent  string          `json:"user_agent"`
		UAHTML     string          `json:"ua_html"`
		UUID       string          `json:"uuid"`
	} `json:"hls_keys"`
	Rawurlencode []struct {
		InHex string `json:"in_hex"`
		Out   string `json:"out"`
	} `json:"rawurlencode"`
	Reconcile []struct {
		Daemon int64     `json:"daemon"`
		Floor  int64     `json:"floor"`
		State  *seqState `json:"state"`
		Now    *int64    `json:"now"`
		Target int64     `json:"target"`
		Seq    int64     `json:"seq"`
		Next   seqState  `json:"next"`
	} `json:"reconcile"`
	Playlist struct {
		Input    string `json:"input"`
		StreamID int    `json:"stream_id"`
		UUID     string `json:"uuid"`
		IP       string `json:"ip"`
		Codec    string `json:"codec"`
		IVHex    string `json:"iv_hex"`
		Cases    []struct {
			Name         string                     `json:"name"`
			ViewerKeyHex *string                    `json:"viewer_key_hex"`
			Settings     map[string]json.RawMessage `json:"settings"`
			Username     *string                    `json:"username"`
			Password     *string                    `json:"password"`
			HmacID       *int64                     `json:"hmac_id"`
			Identifier   *string                    `json:"identifier"`
			Output       string                     `json:"output"`
		} `json:"cases"`
	} `json:"playlist"`
}

func loadLive(t *testing.T) liveVectors {
	t.Helper()
	b, err := os.ReadFile("testdata/gateway_live_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v liveVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestLiveConnectionIDMatchesThePanel(t *testing.T) {
	for _, k := range loadLive(t).HLSKeys {
		ua, ok := htmlentitiesASCII(k.UserAgent)
		if !ok || ua != k.UAHTML {
			t.Fatalf("%q: %q %v; want %q", k.UserAgent, ua, ok, k.UAHTML)
		}
		var identity string
		if isNull(k.HmacID) {
			id, _ := jsonInt(k.UserID)
			identity = "u" + itoa(id)
		} else {
			ident := ""
			if k.Identifier != nil {
				ident = *k.Identifier
			}
			h, _ := jsonInt(k.HmacID)
			identity = "h" + itoa(h) + "_" + ident
		}
		if got := hlsConnectionKey(identity, k.StreamID, k.IP, ua); got != k.UUID {
			t.Errorf("%q: %s; want %s", k.UserAgent, got, k.UUID)
		}
	}
	if _, ok := htmlentitiesASCII("Mozilla/5.0 (é)"); ok {
		t.Fatal("a non-ASCII agent is PHP's to read")
	}
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

func TestRawurlencodeAndReconcileMatchThePanel(t *testing.T) {
	v := loadLive(t)
	for _, c := range v.Rawurlencode {
		in, _ := hex.DecodeString(c.InHex)
		if got := rawurlencode(string(in)); got != c.Out {
			t.Errorf("rawurlencode(%q) = %q; want %q", in, got, c.Out)
		}
	}
	for i, c := range v.Reconcile {
		seq, next := reconcile(c.Daemon, c.Floor, c.State, c.Now, c.Target)
		gb, _ := json.Marshal(next)
		wb, _ := json.Marshal(c.Next)
		if seq != c.Seq || string(gb) != string(wb) {
			t.Errorf("reconcile #%d: %d %s; want %d %s", i, seq, gb, c.Seq, wb)
		}
	}
}

var tokenInPlaylist = regexp.MustCompile(`/(hls|key)/([A-Za-z0-9_-]+)`)

// opened is a playlist with each token replaced by what it opens to.
func opened(t *testing.T, playlist string, keys Keys) string {
	return tokenInPlaylist.ReplaceAllStringFunc(playlist, func(m string) string {
		parts := tokenInPlaylist.FindStringSubmatch(m)
		plain, v := keys.Read(parts[2], true, time.Now().Unix())
		if v != Opened {
			t.Fatalf("a token that does not open: %s", m)
		}
		return "/" + parts[1] + "/{" + string(plain) + "}"
	})
}

func TestTokenizeMatchesThePanel(t *testing.T) {
	v := loadLive(t)
	dir := t.TempDir() + "/"
	iv, _ := hex.DecodeString(v.Playlist.IVHex)
	_ = os.WriteFile(dir+"9001_.iv", iv, 0o644)
	ctx, _ := hex.DecodeString(v.Context)
	for _, c := range v.Playlist.Cases {
		var secure, encrypt int
		var shared string
		_ = json.Unmarshal(c.Settings["secure_stream_tokens"], &secure)
		_ = json.Unmarshal(c.Settings["encrypt_hls"], &encrypt)
		_ = json.Unmarshal(c.Settings["live_streaming_pass"], &shared)
		p := &Policy{ServerID: v.ServerID, EncryptHLS: encrypt == 1}
		p.RawKeys.AcceptLegacyCBC = secure == 0
		p.Paths.Streams = dir
		p.Keys = Keys{Shared: []Key{{Value: []byte(shared)}}, Context: []Key{{Value: ctx}}}
		if c.ViewerKeyHex != nil {
			vk, _ := hex.DecodeString(*c.ViewerKeyHex)
			p.Keys.Viewer = []Key{{Value: vk}}
		}
		r := &Refresh{Stream: v.Playlist.StreamID, UUID: v.Playlist.UUID, Codec: v.Playlist.Codec, Playlist: v.Playlist.Input}
		if c.HmacID != nil {
			r.HMAC, r.Identifier = itoa(*c.HmacID), *c.Identifier
		} else {
			r.Username, r.Password = *c.Username, *c.Password
		}
		got, err := Tokenize(p, r, v.Playlist.IP, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if g, w := opened(t, got, p.Keys), opened(t, c.Output, p.Keys); g != w {
			t.Errorf("%s:\n%s\nwant\n%s", c.Name, g, w)
		}
	}
}

// ── JudgeLive ───────────────────────────────────────────────────────────

func liveTokenJSON(edit func(map[string]any)) string {
	t := map[string]any{
		"username": "user1", "password": "pass1", "stream_id": 12, "extension": "m3u8", "video_codec": "h264",
		"channel_info": map[string]any{"redirect_id": 3, "originator_id": nil, "on_demand": 0, "proxy": false, "pid": 4242},
		"user_info":    map[string]any{"id": 41, "max_connections": 2, "is_restreamer": 0},
		"uuid":         testUUID, "activity_start": 1799999000,
	}
	if edit != nil {
		edit(t)
	}
	b, _ := json.Marshal(t)
	return tok(string(b))
}

const testUA = "VLC/3.0.18 LibVLC/3.0.18"

func liveUUID() string { return hlsConnectionKey("u41", 12, "198.51.100.7", testUA) }

func livePolicy() *Policy {
	p := testPolicy()
	p.ConnStore, p.Live.UseBuffer = "agent", true
	return p
}

type liveEnvState struct {
	alive, onDisk, agentAnswers, agentAlive, fed bool
	playlist                                     string
	rec                                          map[string]any
	recUUID                                      string // the record's key in the agent
}

func (st *liveEnvState) env() LiveEnv {
	// The record as the agent's JSON, decoded once (as each real call would be, by the client).
	var recRaw map[string]json.RawMessage
	if st.rec != nil {
		b, _ := json.Marshal(st.rec)
		_ = json.Unmarshal(b, &recRaw)
	}
	return LiveEnv{
		StreamAlive: func(int, json.RawMessage) bool { return st.alive },
		OnDisk:      func(int) bool { return st.onDisk },
		Playlist:    func(int) string { return st.playlist },
		Fed:         func(int) bool { return st.fed },
		Record: func(uuid string) (map[string]json.RawMessage, bool, bool) {
			if !st.agentAnswers {
				return nil, false, false
			}
			if recRaw == nil || uuid != st.recUUID {
				return nil, false, true
			}
			return recRaw, true, true
		},
		AgentAlive: func() bool { return st.agentAlive },
	}
}

func goodLiveEnv() *liveEnvState {
	return &liveEnvState{alive: true, onDisk: true, agentAnswers: true, agentAlive: true, fed: true, recUUID: liveUUID(),
		playlist: "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:5\n#EXTINF:4.0,\n5.ts\n",
		rec:      map[string]any{"uuid": liveUUID(), "identity": "u41", "user_id": 41, "server_id": 3, "stream_id": 12, "container": "hls", "user_ip": "198.51.100.7", "hls_end": 0, "pid": 0}}
}

// goodTSEnv is a TS viewer's connection: the token's uuid, fanout's (pid 0).
func goodTSEnv() *liveEnvState {
	st := goodLiveEnv()
	st.recUUID, st.rec["uuid"], st.rec["container"] = testUUID, testUUID, "ts"
	return st
}

func tsTokenJSON(edit func(map[string]any)) string {
	return liveTokenJSON(func(m map[string]any) {
		m["extension"] = "ts"
		if edit != nil {
			edit(m)
		}
	})
}

func tsPolicy() *Policy {
	p := livePolicy()
	p.Live.TS, p.Live.ClientPrebuffer, p.Live.RestreamerPrebuffer, p.Live.SegTime = true, 3, 7, 10
	return p
}

func TestJudgeLive(t *testing.T) {
	ip := "198.51.100.7"
	cases := []struct {
		name   string
		token  string
		ua     string
		policy func(*Policy)
		env    func(*liveEnvState)
		want   Action
		reason string
	}{
		{"a known viewer's refresh", liveTokenJSON(nil), testUA, nil, nil, Serve, "playlist"},
		{"viewers in MAIN's store", liveTokenJSON(nil), testUA, func(p *Policy) { p.ConnStore = "php" }, nil, PHP, "conn-store"},
		{"a cookie per response", liveTokenJSON(nil), testUA, func(p *Policy) { p.Live.UniqueHeader = true }, nil, PHP, "unique-header"},
		{"a token this node does not open", tok("x"), testUA, nil, nil, PHP, "token"},
		{"an off-air token", liveTokenJSON(func(m map[string]any) { m["off_air"] = 1 }), testUA, nil, nil, PHP, "off-air"},
		{"an adaptive token", liveTokenJSON(func(m map[string]any) { m["adaptive"] = []int{1} }), testUA, nil, nil, PHP, "off-air"},
		{"a malformed uuid", liveTokenJSON(func(m map[string]any) { m["uuid"] = "../x" }), testUA, nil, nil, PHP, "token"},
		{"an expired token", liveTokenJSON(func(m map[string]any) { m["expires"] = testNow - 1 }), testUA, nil, nil, PHP, "expired"},
		{"TS, from a panel without its settings", liveTokenJSON(func(m map[string]any) { m["extension"] = "ts" }), testUA, nil, nil, PHP, "extension"},
		{"another container", liveTokenJSON(func(m map[string]any) { m["extension"] = "mp4" }), testUA, nil, nil, PHP, "extension"},
		{"a proxied channel", liveTokenJSON(func(m map[string]any) { m["channel_info"].(map[string]any)["proxy"] = 1 }), testUA, nil, nil, PHP, "proxy"},
		{"behind a proxy server", liveTokenJSON(func(m map[string]any) {
			ci := m["channel_info"].(map[string]any)
			ci["originator_id"], ci["redirect_id"] = 3, 9
		}), testUA, nil, nil, PHP, "proxy"},
		{"instant off", liveTokenJSON(func(m map[string]any) { m["channel_info"].(map[string]any)["on_demand"] = 1 }), testUA, func(p *Policy) { p.Live.InstantOff = true }, nil, PHP, "instant-off"},
		{"a producer that is not running", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.alive = false }, PHP, "stream-down"},
		{"no playlist on disk yet", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.onDisk = false }, PHP, "no-playlist"},
		{"the second-address rule", liveTokenJSON(nil), testUA, func(p *Policy) { p.Live.Disallow2ndIP = true }, nil, PHP, "second-ip"},
		{"the rule, a restreamer", liveTokenJSON(func(m map[string]any) { m["user_info"].(map[string]any)["is_restreamer"] = 1 }), testUA, func(p *Policy) { p.Live.Disallow2ndIP = true }, nil, Serve, "playlist"},
		{"the rule, above its maximum", liveTokenJSON(nil), testUA, func(p *Policy) { p.Live.Disallow2ndIP, p.Live.Disallow2ndIPMax = true, 1 }, nil, Serve, "playlist"},
		{"a non-ASCII agent", liveTokenJSON(nil), "Player (é)", nil, nil, PHP, "user-agent"},
		{"the agent does not answer", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.agentAnswers = false }, PHP, "agent"},
		{"no connection yet", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.rec = nil }, PHP, "new-connection"},
		{"another stream's connection", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.rec["stream_id"] = 13 }, PHP, "new-connection"},
		{"a closed connection", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.rec["hls_end"] = 1 }, PHP, "new-connection"},
		{"another address", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.rec["user_ip"] = "203.0.113.9" }, PHP, "ip"},
		{"a limit and no agent draining the spool", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.agentAlive = false }, PHP, "spool"},
		{"no limit: no spool needed", liveTokenJSON(func(m map[string]any) { m["user_info"].(map[string]any)["max_connections"] = 0 }), testUA, nil, func(s *liveEnvState) { s.agentAlive = false }, Serve, "playlist"},
		{"not on air", liveTokenJSON(nil), testUA, nil, func(s *liveEnvState) { s.playlist = "" }, PHP, "not-on-air"},
	}
	for _, c := range cases {
		p := livePolicy()
		if c.policy != nil {
			c.policy(p)
		}
		st := goodLiveEnv()
		if c.env != nil {
			c.env(st)
		}
		v, r := JudgeLive(p, c.token, ip, c.ua, testNow, st.env())
		if v.Action != c.want || v.Reason != c.reason {
			t.Errorf("%s: %s (%s); want %s (%s)", c.name, v.Action, v.Reason, c.want, c.reason)
		}
		if v.Action == Serve && (r == nil || r.UUID != liveUUID() || string(r.ServerID) != "3" || r.UserID != 41) {
			t.Errorf("%s: refresh %+v", c.name, r)
		}
	}
}

func TestJudgeLiveTS(t *testing.T) {
	ip := "198.51.100.7"
	cases := []struct {
		name      string
		token     string
		policy    func(*Policy)
		env       func(*liveEnvState)
		want      Action
		reason    string
		prebuffer int64
	}{
		{"a known viewer's reconnect", tsTokenJSON(nil), nil, nil, Serve, "ts", 3},
		{"a restreamer", tsTokenJSON(func(m map[string]any) { m["user_info"].(map[string]any)["is_restreamer"] = 1 }), nil, nil, Serve, "ts", 7},
		{"a restreamer's link asking for a prebuffer", tsTokenJSON(func(m map[string]any) {
			m["user_info"].(map[string]any)["is_restreamer"], m["prebuffer"] = 1, 1
		}), nil, nil, Serve, "ts", 10},
		{"a viewer's link asking for one", tsTokenJSON(func(m map[string]any) { m["prebuffer"] = 1 }), nil, nil, Serve, "ts", 3},
		{"a proxied channel (PHP registers its source)", tsTokenJSON(func(m map[string]any) { m["channel_info"].(map[string]any)["proxy"] = 1 }), nil, nil, PHP, "proxy", 0},
		{"no uuid in the token", tsTokenJSON(func(m map[string]any) { delete(m, "uuid") }), nil, nil, PHP, "token", 0},
		{"the first request", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.rec = nil }, PHP, "new-connection", 0},
		{"an HLS record under the uuid", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.rec["container"] = "hls" }, PHP, "new-connection", 0},
		{"an ended connection (PHP re-opens it)", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.rec["hls_end"] = 1 }, PHP, "new-connection", 0},
		{"a PHP worker feeding it", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.rec["pid"] = 777 }, PHP, "worker", 0},
		{"another address", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.rec["user_ip"] = "203.0.113.9" }, PHP, "ip", 0},
		{"another address, not restricted", tsTokenJSON(nil), func(p *Policy) { p.RestrictSameIP = false }, func(s *liveEnvState) { s.rec["user_ip"] = "203.0.113.9" }, Serve, "ts", 3},
		{"the second-address rule", tsTokenJSON(nil), func(p *Policy) { p.Live.Disallow2ndIP = true }, nil, PHP, "second-ip", 0},
		{"instant off", tsTokenJSON(func(m map[string]any) { m["channel_info"].(map[string]any)["on_demand"] = 1 }), func(p *Policy) { p.Live.InstantOff = true }, nil, PHP, "instant-off", 0},
		{"a producer that is not running", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.alive = false }, PHP, "stream-down", 0},
		{"no playlist on disk yet", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.onDisk = false }, PHP, "no-playlist", 0},
		{"fanout not fed", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.fed = false }, PHP, "not-on-air", 0},
		{"a limit and no agent draining the spool", tsTokenJSON(nil), nil, func(s *liveEnvState) { s.agentAlive = false }, PHP, "spool", 0},
		{"viewers in MAIN's store", tsTokenJSON(nil), func(p *Policy) { p.ConnStore = "php" }, nil, PHP, "conn-store", 0},
	}
	for _, c := range cases {
		p := tsPolicy()
		if c.policy != nil {
			c.policy(p)
		}
		st := goodTSEnv()
		if c.env != nil {
			c.env(st)
		}
		v, r := JudgeLive(p, c.token, ip, testUA, testNow, st.env())
		if v.Action != c.want || v.Reason != c.reason {
			t.Errorf("%s: %s (%s); want %s (%s)", c.name, v.Action, v.Reason, c.want, c.reason)
			continue
		}
		if v.Action == Serve && (r == nil || !r.TS || r.UUID != testUUID || r.Prebuffer != c.prebuffer || r.Codec != "h264" || r.UserAgent != testUA) {
			t.Errorf("%s: reconnect %+v", c.name, r)
		}
	}
}

// ── A refresh served ────────────────────────────────────────────────────

func TestServeLiveAnswersAsLivePHPDoes(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"cons", "streams", "signals", "spool"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	flows := filepath.Join(dir, "flows.json")
	_ = os.WriteFile(flows, []byte("{}"), 0o644)
	st := goodLiveEnv()
	a := &fakeAgent{records: map[string]map[string]any{liveUUID(): st.rec}}
	sock := startAgent(t, a)
	path := writeServePolicy(t, dir, "segments+playlist", func(doc map[string]any) {
		doc["conn_store"] = "agent"
		doc["time_offset"] = 10
		doc["live"] = map[string]any{"use_buffer": false}
		paths := doc["paths"].(map[string]any)
		paths["agent_sock"], paths["streams"], paths["signals"], paths["spool"], paths["flows"] = sock, dir+"/streams/", dir+"/signals/", dir+"/spool", flows
		paths["cons"] = dir + "/cons"
	})
	startProducer(t, filepath.Join(dir, "streams"))
	node := &fakeFiles{playlists: map[string]string{"12": st.playlist}}
	s := NewServer(path, http.NotFoundHandler(), node)

	req := httptest.NewRequest(http.MethodGet, "/stream/live?token=x", nil)
	req.Header.Set("X-XC-Original-URI", "/auth/"+liveTokenJSON(nil))
	req.Header.Set("X-XC-Client-IP", "198.51.100.7")
	req.Header.Set("User-Agent", testUA)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/x-mpegurl" || rec.Header().Get("Cache-Control") != "no-store, no-cache, must-revalidate" || rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("refresh: %d %v %s; stats %v", rec.Code, rec.Header(), rec.Body.String(), s.Stats())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:") || strings.Contains(body, "\n5.ts") {
		t.Fatalf("playlist: %s", body)
	}
	if len(a.puts) != 1 || a.records[liveUUID()]["hls_end"] != float64(0) || a.records[liveUUID()]["proxy_id"] != nil {
		t.Fatalf("the record refreshed in the agent: %v %v", a.puts, a.records[liveUUID()])
	}
	spooled, _ := filepath.Glob(filepath.Join(dir, "spool", "p0", "*.ndjson"))
	if len(spooled) != 1 || !strings.Contains(string(mustRead(t, spooled[0])), `"type":"conn.limit"`) {
		t.Fatalf("conn.limit spooled for MAIN: %v", spooled)
	}
	if _, err := os.Stat(filepath.Join(dir, "signals", "hlsseq_12")); err != nil {
		t.Fatalf("the sequence's state kept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cons", liveUUID())); err != nil {
		t.Fatalf("the viewer's marker touched: %v", err)
	}
}

// startProducer runs stream 12's producer: a process whose command line
// names the stream (isStreamAlive reads it), in the stream's pid file; the
// on-disk playlist exists.
func startProducer(t *testing.T, streams string) {
	t.Helper()
	producer := exec.Command("sleep", "12")
	if err := producer.Start(); err != nil {
		t.Skip("no sleep binary: ", err)
	}
	t.Cleanup(func() { _ = producer.Process.Kill(); _ = producer.Wait() })
	// Start returns once exec is under way; the command line isStreamAlive
	// reads can still be empty for a moment after.
	for deadline := time.Now().Add(3 * time.Second); !isStreamAlive(int64(producer.Process.Pid), 12); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			cmd, _ := os.ReadFile("/proc/" + itoa(int64(producer.Process.Pid)) + "/cmdline")
			t.Fatalf("the producer never looked alive: cmdline %q", cmd)
		}
	}
	_ = os.WriteFile(filepath.Join(streams, "12_.pid"), []byte(itoa(int64(producer.Process.Pid))+"\n"), 0o644)
	_ = os.WriteFile(filepath.Join(streams, "12_.m3u8"), []byte("#EXTM3U\n"), 0o644)
}

// A TS viewer's reconnect: the record refreshed as live.php's TS arm does,
// then the stream from fanout's /live/<id> with the viewer's uuid, prebuffer
// and codec, as its X-Accel-Redirect to /xc_fanout/<id> hands it.
func TestServeTSReconnectAnswersAsLivePHPDoes(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{"cons", "streams", "signals", "spool"} {
		_ = os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	flows := filepath.Join(dir, "flows.json")
	_ = os.WriteFile(flows, []byte("{}"), 0o644)
	st := goodTSEnv()
	st.rec["server_id"], st.rec["proxy_id"] = 3, nil
	a := &fakeAgent{records: map[string]map[string]any{testUUID: st.rec}}
	sock := startAgent(t, a)
	path := writeServePolicy(t, dir, "segments+playlist", func(doc map[string]any) {
		doc["conn_store"] = "agent"
		doc["time_offset"] = 10
		doc["live"] = map[string]any{"use_buffer": true, "ts": true, "client_prebuffer": 4, "restreamer_prebuffer": 8, "seg_time": 10}
		paths := doc["paths"].(map[string]any)
		paths["agent_sock"], paths["streams"], paths["signals"], paths["spool"], paths["flows"] = sock, dir+"/streams/", dir+"/signals/", dir+"/spool", flows
		paths["cons"] = dir + "/cons"
	})
	startProducer(t, filepath.Join(dir, "streams"))
	var asked *http.Request
	fanout := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = r
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write([]byte("TS"))
	})
	s := NewServer(path, fanout, &fakeFiles{playlists: map[string]string{"12": st.playlist}})

	req := httptest.NewRequest(http.MethodGet, "/stream/live?token=x", nil)
	req.Header.Set("X-XC-Original-URI", "/auth/"+tsTokenJSON(nil))
	req.Header.Set("X-XC-Client-IP", "198.51.100.7")
	req.Header.Set("User-Agent", testUA)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.String() != "TS" || rec.Header().Get("Content-Type") != "video/mp2t" || rec.Header().Get("X-Accel-Buffering") != "no" || rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("reconnect: %d %v %q; stats %v", rec.Code, rec.Header(), rec.Body.String(), s.Stats())
	}
	if asked == nil || asked.URL.Path != "/live/12" || asked.URL.Query().Get("c") != testUUID || asked.URL.Query().Get("prebuffer") != "4" || asked.URL.Query().Get("vc") != "h264" {
		t.Fatalf("fanout asked for %v", asked)
	}
	got := a.records[testUUID]
	if len(a.puts) != 1 || got["pid"] != float64(0) || got["hls_end"] != float64(0) || got["server_id"] != float64(3) || got["container"] != "ts" {
		t.Fatalf("the record refreshed in the agent: %v %v", a.puts, got)
	}
	if last, ok := got["hls_last_read"].(float64); !ok || int64(last) < time.Now().Unix()-20 {
		t.Fatalf("hls_last_read: %v", got["hls_last_read"])
	}
	spooled, _ := filepath.Glob(filepath.Join(dir, "spool", "p0", "*.ndjson"))
	if len(spooled) != 1 || !strings.Contains(string(mustRead(t, spooled[0])), `"uuid":"`+testUUID+`"`) {
		t.Fatalf("conn.limit spooled for MAIN: %v", spooled)
	}
	if _, err := os.Stat(filepath.Join(dir, "cons", testUUID)); !os.IsNotExist(err) {
		t.Fatalf("a TS viewer has no marker, as after live.php's hand-off: %v", err)
	}
	if s.Stats()["live serve ts"] != 1 {
		t.Fatalf("stats: %v", s.Stats())
	}

	// The first request of a session stays PHP's: no record yet.
	delete(a.records, testUUID)
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Header().Get("X-Accel-Redirect") != "@gw_live_php" {
		t.Fatalf("first request: %d %v", rec.Code, rec.Header())
	}
}

func mustRead(t *testing.T, path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// What a live token carries, once it opens: no field, type or shape may panic the judge.
func FuzzLiveToken(f *testing.F) {
	f.Add(`{"stream_id":12,"extension":"m3u8","channel_info":{"redirect_id":3},"user_info":{"id":41}}`)
	f.Add(`{"hmac_id":"5","identifier":7,"extension":"m3u8","channel_info":{"x":1},"user_info":{},"stream_id":"12"}`)
	f.Add(`{"extension":"m3u8","channel_info":[],"user_info":null}`)
	f.Add(`[]`)
	f.Add(`{"stream_id":12,"extension":"ts","uuid":7,"prebuffer":[1],"channel_info":{"redirect_id":3},"user_info":{"id":41,"is_restreamer":"1"}}`)
	p := tsPolicy()
	f.Fuzz(func(t *testing.T, plain string) {
		JudgeLive(p, tok(plain), "198.51.100.7", testUA, testNow, goodLiveEnv().env())
	})
}
