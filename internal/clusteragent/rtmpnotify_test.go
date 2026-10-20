package clusteragent

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// rtmpHandlerRig is an agent that answers /stream/rtmp: a real registry, a fake
// MAIN that answers rtmp_auth as the test asks (rig.auth; nil = a verified
// refusal), a stub fanout control socket (stream 100 held+running, 200
// held+idle, others unknown), and a secrets.json carrying live_streaming_pass.
type rtmpHandlerRig struct {
	a    *Agent
	f    *fakeMain
	auth func(w http.ResponseWriter, reqCtx []byte)
}

func newRtmpHandlerRig(t *testing.T) *rtmpHandlerRig {
	t.Helper()
	csock := filepath.Join(t.TempDir(), "ctl.sock")
	cl, err := net.Listen("unix", csock)
	if err != nil {
		t.Fatal(err)
	}
	csrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/streams/100":
			_ = json.NewEncoder(w).Encode(map[string]any{"running": true, "has_data": true})
		case "/streams/200":
			_ = json.NewEncoder(w).Encode(map[string]any{"running": false, "has_data": false})
		case "/streams/200/ensure", "/streams/100/ensure":
			_ = json.NewEncoder(w).Encode(map[string]any{"running": true})
		default:
			http.NotFound(w, r)
		}
	})}
	go csrv.Serve(cl)
	t.Cleanup(func() { csrv.Close() })

	f, st := newFake(t) // ServerID = 3
	rig := &rtmpHandlerRig{f: f}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		if rig.auth != nil {
			rig.auth(w, reqCtx)
			return
		}
		f.refuse(w, 404, nonce, "NO", nil) // a verified refusal → *Denial → REFUSED
	}
	msrv := httptest.NewServer(f)
	t.Cleanup(msrv.Close)
	st.MainURLs = []string{msrv.URL + "/cluster/v1/"}

	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, FanoutCtl: csock, ReplicaDir: t.TempDir()}
	reg, _, _ := newTestRegistry(t)
	reg.now = time.Now
	a.Registry, reg.Admit = reg, a.admit
	a.flows.Store(FlowConnections)
	a.state.Store("active")
	doc, _ := json.Marshal(map[string]any{"etag": "e", "data": map[string]any{"live_streaming_pass": "s3cret"}})
	if err := os.WriteFile(filepath.Join(a.ReplicaDir, "secrets.json"), doc, 0o600); err != nil {
		t.Fatal(err)
	}
	rig.a = a
	return rig
}

func (rig *rtmpHandlerRig) okAuth() {
	rig.auth = func(w http.ResponseWriter, reqCtx []byte) {
		rig.f.box(w, reqCtx, map[string]any{
			"ok":           true,
			"country_code": "US",
			"user":         map[string]any{"id": 7, "max_connections": 2, "con_isp_name": "isp", "exp_date": 1800000000 + 3600},
			"token":        map[string]any{"uuid": "u1", "adm": map[string]any{"exp": 1800000000 + 600, "sid": 3}, "adm_uuid": "au1", "prf": map[string]any{"iat": 1800000000, "p": strings.Repeat("ab", 16)}},
		})
	}
}

func (rig *rtmpHandlerRig) do(query string, header map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/stream/rtmp?"+query, nil)
	for k, v := range header {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	rig.a.serveRtmpNotify(rec, r)
	return rec
}

func TestRtmpLoopbackPublishFastPath(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	if rec := rig.do("addr=127.0.0.1&clientid=1&call=publish&name=100", nil); rec.Code != 200 {
		t.Fatalf("loopback publish: %d", rec.Code)
	}
}

func TestRtmpPublishGate(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	if rec := rig.do("addr=203.0.113.9&clientid=1&call=publish&name=100&password=s3cret", nil); rec.Code != 200 {
		t.Fatalf("publish right password: %d", rec.Code)
	}
	if rec := rig.do("addr=203.0.113.9&clientid=1&call=publish&name=100&password=wrong", nil); rec.Code != 404 {
		t.Fatalf("publish wrong password: %d, want 404", rec.Code)
	}
}

func TestRtmpPlayLineAuth(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	rig.okAuth()
	rec := rig.do("addr=203.0.113.9&clientid=7&call=play&name=100&tcurl=rtmp://h/live&app=live&username=u&password=p", nil)
	if rec.Code != 200 {
		t.Fatalf("valid line: %d", rec.Code)
	}
	if rig.a.Registry.Get(rtmpUUID(3, "7")) == nil {
		t.Fatal("a line viewer should have a record")
	}
}

func TestRtmpPlayAuthFailedTicksFlood(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	rig.auth = func(w http.ResponseWriter, reqCtx []byte) {
		rig.f.box(w, reqCtx, map[string]any{"ok": false, "reason": "AUTH_FAILED"})
	}
	if rec := rig.do("addr=203.0.113.9&clientid=8&call=play&name=100&tcurl=rtmp://h/live&app=live&username=u&password=p", nil); rec.Code != 404 {
		t.Fatalf("AUTH_FAILED: %d", rec.Code)
	}
	if rig.a.rtmpFloodCount("203.0.113.9") != 1 {
		t.Fatal("AUTH_FAILED should tick the flood guard")
	}
}

func TestRtmpPlayRefusedNoFlood(t *testing.T) {
	rig := newRtmpHandlerRig(t) // rig.auth nil → the fake MAIN refuses (a *Denial)
	if rec := rig.do("addr=203.0.113.9&clientid=9&call=play&name=100&tcurl=rtmp://h/live&app=live&username=u&password=p", nil); rec.Code != 404 {
		t.Fatalf("REFUSED: %d", rec.Code)
	}
	if rig.a.rtmpFloodCount("203.0.113.9") != 0 {
		t.Fatal("a REFUSED must not tick the flood guard")
	}
}

func TestRtmpPlayRelayBypass(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	rec := rig.do("addr=203.0.113.9&clientid=10&call=play&name=100&tcurl=rtmp://h/live&app=live&password=s3cret", nil)
	if rec.Code != 200 {
		t.Fatalf("relay bypass: %d", rec.Code)
	}
	if rig.a.Registry.Get(rtmpUUID(3, "10")) != nil {
		t.Fatal("a relay opens no line record")
	}
}

func TestRtmpPlayNotHeld(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	rig.okAuth()
	rec := rig.do("addr=203.0.113.9&clientid=11&call=play&name=999&tcurl=rtmp://h/live&app=live&username=u&password=p", nil)
	if rec.Code != 404 {
		t.Fatalf("line viewer of an unheld stream: %d, want 404", rec.Code)
	}
}

func TestRtmpPlayDoneClosesRecord(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	uuid := rtmpUUID(3, "12")
	rig.a.Registry.Put(uuid, map[string]any{"uuid": uuid, "pid": "12", "stream_id": 100, "user_id": 7, "hls_end": 0})
	if rec := rig.do("addr=203.0.113.9&clientid=12&call=play_done&name=100", nil); rec.Code != 200 {
		t.Fatalf("play_done: %d", rec.Code)
	}
	if r := rig.a.Registry.Get(uuid); r == nil || r["hls_end"] != 1 {
		t.Fatalf("play_done did not close the record: %v", r)
	}
}

func TestRtmpParseFailure(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	if rec := rig.do("call=play&call=publish&name=100", nil); rec.Code != 404 {
		t.Fatalf("a differing-duplicate call must 404: %d", rec.Code)
	}
}

// rtmp.php keys restream-detect on the header's PRESENCE (any value), so a
// present-but-empty header still means restream.
func TestRtmpRestreamDetectPresence(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/stream/rtmp", nil)
	r.Header.Set("X-XC_VM-DETECT", "")
	if !rtmpRestreamDetect(r) {
		t.Fatal("a present (even empty) X-XC_VM-DETECT means restream")
	}
	if rtmpRestreamDetect(httptest.NewRequest(http.MethodGet, "/stream/rtmp", nil)) {
		t.Fatal("an absent header is not restream")
	}
}

func TestRtmpFloodBounded(t *testing.T) {
	a := &Agent{Logf: t.Logf}
	for i := 0; i < rtmpFloodMax+100; i++ {
		a.rtmpFlood(fmt.Sprintf("10.0.%d.%d", i/256, i%256))
	}
	a.rtmpFloodMu.Lock()
	n := len(a.rtmpFloodN)
	a.rtmpFloodMu.Unlock()
	if n > rtmpFloodMax {
		t.Fatalf("flood map grew to %d, cap %d", n, rtmpFloodMax)
	}
}

func TestRtmpPublishAllowedRTMPNoStreamPass(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	writeBlocklist(t, rig.a.ReplicaDir, map[string]any{"ip": []string{},
		"rtmp": []map[string]any{{"id": 1, "ip": "203.0.113.5", "password": "", "push": 1, "pull": 0}}})
	rec := rig.do("addr=203.0.113.5&clientid=1&call=publish&name=100", nil) // no password
	if rec.Code != 200 {
		t.Fatalf("push-listed publish without stream pass: %d", rec.Code)
	}
}

func TestRtmpPublishAllowedRTMPEntryPassword(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	writeBlocklist(t, rig.a.ReplicaDir, map[string]any{"ip": []string{},
		"rtmp": []map[string]any{{"id": 1, "ip": "203.0.113.5", "password": "pub-pw", "push": 1, "pull": 0}}})
	if rig.do("addr=203.0.113.5&clientid=1&call=publish&name=100&password=pub-pw", nil).Code != 200 {
		t.Fatal("correct entry password must admit")
	}
	if rig.do("addr=203.0.113.5&clientid=1&call=publish&name=100&password=wrong", nil).Code != 404 {
		t.Fatal("wrong entry password must deny")
	}
}

func TestRtmpPublishPushFlagRequired(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	writeBlocklist(t, rig.a.ReplicaDir, map[string]any{"ip": []string{},
		"rtmp": []map[string]any{{"id": 1, "ip": "203.0.113.5", "password": "", "push": 0, "pull": 1}}})
	if rig.do("addr=203.0.113.5&clientid=1&call=publish&name=100", nil).Code != 404 {
		t.Fatal("pull-only entry must not admit a publish")
	}
}

func TestRtmpPlayRelayAllowedRTMPPull(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	writeBlocklist(t, rig.a.ReplicaDir, map[string]any{"ip": []string{},
		"rtmp": []map[string]any{{"id": 1, "ip": "203.0.113.5", "password": "", "push": 0, "pull": 1}}})
	// relay-bypass arm: play with no line creds, pull-listed IP, running stream 100
	rec := rig.do("addr=203.0.113.5&clientid=1&call=play&name=100", nil)
	if rec.Code != 200 {
		t.Fatalf("pull-listed relay play: %d", rec.Code)
	}
}

func TestRtmpRelayOnDemandGate(t *testing.T) {
	// relay-bypass arm, held+not-running stream 200
	run := func(onDemand int) int {
		rig := newRtmpHandlerRig(t)
		writeStreamRecord(t, rig.a.ReplicaDir, "200", map[string]any{"server": map[string]any{"on_demand": onDemand}})
		return rig.do("addr=1.2.3.4&clientid=1&call=play&name=200&password=s3cret", nil).Code // stream-pass relay
	}
	if run(1) != 200 {
		t.Fatal("held+idle+on_demand=1 relay must start and admit")
	}
	if run(0) != 404 {
		t.Fatal("held+idle+on_demand!=1 relay must 404")
	}
}

func TestRtmpLineOnDemandGate(t *testing.T) {
	run := func(onDemand int) int {
		rig := newRtmpHandlerRig(t)
		rig.okAuth()
		writeStreamRecord(t, rig.a.ReplicaDir, "200", map[string]any{"server": map[string]any{"on_demand": onDemand}})
		return rig.do("addr=1.2.3.4&clientid=1&call=play&name=200&tcurl=rtmp://h/live&app=live&username=u&password=p", nil).Code
	}
	if run(1) != 200 {
		t.Fatal("held+idle+on_demand=1 line must start and admit")
	}
	if run(0) != 404 {
		t.Fatal("held+idle+on_demand!=1 line must 404")
	}
}

func TestRtmpPlayNotHeldUnchanged(t *testing.T) {
	// name=999 → control socket 404 → not held; relay 200, line 404 (unchanged, rtmp.php:117)
	rig := newRtmpHandlerRig(t)
	if rig.do("addr=1.2.3.4&clientid=1&call=play&name=999&password=s3cret", nil).Code != 200 {
		t.Fatal("not-held relay stays 200")
	}
	rig2 := newRtmpHandlerRig(t)
	rig2.okAuth()
	if rig2.do("addr=1.2.3.4&clientid=1&call=play&name=999&tcurl=rtmp://h/live&app=live&username=u&password=p", nil).Code != 404 {
		t.Fatal("not-held line stays 404")
	}
}

func TestRtmpBlockedIPRefusedAtTop(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	rig.a.FloodDir = t.TempDir()
	rig.okAuth() // would admit if reached
	if err := os.WriteFile(filepath.Join(rig.a.FloodDir, "block_203.0.113.9"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	rec := rig.do("addr=203.0.113.9&clientid=7&call=play&name=100&tcurl=rtmp://h/live&app=live&username=u&password=p", nil)
	if rec.Code != 404 {
		t.Fatalf("blocked IP must be refused: %d", rec.Code)
	}
	if rig.a.Registry.Get(rtmpUUID(3, "7")) != nil {
		t.Fatal("blocked IP must not reach auth / open a record")
	}
}

func TestRtmpBlockedInvalidIPNoCrash(t *testing.T) {
	rig := newRtmpHandlerRig(t)
	rig.a.FloodDir = t.TempDir()
	if rig.do("addr=not-an-ip&clientid=7&call=publish&name=100&password=s3cret", nil).Code == 0 {
		t.Fatal("a non-IP addr must not crash the handler")
	}
}
