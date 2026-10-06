package clusteragent

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// box answers with a MAC'd, BOXed reply under the session's down keys.
func (f *fakeMain) box(w http.ResponseWriter, reqCtx []byte, doc any) {
	ts := uint64(time.Now().UnixMilli())
	rn := make([]byte, 16)
	resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, ts, rn)
	plain, _ := json.Marshal(doc)
	body, _ := cc.Box(f.keys.EncDown, resCtx, plain)
	w.Header().Set("Content-Type", octet)
	w.Header().Set(cc.HTs, strconv.FormatUint(ts, 10))
	w.Header().Set(cc.HNonce, hex.EncodeToString(rn))
	w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(f.keys.MacDown, resCtx, body)))
	w.Write(body)
}

// refuse answers with a panel-signed denial naming this node and request.
func (f *fakeMain) refuse(w http.ResponseWriter, status int, nonce []byte, reason string, extra map[string]any) {
	doc := map[string]any{"v": 1, "typ": "xcvm-denial", "reason": reason, "node": f.uuid, "req_nonce": hex.EncodeToString(nonce), "main_time_ms": time.Now().UnixMilli()}
	for k, v := range extra {
		doc[k] = v
	}
	b, _ := json.Marshal(doc)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("den", b))))
	w.WriteHeader(status)
	w.Write(b)
}

// opened is the JSON body of a request, opened with the session's up keys.
func (f *fakeMain) opened(r *http.Request, reqCtx []byte) map[string]any {
	body, _ := io.ReadAll(r.Body)
	plain, err := cc.Unbox(f.keys.EncUp, reqCtx, body)
	if err != nil {
		return nil
	}
	var out map[string]any
	json.Unmarshal(plain, &out)
	return out
}

type admitMain struct {
	*fakeMain
	calls atomic.Int32
	last  atomic.Value // map[string]any: the last conn_admit payload
	reply func(w http.ResponseWriter, reqCtx, nonce []byte)
}

func newAdmitAgent(t *testing.T) (*admitMain, *Agent, *sink, *httptest.Server) {
	f, st := newFake(t)
	m := &admitMain{fakeMain: f}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		if !strings.HasSuffix(r.URL.Path, "/conn_admit") {
			http.Error(w, "unexpected", 500)
			return
		}
		m.calls.Add(1)
		m.last.Store(f.opened(r, reqCtx))
		m.reply(w, reqCtx, nonce)
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	reg, s, _ := newTestRegistry(t)
	reg.now = time.Now
	a.Registry, reg.Admit = reg, a.admit
	a.flows.Store(FlowConnections)
	a.state.Store("active")
	sock := httptest.NewServer(a.socketHandler())
	t.Cleanup(sock.Close)
	return m, a, s, sock
}

func putViewer(t *testing.T, sock *httptest.Server, uuid string, rec map[string]any, header string) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(rec)
	req, _ := http.NewRequest(http.MethodPut, sock.URL+"/v1/conn/"+uuid, strings.NewReader(string(b)))
	if header != "" {
		req.Header.Set(AdmissionHeader, header)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func viewer(line int, ip, ua string) map[string]any {
	return map[string]any{"user_id": line, "stream_id": 7, "server_id": 3, "user_ip": ip, "user_agent": ua, "container": "ts", "hls_end": 0, "pid": 0}
}

func TestParseAdmissionFollowsTheContract(t *testing.T) {
	cases := map[string]bool{
		``:                                     false,
		`[]`:                                   false,
		`{"line_id":42,"max_connections":1}`:   true,
		`{"line_id":"42","max_connections":1}`: false, // a string where an int belongs
		`{"line_id":42.5,"max_connections":1}`: false,
		`{"line_id":42,"max_connections":0}`:   false,
		`{"line_id":42}`:                       false,
		`{"hmac_id":3,"identifier":"x","max_connections":2}`:              true,
		`{"hmac_id":3,"max_connections":2}`:                               false, // no identifier
		`{"line_id":42,"hmac_id":3,"identifier":"x","max_connections":2}`: false, // two identities
		`{"line_id":0,"hmac_id":3,"identifier":"x","max_connections":2}`:  true,
		`{"line_id":42,"max_connections":1} {}`:                           false,
	}
	for h, want := range cases {
		if got := parseAdmission(h) != nil; got != want {
			t.Errorf("%s: parsed %v, want %v", h, got, want)
		}
	}
	if r := parseAdmission(`{"line_id":42,"max_connections":1,"adm":{"exp":"1","sid":3}}`); r == nil || r.adm {
		t.Errorf("a malformed adm must be ignored, not refuse the header: %+v", r)
	}
	if r := parseAdmission(`{"line_id":42,"max_connections":1,"adm":{"exp":1800000000,"sid":3}}`); r == nil || !r.adm || r.admExp != 1800000000 {
		t.Errorf("adm: %+v", r)
	}
}

func TestAdmissionWithAClaimMakesNoWANCall(t *testing.T) {
	m, a, s, sock := newAdmitAgent(t)
	m.reply = func(w http.ResponseWriter, _, nonce []byte) { m.refuse(w, 503, nonce, "DB", nil) }
	a.setOfflineAdmission(OfflineDeny)
	exp := time.Now().Unix() + 60
	h := `{"adm":{"exp":` + strconv.FormatInt(exp, 10) + `,"sid":3},"line_id":42,"stream_id":7,"max_connections":1,"ip":"1.2.3.4","ua":"VLC"}`
	rec := viewer(42, "1.2.3.4", "VLC")
	rec["adm_uuid"] = "reserved-at-mint"
	if st, _ := putViewer(t, sock, "v1", rec, h); st != 200 || m.calls.Load() != 0 {
		t.Fatalf("a live claim: status %d, %d conn_admit calls", st, m.calls.Load())
	}
	// adm_uuid is a record key like any other: stored and mirrored.
	if got := a.Registry.Get("v1")["adm_uuid"]; got != "reserved-at-mint" {
		t.Fatalf("adm_uuid stored as %v", got)
	}
	if d := s.events[0]["d"].(map[string]any)["record"].(map[string]any); d["adm_uuid"] != "reserved-at-mint" {
		t.Fatalf("adm_uuid not mirrored: %v", d)
	}
	// An expired claim asks MAIN, and here gets the offline policy.
	h = strings.Replace(h, strconv.FormatInt(exp, 10), strconv.FormatInt(time.Now().Unix()-5, 10), 1)
	st, out := putViewer(t, sock, "v2", viewer(42, "5.6.7.8", "VLC"), h)
	if st != 403 || out["admit"] != false || out["reason"] != "OFFLINE" || m.calls.Load() != 1 {
		t.Fatalf("an expired claim: %d %v (%d calls)", st, out, m.calls.Load())
	}
	if a.Registry.Get("v2") != nil || len(s.events) != 1 {
		t.Fatal("a refused viewer was stored or mirrored")
	}
}

func TestAdmissionAsksMainAndFollowsItsAnswer(t *testing.T) {
	m, a, s, sock := newAdmitAgent(t)
	h := `{"line_id":42,"stream_id":7,"max_connections":2,"ip":"1.2.3.4","ua":"VLC"}`

	m.reply = func(w http.ResponseWriter, reqCtx, _ []byte) {
		m.box(w, reqCtx, map[string]any{"admit": true, "exp": time.Now().Unix() + 30, "main_time_ms": time.Now().UnixMilli()})
	}
	if st, _ := putViewer(t, sock, "v1", viewer(42, "1.2.3.4", "VLC"), h); st != 200 {
		t.Fatalf("admitted: %d", st)
	}
	sent := m.last.Load().(map[string]any)
	if sent["uuid"] != "v1" || sent["line_id"] != float64(42) || sent["stream_id"] != float64(7) || sent["ip"] != "1.2.3.4" || sent["ua"] != "VLC" {
		t.Fatalf("conn_admit payload %v", sent)
	}
	if _, ok := sent["max_connections"]; ok {
		t.Fatal("max_connections went to MAIN")
	}
	if _, ok := sent["hmac_id"]; ok {
		t.Fatal("the other identity went to MAIN")
	}
	// The admitting answer holds for that uuid until its exp.
	putViewer(t, sock, "v1", viewer(42, "1.2.3.4", "VLC"), h)
	if m.calls.Load() != 1 {
		t.Fatalf("a cached admission asked again (%d calls)", m.calls.Load())
	}

	// A MAC'd refusal refuses with MAIN's reason and records nothing.
	m.reply = func(w http.ResponseWriter, reqCtx, _ []byte) {
		m.box(w, reqCtx, map[string]any{"admit": false, "exp": 0, "reason": "EXPIRED"})
	}
	n := len(s.events)
	if st, out := putViewer(t, sock, "v3", viewer(42, "9.9.9.9", "VLC"), h); st != 403 || out["reason"] != "EXPIRED" {
		t.Fatalf("refused: %d %v", st, out)
	}
	if a.Registry.Get("v3") != nil || len(s.events) != n {
		t.Fatal("a refused viewer was recorded")
	}
	// A refused uuid already held, such as an ended HLS record, stays as it was.
	a.Registry.Seed([]map[string]any{{"uuid": "held", "user_id": 42, "hls_end": 1}}, false)
	if st, _ := putViewer(t, sock, "held", viewer(42, "9.9.9.9", "VLC"), h); st != 403 || num(a.Registry.Get("held")["hls_end"]) != 1 {
		t.Fatalf("a refused re-register changed the held record: %d %v", st, a.Registry.Get("held"))
	}

	// An HMAC identity sends hmac_id and identifier, not line_id.
	putViewer(t, sock, "v4", map[string]any{"hmac_id": 3, "hmac_identifier": "dev-1", "user_ip": "1.1.1.1"}, `{"hmac_id":3,"identifier":"dev-1","max_connections":1}`)
	if sent := m.last.Load().(map[string]any); sent["hmac_id"] != float64(3) || sent["identifier"] != "dev-1" || sent["line_id"] != nil {
		t.Fatalf("hmac payload %v", sent)
	}
}

func TestAdmissionDenialsThatAdmitAndThatApplyTheOfflinePolicy(t *testing.T) {
	m, a, _, sock := newAdmitAgent(t)
	a.setOfflineAdmission(OfflineDeny)
	h := `{"line_id":42,"max_connections":1}`
	for i, reason := range []string{"NOT_ACTIVE", "FLOW_OFF", "BAD_REQUEST"} {
		m.reply = func(w http.ResponseWriter, _, nonce []byte) { m.refuse(w, 409, nonce, reason, nil) }
		if st, _ := putViewer(t, sock, "a"+strconv.Itoa(i), viewer(42, "1.1.1.1", "x"), h); st != 200 {
			t.Errorf("%s: %d, want admitted", reason, st)
		}
	}
	offline := map[string]func(w http.ResponseWriter, reqCtx, nonce []byte){
		"DB":            func(w http.ResponseWriter, _, nonce []byte) { m.refuse(w, 503, nonce, "DB", nil) },
		"TOKEN_EXPIRED": func(w http.ResponseWriter, _, nonce []byte) { m.refuse(w, 401, nonce, "TOKEN_EXPIRED", nil) },
		"RATE_LIMITED": func(w http.ResponseWriter, _, nonce []byte) {
			m.refuse(w, 503, nonce, "RATE_LIMITED", map[string]any{"retry_after_ms": 1000, "op": "conn_admit"})
		},
		"STARTING": func(w http.ResponseWriter, _, nonce []byte) {
			m.refuse(w, 503, nonce, "STARTING", map[string]any{"retry_after_ms": 5000})
		},
		"UNKNOWN_OP (unbound)": func(w http.ResponseWriter, _, _ []byte) {
			m.refuse(w, 404, nil, "UNKNOWN_OP", map[string]any{"node": nil, "req_nonce": nil})
		},
		"nginx error": func(w http.ResponseWriter, _, _ []byte) { http.Error(w, "bad gateway", 502) },
		"unverified reply": func(w http.ResponseWriter, _, _ []byte) {
			w.Header().Set("Content-Type", octet)
			w.Write([]byte("xb1 forged"))
		},
	}
	for name, reply := range offline {
		m.reply = reply
		if st, out := putViewer(t, sock, "o-"+strings.Fields(name)[0], viewer(42, "2.2.2.2", "y"), h); st != 403 || out["reason"] != "OFFLINE" {
			t.Errorf("%s: %d %v, want the offline policy (deny)", name, st, out)
		}
	}
}

func TestAdmissionWaitsAtMostAdmitWaitForMain(t *testing.T) {
	m, a, _, sock := newAdmitAgent(t)
	release := make(chan struct{})
	defer close(release)
	m.reply = func(w http.ResponseWriter, _, _ []byte) { <-release }
	a.setOfflineAdmission(OfflineAllow)
	start := time.Now()
	if st, _ := putViewer(t, sock, "slow", viewer(42, "1.1.1.1", "x"), `{"line_id":42,"max_connections":1}`); st != 200 {
		t.Fatalf("allow: %d", st)
	}
	if d := time.Since(start); d < AdmitWait-100*time.Millisecond || d > AdmitWait+time.Second {
		t.Fatalf("the register took %s, want about %s", d, AdmitWait)
	}
}

func TestAdmissionSkipsMainWhenItDoesNotApply(t *testing.T) {
	m, a, _, sock := newAdmitAgent(t)
	m.reply = func(w http.ResponseWriter, _, nonce []byte) { m.refuse(w, 503, nonce, "DB", nil) }
	a.setOfflineAdmission(OfflineDeny)
	h := `{"line_id":42,"max_connections":1}`
	a.state.Store("quarantined")
	if st, _ := putViewer(t, sock, "q", viewer(42, "1.1.1.1", "x"), h); st != 200 || m.calls.Load() != 0 {
		t.Fatalf("quarantined: %d, %d calls", st, m.calls.Load())
	}
	a.state.Store("active")
	a.flows.Store(0)
	if st, _ := putViewer(t, sock, "f", viewer(42, "1.1.1.1", "x"), h); st != 200 || m.calls.Load() != 0 {
		t.Fatalf("CONNECTIONS off: %d, %d calls", st, m.calls.Load())
	}
	a.flows.Store(FlowConnections)
	// No header, or one that is not an admission request: a plain register.
	for _, h := range []string{"", "not json", `{"line_id":42}`} {
		if st, _ := putViewer(t, sock, "p", viewer(42, "1.1.1.1", "x"), h); st != 200 {
			t.Errorf("header %q: %d", h, st)
		}
	}
	if m.calls.Load() != 0 {
		t.Fatalf("%d conn_admit calls for plain registers", m.calls.Load())
	}
}

func TestLocalOfflinePolicyCountsTheOwnersOtherDevices(t *testing.T) {
	m, a, _, sock := newAdmitAgent(t)
	m.reply = func(w http.ResponseWriter, _, _ []byte) { http.Error(w, "down", 502) }
	h := `{"line_id":42,"max_connections":2}`
	if a.OfflineAdmission() != OfflineLocal {
		t.Fatalf("default %q, want local", a.OfflineAdmission())
	}
	a.Registry.Seed([]map[string]any{
		{"uuid": "d1", "user_id": 42, "user_ip": "1.1.1.1", "user_agent": "A", "hls_end": 0},
		{"uuid": "d2", "user_id": 42, "user_ip": "2.2.2.2", "user_agent": "B", "hls_end": 1}, // ended
		{"uuid": "d3", "user_id": 43, "user_ip": "3.3.3.3", "user_agent": "C", "hls_end": 0}, // another line
		{"uuid": "d4", "user_id": 42, "user_ip": "4.4.4.4", "user_agent": "D", "hls_end": 0},
	}, true)
	// Two open (d1, d4) at a limit of 2: a newcomer from a third device is refused.
	if st, out := putViewer(t, sock, "n1", viewer(42, "5.5.5.5", "E"), h); st != 403 || out["reason"] != "LIMIT" {
		t.Fatalf("third device: %d %v", st, out)
	}
	// The uuid itself is left out: re-registering d4 is admitted.
	if st, _ := putViewer(t, sock, "d4", viewer(42, "4.4.4.4", "Z"), h); st != 200 {
		t.Fatalf("own uuid: %d", st)
	}
	// The same device as d1 (a channel switch) is left out of the count.
	if st, _ := putViewer(t, sock, "n2", viewer(42, "1.1.1.1", "A"), h); st != 200 {
		t.Fatalf("same device: %d", st)
	}
	if a.Registry.Get("d1") == nil || num(a.Registry.Get("d1")["hls_end"]) != 0 {
		t.Fatal("local ended a record")
	}
}

func TestOfflinePolicyComesFromRepliesAndSurvivesARestart(t *testing.T) {
	_, a, _, _ := newAdmitAgent(t)
	a.publish(&Reply{State: "active", Flows: FlowConnections, OfflineAdmission: "deny"})
	if a.OfflineAdmission() != OfflineDeny {
		t.Fatalf("policy %q", a.OfflineAdmission())
	}
	a.publish(&Reply{State: "active", Flows: FlowConnections, OfflineAdmission: "maybe"})
	a.publish(&Reply{State: "active", Flows: FlowConnections}) // an older MAIN: no key
	if a.OfflineAdmission() != OfflineDeny {
		t.Fatalf("an unknown value replaced the policy: %q", a.OfflineAdmission())
	}
	back, err := loadRaw(a.Client.State.path)
	if err != nil || back.OfflineAdmission != OfflineDeny {
		t.Fatalf("not kept in the state file: %v %q", err, back.OfflineAdmission)
	}
}

func TestAdmissionCopiesTheMintProofIntoConnAdmit(t *testing.T) {
	m, a, s, sock := newAdmitAgent(t)
	m.reply = func(w http.ResponseWriter, reqCtx, _ []byte) {
		m.box(w, reqCtx, map[string]any{"admit": true, "exp": time.Now().Unix() + 30, "main_time_ms": time.Now().UnixMilli()})
	}
	mint := "tok1.1800000000." + strings.Repeat("ab", 16)
	rec := viewer(42, "1.2.3.4", "VLC")
	rec["mint"] = mint
	if st, _ := putViewer(t, sock, "v1", rec, `{"line_id":42,"stream_id":7,"max_connections":2,"ip":"1.2.3.4","ua":"VLC","mint":"`+mint+`"}`); st != 200 {
		t.Fatalf("admitted: %d", st)
	}
	if sent := m.last.Load().(map[string]any); sent["mint"] != mint {
		t.Fatalf("conn_admit carries no mint: %v", sent)
	}
	// The record keeps it, and mirrors it to MAIN, like any other key.
	if a.Registry.Get("v1")["mint"] != mint || s.events[0]["d"].(map[string]any)["record"].(map[string]any)["mint"] != mint {
		t.Fatalf("the record's mint was not kept and mirrored: %v", a.Registry.Get("v1"))
	}
	// None without one, nor for a malformed one.
	for i, h := range []string{
		`{"line_id":42,"stream_id":7,"max_connections":2,"ip":"1.2.3.4","ua":"VLC"}`,
		`{"line_id":42,"stream_id":7,"max_connections":2,"ip":"1.2.3.4","ua":"VLC","mint":"tok1.1800000000.XYZ"}`,
		`{"line_id":42,"stream_id":7,"max_connections":2,"ip":"1.2.3.4","ua":"VLC","mint":42}`,
	} {
		putViewer(t, sock, "w"+strconv.Itoa(i), viewer(42, "1.2.3.4", "VLC"), h)
		if _, ok := m.last.Load().(map[string]any)["mint"]; ok {
			t.Fatalf("case %d: a mint went to MAIN: %v", i, m.last.Load())
		}
	}
	if m.calls.Load() != 4 {
		t.Fatalf("%d conn_admit calls", m.calls.Load())
	}
}
