package clusteragent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto/ed25519"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

func TestPolicyVersionsNeverGoBack(t *testing.T) {
	_, st := newFake(t)
	st.PolicyVer, st.MainURLs = 2, []string{"https://main:443/cluster/v1/"}
	if ok, _ := st.adoptPolicy(&Policy{PolicyVer: 1, Transport: "auto", MainURLs: []string{"http://old/cluster/v1/"}}, false); ok {
		t.Fatal("adopted a lower version")
	}
	if ok, _ := st.adoptPolicy(&Policy{PolicyVer: 2, Transport: "https_required", MainURLs: []string{"https://main:443/cluster/v1/"}}, true); ok {
		t.Fatal("a challenge's policy of the same version was adopted")
	}
	if ok, _ := st.adoptPolicy(&Policy{PolicyVer: 2, Transport: "https_required", MainURLs: []string{"https://main:443/cluster/v1/"}}, false); !ok {
		t.Fatal("a MAC'd reply's policy of the same version was refused")
	}
	if ok, _ := st.adoptPolicy(&Policy{PolicyVer: 3, Transport: "auto", MainURLs: nil}, false); ok {
		t.Fatal("a policy without URLs was adopted")
	}
	st.adoptPolicy(&Policy{PolicyVer: 3, Transport: "auto", MainURLs: []string{"http://10.0.0.1:25461/cluster/v1/"}}, false)
	st.adoptPolicy(&Policy{PolicyVer: 4, Transport: "https_required", MainURLs: []string{"https://main:443/cluster/v1/"}}, false)
	if got := st.plainURLs(); len(got) != 1 || got[0] != "http://10.0.0.1:25461/cluster/v1/" {
		t.Fatalf("plain-HTTP URLs remembered: %v", got)
	}
	back, _ := loadRaw(st.path)
	if back.PolicyVer != 4 || back.Transport != "https_required" || len(back.HTTPURLs) != 1 {
		t.Fatalf("state file: %d %q %v", back.PolicyVer, back.Transport, back.HTTPURLs)
	}
}

// The fleet's heartbeat is MAIN's to set: the setting
// (lb_telemetry_interval_sec) travels with the policy, because the agent's
// -interval flag is passed by nothing and every node kept the built-in 2 s.
func TestHeartbeatFollowsThePolicy(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	urls := []string{"http://main:25461/cluster/v1/"}

	if got := a.heartbeatEvery(); got != 2*time.Second {
		t.Fatalf("with no policy and no flag: %s, want 2s", got)
	}

	st.adoptPolicy(&Policy{PolicyVer: 1, Transport: "auto", MainURLs: urls, HeartbeatSec: 1}, false)
	if got := a.heartbeatEvery(); got != time.Second {
		t.Fatalf("policy asked for 1s, got %s", got)
	}
	if back, _ := loadRaw(st.path); back.HeartbeatSec != 1 {
		t.Fatalf("state file kept %d, want 1", back.HeartbeatSec)
	}

	// Out of MAIN's own bounds: clamped, never taken as asked.
	st.adoptPolicy(&Policy{PolicyVer: 2, Transport: "auto", MainURLs: urls, HeartbeatSec: 30}, false)
	if got := a.heartbeatEvery(); got != MaxHeartbeat {
		t.Fatalf("policy asked for 30s, got %s, want %s", got, MaxHeartbeat)
	}

	// A policy that says nothing keeps the pace the node holds.
	st.adoptPolicy(&Policy{PolicyVer: 3, Transport: "auto", MainURLs: urls}, false)
	if got := a.heartbeatEvery(); got != MaxHeartbeat {
		t.Fatalf("a policy with no heartbeat changed the pace to %s", got)
	}

	// The flag still decides while no policy has (a node run by hand).
	_, st2 := newFake(t)
	b := &Agent{Client: NewClient(st2, "xc_agent/test"), Interval: 3 * time.Second, Logf: t.Logf}
	if got := b.heartbeatEvery(); got != 3*time.Second {
		t.Fatalf("the -interval flag: %s", got)
	}
}

// httpsDrillMain is MAIN over plain HTTP under https_required: the signed
// challenge (with the policy it holds now) and a signed HTTPS_REQUIRED for
// every other op, until the admin switches back to auto.
type httpsDrillMain struct {
	*fakeMain
	mu         sync.Mutex
	policy     Policy
	replay     *Policy // a MITM replays an older signed challenge
	challenges int
	served     map[string]int
}

func (m *httpsDrillMain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	pol, replay := m.policy, m.replay
	m.mu.Unlock()
	if strings.HasSuffix(r.URL.Path, "/challenge") {
		m.mu.Lock()
		m.challenges++
		m.mu.Unlock()
		if replay != nil {
			pol = *replay
		}
		doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-challenge", "cn": r.URL.Query().Get("cn"), "challenge": base64.StdEncoding.EncodeToString(make([]byte, 32)), "main_time_ms": time.Now().UnixMilli(), "licence_ok": true, "policy": pol})
		w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("hlt", doc))))
		w.Write(doc)
		return
	}
	if pol.Transport == "https_required" {
		nonce := r.Header.Get(cc.HNonce)
		doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-denial", "reason": "HTTPS_REQUIRED", "node": r.Header.Get(cc.HNode), "req_nonce": nonce, "main_time_ms": time.Now().UnixMilli()})
		w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("den", doc))))
		w.WriteHeader(403)
		w.Write(doc)
		return
	}
	m.mu.Lock()
	m.served[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]]++
	m.mu.Unlock()
	m.fakeMain.ServeHTTP(w, r)
}

func TestHTTPSRequiredRecoversOverTheHTTPChallenge(t *testing.T) {
	old := PolicyPoll
	PolicyPoll = 30 * time.Millisecond
	defer func() { PolicyPoll = old }()
	f, st := newFake(t)
	m := &httpsDrillMain{fakeMain: f, served: map[string]int{}}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, _ []byte) {
		f.box(w, reqCtx, map[string]any{"state": "active", "mode": 1, "policy_ver": 3})
	}
	httpSrv := httptest.NewServer(m)
	defer httpSrv.Close()
	plain := httpSrv.URL + "/cluster/v1/"
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	httpsURL := "https://" + l.Addr().String() + "/cluster/v1/" // MAIN's certificate is gone: HTTPS fails
	l.Close()

	// Enrolled under auto (plain HTTP), then moved to https_required (v2).
	st.Enrolled = true
	st.adoptPolicy(&Policy{PolicyVer: 1, Transport: "auto", MainURLs: []string{plain}}, false)
	st.adoptPolicy(&Policy{PolicyVer: 2, Transport: "https_required", MainURLs: []string{httpsURL}}, false)
	m.policy = Policy{PolicyVer: 2, Transport: "https_required", MainURLs: []string{httpsURL}}
	old1 := Policy{PolicyVer: 1, Transport: "auto", MainURLs: []string{plain}}
	m.replay = &old1

	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf, Interval: 30 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	wait := func(what string, cond func() bool) {
		t.Helper()
		for ctx.Err() == nil && !cond() {
			time.Sleep(10 * time.Millisecond)
		}
		if !cond() {
			t.Fatalf("timed out: %s", what)
		}
	}
	// HTTPS fails: the agent polls the challenge over HTTP, and does not
	// adopt the older policy a MITM replays.
	wait("challenge polls", func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.challenges >= 3 })
	if st.policyVer() != 2 {
		t.Fatalf("adopted a replayed older policy: v%d", st.policyVer())
	}
	// The admin switches back to auto: v3 lists the HTTP URL.
	m.mu.Lock()
	m.replay = nil
	m.policy = Policy{PolicyVer: 3, Transport: "auto", MainURLs: []string{plain}}
	m.mu.Unlock()
	wait("heartbeats over HTTP", func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.served["heartbeat"] >= 2 })
	cancel()
	<-done
	if st.policyVer() != 3 || st.Transport != "auto" || st.MainURLs[0] != plain {
		t.Fatalf("policy after the switch: v%d %s %v", st.PolicyVer, st.Transport, st.MainURLs)
	}
}

func TestHTTPSRequiredDenialStartsThePoll(t *testing.T) {
	f, st := newFake(t)
	m := &httpsDrillMain{fakeMain: f, served: map[string]int{}, policy: Policy{PolicyVer: 5, Transport: "https_required", MainURLs: []string{"https://main.example:443/cluster/v1/"}}}
	srv := httptest.NewServer(m)
	defer srv.Close()
	st.Enrolled = true
	st.adoptPolicy(&Policy{PolicyVer: 4, Transport: "auto", MainURLs: []string{srv.URL + "/cluster/v1/"}}, false)
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf}
	_, err := a.Heartbeat(context.Background())
	a.httpsTrouble(err)
	if !a.httpsFailing.Load() {
		t.Fatalf("HTTPS_REQUIRED (%v) did not mark HTTPS as failing", err)
	}
	if err := a.PolicyOverHTTP(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st.policyVer() != 5 || st.Transport != "https_required" {
		t.Fatalf("policy v%d %s", st.PolicyVer, st.Transport)
	}
}
