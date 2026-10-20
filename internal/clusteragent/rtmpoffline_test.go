package clusteragent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The last-yes cache keeps MAIN's last rtmp_auth yes and serves it only while
// the node could play a new viewer: fresh (within TTL), the line not expired,
// the offline policy not "deny", the node active, and the lease not refusing
// new sessions.
func TestRtmpLastYes(t *testing.T) {
	_, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	a.state.Store("active")
	fixed := time.Unix(1800000000, 0)
	a.rtmpYes.now = func() time.Time { return fixed }
	a.Client.now = func() time.Time { return fixed } // MainNowMs == fixed ms (offset 0)

	key := rtmpYesKey(100, "203.0.113.9", false, map[string]string{"username": "u", "password": "p"})
	ok := rtmpAuthReply{OK: true, User: rtmpUser{ID: 7, MaxConnections: 2, ExpDate: fixed.Unix() + 3600}}

	a.rtmpYes.keep(key, ok)
	if got := a.lastYes(key); got == nil || got.User.ID != 7 {
		t.Fatal("a fresh cached yes should serve")
	}
	// expired line
	exp := ok
	exp.User.ExpDate = fixed.Unix() - 1
	a.rtmpYes.keep(key, exp)
	if a.lastYes(key) != nil {
		t.Fatal("an expired-user cached yes must not serve")
	}
	// deny policy
	a.rtmpYes.keep(key, ok)
	a.Client.State.OfflineAdmission = "deny"
	if a.lastYes(key) != nil {
		t.Fatal("offline policy deny must refuse the cached yes")
	}
	a.Client.State.OfflineAdmission = ""
	// node not active
	a.state.Store("quarantined")
	if a.lastYes(key) != nil {
		t.Fatal("an inactive node must refuse")
	}
	a.state.Store("active")
	// forget erases it
	a.rtmpYes.forget(key)
	if a.lastYes(key) != nil {
		t.Fatal("a forgotten entry must not serve")
	}
	// TTL
	a.rtmpYes.keep(key, ok)
	a.rtmpYes.now = func() time.Time { return fixed.Add(rtmpLastYesTTL + time.Second) }
	if a.lastYes(key) != nil {
		t.Fatal("a stale entry must not serve")
	}
}

// rtmpAsk calls MAIN's rtmp_auth: a MAIN answer is returned as-is (an ok cached,
// a non-ok forgotten); a verified refusal (a *Denial) erases the cache and
// returns REFUSED; an unreachable MAIN returns the cached yes, or NO_ANSWER.
// (The client snapshots MainURLs on first use, so reachable and unreachable are
// two agents, not one with its URL flipped.)
func TestRtmpAskReachable(t *testing.T) {
	f, st := newFake(t)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	a.state.Store("active")
	deny := false
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		if deny {
			f.refuse(w, 409, nonce, "SOME_REASON", nil)
			return
		}
		f.box(w, reqCtx, map[string]any{
			"ok":           true,
			"user":         map[string]any{"id": 7, "max_connections": 2, "con_isp_name": "isp", "exp_date": time.Now().Unix() + 3600},
			"country_code": "US",
			"token":        map[string]any{"uuid": "u1", "adm": "a1", "adm_uuid": "au1", "prf": "p1"},
		})
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	ctx := context.Background()
	creds := map[string]string{"username": "u", "password": "p"}
	key := rtmpYesKey(100, "203.0.113.9", false, creds)

	rep := a.rtmpAsk(ctx, 100, "203.0.113.9", false, "uuid1", creds)
	if !rep.OK || rep.User.ID != 7 || rep.Token == nil || rep.Token.Prf != "p1" {
		t.Fatalf("ok branch: %+v", rep)
	}
	if a.rtmpYes.answer(key, a.Client.MainNowMs()) == nil {
		t.Fatal("an ok answer should have been cached")
	}
	// MAIN refuses → forget + REFUSED, and the cache is erased.
	deny = true
	if rep2 := a.rtmpAsk(ctx, 100, "203.0.113.9", false, "uuid1", creds); rep2.OK || rep2.Reason != "REFUSED" {
		t.Fatalf("refused branch: %+v", rep2)
	}
	if a.rtmpYes.answer(key, a.Client.MainNowMs()) != nil {
		t.Fatal("a refusal should have erased the cache")
	}
}

func TestRtmpAskUnreachable(t *testing.T) {
	_, st := newFake(t)
	st.MainURLs = []string{"http://127.0.0.1:1/cluster/v1/"} // dead from construction
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	a.state.Store("active")
	ctx := context.Background()
	creds := map[string]string{"username": "u", "password": "p"}
	key := rtmpYesKey(100, "203.0.113.9", false, creds)

	// With a cached yes, an unreachable MAIN serves it.
	a.rtmpYes.keep(key, rtmpAuthReply{OK: true, User: rtmpUser{ID: 7, MaxConnections: 2}}) // ExpDate 0 = no expiry
	if rep := a.rtmpAsk(ctx, 100, "203.0.113.9", false, "uuid1", creds); !rep.OK || rep.User.ID != 7 {
		t.Fatalf("unreachable with a cached yes: %+v", rep)
	}
	// Without one, NO_ANSWER.
	a.rtmpYes.forget(key)
	if rep := a.rtmpAsk(ctx, 100, "203.0.113.9", false, "uuid1", creds); rep.OK || rep.Reason != "NO_ANSWER" {
		t.Fatalf("unreachable with no cached yes: %+v", rep)
	}
}
