package clusteragent

import (
	"context"
	"crypto/md5"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRtmpUUID(t *testing.T) {
	got := rtmpUUID(3, "42")
	want := fmt.Sprintf("%x", md5.Sum([]byte("3/42")))
	if got != want {
		t.Fatalf("rtmpUUID = %s, want %s", got, want)
	}
}

func newRtmpRecordAgent(t *testing.T) *Agent {
	t.Helper()
	_, st := newFake(t) // ServerID = 3
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	reg, _, _ := newTestRegistry(t)
	reg.now = time.Now
	a.Registry, reg.Admit = reg, a.admit
	a.flows.Store(FlowConnections)
	a.state.Store("active")
	return a
}

func TestOpenRtmpRecordAdmitsAndStores(t *testing.T) {
	a := newRtmpRecordAgent(t)
	a.Client.now = func() time.Time { return time.Unix(1800000000, 0) }
	n := &rtmpNotify{Addr: "203.0.113.9", ClientID: "42", Call: "play", Name: "100"}
	reply := rtmpAuthReply{OK: true, CountryCode: "US", User: rtmpUser{ID: 7, MaxConnections: 2, ConISPName: "isp"},
		Token: &rtmpMint{UUID: "u1", Adm: &rtmpAdm{Exp: 1800000000 + 600, Sid: 3}, AdmUUID: "k0123456789abcdefghijklm", Prf: &rtmpPrf{Iat: 1800000000, P: strings.Repeat("ab", 16)}}}

	if !a.openRtmpRecord(context.Background(), n, reply, 100) {
		t.Fatal("a valid adm claim should admit and store")
	}
	rec := a.Registry.Get(rtmpUUID(3, "42"))
	if rec == nil || fmt.Sprint(rec["pid"]) != "42" || fmt.Sprint(rec["stream_id"]) != "100" || fmt.Sprint(rec["user_id"]) != "7" {
		t.Fatalf("record not stored correctly: %v", rec)
	}
	if rec["mint"] != "k0123456789abcdefghijklm.1800000000."+strings.Repeat("ab", 16) {
		t.Fatalf("mint = %v", rec["mint"])
	}
}

func TestOpenRtmpRecordNoGateWhenUnlimited(t *testing.T) {
	a := newRtmpRecordAgent(t)
	n := &rtmpNotify{Addr: "203.0.113.9", ClientID: "43", Call: "play", Name: "100"}
	reply := rtmpAuthReply{OK: true, User: rtmpUser{ID: 7, MaxConnections: 0}} // unlimited → no admission gate
	if !a.openRtmpRecord(context.Background(), n, reply, 100) {
		t.Fatal("an unlimited line should store with no gate")
	}
	if a.Registry.Get(rtmpUUID(3, "43")) == nil {
		t.Fatal("not stored")
	}
}

func TestOpenRtmpRecordDeniedByAdmission(t *testing.T) {
	_, st := newFake(t)
	st.MainURLs = []string{"http://127.0.0.1:1/cluster/v1/"} // MAIN unreachable
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf}
	reg, _, _ := newTestRegistry(t)
	reg.now = time.Now
	a.Registry, reg.Admit = reg, a.admit
	a.flows.Store(FlowConnections)
	a.state.Store("active")
	a.Client.State.OfflineAdmission = "deny"

	n := &rtmpNotify{Addr: "203.0.113.9", ClientID: "44", Call: "play", Name: "100"}
	reply := rtmpAuthReply{OK: true, User: rtmpUser{ID: 7, MaxConnections: 2}} // limited, no mint → conn_admit → unreachable → offline deny
	if a.openRtmpRecord(context.Background(), n, reply, 100) {
		t.Fatal("offline-deny must refuse")
	}
	if a.Registry.Get(rtmpUUID(3, "44")) != nil {
		t.Fatal("a refused viewer must not be stored")
	}
}

func TestCloseRtmpRecord(t *testing.T) {
	a := newRtmpRecordAgent(t)
	uuid := rtmpUUID(3, "50")
	if err := a.Registry.Put(uuid, map[string]any{"uuid": uuid, "pid": "50", "stream_id": 100, "user_id": 7, "hls_end": 0}); err != nil {
		t.Fatal(err)
	}
	a.closeRtmpRecord("50")
	if rec := a.Registry.Get(uuid); rec == nil || fmt.Sprint(rec["hls_end"]) != "1" {
		t.Fatalf("hls_end not set on close: %v", rec)
	}

	// A clientid with no record is a no-op (no panic).
	a.closeRtmpRecord("51")

	// A record whose pid does not match the clientid is not closed (the guard
	// prevents closing another client's record).
	u52 := rtmpUUID(3, "52")
	if err := a.Registry.Put(u52, map[string]any{"uuid": u52, "pid": "999", "hls_end": 0}); err != nil {
		t.Fatal(err)
	}
	a.closeRtmpRecord("52")
	if fmt.Sprint(a.Registry.Get(u52)["hls_end"]) == "1" {
		t.Fatal("a pid mismatch must not close the record")
	}
}
