package gateway

import (
	"encoding/json"
	"testing"
)

func openRec(ip string, start int) map[string]any {
	return map[string]any{"user_id": 41, "user_ip": ip, "date_start": start, "hls_end": 0}
}

func TestAgentConnsOldest(t *testing.T) {
	fa := &fakeAgent{records: map[string]map[string]any{}}
	c := NewAgentConns(startAgent(t, fa))
	if _, found, ok := c.Oldest(41); found || !ok {
		t.Fatalf("no connection: found=%v answered=%v", found, ok)
	}
	fa.records["a"], fa.records["b"] = openRec("203.0.113.9", 200), openRec("198.51.100.7", 100)
	if ip, found, ok := c.Oldest(41); !found || !ok || ip != "198.51.100.7" {
		t.Fatalf("oldest: %q %v %v", ip, found, ok)
	}
	fa.down = true
	if _, found, ok := c.Oldest(41); found || ok {
		t.Fatalf("down agent: found=%v answered=%v", found, ok)
	}
}

func TestJudgeLiveSecondIPServeOnly(t *testing.T) {
	ip := "198.51.100.7"
	cases := []struct {
		name      string
		oldest    string // "" none
		subnet    bool
		serveOnly bool
		down      bool
		want      Action
		reason    string
	}{
		{"no prior connection", "", false, true, false, Serve, "playlist"},
		{"same address", ip, false, true, false, Serve, "playlist"},
		{"different address", "203.0.113.9", false, true, false, Deny, "second-ip"},
		{"different address, same /24 with subnet match", "198.51.100.200", true, true, false, Serve, "playlist"},
		{"different /24 with subnet match", "203.0.113.9", true, true, false, Deny, "second-ip"},
		{"agent down degrades to serve", "", false, true, true, Serve, "playlist"},
		{"not serve-only defers", ip, false, false, false, PHP, "second-ip"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fa := &fakeAgent{records: map[string]map[string]any{}, down: c.down}
			if c.oldest != "" {
				fa.records["x"] = openRec(c.oldest, 100)
			}
			ac := NewAgentConns(startAgent(t, fa))
			p := livePolicy()
			p.Live.Disallow2ndIP, p.IPSubnetMatch, p.ServeOnly = true, c.subnet, c.serveOnly
			st := goodLiveEnv()
			env := st.env()
			env.Oldest = ac.Oldest
			v, _ := JudgeLive(p, liveTokenJSON(nil), ip, testUA, testNow, env)
			if v.Action != c.want || v.Reason != c.reason {
				t.Fatalf("got %s/%s; want %s/%s", v.Action, v.Reason, c.want, c.reason)
			}
		})
	}
}

func TestJudgeLiveInstantOffServeOnly(t *testing.T) {
	ip := "198.51.100.7"
	od := func(m map[string]any) { m["channel_info"].(map[string]any)["on_demand"] = 1 }
	for _, c := range []struct {
		name  string
		token string
		so    bool
		want  Action
		why   string
	}{
		{"HLS serve-only serves", liveTokenJSON(od), true, Serve, "playlist"},
		{"TS serve-only serves", tsTokenJSON(od), true, Serve, "ts"},
		{"HLS defers", liveTokenJSON(od), false, PHP, "instant-off"},
		{"TS defers", tsTokenJSON(od), false, PHP, "instant-off"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := tsPolicy()
			p.Live.InstantOff, p.ServeOnly = true, c.so
			st := goodLiveEnv()
			if c.token == tsTokenJSON(od) {
				st = goodTSEnv()
			}
			v, _ := JudgeLive(p, c.token, ip, testUA, testNow, st.env())
			if v.Action != c.want || v.Reason != c.why {
				t.Fatalf("got %s/%s; want %s/%s", v.Action, v.Reason, c.want, c.why)
			}
		})
	}
}

func TestJudgeLiveNewConnectionServeOnly(t *testing.T) {
	ip := "198.51.100.7"
	first := func(p *Policy, so bool) { p.Live.CreateExpiration, p.TimeOffset, p.ServeOnly = 5, 0, so }
	t.Run("HLS ended record is re-created", func(t *testing.T) {
		p := livePolicy()
		first(p, true)
		st := goodLiveEnv()
		st.rec["hls_end"] = 1
		v, r := JudgeLive(p, firstHLSToken(nil), ip, testUA, testNow, st.env())
		if v.Action != Serve || v.Reason != "playlist-new" || r == nil || !r.New || r.UUID != liveUUID() {
			t.Fatalf("got %s/%s %+v", v.Action, v.Reason, r)
		}
	})
	t.Run("HLS mismatched record is re-created", func(t *testing.T) {
		p := livePolicy()
		first(p, true)
		st := goodLiveEnv()
		st.rec["stream_id"] = 99
		if v, r := JudgeLive(p, firstHLSToken(nil), ip, testUA, testNow, st.env()); v.Reason != "playlist-new" || !r.New {
			t.Fatalf("got %s/%s", v.Action, v.Reason)
		}
	})
	t.Run("HLS expired token", func(t *testing.T) {
		p := livePolicy()
		first(p, true)
		st := goodLiveEnv()
		st.rec["hls_end"] = 1
		tk := firstHLSToken(func(m map[string]any) { m["activity_start"] = testNow - 100 })
		if v, _ := JudgeLive(p, tk, ip, testUA, testNow, st.env()); v.Reason != "expired" {
			t.Fatalf("got %s/%s", v.Action, v.Reason)
		}
	})
	t.Run("TS ended record is reopened, not re-created", func(t *testing.T) {
		p := tsPolicy()
		first(p, true)
		st := goodTSEnv()
		st.rec["hls_end"] = 1
		v, r := JudgeLive(p, firstTSToken(nil), ip, testUA, testNow, st.env())
		if v.Action != Serve || v.Reason != "ts" || r.New {
			t.Fatalf("got %s/%s new=%v", v.Action, v.Reason, r != nil && r.New)
		}
	})
	t.Run("TS mismatched record is re-created", func(t *testing.T) {
		p := tsPolicy()
		first(p, true)
		st := goodTSEnv()
		st.rec["stream_id"] = 99
		v, r := JudgeLive(p, firstTSToken(nil), ip, testUA, testNow, st.env())
		if v.Reason != "ts-new" || !r.New {
			t.Fatalf("got %s/%s", v.Action, v.Reason)
		}
	})
	t.Run("not serve-only defers", func(t *testing.T) {
		p := livePolicy()
		first(p, false)
		st := goodLiveEnv()
		st.rec["hls_end"] = 1
		if v, _ := JudgeLive(p, firstHLSToken(nil), ip, testUA, testNow, st.env()); v.Action != PHP || v.Reason != "new-connection" {
			t.Fatalf("got %s/%s", v.Action, v.Reason)
		}
	})
}

func TestJudgeLiveSpoolAndAgentServeOnly(t *testing.T) {
	ip := "198.51.100.7"
	for _, c := range []struct {
		name  string
		edit  func(*liveEnvState)
		so    bool
		want  Action
		why   string
		limit bool
	}{
		{"spool: serve-only serves with the limit set", func(s *liveEnvState) { s.agentAlive = false }, true, Serve, "playlist", true},
		{"spool: defers", func(s *liveEnvState) { s.agentAlive = false }, false, PHP, "spool", false},
		{"agent fault: serve-only serves new", func(s *liveEnvState) { s.agentAnswers = false }, true, Serve, "playlist-new", true},
		{"agent fault: defers", func(s *liveEnvState) { s.agentAnswers = false }, false, PHP, "agent", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := livePolicy()
			p.Live.CreateExpiration, p.TimeOffset, p.ServeOnly = 5, 0, c.so
			st := goodLiveEnv()
			c.edit(st)
			v, r := JudgeLive(p, firstHLSToken(nil), ip, testUA, testNow, st.env())
			if v.Action != c.want || v.Reason != c.why {
				t.Fatalf("got %s/%s; want %s/%s", v.Action, v.Reason, c.want, c.why)
			}
			if c.want == Serve && (!r.Limit || r.MaxConns != 2) {
				t.Fatalf("limit not set: %+v", r)
			}
		})
	}
	_ = json.RawMessage(nil)
}
