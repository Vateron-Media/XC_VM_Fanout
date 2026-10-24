package gateway

import "testing"

// Serve-only proxy relay, local-file sites: a direct-proxy live TS is pulled
// and served from this node's ring; front-proxy live/VOD/timeshift (this node
// is the originator) are served locally with proxy_id = redirect_id.
func TestServeOnlyProxyLocalSites(t *testing.T) {
	ip := "198.51.100.7"
	front := func(m map[string]any) { ci := m["channel_info"].(map[string]any); ci["originator_id"], ci["redirect_id"] = 3, 9 }
	so := func(p *Policy) *Policy { p.ServeOnly = true; return p }

	t.Run("site 1 direct-proxy TS", func(t *testing.T) {
		tok := firstTSToken(func(m map[string]any) { m["channel_info"].(map[string]any)["proxy"] = 1 })
		for _, serveOnly := range []bool{true, false} {
			p := tsPolicy()
			p.Live.CreateExpiration, p.Live.Admission = 5, true
			p.ServeOnly = serveOnly
			st := goodTSEnv()
			st.rec, st.fed, st.registered = nil, false, true
			v, r := JudgeLive(p, tok, ip, testUA, testNow, st.env())
			if serveOnly && (v.Action != Serve || v.Reason != "ts-new" || r == nil || !r.TS) {
				t.Fatalf("serve-only: %s/%s", v.Action, v.Reason)
			}
			if !serveOnly && (v.Action != PHP || v.Reason != "proxy") {
				t.Fatalf("not serve-only: %s/%s", v.Action, v.Reason)
			}
		}
	})

	t.Run("site 2 front-proxy live", func(t *testing.T) {
		p := tsPolicy()
		p.Live.CreateExpiration, p.Live.Admission = 5, true
		tok := firstTSToken(front)
		st := goodTSEnv()
		st.rec = nil
		if v, _ := JudgeLive(p, tok, ip, testUA, testNow, st.env()); v.Action != PHP || v.Reason != "proxy" {
			t.Fatalf("not serve-only: %s/%s", v.Action, v.Reason)
		}
		v, r := JudgeLive(so(p), tok, ip, testUA, testNow, st.env())
		if v.Action != Serve || r == nil || string(r.Record["proxy_id"]) != "9" || string(r.Record["server_id"]) != "3" {
			t.Fatalf("serve-only: %s/%s %+v", v.Action, v.Reason, r)
		}
	})

	t.Run("site 4 front-proxy VOD, local file", func(t *testing.T) {
		p := vodPolicy()
		tok := vodToken(func(m map[string]any) { front(m) })
		st := vodEnv()
		st.rec = nil
		if v, _ := JudgeVOD(p, tok, ip, testUA, testNow, st.env()); v.Action != PHP || v.Reason != "proxy" {
			t.Fatalf("not serve-only: %s/%s", v.Action, v.Reason)
		}
		v, r := JudgeVOD(so(p), tok, ip, testUA, testNow, st.env())
		if v.Action != Serve || r == nil || string(r.Record["proxy_id"]) != "9" || string(r.Record["server_id"]) != "3" {
			t.Fatalf("serve-only: %s/%s %+v", v.Action, v.Reason, r)
		}
		st.fileMissing = true
		if v, _ := JudgeVOD(so(p), tok, ip, testUA, testNow, st.env()); v.Action != Deny {
			t.Fatalf("missing file: %s/%s", v.Action, v.Reason)
		}
		// A direct-proxy movie (remote URL) still defers: Task 7.
		d := vodToken(func(m map[string]any) { m["channel_info"].(map[string]any)["proxy"] = 1 })
		if v, _ := JudgeVOD(so(p), d, ip, testUA, testNow, vodEnv().env()); v.Action == Serve {
			t.Fatalf("direct-proxy VOD must not serve: %s/%s", v.Action, v.Reason)
		}
	})

	t.Run("site 5 front-proxy timeshift", func(t *testing.T) {
		p := tsPolicyTS()
		tok := tsToken(func(m map[string]any) { m["originator_id"], m["redirect_id"] = 3, 9 })
		st := tsEnv()
		st.rec = nil
		if v, _ := JudgeTimeshift(p, tok, ip, testUA, testNow, st.env()); v.Action != PHP || v.Reason != "proxy" {
			t.Fatalf("not serve-only: %s/%s", v.Action, v.Reason)
		}
		v, r := JudgeTimeshift(so(p), tok, ip, testUA, testNow, st.env())
		if v.Action != Serve || r == nil || string(r.Record["proxy_id"]) != "9" || string(r.Record["server_id"]) != "3" {
			t.Fatalf("serve-only: %s/%s %+v", v.Action, v.Reason, r)
		}
		st.sizes = map[string]int64{}
		if v, _ := JudgeTimeshift(so(p), tok, ip, testUA, testNow, st.env()); v.Action != Deny {
			t.Fatalf("missing archive: %s/%s", v.Action, v.Reason)
		}
	})
}
