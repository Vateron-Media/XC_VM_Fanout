package gateway

import "testing"

func TestJudgeLiveExtensionApiContainerServeOnly(t *testing.T) {
	ip := "198.51.100.7"
	odd := func(m map[string]any) { m["extension"] = "mkv" }
	for _, c := range []struct {
		name string
		ac   string
		so   bool
		ts   bool // token env: TS
		want Action
		why  string
	}{
		{"api_container ts routes as TS", "ts", true, true, Serve, "ts"},
		{"api_container m3u8 routes as HLS", "m3u8", true, false, Serve, "playlist"},
		{"empty api_container keeps the defer", "", true, false, PHP, "extension"},
		{"unusable api_container keeps the defer", "mp4", true, false, PHP, "extension"},
		{"not serve-only defers", "ts", false, true, PHP, "extension"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := tsPolicy()
			p.ServeOnly, p.Live.ApiContainer = c.so, c.ac
			st := goodLiveEnv()
			tok := liveTokenJSON(odd)
			if c.ts {
				st, tok = goodTSEnv(), tsTokenJSON(odd)
			}
			v, _ := JudgeLive(p, tok, ip, testUA, testNow, st.env())
			if v.Action != c.want || v.Reason != c.why {
				t.Fatalf("got %s/%s; want %s/%s", v.Action, v.Reason, c.want, c.why)
			}
		})
	}
}

func TestJudgeLiveNonASCIIAgentServeOnly(t *testing.T) {
	ip := "198.51.100.7"
	ua := "Mozilla/5.0 (é) <x> & \"y\""
	for _, so := range []bool{true, false} {
		p := tsPolicy()
		p.ServeOnly = so
		v, r := JudgeLive(p, tsTokenJSON(nil), ip, ua, testNow, goodTSEnv().env())
		if so && (v.Action != Serve || r == nil || r.UserAgent != "Mozilla/5.0 (é) &lt;x&gt; &amp; &quot;y&quot;") {
			t.Fatalf("serve-only: %s/%s %+v", v.Action, v.Reason, r)
		}
		if !so && (v.Action != PHP || v.Reason != "user-agent") {
			t.Fatalf("not serve-only: %s/%s", v.Action, v.Reason)
		}
	}
}

func TestJudgeTimeshiftBadTimezoneServeOnly(t *testing.T) {
	ip := "198.51.100.7"
	for _, so := range []bool{true, false} {
		p := tsPolicyTS()
		p.ServeOnly, p.DefaultTimezone = so, "No/Such"
		st := tsEnv()
		st.rec = nil
		v, _ := JudgeTimeshift(p, tsToken(func(m map[string]any) { m["start"] = "2026-10-16:14-00" }), ip, testUA, testNow, st.env())
		want := "timezone"
		if so {
			want = "missing" // past the zone: the UTC minute is not in the archive
		}
		if v.Reason != want {
			t.Fatalf("serve-only=%v: got %s/%s; want %s", so, v.Action, v.Reason, want)
		}
	}
}
