package gateway

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A request PHP answers with its own 404 is denied by the gateway on a
// serve-only node (no PHP behind the front: a defer would be a 502), and
// still deferred to PHP on a transition node.
func TestServeOnlyRejectsWherePHPWould404(t *testing.T) {
	ip := "198.51.100.7"
	const exp = `{"expires":1800003600,"stream":12}`
	type run func(serveOnly bool) Verdict
	live := func(token string, edit func(*Policy), env func(*liveEnvState)) run {
		return func(so bool) Verdict {
			p := livePolicy()
			p.ServeOnly = so
			if edit != nil {
				edit(p)
			}
			st := goodLiveEnv()
			if env != nil {
				env(st)
			}
			v, _ := JudgeLive(p, token, ip, testUA, testNow, st.env())
			return v
		}
	}
	first := func(p *Policy) { p.Live.CreateExpiration, p.Live.Admission, p.TimeOffset = 5, true, 0 }
	cases := []struct {
		name   string
		run    run
		reason string
	}{
		{"live token", live(tok("x"), nil, nil), "token"},
		{"live lease", live(liveTokenJSON(nil), func(p *Policy) { p.ServeUntil = testNow - 1 }, nil), "lease"},
		{"live expired", live(liveTokenJSON(func(m map[string]any) { m["expires"] = testNow - 1 }), nil, nil), "expired"},
		{"live ip", live(liveTokenJSON(nil), func(p *Policy) { p.RestrictSameIP = true }, func(s *liveEnvState) { s.rec["user_ip"] = "203.0.113.9" }), "ip"},
		{"live create-expired", live(firstHLSToken(func(m map[string]any) { m["activity_start"] = testNow - 6 }), first, func(s *liveEnvState) { s.rec = nil }), "expired"},
		{"live not-on-air (unknown stream)", live(firstHLSToken(nil), first, func(s *liveEnvState) { s.rec, s.fed, s.playlist, s.registered = nil, false, "", false }), "not-on-air"},
		{"ts not-on-air (unknown stream)", func(so bool) Verdict {
			p := tsPolicy()
			first(p)
			p.ServeOnly = so
			st := goodTSEnv()
			st.rec, st.fed, st.registered = nil, false, false
			v, _ := JudgeLive(p, firstTSToken(nil), ip, testUA, testNow, st.env())
			return v
		}, "not-on-air"},
		{"vod missing", func(so bool) Verdict {
			p := vodPolicy()
			p.ServeOnly = so
			st := vodEnv()
			st.fileMissing = true
			v, _ := JudgeVOD(p, vodToken(nil), ip, testUA, testNow, st.env())
			return v
		}, "missing"},
		{"timeshift missing", func(so bool) Verdict {
			p := tsPolicyTS()
			p.ServeOnly = so
			st := tsEnv()
			st.sizes = map[string]int64{}
			v, _ := JudgeTimeshift(p, tsToken(nil), ip, testUA, testNow, st.env())
			return v
		}, "missing"},
		{"timeshift timestamp", func(so bool) Verdict {
			p := tsPolicyTS()
			p.ServeOnly = so
			v, _ := JudgeTimeshift(p, tsToken(func(m map[string]any) { m["start"] = "notadate" }), ip, testUA, testNow, tsEnv().env())
			return v
		}, "timestamp"},
		{"timeshift stream-id", func(so bool) Verdict {
			p := tsPolicyTS()
			p.ServeOnly = so
			v, _ := JudgeTimeshift(p, tsToken(func(m map[string]any) { m["stream"] = 0 }), ip, testUA, testNow, tsEnv().env())
			return v
		}, "stream-id"},
		{"segment lease", func(so bool) Verdict {
			p := testPolicy()
			p.ServeOnly, p.ServeUntil = so, testNow-1
			return JudgeSegment(p, liveTok("3", "12_d345.ts", ip), ip, testNow, envOf(true, true, false, true))
		}, "lease"},
		{"key lease", func(so bool) Verdict {
			p := testPolicy()
			p.ServeOnly, p.ServeUntil = so, testNow-1
			return JudgeKey(p, tok(ip+"/12"), ip, testNow)
		}, "lease"},
		{"thumb token", func(so bool) Verdict {
			p := testPolicy()
			p.ServeOnly = so
			return JudgeThumb(p, Seal([]byte(exp), []byte("wrong-secret"), testContext, testNonce), testNow, thumbEnv(testNow, true))
		}, "token"},
		{"thumb expired", func(so bool) Verdict {
			p := testPolicy()
			p.ServeOnly = so
			return JudgeThumb(p, tok(`{"expires":1,"stream":12}`), testNow, thumbEnv(testNow, true))
		}, "expired"},
		{"subtitle lease", func(so bool) Verdict {
			p := testPolicy()
			p.ServeOnly, p.ServeUntil = so, testNow-1
			return JudgeSubtitle(p, tok(exp), testNow, subEnv(true))
		}, "lease"},
	}
	for _, c := range cases {
		if v := c.run(true); v.Action != Deny || v.Reason != c.reason {
			t.Errorf("%s serve-only: %s (%s); want Deny (%s)", c.name, v.Action, v.Reason, c.reason)
		}
		if v := c.run(false); v.Action != PHP || v.Reason != c.reason {
			t.Errorf("%s transition: %s (%s); want PHP (%s)", c.name, v.Action, v.Reason, c.reason)
		}
	}
}

func TestServeOnlyRejectsMore(t *testing.T) {
	ip := "198.51.100.7"
	const exp = `{"expires":1800003600,"stream":%s}`
	sub := func(extra string) string { return tok(`{"expires":1800003600,"stream_id":` + extra + `,"sub_id":1}`) }
	cases := []struct {
		name   string
		run    func(p *Policy) Verdict
		reason string
	}{
		{"segment token-form", func(p *Policy) Verdict {
			return JudgeSegment(p, "not-a-token", ip, testNow, envOf(true, true, false, true))
		}, "token"},
		{"segment stream-id", func(p *Policy) Verdict {
			return JudgeSegment(p, tok("TS/u/p/"+ip+"/x_d1.ts/"+testUUID+"/3"), ip, testNow, envOf(true, true, false, true))
		}, "stream-id"},
		{"key stream-id", func(p *Policy) Verdict { return JudgeKey(p, tok(ip+"/abc"), ip, testNow) }, "stream-id"},
		{"thumb token-json", func(p *Policy) Verdict { return JudgeThumb(p, tok("x"), testNow, thumbEnv(testNow, true)) }, "token-json"},
		{"thumb stream", func(p *Policy) Verdict {
			return JudgeThumb(p, tok(`{"expires":1800003600,"stream":0}`), testNow, thumbEnv(testNow, true))
		}, "stream"},
		{"thumb stale", func(p *Policy) Verdict {
			return JudgeThumb(p, tok(`{"expires":1800003600,"stream":12}`), testNow, thumbEnv(testNow-120, true))
		}, "stale"},
		{"subtitle token-json", func(p *Policy) Verdict { return JudgeSubtitle(p, tok("x"), testNow, subEnv(true)) }, "token-json"},
		{"subtitle stream", func(p *Policy) Verdict { return JudgeSubtitle(p, sub("0"), testNow, subEnv(true)) }, "stream"},
		{"subtitle missing", func(p *Policy) Verdict { return JudgeSubtitle(p, sub("12"), testNow, subEnv(false)) }, "missing"},
	}
	_ = exp
	for _, c := range cases {
		p := testPolicy()
		p.ServeOnly = true
		if v := c.run(p); v.Action != Deny {
			t.Errorf("%s serve-only: %s (%s); want Deny", c.name, v.Action, v.Reason)
		}
		p.ServeOnly = false
		if v := c.run(p); v.Action == Deny && c.name != "segment token-form" {
			t.Errorf("%s transition: got Deny, want a PHP defer", c.name)
		}
	}
}

// A Deny reaches the viewer through the front as a real 404 page, never an
// X-Accel-Redirect to a PHP backend that a serve-only node does not have; and a
// thumbnail that vanishes between judge and serve is the same 404.
func TestServeOnlyDenyIsA404AtTheFront(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "streams"), 0o755)
	ip := "198.51.100.7"
	edit := func(doc map[string]any) { doc["serve_only"] = true }
	s := NewServer(writeServePolicy(t, dir, "segments+playlist+thumb", edit), http.NotFoundHandler(), nil)
	check := func(name, uri string) {
		rec := serveReq(s, uri, ip)
		if rec.Code != 404 || !strings.Contains(rec.Body.String(), "<h1>404 Not Found</h1>") || rec.Header().Get("X-Accel-Redirect") != "" {
			t.Errorf("%s: %d %q accel=%q", name, rec.Code, rec.Body.String(), rec.Header().Get("X-Accel-Redirect"))
		}
	}
	check("thumb stale (no file)", "/thauth/"+tok(`{"expires":`+decs(time.Now().Unix()+3600)+`,"stream":12}`))
	check("thumb bad token", "/thauth/"+tok("x"))
	check("key stream-id", "/key/"+tok(ip+"/abc"))
}
