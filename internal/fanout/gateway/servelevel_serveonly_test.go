package gateway

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The SERVE-level defer reasons (serveLive / serveFileConn), not the judge's:
// on a serve-only node an agent/spool hiccup degrades-and-serves, an admission
// the agent refuses is a Deny (404), never a 502; a transition node is unchanged.
func TestServeFileConnServeOnlyDegradesAndRefuses(t *testing.T) {
	ip := "198.51.100.7"
	type setup struct {
		name     string
		new      bool
		agent    func(*fakeAgent)
		badSpool bool
		so       bool
		want     string // serveFileConn's return
		deny     bool   // reject() verdict when want != ""
	}
	for _, c := range []setup{
		{"register fails, serve-only", true, func(a *fakeAgent) { a.down = true }, false, true, "", false},
		{"register fails, transition", true, func(a *fakeAgent) { a.down = true }, false, false, "agent-put", false},
		{"refresh put fails, serve-only", false, func(a *fakeAgent) { a.down = true }, false, true, "", false},
		{"refresh put fails, transition", false, func(a *fakeAgent) { a.down = true }, false, false, "agent-put", false},
		{"spool fails, serve-only", true, nil, true, true, "", false},
		{"spool fails, transition", true, nil, true, false, "spool", false},
		{"refused, serve-only", true, func(a *fakeAgent) { a.refuse = "LIMIT" }, false, true, "refused", true},
		{"refused, transition", true, func(a *fakeAgent) { a.refuse = "LIMIT" }, false, false, "refused", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			_ = os.MkdirAll(filepath.Join(dir, "cons"), 0o755)
			spool := filepath.Join(dir, "spool")
			if c.badSpool {
				_ = os.WriteFile(spool, nil, 0o644) // a file where the directory belongs
			}
			a := &fakeAgent{records: map[string]map[string]any{}}
			if c.agent != nil {
				c.agent(a)
			}
			p := &Policy{ServeOnly: c.so}
			p.Paths.AgentSock, p.Paths.Spool, p.Paths.Cons = startAgent(t, a), spool, dir+"/cons"
			ref := &Refresh{New: c.new, UUID: testUUID, Limit: true, Admission: `{"line_id":41}`, Record: nil}
			s := NewServer(writeServePolicy(t, dir, "segments+playlist+vod", nil), nil, &fakeFiles{})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/x", nil)
			req.Header.Set("X-XC-Client-IP", ip)
			got := s.serveFileConn(rec, req, p, ref, time.Now(), nil)
			if got != c.want {
				t.Fatalf("serveFileConn: %q; want %q", got, c.want)
			}
			if got != "" {
				v := reject(p, got)
				if (v.Action == Deny) != c.deny {
					t.Fatalf("reject(%q): %s; want deny=%v", got, v.Action, c.deny)
				}
			}
		})
	}
}

// serveLive's refusal is the same: Deny on serve-only, PHP otherwise.
func TestServeLiveRefusedServeOnly(t *testing.T) {
	for _, so := range []bool{true, false} {
		dir := t.TempDir()
		a := &fakeAgent{records: map[string]map[string]any{}, refuse: "LIMIT"}
		p := &Policy{ServeOnly: so}
		p.Paths.AgentSock = startAgent(t, a)
		s := NewServer(writeServePolicy(t, dir, "segments+playlist", nil), nil, &fakeFiles{})
		req := httptest.NewRequest("GET", "/x", nil)
		why := s.serveLive(httptest.NewRecorder(), req, p, &Refresh{New: true, UUID: testUUID, Admission: `{"line_id":41}`}, time.Now())
		v := reject(p, why)
		if why != "refused" || (v.Action == Deny) != so {
			t.Fatalf("serve-only=%v: why %q verdict %s", so, why, v.Action)
		}
	}
}

// second-ip's unreadable-rule case (a corrupt max_connections) is a Deny on a
// serve-only node, a PHP defer on any other.
func TestJudgeLiveSecondIPUnsureServeOnly(t *testing.T) {
	for _, so := range []bool{true, false} {
		p := livePolicy()
		p.Live.Disallow2ndIP, p.ServeOnly = true, so
		tk := liveTokenJSON(func(m map[string]any) { m["user_info"].(map[string]any)["max_connections"] = "abc" })
		v, _ := JudgeLive(p, tk, "198.51.100.7", testUA, testNow, goodLiveEnv().env())
		want := PHP
		if so {
			want = Deny
		}
		if v.Action != want || v.Reason != "second-ip" {
			t.Fatalf("serve-only=%v: %s/%s; want %s/second-ip", so, v.Action, v.Reason, want)
		}
	}
}
