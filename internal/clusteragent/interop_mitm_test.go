package clusteragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/mitm"
)

// TestInteropMITM is the MITM harness's other half (XC_VM ADR 0004, "The MITM
// harness"): the attacker on the wire between this agent and MAIN's real PHP
// ClusterApi (XCVM_PANEL_DIR, as TestInteropWithPanel). Each change to a
// request is refused by MAIN, before anything about the node moves, under the
// reason its order of checks gives; a tampered copy sent first does not spend
// the genuine request's nonce, and the genuine request sent twice is a
// REPLAY; a real reply changed on its way back is not taken. After each, the
// clean wire works again.
func TestInteropMITM(t *testing.T) {
	a, _, ctx := interopNode(t)
	if _, err := a.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	st := a.Client.State
	st.mu.Lock()
	real, _ := url.Parse(st.MainURLs[0])
	p := mitm.New(real.Scheme + "://" + real.Host)
	wire := httptest.NewServer(p)
	defer wire.Close()
	// The proxy is MAIN's only URL: no known-good set dials past it.
	st.MainURLs, st.KnownGoodURLs = []string{wire.URL + real.Path}, nil
	st.mu.Unlock()

	if _, err := a.Heartbeat(ctx); err != nil {
		t.Fatalf("a heartbeat through the clean wire: %v", err)
	}
	clean := func(name string) {
		t.Helper()
		p.Clear()
		if _, err := a.Heartbeat(ctx); err != nil {
			t.Fatalf("after %s, the clean wire: %v", name, err)
		}
	}

	req := func(edit func(e *mitm.Exchange)) mitm.Attack { return edit }
	for _, c := range []struct {
		name, op     string
		attack       mitm.Attack
		status       int
		reason, path string
	}{
		{"the body flipped", "heartbeat", req(func(e *mitm.Exchange) { e.ReqBody = mitm.Flip(e.ReqBody, len(e.ReqBody)/2) }), 401, "BAD_MAC", ""},
		{"the body cut short", "heartbeat", req(func(e *mitm.Exchange) { e.ReqBody = e.ReqBody[:len(e.ReqBody)-1] }), 401, "BAD_MAC", ""},
		{"the MAC flipped", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.FlipHeader(e.ReqHeader, cc.HSig) }), 401, "BAD_MAC", ""},
		{"no MAC", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.Without(e.ReqHeader, cc.HSig) }), 400, "BAD_REQUEST", ""},
		{"another op's path", "heartbeat", req(func(e *mitm.Exchange) { e.Path = strings.TrimSuffix(e.Path, "heartbeat") + "events" }), 401, "BAD_MAC", "/events"},
		{"a query added", "heartbeat", req(func(e *mitm.Exchange) { e.RawQuery = "x=1" }), 401, "BAD_MAC", ""},
		{"the content type changed", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.With(e.ReqHeader, "Content-Type", "application/json") }), 401, "BAD_MAC", ""},
		{"the agent header changed", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.With(e.ReqHeader, cc.HAgent, "xc_agent/other") }), 401, "BAD_MAC", ""},
		{"the stamp moved a millisecond", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.Shift(e.ReqHeader, cc.HTs, 1) }), 401, "BAD_MAC", ""},
		{"the stamp ten minutes old", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.Shift(e.ReqHeader, cc.HTs, -600000) }), 401, "CLOCK_SKEW", ""},
		{"the nonce changed", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.FlipHeader(e.ReqHeader, cc.HNonce) }), 401, "BAD_MAC", ""},
		{"another node named", "heartbeat", req(func(e *mitm.Exchange) {
			e.ReqHeader = mitm.With(e.ReqHeader, cc.HNode, "11111111-1111-4111-a111-111111111111")
		}), 401, "UNKNOWN_NODE", ""},
		{"another protocol", "heartbeat", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.With(e.ReqHeader, cc.HProto, "2") }), 426, "PROTO", ""},
		{"no node signature on token_refresh", "token_refresh", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.Without(e.ReqHeader, cc.HNodeSig) }), 401, "BAD_NODE_SIG", ""},
		{"the node signature flipped", "token_refresh", req(func(e *mitm.Exchange) { e.ReqHeader = mitm.FlipHeader(e.ReqHeader, cc.HNodeSig) }), 401, "BAD_NODE_SIG", ""},
	} {
		p.Set(c.attack, nil)
		var err error
		if c.op == "token_refresh" {
			_, err = a.Client.Refresh(ctx)
		} else {
			_, err = a.Heartbeat(ctx)
		}
		if err == nil {
			t.Fatalf("%s: MAIN answered", c.name)
		}
		path := c.path
		if path == "" {
			path = "/" + c.op
		}
		e, ok := p.Last(path)
		if !ok {
			t.Fatalf("%s: nothing crossed the wire to %s", c.name, path)
		}
		if got := reasonOf(e); e.Upstream != c.status || got != c.reason {
			t.Errorf("%s: MAIN answered %d %s, want %d %s", c.name, e.Upstream, got, c.status, c.reason)
		}
		clean(c.name)
	}
	if _, err := a.Client.Refresh(ctx); err != nil {
		t.Fatalf("a refresh through the clean wire: %v", err)
	}

	// The attacker holds the genuine request back and sends a tampered copy
	// first: MAIN refuses the copy without spending the nonce, takes the
	// genuine request once, and refuses it as a REPLAY after that.
	p.Set(mitm.Refuse(http.StatusBadGateway), nil)
	if _, err := a.Heartbeat(ctx); err == nil {
		t.Fatal("a request held back was answered")
	}
	held, _ := p.Last("/heartbeat")
	if held.Upstream != 0 {
		t.Fatal("the held request reached MAIN")
	}
	for name, edit := range map[string]mitm.Attack{
		"its body flipped": func(e *mitm.Exchange) { e.ReqBody = mitm.Flip(e.ReqBody, 0) },
		"its MAC flipped":  func(e *mitm.Exchange) { e.ReqHeader = mitm.FlipHeader(e.ReqHeader, cc.HSig) },
	} {
		if r := replay(t, ctx, p, held, edit); r.Upstream != 401 || reasonOf(r) != "BAD_MAC" {
			t.Fatalf("the held request with %s: %d %s", name, r.Upstream, reasonOf(r))
		}
	}
	if r := replay(t, ctx, p, held, nil); r.Upstream != 200 {
		t.Fatalf("the held request after its tampered copies: %d %s — a copy spent its nonce", r.Upstream, reasonOf(r))
	}
	if r := replay(t, ctx, p, held, nil); r.Upstream != 401 || reasonOf(r) != "REPLAY" {
		t.Fatalf("the held request sent again: %d %s", r.Upstream, reasonOf(r))
	}
	clean("a held request")

	// MAIN's real replies, changed on their way back, are not taken; nor is
	// an old one played to a new request.
	old, _ := p.Last("/heartbeat")
	for name, c := range map[string][2]mitm.Attack{
		"a byte of the BOX flipped": {nil, func(e *mitm.Exchange) { e.ResBody = mitm.Flip(e.ResBody, len(e.ResBody)/2) }},
		"the MAC flipped":           {nil, func(e *mitm.Exchange) { e.ResHeader = mitm.FlipHeader(e.ResHeader, cc.HSig) }},
		"the reply's stamp moved":   {nil, func(e *mitm.Exchange) { e.ResHeader = mitm.Shift(e.ResHeader, cc.HTs, 1) }},
		"an old reply":              {mitm.Answer(old), nil},
	} {
		p.Set(c[0], c[1])
		_, err := a.Heartbeat(ctx)
		var d *Denial
		if !errors.Is(err, ErrTransport) || errors.As(err, &d) {
			t.Errorf("%s: %v, want an unauthenticated reply", name, err)
		}
		clean(name)
	}

	// A refusal's status is not under the signature, its document is: MAIN's
	// BAD_MAC answered as a 200 is still that BAD_MAC, for this request, and
	// no reply the agent would act on (TestMITMADenialsStatusOnlyEverLosesItsHandling).
	p.Set(func(e *mitm.Exchange) { e.ReqHeader = mitm.FlipHeader(e.ReqHeader, cc.HSig) }, func(e *mitm.Exchange) { e.Status = 200 })
	_, err := a.Heartbeat(ctx)
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "BAD_MAC" {
		t.Fatalf("a refusal answered as 200: %v", err)
	}
	clean("a refusal answered as 200")
}

// TestInteropMITMDataPlane: the same attacker between the agent's loopback
// relay proxy and a parent or owner running MAIN's real RelayGuard and
// FileTicketServer — the plan's "a refused MITM body, replayed headers"
// (ADR 0004, Phase 8, fourth increment). A relay whose target, ticket or
// proof the wire changed is refused, as is a proof or a whole request sent
// again; a file chunk changed, cut, moved or answered with another chunk's
// bytes or digest is never passed on, and what was passed on before it is the
// file's own bytes.
func TestInteropMITMDataPlane(t *testing.T) {
	rig := interopDataPlane(t)
	p := mitm.New(rig.parent(t, nil, nil))
	wire := httptest.NewServer(p)
	defer wire.Close()
	get := rig.proxyTo(t, wire.URL)
	relay := func() bool {
		code, body := get("/relay/interop-loopback-key-0123/100.ts")
		return code == 200 && string(body) == "TS-FROM-MAIN"
	}
	file := "/xfile/interop-loopback-key-0123/" + rig.ref + ".mkv"
	whole := func() bool {
		code, body := get(file)
		return code == 200 && bytes.Equal(body, rig.want)
	}
	if !relay() || !whole() {
		t.Fatal("the clean wire")
	}
	genuine, _ := p.Last("/admin/live")
	var first, second mitm.Exchange
	for _, e := range p.Log() {
		if e.Path == "/xfile" && e.Status == 200 {
			if o := queryOf(e, "o"); o == "0" {
				first = e
			} else if second.Path == "" {
				second = e
			}
		}
	}
	if first.Path == "" || second.Path == "" {
		t.Fatalf("the file came in %d exchanges, want two chunks", len(p.Log())-1)
	}

	for name, a := range map[string]mitm.Attack{
		"another target":     func(e *mitm.Exchange) { e.RawQuery = strings.Replace(e.RawQuery, "extension=ts", "extension=m3u8", 1) },
		"a query added":      func(e *mitm.Exchange) { e.RawQuery += "&x=1" },
		"the ticket flipped": func(e *mitm.Exchange) { e.ReqHeader = mitm.FlipHeader(e.ReqHeader, "X-XCVM-Relay") },
		"the proof flipped":  func(e *mitm.Exchange) { e.ReqHeader = mitm.FlipHeader(e.ReqHeader, "X-XCVM-Relay-Auth") },
		"no proof":           func(e *mitm.Exchange) { e.ReqHeader = mitm.Without(e.ReqHeader, "X-XCVM-Relay-Auth") },
		"an earlier proof": func(e *mitm.Exchange) {
			e.ReqHeader = mitm.With(e.ReqHeader, "X-XCVM-Relay-Auth", genuine.ReqHeader.Get("X-XCVM-Relay-Auth"))
		},
		"the password nudged": func(e *mitm.Exchange) { e.RawQuery += "&password=InteropStreamPass" },
	} {
		p.Set(a, nil)
		if relay() {
			t.Errorf("relay with %s: admitted", name)
		}
		p.Clear()
		if !relay() {
			t.Fatalf("after %s, the clean relay", name)
		}
	}
	if r := replay(t, context.Background(), p, genuine, nil); r.Upstream == 200 {
		t.Error("a relay request sent again was admitted")
	}

	later := func(e *mitm.Exchange) bool { return e.Path == "/xfile" && queryOf(*e, "o") != "0" }
	for name, a := range map[string][2]mitm.Attack{
		"a byte of the second chunk flipped": {nil, func(e *mitm.Exchange) {
			if later(e) {
				e.ResBody = mitm.Flip(e.ResBody, 7)
			}
		}},
		"a byte of the first chunk flipped": {nil, func(e *mitm.Exchange) {
			if !later(e) {
				e.ResBody = mitm.Flip(e.ResBody, -1)
			}
		}},
		"the second chunk cut short": {nil, func(e *mitm.Exchange) {
			if later(e) {
				e.ResBody = e.ResBody[:len(e.ResBody)-1]
			}
		}},
		"the first chunk's digest on the second": {nil, func(e *mitm.Exchange) {
			if later(e) {
				e.ResHeader = mitm.With(e.ResHeader, "X-XCVM-File-Digest", first.ResHeader.Get("X-XCVM-File-Digest"))
			}
		}},
		"the first chunk played for the second": {func(e *mitm.Exchange) {
			if later(e) {
				mitm.Answer(first)(e)
			}
		}, nil},
		"the second chunk's range moved": {func(e *mitm.Exchange) {
			if later(e) {
				e.RawQuery = strings.Replace(e.RawQuery, "o="+queryOf(*e, "o"), "o=0", 1)
			}
		}, nil},
	} {
		p.Set(a[0], a[1])
		// Refused: an error before any byte, or the file's own bytes up to
		// the chunk that failed.
		code, body := get(file)
		if code == 200 && (len(body) >= len(rig.want) || !bytes.Equal(body, rig.want[:len(body)])) {
			t.Errorf("%s: %d bytes passed on, not a part of the file", name, len(body))
		}
		p.Clear()
		if !whole() {
			t.Fatalf("after %s, the clean file", name)
		}
	}
	if r := replay(t, context.Background(), p, second, nil); r.Upstream == 200 {
		t.Error("a chunk request sent again was served")
	}

	// A limit, pinned so that closing it is a decision (ADR 0004, "The MITM
	// harness"): a chunk's digest names the ticket, the offset, the size,
	// the hash and the total, not the request, so an old answer for the same
	// chunk under the same ticket is taken. It is the file's own bytes at that
	// offset; a file rewritten in place within one ticket's life could be read
	// mixed.
	p.Set(func(e *mitm.Exchange) {
		if later(e) {
			mitm.Answer(second)(e)
		}
	}, nil)
	if !whole() {
		t.Error("an old answer for the same chunk is refused now: update ADR 0004's limit and this test")
	}
	p.Clear()
}

// queryOf is the query parameter name of e's request.
func queryOf(e mitm.Exchange, name string) string {
	q, _ := url.ParseQuery(e.RawQuery)
	return q.Get(name)
}

// reasonOf is the reason of MAIN's refusal in e, as MAIN sent it.
func reasonOf(e mitm.Exchange) string {
	var d struct {
		Reason string `json:"reason"`
	}
	json.Unmarshal(e.UpstreamBody, &d)
	return d.Reason
}

func replay(t *testing.T, ctx context.Context, p *mitm.Proxy, e mitm.Exchange, edit mitm.Attack) mitm.Exchange {
	t.Helper()
	r, err := p.Replay(ctx, e, edit)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
