package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/mitm"
)

// The MITM harness, the agent's half (XC_VM ADR 0004, "The MITM harness"):
// an attacker on the wire between the agent and MAIN, holding no key, with
// an honest MAIN behind it. Whatever it does to a reply (a flipped byte, a
// header changed or dropped, a reply cut short, an old reply played to a new
// request, a denial for another request), the agent never takes it: the call
// fails as ErrTransport and nothing the reply says (MAIN's time, a command, a
// refusal) reaches the node. What it cannot stop is an outage: a dropped or
// refused request is one. TestInteropMITM is the other half, against MAIN's
// real PHP.

// honestMain is fakeMain answering as MAIN would: heartbeat a BOXed reply
// with MAIN's time (the local clock moved by skew, so taking it shows), commands
// one command signed for this node, refused a signed denial with the reason
// and status in deny, and GET /health a signed health document.
type honestMain struct {
	*fakeMain
	skew    atomic.Int64 // ms
	seq     atomic.Uint64
	deny    atomic.Value // [2]string{status, reason}
	answers atomic.Int64
}

func newHonestMain(t *testing.T) (*honestMain, *Client, *mitm.Proxy) {
	t.Helper()
	f, st := newFake(t)
	m := &honestMain{fakeMain: f}
	m.deny.Store([2]string{"409", "NOT_ACTIVE"})
	f.answer = m.serve
	main := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/health") {
			doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-health", "proto": map[string]int{"min": 1, "max": 1}, "main_time_ms": time.Now().UnixMilli()})
			w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(f.panel, cc.PanelSigInput("hlt", doc))))
			w.Header().Set("Content-Type", "application/json")
			w.Write(doc)
			return
		}
		f.ServeHTTP(w, r)
	}))
	t.Cleanup(main.Close)
	p := mitm.New(main.URL)
	wire := httptest.NewServer(p)
	t.Cleanup(wire.Close)
	// The proxy is MAIN's only URL: no fallback reaches MAIN past it.
	st.MainURLs = []string{wire.URL + "/cluster/v1/"}
	return m, NewClient(st, "xc_agent/mitm"), p
}

func (m *honestMain) mainNow() int64 { return time.Now().UnixMilli() + m.skew.Load() }

func (m *honestMain) serve(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
	m.answers.Add(1)
	op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	now := m.mainNow()
	switch op {
	case "refused":
		d := m.deny.Load().([2]string)
		doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-denial", "reason": d[1], "node": m.uuid, "req_nonce": hex.EncodeToString(nonce), "main_time_ms": now, "retry_after_ms": 1, "lane": "p0"})
		status, _ := strconv.Atoi(d[0])
		w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("den", doc))))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write(doc)
		return
	}
	reply := map[string]any{"state": "active", "op": op, "main_time_ms": now}
	if op == "commands" {
		seq := m.seq.Add(1)
		doc, _ := json.Marshal(Command{V: 1, Type: "noop", Exp: now/1000 + 300, Iat: now / 1000, CmdID: fmt.Sprintf("c%d", seq), Seq: seq, NodeUUID: m.uuid, Gen: 1})
		sig := base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("cmd", doc)))
		reply["commands"] = []WireCommand{{Doc: string(doc), Sig: sig, Seq: seq}}
	}
	plain, _ := json.Marshal(reply)
	rnonce := make([]byte, 16)
	rnonce[0] = byte(m.answers.Load())
	ts := uint64(now)
	resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, ts, rnonce)
	body, _ := cc.Box(m.keys.EncDown, resCtx, plain)
	w.Header().Set("Content-Type", octet)
	w.Header().Set(cc.HTs, strconv.FormatUint(ts, 10))
	w.Header().Set(cc.HNonce, hex.EncodeToString(rnonce))
	w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(m.keys.MacDown, resCtx, body)))
	w.Write(body)
}

// near reports whether the client's idea of MAIN's time is within a second of MAIN's.
func near(c *Client, m *honestMain) bool {
	d := c.MainNowMs() - m.mainNow()
	return d > -1000 && d < 1000
}

// untaken is what an attacked reply must come to: a transport failure, never
// a denial the node would act on.
func untaken(t *testing.T, name string, err error) {
	t.Helper()
	var d *Denial
	if !errors.Is(err, ErrTransport) || errors.As(err, &d) {
		t.Errorf("%s: got %v, want an unauthenticated reply", name, err)
	}
}

func TestMITMRepliesThatDoNotAuthenticateAreNeverTaken(t *testing.T) {
	m, c, p := newHonestMain(t)
	ctx := context.Background()

	// Through the proxy untouched, MAIN's reply is taken, and its time with it.
	m.skew.Store(int64(10 * time.Minute / time.Millisecond))
	var r struct {
		Op string `json:"op"`
	}
	if err := c.Call(ctx, "heartbeat", map[string]any{}, &r, false); err != nil || r.Op != "heartbeat" || !near(c, m) {
		t.Fatalf("an honest reply: %v %+v", err, r)
	}
	old, _ := p.Last("/heartbeat")
	if _, err := c.PollCommands(ctx, 0); err != nil {
		t.Fatal(err)
	}
	oldCommands, _ := p.Last("/commands")
	// MAIN's clock moves on; the node follows it from the next honest reply.
	m.skew.Store(int64(20 * time.Minute / time.Millisecond))
	if err := c.Call(ctx, "heartbeat", map[string]any{}, nil, false); err != nil || !near(c, m) {
		t.Fatalf("the next honest reply: %v", err)
	}

	res := func(edit func(e *mitm.Exchange)) [2]mitm.Attack { return [2]mitm.Attack{nil, edit} }
	attacks := map[string][2]mitm.Attack{
		"the BOX's first byte flipped":  res(func(e *mitm.Exchange) { e.ResBody = mitm.Flip(e.ResBody, 0) }),
		"a byte of the BOX flipped":     res(func(e *mitm.Exchange) { e.ResBody = mitm.Flip(e.ResBody, len(e.ResBody)/2) }),
		"the BOX's last byte flipped":   res(func(e *mitm.Exchange) { e.ResBody = mitm.Flip(e.ResBody, -1) }),
		"the BOX cut short":             res(func(e *mitm.Exchange) { e.ResBody = e.ResBody[:len(e.ResBody)-1] }),
		"a byte added to the BOX":       res(func(e *mitm.Exchange) { e.ResBody = append(e.ResBody, 0) }),
		"no body":                       res(func(e *mitm.Exchange) { e.ResBody = nil }),
		"the MAC flipped":               res(func(e *mitm.Exchange) { e.ResHeader = mitm.FlipHeader(e.ResHeader, cc.HSig) }),
		"no MAC":                        res(func(e *mitm.Exchange) { e.ResHeader = mitm.Without(e.ResHeader, cc.HSig) }),
		"the reply's stamp moved":       res(func(e *mitm.Exchange) { e.ResHeader = mitm.Shift(e.ResHeader, cc.HTs, 1) }),
		"the reply's nonce changed":     res(func(e *mitm.Exchange) { e.ResHeader = mitm.FlipHeader(e.ResHeader, cc.HNonce) }),
		"the content type changed":      res(func(e *mitm.Exchange) { e.ResHeader = mitm.With(e.ResHeader, "Content-Type", "application/json") }),
		"the status changed":            res(func(e *mitm.Exchange) { e.Status = http.StatusCreated }),
		"an error page":                 res(func(e *mitm.Exchange) { e.Status, e.ResBody = http.StatusBadGateway, []byte("bad gateway") }),
		"an old reply to a new request": {mitm.Answer(old), nil},
		"another op's reply":            {mitm.Answer(oldCommands), nil},
		"refused on the wire":           {mitm.Refuse(http.StatusForbidden), nil},
	}
	for name, a := range attacks {
		p.Set(a[0], a[1])
		var out struct {
			Op string `json:"op"`
		}
		untaken(t, name, c.Call(ctx, "heartbeat", map[string]any{}, &out, false))
		if out.Op != "" {
			t.Errorf("%s: the reply reached the caller: %+v", name, out)
		}
		if !near(c, m) {
			t.Errorf("%s: the node took MAIN's time from it (off by %d ms)", name, c.MainNowMs()-m.mainNow())
		}
	}

	// Commands: an old batch played to a new poll delivers nothing, so a
	// command runs once whatever the wire does.
	p.Set(mitm.Answer(oldCommands), nil)
	if cmds, err := c.PollCommands(ctx, 0); cmds != nil {
		t.Fatalf("an old batch was delivered: %v %v", cmds, err)
	} else {
		untaken(t, "an old commands batch", err)
	}

	p.Clear()
	if err := c.Call(ctx, "heartbeat", map[string]any{}, nil, false); err != nil {
		t.Fatalf("once the wire is clean: %v", err)
	}
}

func TestMITMDenialsMustBeSignedForThisRequest(t *testing.T) {
	m, c, p := newHonestMain(t)
	ctx := context.Background()
	m.skew.Store(int64(10 * time.Minute / time.Millisecond))
	err := c.Call(ctx, "refused", map[string]any{}, nil, false)
	var d *Denial
	if !errors.As(err, &d) || d.Status != http.StatusConflict || d.Reason != "NOT_ACTIVE" {
		t.Fatalf("an honest denial: %v", err)
	}
	old, _ := p.Last("/refused")
	m.skew.Store(int64(20 * time.Minute / time.Millisecond))
	if err := c.Call(ctx, "heartbeat", map[string]any{}, nil, false); err != nil {
		t.Fatal(err)
	}

	attacks := map[string][2]mitm.Attack{
		"an old denial to a new request": {mitm.Answer(old), nil},
		"a byte of the denial flipped":   {nil, func(e *mitm.Exchange) { e.ResBody = mitm.Flip(e.ResBody, len(e.ResBody)/2) }},
		"the signature flipped": {nil, func(e *mitm.Exchange) {
			sig, _ := base64.RawURLEncoding.DecodeString(e.ResHeader.Get(cc.HPanelSig))
			e.ResHeader = mitm.With(e.ResHeader, cc.HPanelSig, base64.RawURLEncoding.EncodeToString(mitm.Flip(sig, 0)))
		}},
		"no signature": {nil, func(e *mitm.Exchange) { e.ResHeader = mitm.Without(e.ResHeader, cc.HPanelSig) }},
		"the reason rewritten": {nil, func(e *mitm.Exchange) {
			e.ResBody = []byte(strings.Replace(string(e.ResBody), "NOT_ACTIVE", "NODE_REVOKED", 1))
		}},
	}
	for name, a := range attacks {
		p.Set(a[0], a[1])
		untaken(t, name, c.Call(ctx, "refused", map[string]any{}, nil, false))
		if !near(c, m) {
			t.Errorf("%s: the node took MAIN's time from it", name)
		}
	}
	p.Clear()
}

// A denial's HTTP status is not under the panel's signature, its document
// is. Every branch the agent takes on a refusal pairs the status with the
// signed reason (retry.go, streams.go, touch.go; fatal() reads the reason
// alone), so a status the wire changed can only turn a refusal the agent
// handles into a plain one: what dropping the reply does anyway.
func TestMITMADenialsStatusOnlyEverLosesItsHandling(t *testing.T) {
	m, c, p := newHonestMain(t)
	ctx := context.Background()
	for _, d := range [][2]string{{"401", "REPLAY"}, {"401", "CLOCK_SKEW"}, {"503", "RATE_LIMITED"}, {"409", "FLOW_OFF"}, {"403", "NODE_REVOKED"}} {
		m.deny.Store(d)
		for _, status := range []int{200, 400, 401, 403, 409, 500, 503} {
			p.Set(nil, func(e *mitm.Exchange) { e.Status = status })
			err := c.Call(ctx, "refused", map[string]any{}, nil, false)
			var got *Denial
			if !errors.As(err, &got) {
				if strconv.Itoa(status) == d[0] {
					t.Errorf("%s under its own status %d was not taken: %v", d[1], status, err)
				}
				continue // not taken at all
			}
			if got.Reason != d[1] {
				t.Errorf("%s answered %d: the reason read %q", d[1], status, got.Reason)
			}
			if fatal(err) != (d[1] == "NODE_REVOKED") {
				t.Errorf("%s answered %d: fatal=%v", d[1], status, fatal(err))
			}
			_, replay := replayWait(got)
			_, skew := skewClock(got)
			_, busy := busyWait(err)
			handled := replay || skew || busy || laneRefusal(err) != nil
			if own := strconv.Itoa(status) == d[0]; handled && !own {
				t.Errorf("%s answered %d is still handled as one", d[1], status)
			}
		}
	}
	p.Clear()
}

func TestMITMHealthMustBeSigned(t *testing.T) {
	_, c, p := newHonestMain(t)
	ctx := context.Background()
	base := c.State.MainURLs[0]
	if _, err := c.Health(ctx, base); err != nil {
		t.Fatalf("an honest health document: %v", err)
	}
	for name, a := range map[string]mitm.Attack{
		"a byte flipped": func(e *mitm.Exchange) { e.ResBody = mitm.Flip(e.ResBody, 3) },
		"another key's": func(e *mitm.Exchange) {
			e.ResHeader = mitm.With(e.ResHeader, cc.HPanelSig, base64.RawURLEncoding.EncodeToString(make([]byte, 64)))
		},
		"no signature":       func(e *mitm.Exchange) { e.ResHeader = mitm.Without(e.ResHeader, cc.HPanelSig) },
		"the document added": func(e *mitm.Exchange) { e.ResBody = append(e.ResBody, ' ') },
	} {
		p.Set(nil, a)
		if _, err := c.Health(ctx, base); !errors.Is(err, ErrTransport) {
			t.Errorf("%s: %v", name, err)
		}
		if _, err := Probe(ctx, c.State.PanelSignPub, c.State.MainURLs); err == nil {
			t.Errorf("%s: the install probe accepted it", name)
		}
	}
	p.Clear()
}
