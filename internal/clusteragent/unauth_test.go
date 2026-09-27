package clusteragent

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// An answer that does not authenticate is that URL's failure (ADR 0004,
// "MAIN endpoint changes (Phase 3, third increment)", the agent's contract,
// item 3).

// urlMain is one MAIN URL that answers as told.
type urlMain struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits int
}

func (u *urlMain) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits
}

// answers a URL can give.
const (
	answerGood      = "authenticated"
	answerBadMAC    = "200 with a MAC that does not verify"
	answerBadHeader = "200 with a stamp that does not parse"
	answerBadBox    = "200 whose BOX does not open"
	answerUndecoded = "authenticated, JSON that does not decode"
	answerDenial    = "verified refusal"
	answerForged    = "refusal under another key"
	answer502       = "502"
)

func newURLMain(t *testing.T, f *fakeMain, answer string) *urlMain {
	u := &urlMain{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.hits++
		u.mu.Unlock()
		ts := r.Header.Get(cc.HTs)
		nonce := r.Header.Get(cc.HNonce)
		reqCtx := func() []byte {
			var n [16]byte
			copy(n[:], mustHex(nonce))
			c, _ := cc.RequestContext(cc.Request{Proto: 1, Agent: r.Header.Get(cc.HAgent), Method: r.Method, Path: r.URL.Path,
				ContentType: r.Header.Get("Content-Type"), Node: r.Header.Get(cc.HNode), Epoch: 1, TsMs: parseU(ts), Nonce: n[:]})
			return c
		}
		switch answer {
		case answerGood:
			f.box(w, reqCtx(), map[string]any{"state": "active", "mode": 1})
		case answerUndecoded:
			f.box(w, reqCtx(), map[string]any{"state": 7})
		case answerBadMAC, answerBadHeader, answerBadBox:
			rec := httptest.NewRecorder()
			f.box(rec, reqCtx(), map[string]any{"state": "active"})
			for k, v := range rec.Header() {
				w.Header()[k] = v
			}
			body := rec.Body.Bytes()
			switch answer {
			case answerBadMAC:
				w.Header().Set(cc.HSig, strings.Repeat("00", 32))
			case answerBadHeader:
				w.Header().Set(cc.HTs, "soon")
			case answerBadBox:
				// A MAC over a body that is not a BOX, from someone holding the key.
				body = []byte("not a box")
				mustSig(w, f, reqCtx(), body)
			}
			w.Write(body)
		case answerDenial:
			f.refuse(w, 503, mustHex(nonce), "STARTING", map[string]any{"retry_after_ms": 5000})
		case answerForged:
			other, _ := newFake(t)
			other.uuid = f.uuid
			other.refuse(w, 503, mustHex(nonce), "STARTING", nil)
		case answer502:
			http.Error(w, "bad gateway", http.StatusBadGateway)
		}
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func TestAnAnswerThatDoesNotAuthenticateIsThatURLsFailure(t *testing.T) {
	cases := []struct {
		name     string
		answers  []string // per URL, in the policy's order
		preFail  []int    // URLs already backed off before the request
		wantErr  string   // "" ok, "transport", "denial", "decode"
		wantHits []int    // requests each URL got
		backedOf []int    // URLs backed off after the request
	}{
		{"a bad MAC moves on to the next URL in the same request", []string{answerBadMAC, answerGood}, nil, "", []int{1, 1}, []int{0}},
		{"a stamp that does not parse", []string{answerBadHeader, answerGood}, nil, "", []int{1, 1}, []int{0}},
		{"a BOX that does not open", []string{answerBadBox, answerGood}, nil, "", []int{1, 1}, []int{0}},
		{"an authenticated reply is final even when it does not decode", []string{answerUndecoded, answerGood}, nil, "decode", []int{1, 0}, nil},
		{"a verified refusal is final", []string{answerDenial, answerGood}, nil, "denial", []int{1, 0}, nil},
		{"a refusal that does not verify moves on, as before", []string{answerForged, answerGood}, nil, "", []int{1, 1}, nil},
		{"every URL bad: a transport error", []string{answerBadMAC, answerBadBox}, nil, "transport", []int{1, 1}, []int{0, 1}},
		{"a URL in back-off stays there when it answers unauthenticated", []string{answerBadMAC, answerBadMAC}, []int{0}, "transport", []int{1, 1}, []int{0, 1}},
		{"an authenticated answer clears a back-off", []string{answerGood, answer502}, []int{0}, "", []int{1, 1}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, st := newFake(t)
			var mains []*urlMain
			st.MainURLs = nil
			for _, a := range c.answers {
				u := newURLMain(t, f, a)
				mains = append(mains, u)
				st.MainURLs = append(st.MainURLs, u.srv.URL+"/cluster/v1/")
			}
			cl := NewClient(st, "xc_agent/test")
			for _, i := range c.preFail {
				cl.reached(context.Background(), st.MainURLs[i], context.DeadlineExceeded)
			}
			var out Reply
			err := cl.Call(context.Background(), "heartbeat", map[string]any{}, &out, false)
			var d *Denial
			switch c.wantErr {
			case "":
				if err != nil {
					t.Fatalf("got %v", err)
				}
			case "transport":
				if !errors.Is(err, ErrTransport) {
					t.Fatalf("got %v, want ErrTransport", err)
				}
			case "denial":
				if !errors.As(err, &d) {
					t.Fatalf("got %v, want the refusal", err)
				}
			case "decode":
				if err == nil || errors.Is(err, ErrTransport) || errors.As(err, &d) {
					t.Fatalf("got %v, want the decode error", err)
				}
			}
			for i, u := range mains {
				if u.count() != c.wantHits[i] {
					t.Fatalf("URL %d got %d request(s), want %d", i, u.count(), c.wantHits[i])
				}
			}
			cl.mu.Lock()
			defer cl.mu.Unlock()
			for i, u := range st.MainURLs {
				_, off := cl.failed[u]
				want := false
				for _, j := range c.backedOf {
					want = want || i == j
				}
				if off != want {
					t.Fatalf("URL %d backed off %v, want %v", i, off, want)
				}
				if off && time.Until(cl.failed[u]) > URLRetry {
					t.Fatalf("URL %d backed off past URLRetry", i)
				}
			}
		})
	}
}

func TestABackedOffURLIsStillTriedLast(t *testing.T) {
	f, st := newFake(t)
	bad, good := newURLMain(t, f, answerBadMAC), newURLMain(t, f, answerGood)
	st.MainURLs = []string{good.srv.URL + "/cluster/v1/", bad.srv.URL + "/cluster/v1/"}
	cl := NewClient(st, "xc_agent/test")
	cl.reached(context.Background(), st.MainURLs[0], context.DeadlineExceeded)
	// The good URL is backed off, the bad one answers first: the request
	// still reaches MAIN, last, and clears the good URL's back-off.
	if err := cl.Call(context.Background(), "heartbeat", map[string]any{}, nil, false); err != nil {
		t.Fatal(err)
	}
	if got := cl.urls(); got[0] != st.MainURLs[0] || got[1] != st.MainURLs[1] {
		t.Fatalf("order after: %v", got)
	}
}

func TestRekeyBacksOffAURLWhoseReplyDoesNotCheck(t *testing.T) {
	m, st, good := newRekeyMain(t)
	var badHits int
	var mu sync.Mutex
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/token_rekey") {
			mu.Lock()
			badHits++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"typ":"xcvm-rekey"}`)) // no panel signature
			return
		}
		m.ServeHTTP(w, r)
	}))
	t.Cleanup(bad.Close)
	st.MainURLs = []string{bad.URL + "/cluster/v1/", good.URL + "/cluster/v1/"}
	c := NewClient(st, "xc_agent/test")
	tok, err := c.Rekey(context.Background(), nil)
	if err != nil || tok == nil {
		t.Fatalf("rekey: %v", err)
	}
	c.mu.Lock()
	_, off := c.failed[st.MainURLs[0]]
	_, goodOff := c.failed[st.MainURLs[1]]
	c.mu.Unlock()
	if badHits != 1 || !off || goodOff {
		t.Fatalf("bad URL hit %d, backed off %v; good backed off %v", badHits, off, goodOff)
	}
}

func mustHex(s string) []byte { b, _ := hex.DecodeString(s); return b }

func parseU(s string) uint64 { n, _ := strconv.ParseUint(s, 10, 64); return n }

// mustSig MACs body as MAIN would under the reply headers already set.
func mustSig(w http.ResponseWriter, f *fakeMain, reqCtx, body []byte) {
	resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, parseU(w.Header().Get(cc.HTs)), mustHex(w.Header().Get(cc.HNonce)))
	w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(f.keys.MacDown, resCtx, body)))
}
