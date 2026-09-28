package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

const wireNode = "0f8fad5b-d9cb-469f-a165-70867728950e"

func headerKeys(h http.Header) []string {
	var keys []string
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// The three kinds of POST carry the same headers but for what authenticates
// them: a session op a MAC (and a node signature where required), token_rekey
// only the node signature, a code op the code's MAC and, on enrol_code, the
// node signature.
func TestRequestHeadersPerKindOfRequest(t *testing.T) {
	sk := ed25519.NewKeyFromSeed(make([]byte, 32))
	macKey := bytes.Repeat([]byte{9}, 32)
	body := []byte("body")
	const mainMs = 1_700_000_000_123
	r, err := newRequest("xc_agent/test", wireNode, "token_refresh", octet, 7, func() int64 { return mainMs })
	if err != nil {
		t.Fatal(err)
	}
	want, err := cc.RequestContext(cc.Request{
		Proto: Proto, Agent: "xc_agent/test", Method: "POST", Path: cc.PathPrefix + "token_refresh", ContentType: octet,
		Node: wireNode, Epoch: 7, TsMs: mainMs, Nonce: r.nonce,
	})
	if err != nil || !bytes.Equal(r.ctx, want) || len(r.nonce) != 16 || r.ts != mainMs {
		t.Fatalf("request context: %v", err)
	}

	base := []string{"Content-Type", cc.HAgent, cc.HEpoch, cc.HNode, cc.HNonce, cc.HProto, cc.HTs}
	for _, tc := range []struct {
		name    string
		mac     []byte
		sign    ed25519.PrivateKey
		headers []string
	}{
		{"session op", macKey, nil, append([]string{cc.HSig}, base...)},
		{"session op, node-signed", macKey, sk, append([]string{cc.HSig, cc.HNodeSig}, base...)},
		{"token_rekey", nil, sk, append([]string{cc.HNodeSig}, base...)},
	} {
		h := r.header(body, tc.mac, tc.sign)
		wantKeys := make([]string, len(tc.headers))
		for i, k := range tc.headers {
			wantKeys[i] = http.CanonicalHeaderKey(k)
		}
		sort.Strings(wantKeys)
		if got := headerKeys(h); !reflect.DeepEqual(got, wantKeys) {
			t.Fatalf("%s: headers %v, want %v", tc.name, got, wantKeys)
		}
		if h.Get(cc.HProto) != strconv.Itoa(Proto) || h.Get(cc.HAgent) != "xc_agent/test" || h.Get(cc.HNode) != wireNode ||
			h.Get(cc.HEpoch) != "7" || h.Get(cc.HTs) != strconv.FormatUint(mainMs, 10) ||
			h.Get(cc.HNonce) != hex.EncodeToString(r.nonce) || h.Get("Content-Type") != octet {
			t.Fatalf("%s: %v", tc.name, h)
		}
		if tc.mac != nil && h.Get(cc.HSig) != hex.EncodeToString(cc.MAC(macKey, r.ctx, body)) {
			t.Fatalf("%s: MAC", tc.name)
		}
		if tc.sign != nil {
			sig, _ := hex.DecodeString(h.Get(cc.HNodeSig))
			if !cc.VerifyNode(sk.Public().(ed25519.PublicKey), "request", append(append([]byte{}, r.ctx...), cc.SHA256(body)...), sig) {
				t.Fatalf("%s: node signature", tc.name)
			}
		}
	}

	r0, _ := newRequest("xc_agent/test", "sid:3", "enrol_code_status", "application/json", 0, func() int64 { return mainMs })
	if h := r0.header(body, macKey, nil); h.Get(cc.HEpoch) != "0" || h.Get("Content-Type") != "application/json" || h.Get(cc.HNode) != "sid:3" {
		t.Fatalf("code op: %v", h)
	}
	if bytes.Equal(r0.nonce, r.nonce) {
		t.Fatal("a request's nonce is drawn afresh")
	}
}

func TestReplyContextChecksTheMACAgainstTheRequest(t *testing.T) {
	macKey := bytes.Repeat([]byte{5}, 32)
	r, _ := newRequest("a", wireNode, "hello", octet, 1, func() int64 { return 1000 })
	body := []byte("reply")
	nonce := bytes.Repeat([]byte{1}, 16)
	resCtx, err := cc.ResponseContext(r.ctx, 200, octet, 2000, nonce)
	if err != nil {
		t.Fatal(err)
	}
	signed := func() http.Header {
		h := http.Header{}
		h.Set(cc.HTs, "2000")
		h.Set(cc.HNonce, hex.EncodeToString(nonce))
		h.Set(cc.HSig, hex.EncodeToString(cc.MAC(macKey, resCtx, body)))
		h.Set("Content-Type", octet)
		return h
	}
	if got, err := replyContext(macKey, r.ctx, 200, signed(), body); err != nil || !bytes.Equal(got, resCtx) {
		t.Fatalf("a good reply: %v", err)
	}
	other, _ := newRequest("a", wireNode, "hello", octet, 1, func() int64 { return 1000 })
	for name, try := range map[string]func() error{
		"another body":    func() error { _, err := replyContext(macKey, r.ctx, 200, signed(), []byte("replx")); return err },
		"another status":  func() error { _, err := replyContext(macKey, r.ctx, 201, signed(), body); return err },
		"another request": func() error { _, err := replyContext(macKey, other.ctx, 200, signed(), body); return err },
		"another key": func() error {
			_, err := replyContext(bytes.Repeat([]byte{6}, 32), r.ctx, 200, signed(), body)
			return err
		},
		"a short nonce": func() error {
			h := signed()
			h.Set(cc.HNonce, "0101")
			_, err := replyContext(macKey, r.ctx, 200, h, body)
			return err
		},
		"no stamp": func() error {
			h := signed()
			h.Del(cc.HTs)
			_, err := replyContext(macKey, r.ctx, 200, h, body)
			return err
		},
		"no MAC": func() error {
			h := signed()
			h.Del(cc.HSig)
			_, err := replyContext(macKey, r.ctx, 200, h, body)
			return err
		},
	} {
		if err := try(); err != ErrTransport {
			t.Fatalf("%s: %v, want ErrTransport", name, err)
		}
	}
}

func signedDenial(t *testing.T, key ed25519.PrivateKey, tag string, doc map[string]any) (http.Header, []byte) {
	t.Helper()
	body, _ := json.Marshal(doc)
	h := http.Header{}
	h.Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, cc.PanelSigInput(tag, body))))
	return h, body
}

func TestVerifyDenialTakesOnlyARefusalOfThisRequest(t *testing.T) {
	_, panel, _ := ed25519.GenerateKey(nil)
	_, other, _ := ed25519.GenerateKey(nil)
	pub := panel.Public().(ed25519.PublicKey)
	nonce := bytes.Repeat([]byte{3}, 16)
	doc := map[string]any{"reason": "CLOCK_SKEW", "node": wireNode, "req_nonce": hex.EncodeToString(nonce), "main_time_ms": 42}

	h, body := signedDenial(t, panel, "den", doc)
	d := verifyDenial(pub, wireNode, 401, h, body, nonce)
	if d == nil || d.Status != 401 || d.Reason != "CLOCK_SKEW" || d.MainTimeMs != 42 || !bytes.Equal(d.Doc, body) {
		t.Fatalf("a good denial: %+v", d)
	}
	if verifyDenial(pub, "sid:3", 401, h, body, nonce) != nil {
		t.Fatal("a denial for another node")
	}
	if verifyDenial(pub, wireNode, 401, h, body, bytes.Repeat([]byte{4}, 16)) != nil {
		t.Fatal("a denial for another request")
	}
	if h, body := signedDenial(t, other, "den", doc); verifyDenial(pub, wireNode, 401, h, body, nonce) != nil {
		t.Fatal("a denial signed by another key")
	}
	if h, body := signedDenial(t, panel, "hlt", doc); verifyDenial(pub, wireNode, 401, h, body, nonce) != nil {
		t.Fatal("a signature under another tag")
	}
	if verifyDenial(pub, wireNode, 401, http.Header{}, body, nonce) != nil {
		t.Fatal("an unsigned denial")
	}
}

// panelSigned decodes the signature strictly. enrolcode's pin used to ignore
// a decoding error; a header that does not decode cleanly never yields the
// 64 bytes a signature needs, so nothing it accepted is refused now.
func TestPanelSignedRefusesAMangledSignature(t *testing.T) {
	_, panel, _ := ed25519.GenerateKey(nil)
	pub := panel.Public().(ed25519.PublicKey)
	body := []byte(`{"typ":"xcvm-health"}`)
	sig := base64.RawURLEncoding.EncodeToString(ed25519.Sign(panel, cc.PanelSigInput("hlt", body)))
	h := http.Header{}
	h.Set(cc.HPanelSig, sig)
	if !panelSigned(pub, "hlt", h, body) {
		t.Fatal("a good signature")
	}
	if panelSigned(pub, "den", h, body) || panelSigned(pub, "hlt", h, append(body, ' ')) {
		t.Fatal("another tag or body")
	}
	for _, tail := range []string{"=", "==", "!", "A", "AA", "A!", "\x00", ".", "+", "/"} {
		h.Set(cc.HPanelSig, sig+tail)
		lenient, _ := base64.RawURLEncoding.DecodeString(sig + tail)
		if panelSigned(pub, "hlt", h, body) || cc.VerifyPanel(pub, "hlt", body, lenient) {
			t.Fatalf("a signature with %q after it", tail)
		}
	}
}

func TestGetSignedCapsTheDocument(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(cc.HPanelSig, "sig")
		w.WriteHeader(http.StatusTeapot)
		w.Write(bytes.Repeat([]byte("x"), maxSigned+100))
	}))
	defer srv.Close()
	st, h, body, err := getSigned(context.Background(), srv.Client(), srv.URL+"/health")
	if err != nil || st != http.StatusTeapot || h.Get(cc.HPanelSig) != "sig" || len(body) != maxSigned {
		t.Fatalf("status %d, %d bytes: %v", st, len(body), err)
	}
	if _, _, _, err := getSigned(context.Background(), srv.Client(), "http://127.0.0.1:1/health"); err == nil {
		t.Fatal("an unreachable URL")
	}
}

func TestPostWithRefusesAnOversizedReply(t *testing.T) {
	var got http.Header
	size := 10
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.Write(bytes.Repeat([]byte("x"), size))
	}))
	defer srv.Close()
	h := http.Header{}
	h.Set(cc.HNode, wireNode)
	st, _, rb, err := postWith(context.Background(), srv.Client(), srv.URL, h, []byte("b"))
	if err != nil || st != 200 || len(rb) != size || got.Get(cc.HNode) != wireNode {
		t.Fatalf("status %d, %d bytes: %v", st, len(rb), err)
	}
	size = MaxReply + 1
	if _, _, _, err := postWith(context.Background(), srv.Client(), srv.URL, h, []byte("b")); err != ErrTransport {
		t.Fatalf("an oversized reply: %v", err)
	}
}

// A refresh adds its epoch to those held; a re-key keeps only its own and
// takes MAIN's clock from its reply. Both clear the pending key and persist.
func TestTakeTokenAddsOrReplacesTheEpoch(t *testing.T) {
	f, st := newFake(t)
	c := NewClient(st, "xc_agent/test")
	secret := bytes.Repeat([]byte{8}, 32)

	ephSk, sealed := mintToken(t, f.panel, f.uuid, 2, secret)
	st.PendingEphSk = ephSk
	tok, err := c.takeToken(base64.StdEncoding.EncodeToString(sealed), 2, ephSk, nil, false, 0)
	if err != nil || tok.Epoch != 2 {
		t.Fatalf("refresh: %v", err)
	}
	if len(st.Epochs) != 2 || st.Epochs[0].Epoch != 2 || st.Epochs[1].Epoch != 1 || st.PendingEphSk != nil || len(c.sessions) != 2 {
		t.Fatalf("refresh kept %v, %d sessions", st.Epochs, len(c.sessions))
	}
	saved, err := loadRaw(st.path)
	if err != nil || len(saved.Epochs) != 2 {
		t.Fatalf("refresh saved: %v", err)
	}

	ephSk, sealed = mintToken(t, f.panel, f.uuid, 3, secret)
	mainMs := time.Now().Add(time.Hour).UnixMilli()
	if tok, err = c.takeToken(base64.StdEncoding.EncodeToString(sealed), 3, ephSk, nil, true, mainMs); err != nil || tok.Epoch != 3 {
		t.Fatalf("re-key: %v", err)
	}
	if len(st.Epochs) != 1 || st.Epochs[0].Epoch != 3 || len(c.sessions) != 1 {
		t.Fatalf("re-key kept %v, %d sessions", st.Epochs, len(c.sessions))
	}
	if d := c.MainNowMs() - mainMs; d < 0 || d > 5000 {
		t.Fatalf("re-key did not take MAIN's clock: off by %d ms", d)
	}

	if _, err := c.takeToken("not base64!", 4, ephSk, nil, true, 0); !errors.Is(err, ErrTransport) {
		t.Fatalf("a mangled token: %v", err)
	}
	if _, err := c.takeToken(base64.StdEncoding.EncodeToString(sealed), 4, ephSk, nil, true, 0); err == nil {
		t.Fatal("a token for another epoch")
	}
	if len(st.Epochs) != 1 || st.Epochs[0].Epoch != 3 {
		t.Fatalf("a refused token changed the state: %v", st.Epochs)
	}
}
