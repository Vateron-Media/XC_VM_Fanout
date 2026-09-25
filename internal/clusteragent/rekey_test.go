package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// rekeyMain is a MAIN in Go for the re-key path: it issues challenges, opens
// the sealed re-key body with its box key, checks the node signature and the
// challenge, and mints a token for the agent's new per-epoch key. reply lets a
// test corrupt the signed answer.
type rekeyMain struct {
	t        *testing.T
	panel    ed25519.PrivateKey
	boxSk    []byte
	nodePub  ed25519.PublicKey
	uuid     string
	licensed bool

	mu         sync.Mutex
	challenges map[string]bool
	keys       *cc.SessionKeys
	rekeys     int
	reply      func(doc map[string]any) (tag string, body map[string]any)
	healthDoc  []byte
}

func newRekeyMain(t *testing.T) (*rekeyMain, *State, *httptest.Server) {
	_, panel, _ := ed25519.GenerateKey(nil)
	boxSk, boxPub, _ := cc.NewX25519()
	st := NewState(filepath.Join(t.TempDir(), "state.json"))
	st.NodeUUID, st.ServerID, st.NodeSignSeed, st.Enrolled = "0f8fad5b-d9cb-469f-a165-70867728950e", 3, make([]byte, 32), true
	st.PanelSignPub = panel.Public().(ed25519.PublicKey)
	m := &rekeyMain{
		t: t, panel: panel, boxSk: boxSk, nodePub: st.SignKey().Public().(ed25519.PublicKey), uuid: st.NodeUUID,
		licensed: true, challenges: map[string]bool{},
		reply: func(doc map[string]any) (string, map[string]any) { return "pre", doc },
	}
	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	m.healthDoc, _ = json.Marshal(map[string]any{"v": 1, "typ": "xcvm-health", "panel_box_pub": base64.StdEncoding.EncodeToString(boxPub)})
	return m, st, srv
}

func (m *rekeyMain) signed(w http.ResponseWriter, status int, tag string, doc any) {
	b, _ := json.Marshal(doc)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput(tag, b))))
	w.WriteHeader(status)
	w.Write(b)
}

func (m *rekeyMain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch r.URL.Path {
	case "/cluster/v1/health":
		w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.panel, cc.PanelSigInput("hlt", m.healthDoc))))
		w.Write(m.healthDoc)
	case "/cluster/v1/challenge":
		raw := make([]byte, 32)
		raw[0] = byte(len(m.challenges) + 1)
		m.challenges[string(raw)] = true
		m.signed(w, 200, "hlt", map[string]any{"v": 1, "typ": "xcvm-challenge", "cn": r.URL.Query().Get("cn"), "challenge": base64.StdEncoding.EncodeToString(raw), "licence_ok": m.licensed, "main_time_ms": time.Now().UnixMilli()})
	case "/cluster/v1/token_rekey":
		m.rekey(w, r)
	default:
		// Session ops: boxed replies under the re-keyed epoch, TOKEN_EXPIRED before it.
		ts, _ := strconv.ParseUint(r.Header.Get(cc.HTs), 10, 64)
		nonce, _ := hex.DecodeString(r.Header.Get(cc.HNonce))
		if m.keys == nil {
			m.signed(w, 401, "den", map[string]any{"v": 1, "typ": "xcvm-denial", "reason": "TOKEN_EXPIRED", "node": m.uuid, "req_nonce": hex.EncodeToString(nonce)})
			return
		}
		epoch, _ := strconv.ParseUint(r.Header.Get(cc.HEpoch), 10, 64)
		reqCtx, _ := cc.RequestContext(cc.Request{Proto: 1, Agent: r.Header.Get(cc.HAgent), Method: r.Method, Path: r.URL.Path, ContentType: r.Header.Get("Content-Type"), Node: m.uuid, Epoch: epoch, TsMs: ts, Nonce: nonce})
		rn := make([]byte, 16)
		rts := uint64(time.Now().UnixMilli())
		resCtx, _ := cc.ResponseContext(reqCtx, 200, octet, rts, rn)
		body, _ := cc.Box(m.keys.EncDown, resCtx, []byte(`{"state":"active"}`))
		w.Header().Set("Content-Type", octet)
		w.Header().Set(cc.HTs, strconv.FormatUint(rts, 10))
		w.Header().Set(cc.HNonce, hex.EncodeToString(rn))
		w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(m.keys.MacDown, resCtx, body)))
		w.Write(body)
	}
}

func (m *rekeyMain) rekey(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	ts, _ := strconv.ParseUint(r.Header.Get(cc.HTs), 10, 64)
	nonce, _ := hex.DecodeString(r.Header.Get(cc.HNonce))
	reqCtx, err := cc.RequestContext(cc.Request{Proto: 1, Agent: r.Header.Get(cc.HAgent), Method: "POST", Path: r.URL.Path, ContentType: r.Header.Get("Content-Type"), Node: r.Header.Get(cc.HNode), Epoch: 0, TsMs: ts, Nonce: nonce})
	sig, _ := hex.DecodeString(r.Header.Get(cc.HNodeSig))
	if err != nil || r.Header.Get(cc.HEpoch) != "0" || r.Header.Get(cc.HSig) != "" || !cc.VerifyNode(m.nodePub, "request", append(append([]byte{}, reqCtx...), cc.SHA256(body)...), sig) {
		m.t.Errorf("re-key request is not node-signed epoch 0 without a MAC")
		w.WriteHeader(400)
		return
	}
	plain, err := cc.Open(m.boxSk, "rekey", string(reqCtx), body)
	var raw map[string]string
	if err != nil || json.Unmarshal(plain, &raw) != nil {
		m.t.Errorf("re-key body does not open with the panel box key under the request context: %v", err)
		w.WriteHeader(400)
		return
	}
	ch, _ := base64.StdEncoding.DecodeString(raw["challenge"])
	if !m.challenges[string(ch)] {
		m.signed(w, 401, "den", map[string]any{"reason": "CHALLENGE", "node": m.uuid, "req_nonce": hex.EncodeToString(nonce)})
		return
	}
	delete(m.challenges, string(ch))
	ephPub, _ := base64.StdEncoding.DecodeString(raw["eph_pub"])
	m.rekeys++
	secret := make([]byte, 32)
	secret[1] = byte(m.rekeys)
	keys := cc.DeriveSession(secret)
	m.keys = &keys
	epoch := 4 + m.rekeys
	now := time.Now().Unix()
	doc, _ := json.Marshal(map[string]any{
		"v": 1, "typ": "xcvm-token", "node_uuid": m.uuid, "server_id": 3, "gen": 1, "epoch": epoch,
		"iat": now, "nbf": now - 120, "exp": now + 4500, "kid": "00000000", "rotation_min": 60,
		"grace_min": 15, "refresh_at": now + 1800, "token": hex.EncodeToString(secret),
	})
	tb := binary.BigEndian.AppendUint32(nil, uint32(len(doc)))
	tb = append(append(tb, doc...), ed25519.Sign(m.panel, cc.PanelSigInput("tok", doc))...)
	sealed, _ := cc.Seal(ephPub, "token", m.uuid, tb)
	tag, out := m.reply(map[string]any{"v": 1, "typ": "xcvm-rekey", "node": m.uuid, "req_nonce": hex.EncodeToString(nonce), "token_sealed": base64.StdEncoding.EncodeToString(sealed), "epoch": epoch, "main_time_ms": time.Now().UnixMilli()})
	m.signed(w, 200, tag, out)
}

func TestRekeyTrustsOnlyASignedAnswerToThisRequest(t *testing.T) {
	m, st, _ := newRekeyMain(t)
	c := NewClient(st, "xc_agent/test")
	ctx := context.Background()

	cases := map[string]func(doc map[string]any) (string, map[string]any){
		"signed as a denial":  func(doc map[string]any) (string, map[string]any) { return "den", doc },
		"for another request": func(doc map[string]any) (string, map[string]any) { doc["req_nonce"] = "00"; return "pre", doc },
		"for another node":    func(doc map[string]any) (string, map[string]any) { doc["node"] = "x"; return "pre", doc },
		"not a re-key reply":  func(doc map[string]any) (string, map[string]any) { doc["typ"] = "xcvm-health"; return "pre", doc },
	}
	for name, reply := range cases {
		m.reply = reply
		if _, err := c.Rekey(ctx, nil); !errors.Is(err, ErrTransport) {
			t.Errorf("%s: got %v, want a transport error", name, err)
		}
		if len(st.Epochs) != 0 {
			t.Fatalf("%s: an epoch was stored", name)
		}
	}

	m.reply = func(doc map[string]any) (string, map[string]any) { return "pre", doc }
	m.licensed = false
	if _, err := c.Rekey(ctx, nil); !errors.Is(err, ErrUnlicensed) {
		t.Fatalf("want ErrUnlicensed while MAIN is unlicensed, got %v", err)
	}
	m.licensed = true
	tok, err := c.Rekey(ctx, map[string]any{"instance_id": "i"})
	if err != nil || len(st.Epochs) != 1 || st.Epochs[0].Epoch != tok.Epoch || len(st.PanelBoxPub) != 32 {
		t.Fatalf("genuine re-key: %v %+v", err, st.Epochs)
	}
	if cur, ok := c.Current(); !ok || cur.Epoch != tok.Epoch {
		t.Fatal("the re-keyed epoch is not current")
	}
}

func TestAgentRekeysInsteadOfStopping(t *testing.T) {
	_, st, _ := newRekeyMain(t)
	c := NewClient(st, "xc_agent/test")
	var heartbeats int
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := &Agent{Client: c, Version: "t", Interval: 20 * time.Millisecond, Logf: t.Logf, Telemetry: func() map[string]any {
		if heartbeats++; heartbeats == 2 {
			cancel()
		}
		return nil
	}}
	// No epoch at all (every token expired while the agent was down).
	if err := a.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := c.Current(); !ok || len(st.Epochs) != 1 {
		t.Fatal("the agent did not re-key")
	}

	// An unenrolled node cannot re-key: its enrolment window has passed.
	st.Enrolled, st.Epochs = false, nil
	c = NewClient(st, "xc_agent/test")
	a.Client = c
	if err := a.Run(context.Background()); !errors.Is(err, ErrStop) {
		t.Fatalf("an unenrolled node without a token must stop, got %v", err)
	}
}
