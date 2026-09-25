package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// A minimal MAIN in Go: it mints a token the way xcvm_core does (doc signed
// "tok", sealed to the agent's epoch key) and answers with whatever the test
// asks for, so the client's trust decisions are tested without PHP.
type fakeMain struct {
	panel  ed25519.PrivateKey
	keys   cc.SessionKeys
	uuid   string
	answer func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte)
}

func newFake(t *testing.T) (*fakeMain, *State) {
	_, panel, _ := ed25519.GenerateKey(nil)
	uuid := "0f8fad5b-d9cb-469f-a165-70867728950e"
	secret := make([]byte, 32)
	secret[0] = 7
	now := time.Now().Unix()
	doc, _ := json.Marshal(map[string]any{
		"v": 1, "typ": "xcvm-token", "node_uuid": uuid, "server_id": 3, "gen": 1, "epoch": 1,
		"iat": now, "nbf": now - 120, "exp": now + 4500, "kid": "00000000", "rotation_min": 60,
		"grace_min": 15, "refresh_at": now + 1800, "token": hex.EncodeToString(secret),
	})
	body := binary.BigEndian.AppendUint32(nil, uint32(len(doc)))
	body = append(append(body, doc...), ed25519.Sign(panel, cc.PanelSigInput("tok", doc))...)
	ephSk, ephPub, _ := cc.NewX25519()
	sealed, err := cc.Seal(ephPub, "token", uuid, body)
	if err != nil {
		t.Fatal(err)
	}
	st := NewState(filepath.Join(t.TempDir(), "state.json"))
	st.NodeUUID, st.ServerID, st.NodeSignSeed = uuid, 3, make([]byte, 32)
	st.PanelSignPub = panel.Public().(ed25519.PublicKey)
	st.Epochs = []Epoch{{Epoch: 1, EphSk: ephSk, TokenSealed: sealed}}
	return &fakeMain{panel: panel, keys: cc.DeriveSession(secret), uuid: uuid}, st
}

func (f *fakeMain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ts, _ := strconv.ParseUint(r.Header.Get(cc.HTs), 10, 64)
	nonce, _ := hex.DecodeString(r.Header.Get(cc.HNonce))
	reqCtx, _ := cc.RequestContext(cc.Request{
		Proto: 1, Agent: r.Header.Get(cc.HAgent), Method: r.Method, Path: r.URL.Path,
		ContentType: r.Header.Get("Content-Type"), Node: r.Header.Get(cc.HNode), Epoch: 1, TsMs: ts, Nonce: nonce,
	})
	f.answer(w, r, reqCtx, nonce)
}

func (f *fakeMain) deny(w http.ResponseWriter, key ed25519.PrivateKey, node string, nonce []byte) {
	doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-denial", "reason": "NODE_REVOKED", "node": node, "req_nonce": hex.EncodeToString(nonce)})
	w.Header().Set(cc.HPanelSig, base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, cc.PanelSigInput("den", doc))))
	w.WriteHeader(http.StatusForbidden)
	w.Write(doc)
}

func TestClientTrustsOnlyAuthenticatedReplies(t *testing.T) {
	f, st := newFake(t)
	srv := httptest.NewServer(f)
	defer srv.Close()
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	c := NewClient(st, "xc_agent/test")
	ctx := context.Background()
	_, stranger, _ := ed25519.GenerateKey(nil)

	// A boxed reply under the session's down keys is accepted.
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, _ []byte) {
		nonce := make([]byte, 16)
		ts := uint64(time.Now().UnixMilli())
		resCtx, _ := cc.ResponseContext(reqCtx, 200, "application/octet-stream", ts, nonce)
		body, _ := cc.Box(f.keys.EncDown, resCtx, []byte(`{"state":"active","main_time_ms":`+strconv.FormatUint(ts, 10)+`}`))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(cc.HTs, strconv.FormatUint(ts, 10))
		w.Header().Set(cc.HNonce, hex.EncodeToString(nonce))
		w.Header().Set(cc.HSig, hex.EncodeToString(cc.MAC(f.keys.MacDown, resCtx, body)))
		w.Write(body)
	}
	var r Reply
	if err := c.Call(ctx, "heartbeat", map[string]any{}, &r, false); err != nil || r.State != "active" {
		t.Fatalf("genuine reply: %v %+v", err, r)
	}

	cases := map[string]func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte){
		"denial signed by another key": func(w http.ResponseWriter, _ *http.Request, _, nonce []byte) { f.deny(w, stranger, f.uuid, nonce) },
		"denial for another request": func(w http.ResponseWriter, _ *http.Request, _, _ []byte) {
			f.deny(w, f.panel, f.uuid, make([]byte, 16))
		},
		"denial for another node": func(w http.ResponseWriter, _ *http.Request, _, nonce []byte) {
			f.deny(w, f.panel, "11111111-1111-4111-a111-111111111111", nonce)
		},
		"reply under the wrong MAC": func(w http.ResponseWriter, _ *http.Request, _, _ []byte) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set(cc.HTs, "1")
			w.Header().Set(cc.HNonce, hex.EncodeToString(make([]byte, 16)))
			w.Header().Set(cc.HSig, hex.EncodeToString(make([]byte, 32)))
			w.Write([]byte("xb1 not really"))
		},
		"plain error page": func(w http.ResponseWriter, _ *http.Request, _, _ []byte) { http.Error(w, "bad gateway", 502) },
	}
	for name, answer := range cases {
		f.answer = answer
		err := c.Call(ctx, "heartbeat", map[string]any{}, nil, false)
		var d *Denial
		if !errors.Is(err, ErrTransport) || errors.As(err, &d) {
			t.Errorf("%s: got %v, want a transport error", name, err)
		}
	}

	f.answer = func(w http.ResponseWriter, _ *http.Request, _, nonce []byte) { f.deny(w, f.panel, f.uuid, nonce) }
	err := c.Call(ctx, "heartbeat", map[string]any{}, nil, false)
	var d *Denial
	if !errors.As(err, &d) || d.Reason != "NODE_REVOKED" || !fatal(err) {
		t.Fatalf("genuine denial: %v", err)
	}
}

func TestStateKeepsTwoEpochsAndRoundTrips(t *testing.T) {
	_, st := newFake(t)
	st.AddEpoch(Epoch{Epoch: 2, EphSk: make([]byte, 32)})
	st.AddEpoch(Epoch{Epoch: 3, EphSk: make([]byte, 32)})
	if len(st.Epochs) != 2 || st.Epochs[0].Epoch != 3 || st.Epochs[1].Epoch != 2 {
		t.Fatalf("epochs: %+v", st.Epochs)
	}
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	back, err := LoadState(st.path)
	if err != nil || back.NodeUUID != st.NodeUUID || len(back.Epochs) != 2 {
		t.Fatalf("round trip: %v %+v", err, back)
	}
}

func TestSASMatchesThePanel(t *testing.T) {
	// EnrolmentService::sas() on the panel gives this for the same inputs.
	got := SAS("0f8fad5b-d9cb-469f-a165-70867728950e", bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32))
	if got != "FZPT-OIPM-JQFA-HIKW-RI3Q-73ZK" {
		t.Fatalf("SAS %s", got)
	}
}

func TestKeygenIsIdempotentUntilInstalled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster", "agent.json")
	uuid := "0f8fad5b-d9cb-469f-a165-70867728950e"
	a, err := Keygen(path, uuid)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Keygen(path, uuid)
	if err != nil || *a != *b {
		t.Fatalf("a retried keygen changed the keys: %v", err)
	}
	c, err := Keygen(path, "11111111-1111-4111-a111-111111111111")
	if err != nil || c.SignPub == a.SignPub {
		t.Fatalf("a new uuid kept the old identity: %v", err)
	}
	if _, err := Keygen(path, "not-a-uuid"); err == nil {
		t.Fatal("bad uuid accepted")
	}
	if _, err := LoadState(path); err == nil {
		t.Fatal("a state without a panel key loaded as complete")
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Fatalf("state mode %v", fi.Mode().Perm())
	}
}

func TestInstallRefusesAForeignToken(t *testing.T) {
	_, st := newFake(t) // a token sealed to another key, for another node's keys
	path := filepath.Join(t.TempDir(), "agent.json")
	if _, err := Keygen(path, st.NodeUUID); err != nil {
		t.Fatal(err)
	}
	err := Install(path, InstallData{ServerID: 3, PanelSignPub: st.PanelSignPub, MainURLs: []string{"http://x/cluster/v1/"}, Epoch: 1, TokenSealed: st.Epochs[0].TokenSealed})
	if err == nil {
		t.Fatal("installed a token sealed to another key")
	}
}
