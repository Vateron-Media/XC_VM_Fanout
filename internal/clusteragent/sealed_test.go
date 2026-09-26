package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// licenceMain refuses the session with LICENCE_INVALID carrying the kills it
// holds, sealed to the node, until licensed; then it serves heartbeats, acks
// and a commands long-poll.
type licenceMain struct {
	*fakeMain
	mu        sync.Mutex
	licensed  bool
	sealed    string
	poll      []WireCommand
	acks      []map[string]any
	ops       []string
	challenge int
}

func (m *licenceMain) wire(t *testing.T, key ed25519.PrivateKey, over map[string]any) WireCommand {
	now := time.Now().Unix()
	doc := map[string]any{"v": 1, "type": "conn.drop", "exp": now + 300, "iat": now, "cmd_id": strings.Repeat("k", 32), "seq": 7,
		"node_uuid": m.uuid, "gen": 1, "dedupe_key": nil, "args": map[string]any{"uuid": "viewer1"}}
	for k, v := range over {
		doc[k] = v
	}
	b, _ := json.Marshal(doc)
	return WireCommand{Doc: string(b), Sig: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, cc.PanelSigInput("cmd", b))), Seq: uint64(doc["seq"].(int))}
}

func newLicenceAgent(t *testing.T) (*licenceMain, *Agent, *[]string) {
	f, st := newFake(t)
	sk, pub, _ := cc.NewX25519()
	st.NodeBoxSk, st.Enrolled = sk, true
	m := &licenceMain{fakeMain: f}
	f.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		req := f.opened(r, reqCtx)
		m.mu.Lock()
		defer m.mu.Unlock()
		m.ops = append(m.ops, op)
		if !m.licensed {
			f.refuse(w, 403, nonce, "LICENCE_INVALID", map[string]any{"commands_sealed": m.sealed})
			return
		}
		switch op {
		case "ack":
			m.acks = append(m.acks, req)
			f.box(w, reqCtx, map[string]any{"ok": true})
		case "commands":
			f.box(w, reqCtx, map[string]any{"commands": m.poll})
			m.poll = nil
		default:
			f.box(w, reqCtx, map[string]any{"state": "active", "mode": 1, "flows": FlowCommands})
		}
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	var mu sync.Mutex
	ran := &[]string{}
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf, Interval: 50 * time.Millisecond,
		Exec: func(_ context.Context, cmd *Command, _ WireCommand) (bool, []byte) {
			mu.Lock()
			*ran = append(*ran, cmd.Type+":"+cmd.CmdID[:1])
			mu.Unlock()
			return true, []byte(`{"result":true}`)
		}}
	seal := func(cmds []WireCommand) string {
		b, _ := json.Marshal(cmds)
		sealed, _ := cc.Seal(pub, SealCommands, f.uuid, b)
		return base64.StdEncoding.EncodeToString(sealed)
	}
	_, stranger, _ := ed25519.GenerateKey(nil)
	m.sealed = seal([]WireCommand{
		m.wire(t, f.panel, nil), // the kill
		m.wire(t, stranger, map[string]any{"cmd_id": strings.Repeat("f", 32), "seq": 8}),                                                     // forged
		m.wire(t, f.panel, map[string]any{"cmd_id": strings.Repeat("o", 32), "seq": 9, "node_uuid": "11111111-1111-4111-a111-111111111111"}), // another node's
		m.wire(t, f.panel, map[string]any{"cmd_id": strings.Repeat("e", 32), "seq": 10, "exp": time.Now().Unix() - 1}),                       // expired
	})
	return m, a, ran
}

func TestKillsRideAHardModeLicenceDenial(t *testing.T) {
	m, a, ran := newLicenceAgent(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for i := 0; i < 200 && !cond(); i++ {
			time.Sleep(10 * time.Millisecond)
		}
		if !cond() {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	waitFor("the kill", func() bool { _, ok := a.Client.State.kept(strings.Repeat("k", 32)); return ok })
	// It keeps asking at the heartbeat's pace (it may carry more kills), and
	// never runs a kill twice.
	waitFor("more hellos", func() bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.ops) >= 5 })
	a.sealedMu.Lock()
	if len(*ran) != 1 || (*ran)[0] != "conn.drop:k" {
		t.Fatalf("ran %v, want the one genuine kill once", *ran)
	}
	a.sealedMu.Unlock()
	if a.Client.State.CmdSeq != 0 {
		t.Fatalf("the long-poll high-water moved to %d", a.Client.State.CmdSeq)
	}
	back, _ := loadRaw(a.Client.State.path)
	if len(back.SealedCmds) != 1 || back.SealedCmds[0].Acked {
		t.Fatalf("kept in the state file: %+v", back.SealedCmds)
	}

	// The licence is back: the kill is acked; the long-poll then hands out
	// an RPC queued below it and the kill again: the RPC runs, the kill is
	// acked with its result and not run again.
	m.mu.Lock()
	m.licensed = true
	rpc := m.wire(t, m.panel, map[string]any{"type": "node.rpc", "cmd_id": strings.Repeat("r", 32), "seq": 6})
	m.poll = []WireCommand{rpc, m.wire(t, m.panel, nil)}
	m.mu.Unlock()
	waitFor("the ack", func() bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.acks) >= 3 })
	cancel()
	<-done
	m.mu.Lock()
	defer m.mu.Unlock()
	kills := 0
	for _, a := range m.acks {
		if a["cmd_id"] == strings.Repeat("k", 32) {
			kills++
			if a["ok"] != true || a["result"] != `{"result":true}` {
				t.Fatalf("the kill's ack: %v", a)
			}
		}
	}
	if kills < 1 {
		t.Fatalf("the kill was never acked: %v", m.acks)
	}
	if strings.Join(*ran, ",") != "conn.drop:k,node.rpc:r" {
		t.Fatalf("ran %v", *ran)
	}
	if a.Client.State.CmdSeq != 7 {
		t.Fatalf("high-water %d after the long-poll, want 7", a.Client.State.CmdSeq)
	}
	for _, op := range m.ops {
		if op == "challenge" || op == "token_rekey" {
			t.Fatal("re-keyed while the licence was gone and the token held")
		}
	}
}

func TestSealedCommandsForAnotherNodeDoNotOpen(t *testing.T) {
	_, a, _ := newLicenceAgent(t)
	_, otherPub, _ := cc.NewX25519()
	sealed, _ := cc.Seal(otherPub, SealCommands, a.Client.State.NodeUUID, []byte(`[]`))
	if _, err := a.Client.openSealedCommands(base64.StdEncoding.EncodeToString(sealed)); err == nil {
		t.Fatal("opened a list sealed to another key")
	}
	pub, _ := cc.X25519Public(a.Client.State.NodeBoxSk)
	sealed, _ = cc.Seal(pub, "replica", a.Client.State.NodeUUID, []byte(`[]`))
	if _, err := a.Client.openSealedCommands(base64.StdEncoding.EncodeToString(sealed)); err == nil {
		t.Fatal("opened a list sealed for another purpose")
	}
}
