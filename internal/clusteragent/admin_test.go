package clusteragent

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// relayRig is a parent LB (server 3) receiving an /admin/live relay pull from a
// child (server 7, gen 1), plus the panel key that mints the "rly" ticket.
type relayRig struct {
	a     *Agent
	panel ed25519.PrivateKey
	child ed25519.PrivateKey
	nowMs int64
}

func newRelayRig(t *testing.T) *relayRig {
	t.Helper()
	f, st := newFake(t) // ServerID = 3, PanelSignPub set
	replica := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: replica}
	now := time.Now().Unix()
	a.Client.setMainTime(now * 1000)
	cpub, cpriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	writeServersJSON(t, replica, 7, 1, "active", cpub) // the child node, active at gen 1
	return &relayRig{a: a, panel: f.panel, child: cpriv, nowMs: now * 1000}
}

// mintRly signs a relay ticket as MAIN's TicketService does.
func (rg *relayRig) mintRly(t *testing.T, fields map[string]any) string {
	t.Helper()
	now := rg.nowMs / 1000
	doc := map[string]any{"v": 1, "typ": "xcvm-relay", "tid": "r0123456789", "iat": now - 60, "exp": now + 3600}
	for k, v := range fields {
		doc[k] = v
	}
	b, _ := json.Marshal(doc)
	return cc.JoinSigned(b, ed25519.Sign(rg.panel, cc.PanelSigInput("rly", b)))
}

// req builds the child's signed GET /admin/live?stream=42 relay request.
func (rg *relayRig) req(t *testing.T, wire string, nonce []byte) *http.Request {
	t.Helper()
	target := "/admin/live?stream=42&extension=ts"
	hdr, err := cc.RelayAuthHeader(rg.child, wire, "GET", target, rg.nowMs, nonce)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XCVM-Relay", wire)
	r.Header.Set("X-XCVM-Relay-Auth", hdr)
	return r
}

func relayFields(parent, stream, childSid, childGen int) map[string]any {
	return map[string]any{"parent_sid": parent, "stream_id": stream, "child_sid": childSid, "child_gen": childGen}
}

func TestAdmitRelayValid(t *testing.T) {
	rg := newRelayRig(t)
	wire := rg.mintRly(t, relayFields(3, 42, 7, 1))
	if !rg.a.admitRelay(42, rg.req(t, wire, freshNonce(t)), rg.nowMs) {
		t.Fatal("a valid relay ticket + proof was not admitted")
	}
}

func TestAdmitRelayWrongParent(t *testing.T) {
	rg := newRelayRig(t)
	wire := rg.mintRly(t, relayFields(99, 42, 7, 1)) // parent is not this node
	if rg.a.admitRelay(42, rg.req(t, wire, freshNonce(t)), rg.nowMs) {
		t.Fatal("a ticket naming another parent was admitted")
	}
}

func TestAdmitRelayWrongStream(t *testing.T) {
	rg := newRelayRig(t)
	wire := rg.mintRly(t, relayFields(3, 7, 7, 1)) // ticket for stream 7, request for 42
	if rg.a.admitRelay(42, rg.req(t, wire, freshNonce(t)), rg.nowMs) {
		t.Fatal("a ticket for a different stream was admitted")
	}
}

func TestAdmitRelayInactiveChild(t *testing.T) {
	rg := newRelayRig(t)
	wire := rg.mintRly(t, relayFields(3, 42, 7, 2)) // child_gen 2, servers.json has gen 1
	if rg.a.admitRelay(42, rg.req(t, wire, freshNonce(t)), rg.nowMs) {
		t.Fatal("a ticket naming the child at the wrong generation was admitted")
	}
}

func TestAdmitRelayBadProof(t *testing.T) {
	rg := newRelayRig(t)
	wire := rg.mintRly(t, relayFields(3, 42, 7, 1))
	r := rg.req(t, wire, freshNonce(t))
	bad := r.Header.Get("X-XCVM-Relay-Auth")
	r.Header.Set("X-XCVM-Relay-Auth", bad[:len(bad)-4]+"AAAA") // corrupt the signature
	if rg.a.admitRelay(42, r, rg.nowMs) {
		t.Fatal("a corrupt relay-auth proof was admitted")
	}
}

func TestAdmitRelayNonceReplay(t *testing.T) {
	rg := newRelayRig(t)
	wire := rg.mintRly(t, relayFields(3, 42, 7, 1))
	n := freshNonce(t)
	if !rg.a.admitRelay(42, rg.req(t, wire, n), rg.nowMs) {
		t.Fatal("first use rejected")
	}
	if rg.a.admitRelay(42, rg.req(t, wire, n), rg.nowMs) {
		t.Fatal("a replayed nonce was admitted")
	}
}

func TestAdmitRelayExpiredTicket(t *testing.T) {
	rg := newRelayRig(t)
	now := rg.nowMs / 1000
	doc := map[string]any{"v": 1, "typ": "xcvm-relay", "tid": "r0123456789", "iat": now - 7200, "exp": now - 3600,
		"parent_sid": 3, "stream_id": 42, "child_sid": 7, "child_gen": 1}
	b, _ := json.Marshal(doc)
	wire := cc.JoinSigned(b, ed25519.Sign(rg.panel, cc.PanelSigInput("rly", b)))
	if rg.a.admitRelay(42, rg.req(t, wire, freshNonce(t)), rg.nowMs) {
		t.Fatal("an expired ticket was admitted")
	}
}
