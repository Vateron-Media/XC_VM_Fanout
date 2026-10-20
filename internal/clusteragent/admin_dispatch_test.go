package clusteragent

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

// adminRig wires one parent LB (server 3) with every admission input: a child
// (sid 7, gen 1) for the relay arm, a dataplane-off server (id 5, ip .50) for the
// password arm, the shared pass, a viewer key in the gateway policy for uitokens,
// and a loopback key.
type adminRig struct {
	a        *Agent
	panel    ed25519.PrivateKey
	child    ed25519.PrivateKey
	vkey     []byte
	vctx     []byte
	nowMs    int64
	serverIP string
	pass     string
}

func newAdminRig(t *testing.T) *adminRig {
	t.Helper()
	f, st := newFake(t) // ServerID 3
	dir := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: dir}
	now := time.Now().Unix()
	a.Client.setMainTime(now * 1000)

	cpub, cpriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	// servers.json: the child node (relay) + a dataplane-off server (password).
	servers := fmt.Sprintf(`{"data":{"servers":[{"id":5,"server_ip":"203.0.113.50","private_ip":""}],"nodes":[`+
		`{"sid":7,"gen":1,"state":"active","ed_pub":%q,"box_pub":""},`+
		`{"sid":5,"gen":1,"state":"active","ed_pub":"","box_pub":"","dataplane":false}]}}`,
		base64.StdEncoding.EncodeToString(cpub))
	if err := os.WriteFile(filepath.Join(dir, "servers.json"), []byte(servers), 0o644); err != nil {
		t.Fatal(err)
	}
	writeSecrets(t, dir, "s3cr3t")

	// gateway policy with one viewer key + context, for uitoken decode.
	vkey, vctx := []byte("0123456789abcdef0123456789abcdef"), []byte("openssl-extra")
	policy := fmt.Sprintf(`{"ip_subnet_match":false,"keys":{"viewer":[{"hex":%q,"until":null}],"shared":[],"context":[{"hex":%q,"until":null}],"accept_legacy_cbc":false}}`,
		hex.EncodeToString(vkey), hex.EncodeToString(vctx))
	pf := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(pf, []byte(policy), 0o644); err != nil {
		t.Fatal(err)
	}
	a.GatewayPolicyFile = pf

	kf := filepath.Join(dir, "loopback.key")
	_ = os.WriteFile(kf, []byte("0123456789abcdef0123456789abcdef"), 0o600)
	a.LoopbackKeyFile = kf

	return &adminRig{a: a, panel: f.panel, child: cpriv, vkey: vkey, vctx: vctx, nowMs: now * 1000, serverIP: "203.0.113.50", pass: "s3cr3t"}
}

func (rg *adminRig) relayReq(t *testing.T, stream int64, extra http.Header) *http.Request {
	t.Helper()
	now := rg.nowMs / 1000
	doc := map[string]any{"v": 1, "typ": "xcvm-relay", "tid": "r0123456789", "iat": now - 60, "exp": now + 3600,
		"parent_sid": 3, "stream_id": stream, "child_sid": 7, "child_gen": 1}
	b, _ := json.Marshal(doc)
	wire := cc.JoinSigned(b, ed25519.Sign(rg.panel, cc.PanelSigInput("rly", b)))
	target := fmt.Sprintf("/admin/live?stream=%d&extension=ts", stream)
	hdr, err := cc.RelayAuthHeader(rg.child, wire, "GET", target, rg.nowMs, freshNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XCVM-Relay", wire)
	r.Header.Set("X-XCVM-Relay-Auth", hdr)
	for k, v := range extra {
		r.Header[k] = v
	}
	return r
}

func (rg *adminRig) uitoken(stream int, ip string, expires int64) string {
	b, _ := json.Marshal(map[string]any{"stream_id": stream, "ip": ip, "expires": expires})
	return viewertoken.Seal(b, rg.vkey, rg.vctx, make([]byte, viewertoken.NonceLen))
}

func TestDispatchRelayHeaderNoPasswordFallback(t *testing.T) {
	rg := newAdminRig(t)
	// A relay header is present but the ticket is garbage; a valid password is also
	// supplied. RelayGuard judges a relay-presented request by the relay arm alone.
	target := "/admin/live?stream=42&extension=ts&password=" + rg.pass
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XCVM-Relay", "garbage.ticket")
	if _, ok := rg.a.admitAdmin(42, r, rg.serverIP, true, rg.nowMs); ok {
		t.Fatal("a bad relay ticket fell back to the password arm")
	}
}

func TestDispatchThumbNoPasswordArm(t *testing.T) {
	rg := newAdminRig(t)
	target := "/admin/thumb?stream=42&password=" + rg.pass
	r := httptest.NewRequest(http.MethodGet, target, nil)
	// passwordOK=false (thumb): a valid password must not admit.
	if _, ok := rg.a.admitAdmin(42, r, rg.serverIP, false, rg.nowMs); ok {
		t.Fatal("thumb (no password arm) admitted a password")
	}
	// but a valid relay ticket does.
	if adm, ok := rg.a.admitAdmin(42, rg.relayReq(t, 42, nil), rg.serverIP, false, rg.nowMs); !ok || adm.kind != admitRelayKind {
		t.Fatalf("thumb relay admission: ok=%v kind=%d", ok, adm.kind)
	}
}

func TestDispatchUitokenPrecedenceAndStream(t *testing.T) {
	rg := newAdminRig(t)
	// A valid uitoken for stream 99, presented alongside relay headers for 42: the
	// uitoken wins and its stream (99) is used.
	extra := http.Header{"X-XCVM-Relay": {"garbage"}}
	r := rg.relayReq(t, 42, extra)
	q := r.URL.Query()
	q.Set("uitoken", rg.uitoken(99, rg.serverIP, 2_000_000_000))
	r.URL.RawQuery = q.Encode()
	adm, ok := rg.a.admitAdmin(42, r, rg.serverIP, true, rg.nowMs)
	if !ok || adm.kind != admitUitoken || adm.streamID != 99 {
		t.Fatalf("uitoken precedence: ok=%v kind=%d stream=%d", ok, adm.kind, adm.streamID)
	}
}

func TestDispatchInvalidUitokenNoFallback(t *testing.T) {
	rg := newAdminRig(t)
	// A present-but-invalid uitoken must not fall back to the (otherwise valid) relay arm.
	r := rg.relayReq(t, 42, nil)
	q := r.URL.Query()
	q.Set("uitoken", "not-a-real-token")
	r.URL.RawQuery = q.Encode()
	if _, ok := rg.a.admitAdmin(42, r, rg.serverIP, true, rg.nowMs); ok {
		t.Fatal("an invalid uitoken fell back to the relay arm")
	}
}

func TestDispatchPasswordArm(t *testing.T) {
	rg := newAdminRig(t)
	target := "/admin/vod?stream=42.mp4&password=" + rg.pass
	r := httptest.NewRequest(http.MethodGet, target, nil)
	adm, ok := rg.a.admitAdmin(42, r, rg.serverIP, true, rg.nowMs)
	if !ok || adm.kind != admitPasswordKind {
		t.Fatalf("password admission: ok=%v kind=%d", ok, adm.kind)
	}
}
