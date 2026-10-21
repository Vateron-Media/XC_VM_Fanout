package clusteragent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

// liveFanout is a fake fanout control socket for /admin/live: it answers the
// stream status (GET /streams/<id>), the on-demand start (POST /streams/<id>/
// ensure), the continuous TS (GET /live/<id>) and the HLS playlist/segments
// (GET /hls/<id>/...). It records the last /live query (to assert no viewer
// uuid reaches it) and the ensure count.
type liveFanout struct {
	mu        sync.Mutex
	id        string
	running   bool
	hasData   bool
	startable bool // an ensure brings a down stream up
	ensured   int
	liveQuery string
	liveBody  []byte
	playlist  []byte
	segments  map[string][]byte // seq → bytes
}

func (lf *liveFanout) ensures() int {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	return lf.ensured
}

func (lf *liveFanout) lastLiveQuery() string {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	return lf.liveQuery
}

func newLiveFanout(t *testing.T, lf *liveFanout) string {
	t.Helper()
	dir, _ := os.MkdirTemp("", "lf")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "f.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lf.mu.Lock()
		defer lf.mu.Unlock()
		p := r.URL.Path
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/ensure"):
			lf.ensured++
			if lf.startable {
				lf.running, lf.hasData = true, true
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && strings.HasPrefix(p, "/streams/"):
			_ = json.NewEncoder(w).Encode(map[string]bool{"running": lf.running, "has_data": lf.hasData})
		case r.Method == http.MethodGet && strings.HasPrefix(p, "/live/"):
			lf.liveQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(lf.liveBody)
		case r.Method == http.MethodGet && p == "/hls/"+lf.id+"/index.m3u8":
			if lf.playlist == nil {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(lf.playlist)
		case r.Method == http.MethodGet && strings.HasPrefix(p, "/hls/") && strings.HasSuffix(p, ".ts"):
			seq := strings.TrimSuffix(strings.TrimPrefix(p, "/hls/"+lf.id+"/"), ".ts")
			b, ok := lf.segments[seq]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	})}
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
	return sock
}

type liveRig struct {
	a          *Agent
	dir        string
	panel      ed25519.PrivateKey
	child      ed25519.PrivateKey
	vkey, vctx []byte
	boxPub     []byte // this node's box pub (for sealing rk to it)
	clientIP   string
	nowMs      int64
	lf         *liveFanout
}

func newLiveRig(t *testing.T, lf *liveFanout) *liveRig {
	t.Helper()
	f, st := newFake(t) // ServerID 3
	dir := t.TempDir()
	sock := newLiveFanout(t, lf)
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: dir, FanoutCtl: sock}
	now := time.Now().Unix()
	a.Client.setMainTime(now * 1000)

	// a node box keypair, so a child can seal an rk to this node.
	sk, pub, err := cc.NewX25519()
	if err != nil {
		t.Fatal(err)
	}
	st.NodeBoxSk = sk

	cpub, cpriv, _ := ed25519.GenerateKey(nil)
	servers := fmt.Sprintf(`{"data":{"servers":[{"id":5,"server_ip":"203.0.113.50","private_ip":""}],"nodes":[`+
		`{"sid":7,"gen":1,"state":"active","ed_pub":%q,"box_pub":""},{"sid":5,"gen":1,"state":"active","ed_pub":"","box_pub":"","dataplane":false}]}}`,
		base64.StdEncoding.EncodeToString(cpub))
	_ = os.WriteFile(filepath.Join(dir, "servers.json"), []byte(servers), 0o644)
	writeSecrets(t, dir, "s3cr3t")

	vkey, vctx := []byte("0123456789abcdef0123456789abcdef"), []byte("ctx")
	policy := fmt.Sprintf(`{"ip_subnet_match":false,"keys":{"viewer":[{"hex":%q,"until":null}],"shared":[],"context":[{"hex":%q,"until":null}],"accept_legacy_cbc":false},"paths":{},"default_timezone":"UTC"}`,
		hex.EncodeToString(vkey), hex.EncodeToString(vctx))
	pf := filepath.Join(dir, "policy.json")
	_ = os.WriteFile(pf, []byte(policy), 0o644)
	a.GatewayPolicyFile = pf

	rg := &liveRig{a: a, dir: dir, panel: f.panel, child: cpriv, vkey: vkey, vctx: vctx, boxPub: pub, clientIP: "203.0.113.50", nowMs: now * 1000, lf: lf}
	rg.writeStream(t, 1) // on-demand live stream by default
	return rg
}

// writeStream (re)writes the replica record for stream 42 with the given
// on_demand flag; type.live and direct_source satisfy streamMeta.
func (rg *liveRig) writeStream(t *testing.T, onDemand int) {
	t.Helper()
	sd := streamsDir(rg.dir)
	_ = os.MkdirAll(sd, 0o755)
	rec := fmt.Sprintf(`{"data":{"stream":{"direct_source":0},"type":{"live":1},"server":{"on_demand":%d}}}`, onDemand)
	if err := os.WriteFile(filepath.Join(sd, "42.json"), []byte(rec), 0o644); err != nil {
		t.Fatal(err)
	}
}

// uitokenReq builds a GET /admin/live?uitoken=... request (uitoken forces m3u8).
func (rg *liveRig) uitokenReq(t *testing.T, stream int, extra string) *http.Request {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"stream_id": stream, "ip": rg.clientIP, "expires": int64(2_000_000_000)})
	tok := viewertoken.Seal(b, rg.vkey, rg.vctx, make([]byte, viewertoken.NonceLen))
	r := httptest.NewRequest(http.MethodGet, "/admin/live?uitoken="+tok+extra, nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	return r
}

// pwdReq builds a password-admitted GET /admin/live request (from the
// dataplane-off server IP).
func (rg *liveRig) pwdReq(t *testing.T, target string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	return r
}

// relayReq builds a RELAY-admitted request for stream 42 at target (which must
// already carry any &rk=, since the relay-auth signs the exact target URI).
func (rg *liveRig) relayReq(t *testing.T, target string) *http.Request {
	t.Helper()
	now := rg.nowMs / 1000
	doc := map[string]any{"v": 1, "typ": "xcvm-relay", "tid": "r0123456789", "iat": now - 60, "exp": now + 3600,
		"parent_sid": 3, "stream_id": 42, "child_sid": 7, "child_gen": 1}
	b, _ := json.Marshal(doc)
	wire := cc.JoinSigned(b, ed25519.Sign(rg.panel, cc.PanelSigInput("rly", b)))
	hdr, err := cc.RelayAuthHeader(rg.child, wire, "GET", target, rg.nowMs, freshNonce(t))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XCVM-Relay", wire)
	r.Header.Set("X-XCVM-Relay-Auth", hdr)
	r.Header.Set("X-XC-Client-IP", rg.clientIP)
	return r
}

// --- Task 2: on-demand readiness gate ---

func TestAdminLiveReadyRunning(t *testing.T) {
	lf := &liveFanout{id: "42", running: true, hasData: true}
	rg := newLiveRig(t, lf)
	if !rg.a.adminLiveReady(context.Background(), "42", false) {
		t.Fatal("running+hasData should be ready")
	}
	if lf.ensures() != 0 {
		t.Fatalf("a running stream must not be ensured (got %d)", lf.ensures())
	}
}

func TestAdminLiveReadyOnDemandStarts(t *testing.T) {
	lf := &liveFanout{id: "42", running: false, startable: true}
	rg := newLiveRig(t, lf) // replica record on_demand=1 by default
	if !rg.a.adminLiveReady(context.Background(), "42", false) {
		t.Fatal("a down on-demand stream should start and be ready")
	}
	if lf.ensures() != 1 {
		t.Fatalf("want exactly one ensure, got %d", lf.ensures())
	}
}

func TestAdminLiveReadyNotOnDemandDown(t *testing.T) {
	lf := &liveFanout{id: "42", running: false, startable: true}
	rg := newLiveRig(t, lf)
	rg.writeStream(t, 0) // not on-demand
	if rg.a.adminLiveReady(context.Background(), "42", false) {
		t.Fatal("a down non-on-demand stream must not be served")
	}
	if lf.ensures() != 0 {
		t.Fatalf("a non-on-demand stream must not be ensured (got %d)", lf.ensures())
	}
}

func TestAdminLiveReadyEnsureTimeout(t *testing.T) {
	lf := &liveFanout{id: "42", running: false, startable: false} // ensure never brings it up
	rg := newLiveRig(t, lf)
	if rg.a.adminLiveReady(context.Background(), "42", false) {
		t.Fatal("a stream that stays down after ensure is not ready")
	}
	if lf.ensures() != 1 {
		t.Fatalf("want one ensure attempt, got %d", lf.ensures())
	}
}
