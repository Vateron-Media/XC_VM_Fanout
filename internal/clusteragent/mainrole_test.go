package clusteragent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

const mainUUID = "7f8fad5b-d9cb-469f-a165-70867728950e"

// mainFixture is MAIN (server 1) with its data-plane key, main.json, the
// servers section and tickets its PHP wrote, and an owner (server 5) that
// checks each chunk's proof under MAIN's key, as a node's FileTicketServer
// does through the node list.
type mainFixture struct {
	t      *testing.T
	dir    string
	panel  ed25519.PrivateKey
	mainPk ed25519.PublicKey
	owner  *fakeOwner
	mu     sync.Mutex
	proofs int
	a      *Agent
	proxy  *RelayProxy
	ref    string
	data   []byte
}

func newMainFixture(t *testing.T) *mainFixture {
	t.Helper()
	fx := &mainFixture{t: t, dir: t.TempDir(), ref: "0123456789abcdef0123456789abcdef"}
	_, fx.panel, _ = ed25519.GenerateKey(nil)
	res, err := Keygen(filepath.Join(fx.dir, MainStateFile), mainUUID)
	if err != nil {
		t.Fatal(err)
	}
	pk, _ := hex.DecodeString(res.SignPub)
	fx.mainPk = pk
	_, ownerKey, _ := ed25519.GenerateKey(nil)
	fx.data = bytes.Repeat([]byte("certbot log line\n"), 1000)
	fx.owner = &fakeOwner{t: t, key: ownerKey, data: fx.data}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, ok := cc.VerifyRelayAuth(fx.mainPk, r.Header.Get("X-XCVM-File-Auth"), r.Header.Get("X-XCVM-File"), http.MethodGet, r.URL.RequestURI(), time.Now().UnixMilli())
		if !ok {
			http.Error(w, "proof", http.StatusUnauthorized)
			return
		}
		fx.mu.Lock()
		fx.proofs++
		fx.mu.Unlock()
		fx.owner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	host, port, _ := strings.Cut(u.Host, ":")
	os.MkdirAll(filepath.Join(fx.dir, "replica"), 0o700)
	servers, _ := json.Marshal(map[string]any{"etag": "e", "data": map[string]any{
		"servers": []map[string]any{
			{"id": 1, "is_main": 1, "server_ip": "127.0.0.1", "private_ip": nil, "http_broadcast_port": 1},
			{"id": 5, "is_main": 0, "server_ip": host, "private_ip": nil, "http_broadcast_port": json.Number(port)},
		},
		"nodes": []map[string]any{
			{"sid": 1, "gen": 4, "state": "active", "ed_pub": []byte(fx.mainPk), "dataplane": true},
			{"sid": 5, "gen": 2, "state": "active", "ed_pub": []byte(ownerKey.Public().(ed25519.PublicKey)), "dataplane": true},
		},
	}})
	if err := os.WriteFile(filepath.Join(fx.dir, "replica", "servers.json"), servers, 0o600); err != nil {
		t.Fatal(err)
	}
	fx.identity(1, 4, true)
	fx.tickets(fx.ticket(4, time.Now().Unix()+3600))
	return fx
}

func (fx *mainFixture) identity(sid, gen int64, on bool) {
	fx.t.Helper()
	b, _ := json.Marshal(MainIdentity{V: 1, ServerID: sid, NodeUUID: mainUUID, Gen: gen, PanelSignPub: fx.panel.Public().(ed25519.PublicKey), Dataplane: on})
	// Renamed in, as MAIN's MainDataPlane writes it: an agent re-reading it
	// meanwhile never sees it half written.
	if err := writeFile(filepath.Join(fx.dir, MainIdentityFile), b, fileWrite{perm: 0o600}); err != nil {
		fx.t.Fatal(err)
	}
}

// ticket mints a file ticket naming MAIN as the fetcher, as MainDataPlane does.
func (fx *mainFixture) ticket(gen, exp int64) string {
	now := time.Now().Unix()
	doc, _ := json.Marshal(map[string]any{"v": 1, "typ": "xcvm-file", "tid": "f1-1-" + fx.ref, "iat": now - 60, "exp": exp,
		"fetcher_sid": 1, "fetcher_gen": gen, "owner_sid": 5, "ref": fx.ref, "file": "n.opaque"})
	return cc.JoinSigned(doc, ed25519.Sign(fx.panel, cc.PanelSigInput("fil", doc)))
}

func (fx *mainFixture) tickets(wire string) {
	fx.t.Helper()
	b, _ := json.Marshal(map[string]any{"epoch": 0, "streams": map[string]any{"0": map[string]any{"files": map[string]string{fx.ref: wire}}}})
	path := filepath.Join(fx.dir, "replica", "tickets.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		fx.t.Fatal(err)
	}
	// A rewrite within the same second must still look changed.
	later := time.Now().Add(time.Duration(len(wire)) * time.Millisecond)
	os.Chtimes(path, later, later)
}

func (fx *mainFixture) start() {
	fx.t.Helper()
	a, err := NewMainAgent(filepath.Join(fx.dir, MainStateFile), "", fx.t.Logf)
	if err != nil {
		fx.t.Fatal(err)
	}
	fx.a, fx.proxy = a, a.NewRelayProxy("k0123456789abcdefghijklm")
}

func (fx *mainFixture) get() (int, []byte) {
	fx.t.Helper()
	rec := httptest.NewRecorder()
	fx.proxy.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/xfile/k0123456789abcdefghijklm/"+fx.ref+".log", nil))
	body, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, body
}

// MAIN reads a node's file through its own proxy: the ticket names MAIN at
// main.json's generation, each chunk's proof is signed with MAIN's key, and
// each chunk is checked against the owner's digest.
func TestMainReadsANodesFileWithItsOwnKey(t *testing.T) {
	defer func(d time.Duration) { TicketsFollowEvery = d }(TicketsFollowEvery)
	TicketsFollowEvery = 0
	fx := newMainFixture(t)
	fx.start()
	code, body := fx.get()
	if code != http.StatusOK || !bytes.Equal(body, fx.data) || fx.proofs == 0 {
		t.Fatalf("read %d, %d bytes, %d proofs", code, len(body), fx.proofs)
	}

	// MAIN's PHP writes a new ticket (the next epoch): read at the next look.
	fx.tickets(fx.ticket(4, time.Now().Unix()+7200))
	if code, _ := fx.get(); code != http.StatusOK {
		t.Fatalf("after a refresh: %d", code)
	}
	// A ticket for another generation (MAIN re-keyed since) is refused.
	fx.tickets(fx.ticket(3, time.Now().Unix()+3600))
	if code, _ := fx.get(); code != http.StatusNotFound {
		t.Fatalf("a ticket for gen 3 under gen 4: %d", code)
	}
}

// main.json switches MAIN's data plane and names the generation; a changed
// identity stops the agent, which the supervisor starts again.
func TestMainFollowsItsIdentity(t *testing.T) {
	defer func(d time.Duration) { TicketsFollowEvery = d }(TicketsFollowEvery)
	TicketsFollowEvery = 0
	fx := newMainFixture(t)
	fx.start()

	fx.identity(1, 4, false)
	if err := fx.a.followMainIdentity(); err != nil {
		t.Fatal(err)
	}
	if code, _ := fx.get(); code != http.StatusServiceUnavailable {
		t.Fatalf("data plane off: %d", code)
	}
	fx.identity(1, 5, true)
	fx.a.followMainIdentity()
	if code, _ := fx.get(); code != http.StatusNotFound {
		t.Fatalf("a gen-4 ticket under gen 5: %d", code)
	}
	fx.tickets(fx.ticket(5, time.Now().Unix()+3600))
	if code, _ := fx.get(); code != http.StatusOK {
		t.Fatalf("a gen-5 ticket: %d", code)
	}

	fx.identity(2, 5, true)
	if err := fx.a.followMainIdentity(); err == nil || fx.a.flows.Load() != 0 {
		t.Fatal("another server id was followed")
	}
	os.Remove(filepath.Join(fx.dir, MainIdentityFile))
	if err := fx.a.followMainIdentity(); err == nil {
		t.Fatal("a missing main.json was followed")
	}
}

func TestRunMainEndsWhenTheIdentityChanges(t *testing.T) {
	defer func(d time.Duration) { MainIdentityEvery = d }(MainIdentityEvery)
	MainIdentityEvery = 10 * time.Millisecond
	fx := newMainFixture(t)
	a, err := NewMainAgent(filepath.Join(fx.dir, MainStateFile), "127.0.0.1:0", t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.RunMain(context.Background()) }()
	time.Sleep(50 * time.Millisecond)
	fx.identity(1, 4, true)
	b, _ := os.ReadFile(filepath.Join(fx.dir, MainIdentityFile))
	if err := writeFile(filepath.Join(fx.dir, MainIdentityFile), bytes.Replace(b, []byte(mainUUID), []byte("8f8fad5b-d9cb-469f-a165-70867728950e"), 1), fileWrite{perm: 0o600}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "identity changed") {
			t.Fatalf("RunMain ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunMain kept running on a changed identity")
	}
}

// MAIN's agent sends no heartbeat, so it writes its digest_n1 report beside
// its key state: when the owners change, and again every MainDigestN1Every.
func TestMainReportsTheOwnersWhoseDigestNamedNoRequest(t *testing.T) {
	dir := t.TempDir()
	a := &Agent{MainIdentityPath: filepath.Join(dir, MainIdentityFile), Logf: t.Logf}
	path := filepath.Join(dir, MainDigestN1File)
	owners := func() string {
		t.Helper()
		var doc struct {
			Owners []int64 `json:"owners"`
		}
		b, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(b, &doc) != nil {
			t.Fatalf("%s: %v %s", MainDigestN1File, err, b)
		}
		return fmt.Sprint(doc.Owners)
	}
	var last string
	var lastAt time.Time
	now := time.Now()
	a.writeMainDigestN1(&last, &lastAt, now)
	if got := owners(); got != "[]" {
		t.Fatalf("no owner yet: %s", got)
	}
	a.takeDigestN1(9)
	a.writeMainDigestN1(&last, &lastAt, now.Add(time.Second))
	if got := owners(); got != "[9]" {
		t.Fatalf("owners %s, want [9]", got)
	}
	os.Remove(path)
	a.writeMainDigestN1(&last, &lastAt, now.Add(2*time.Second))
	if _, err := os.Stat(path); err == nil {
		t.Fatal("rewritten although nothing changed")
	}
	a.writeMainDigestN1(&last, &lastAt, now.Add(time.Second+MainDigestN1Every))
	if got := owners(); got != "[9]" {
		t.Fatalf("not written again after MainDigestN1Every: %s", got)
	}
}

func TestNewMainAgentNeedsAWholeIdentity(t *testing.T) {
	fx := newMainFixture(t)
	state := filepath.Join(fx.dir, MainStateFile)
	for name, doc := range map[string]string{
		"not json":     `{`,
		"no version":   `{"server_id":1,"node_uuid":"` + mainUUID + `","gen":1,"panel_sign_pub":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`,
		"no gen":       `{"v":1,"server_id":1,"node_uuid":"` + mainUUID + `","panel_sign_pub":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`,
		"short key":    `{"v":1,"server_id":1,"node_uuid":"` + mainUUID + `","gen":1,"panel_sign_pub":"AAAA"}`,
		"another uuid": `{"v":1,"server_id":1,"node_uuid":"8f8fad5b-d9cb-469f-a165-70867728950e","gen":1,"panel_sign_pub":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`,
	} {
		os.WriteFile(filepath.Join(fx.dir, MainIdentityFile), []byte(doc), 0o600)
		if _, err := NewMainAgent(state, "", t.Logf); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := NewMainAgent(filepath.Join(fx.dir, "nothing.json"), "", t.Logf); err == nil {
		t.Error("no key state: accepted")
	}
}

// TestInteropMainDataPlane runs MAIN's real MainDataPlane (`cluster:main-dataplane
// on`, then a file and a relay MAIN pulls from node 7) and loads what it wrote
// as MAIN's agent: main.json, the servers section with MAIN's entry and the
// tickets naming MAIN, each verified as the proxy verifies it. Opt-in:
//
//	XCVM_PANEL_DIR=/path/to/XC_VM go test ./internal/clusteragent -run InteropMain -v
func TestInteropMainDataPlane(t *testing.T) {
	panel := os.Getenv("XCVM_PANEL_DIR")
	if panel == "" {
		t.Skip("XCVM_PANEL_DIR not set")
	}
	php, err := exec.LookPath("php")
	if err != nil {
		t.Skip("php not found")
	}
	if _, err := os.Stat(filepath.Join(panel, "src/Domain/Cluster/MainDataPlane.php")); err != nil {
		t.Skip("this panel has no MAIN data-plane client")
	}
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config")
	state := filepath.Join(cfg, "cluster", MainStateFile)
	// MAIN chooses the uuid; keygen is asked with it. Here the keys come first
	// and main.json must name the uuid they were made for, so the state is
	// made again with the uuid MAIN chose, as `xc_agent keygen` would.
	nodePub, _, _ := ed25519.GenerateKey(nil)
	nodeBoxSk, nodeBoxPub, err := cc.NewX25519()
	if err != nil {
		t.Fatal(err)
	}
	pre, err := Keygen(state, mainUUID)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(php, filepath.Join("testdata/panel", "main_dataplane.php"), cfg, pre.SignPub, pre.BoxPub, mainUUID, hex.EncodeToString(nodePub), hex.EncodeToString(nodeBoxPub))
	cmd.Env = append(os.Environ(), "XCVM_PANEL_DIR="+panel, "XCVM_INTEROP_DB="+filepath.Join(dir, "main.sqlite"), "XCVM_INTEROP_PORT=80")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("main_dataplane.php: %v\n%s", err, out)
	}
	var res struct{ Ref, UUID string }
	if json.Unmarshal(out, &res) != nil || res.Ref == "" {
		t.Fatalf("main_dataplane.php printed %s", out)
	}
	// The uuid MAIN chose: the agent's key state is made for it.
	st, _ := loadRaw(state)
	st.NodeUUID = res.UUID
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}

	a, err := NewMainAgent(state, "", t.Logf)
	if err != nil {
		t.Fatalf("MAIN's agent does not load what MAIN wrote: %v", err)
	}
	if a.flows.Load() != FlowDataplane || a.Client.State.ServerID != 1 || a.selfGen.Load() != 1 {
		t.Fatalf("flows %d, server %d, gen %d", a.flows.Load(), a.Client.State.ServerID, a.selfGen.Load())
	}
	wire := a.tickets().file(res.Ref)
	ft, ok := a.verifiedTicket("fil", wire)
	if !ok {
		t.Fatal("MAIN's file ticket does not verify for MAIN's agent")
	}
	if owner, _ := ft.Int("owner_sid"); owner != 7 {
		t.Fatalf("owner %d", owner)
	}
	sealed, _ := ft.String("file")
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(sealed, "n."))
	if err != nil {
		t.Fatal(err)
	}
	if path, err := cc.Open(nodeBoxSk, "file", res.Ref, raw); err != nil || string(path) != "/movies/a.mkv" {
		t.Fatalf("the owner opens %q (%v)", path, err)
	}
	if _, ok := a.verifiedTicket("rly", a.tickets().relay(100)); !ok {
		t.Fatal("MAIN's relay ticket does not verify for MAIN's agent")
	}
	p := a.NewRelayProxy("k0123456789abcdefghijklm")
	rt, err := p.serverRoutes()
	if err != nil {
		t.Fatal(err)
	}
	signPub, _ := hex.DecodeString(pre.SignPub)
	if n, ok := rt.nodes[1]; !ok || n.state != "active" || n.gen != 1 || !bytes.Equal(n.pub, signPub) {
		t.Fatalf("MAIN's entry in the node list: %+v", rt.nodes[1])
	}
	if n, ok := rt.nodes[7]; !ok || !bytes.Equal(n.pub, nodePub) || rt.mainSid != 1 {
		t.Fatalf("the owner's entry %+v, MAIN %d", n, rt.mainSid)
	}
}
