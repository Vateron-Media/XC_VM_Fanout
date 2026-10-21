package clusteragent

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// ownerRig is a Go owner (server 3) able to serve a real file, a fetcher
// (server 7, gen 1) registered active in its servers.json, and the panel key
// that mints its tickets — everything serveXFile verifies, built in-process.
type ownerRig struct {
	a       *Agent
	panel   ed25519.PrivateKey
	fetcher ed25519.PrivateKey // the fetcher node's sign key
	boxPub  []byte             // the owner's box public key (seals paths and rk)
	root    string
	now     int64
}

const xfileTid = "t0123456789"

func newOwnerRig(t *testing.T) *ownerRig {
	t.Helper()
	f, st := newFake(t) // ServerID = 3, PanelSignPub, NodeSignSeed set
	boxSk, boxPub, err := cc.NewX25519()
	if err != nil {
		t.Fatal(err)
	}
	st.NodeBoxSk = boxSk
	replica := t.TempDir()
	a := &Agent{Client: NewClient(st, "t"), Logf: t.Logf, ReplicaDir: replica}
	now := time.Now().Unix()
	a.Client.setMainTime(now * 1000)

	fpub, fpriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	writeServersJSON(t, replica, 7, 1, "active", fpub)
	root := filepath.Join(replica, "content")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writeScanRootsSetting(t, replica, `["`+root+`"]`)
	return &ownerRig{a: a, panel: f.panel, fetcher: fpriv, boxPub: boxPub, root: root, now: now}
}

func writeScanRootsSetting(t *testing.T, dir, inner string) {
	t.Helper()
	doc := `{"data":{"lb_scan_roots":` + strconv.Quote(inner) + `}}`
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (rg *ownerRig) writeContent(t *testing.T, name string, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	p := filepath.Join(rg.root, name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p, data
}

// mint signs a file ticket as MAIN's TicketService does.
func (rg *ownerRig) mint(fields map[string]any) string {
	doc := map[string]any{"v": 1, "typ": "xcvm-file", "tid": xfileTid, "iat": rg.now - 60, "exp": rg.now + 3600}
	for k, v := range fields {
		doc[k] = v
	}
	b, _ := json.Marshal(doc)
	return cc.JoinSigned(b, ed25519.Sign(rg.panel, cc.PanelSigInput("fil", b)))
}

// ticketFor mints a ticket for a path, sealing the path to the owner's box key.
func (rg *ownerRig) ticketFor(path string) (wire, ref string) {
	ref = cc.FileRef(3, path)
	sealed, _ := cc.Seal(rg.boxPub, "file", ref, []byte(path))
	file := "n." + base64.RawURLEncoding.EncodeToString(sealed)
	return rg.mint(map[string]any{"fetcher_sid": 7, "fetcher_gen": 1, "owner_sid": 3, "ref": ref, "file": file}), ref
}

// req builds a signed GET /xfile request; the proof is over the exact target.
func (rg *ownerRig) req(t *testing.T, wire, query string, nonce []byte) *http.Request {
	t.Helper()
	target := "/xfile?" + query
	hdr, err := cc.RelayAuthHeader(rg.fetcher, wire, "GET", target, rg.now*1000, nonce)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("X-XCVM-File", wire)
	r.Header.Set("X-XCVM-File-Auth", hdr)
	return r
}

func freshNonce(t *testing.T) []byte {
	t.Helper()
	n := make([]byte, 16)
	if _, err := rand.Read(n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (rg *ownerRig) do(r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	rg.a.serveXFile(rec, r)
	return rec
}

func TestXFileServesChunkAndDigest(t *testing.T) {
	rg := newOwnerRig(t)
	path, data := rg.writeContent(t, "movie.mkv", 1000)
	wire, _ := rg.ticketFor(path)
	nonce := freshNonce(t)
	rec := rg.do(rg.req(t, wire, "o=0&n=1000", nonce))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Error("nosniff missing")
	}
	if rec.Header().Get("Content-Type") != "application/octet-stream" {
		t.Error("content-type")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Error("cache-control")
	}
	if rec.Header().Get("Content-Length") != "1000" {
		t.Errorf("content-length %q", rec.Header().Get("Content-Length"))
	}
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("body mismatch")
	}
	// The digest verifies under the owner node key with exactly the fetcher's
	// verifier (interop bytes), names this chunk, and answers this request.
	nodePub := rg.a.Client.State.SignKey().Public().(ed25519.PublicKey)
	d, ok := cc.VerifyFileDigest(rec.Header().Get("X-XCVM-File-Digest"), xfileTid, nil, nodePub)
	if !ok || !d.ChunkMatches(0, data) || !d.Answers(nonce, rg.now*1000) {
		t.Fatalf("digest did not verify/match/answer: ok=%v", ok)
	}
}

func TestXFileRelaySealBody(t *testing.T) {
	rg := newOwnerRig(t)
	path, data := rg.writeContent(t, "movie.mkv", 5000)
	wire, _ := rg.ticketFor(path)
	key := bytes.Repeat([]byte{4}, 32)
	sealedKey, err := cc.Seal(rg.boxPub, "relay", fileSealContext(3, xfileTid), key)
	if err != nil {
		t.Fatal(err)
	}
	rk := base64.RawURLEncoding.EncodeToString(sealedKey)
	rec := rg.do(rg.req(t, wire, "o=0&n=5000&rk="+rk, freshNonce(t)))

	if rec.Code != http.StatusOK || rec.Header().Get("X-XCVM-Relay-Seal") != "v1" {
		t.Fatalf("status %d, seal header %q", rec.Code, rec.Header().Get("X-XCVM-Relay-Seal"))
	}
	var plain bytes.Buffer
	if err := copyRelayFrames(&plain, bytes.NewReader(rec.Body.Bytes()), key); err != nil {
		t.Fatalf("frames: %v", err)
	}
	if !bytes.Equal(plain.Bytes(), data) {
		t.Fatal("sealed body did not decode to the plaintext")
	}
	// The digest still covers the plaintext, not the frames.
	nodePub := rg.a.Client.State.SignKey().Public().(ed25519.PublicKey)
	if d, ok := cc.VerifyFileDigest(rec.Header().Get("X-XCVM-File-Digest"), xfileTid, nil, nodePub); !ok || !d.ChunkMatches(0, data) {
		t.Fatal("digest does not cover the plaintext chunk")
	}
}

func TestXFileAuthBypass(t *testing.T) {
	assert404 := func(t *testing.T, rec *httptest.ResponseRecorder) {
		t.Helper()
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status %d, want 404", rec.Code)
		}
		if rec.Body.Len() != 0 {
			t.Fatalf("non-empty refusal body (%d bytes)", rec.Body.Len())
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Error("nosniff missing on refusal")
		}
	}

	t.Run("missing ticket header", func(t *testing.T) {
		rg := newOwnerRig(t)
		r := httptest.NewRequest(http.MethodGet, "/xfile?o=0&n=10", nil)
		r.Header.Set("X-XCVM-File-Auth", "1700000000000.0123456789abcdef0123456789abcdef."+base64.RawURLEncoding.EncodeToString(make([]byte, 64)))
		assert404(t, rg.do(r))
	})
	t.Run("forged ticket", func(t *testing.T) {
		rg := newOwnerRig(t)
		path, _ := rg.writeContent(t, "movie.mkv", 100)
		wire, _ := rg.ticketFor(path)
		// Corrupt the first char of the signed doc. A fixed last-char tweak can
		// be a no-op (the final b64 char carries only 2 significant bits), so
		// changing the doc's first byte is the reliable tamper → sig won't verify.
		repl := byte('A')
		if wire[0] == 'A' {
			repl = 'B'
		}
		forged := string(repl) + wire[1:]
		assert404(t, rg.do(rg.req(t, forged, "o=0&n=10", freshNonce(t))))
	})
	t.Run("wrong owner_sid", func(t *testing.T) {
		rg := newOwnerRig(t)
		path, _ := rg.writeContent(t, "movie.mkv", 100)
		ref := cc.FileRef(3, path)
		sealed, _ := cc.Seal(rg.boxPub, "file", ref, []byte(path))
		wire := rg.mint(map[string]any{"fetcher_sid": 7, "fetcher_gen": 1, "owner_sid": 9, "ref": ref, "file": "n." + base64.RawURLEncoding.EncodeToString(sealed)})
		assert404(t, rg.do(rg.req(t, wire, "o=0&n=10", freshNonce(t))))
	})
	t.Run("bad proof signature (wrong fetcher key)", func(t *testing.T) {
		rg := newOwnerRig(t)
		path, _ := rg.writeContent(t, "movie.mkv", 100)
		wire, _ := rg.ticketFor(path)
		_, stranger, _ := ed25519.GenerateKey(nil)
		target := "/xfile?o=0&n=10"
		hdr, _ := cc.RelayAuthHeader(stranger, wire, "GET", target, rg.now*1000, freshNonce(t))
		r := httptest.NewRequest(http.MethodGet, target, nil)
		r.Header.Set("X-XCVM-File", wire)
		r.Header.Set("X-XCVM-File-Auth", hdr)
		assert404(t, rg.do(r))
	})
	t.Run("fetcher gen mismatch", func(t *testing.T) {
		rg := newOwnerRig(t)
		path, _ := rg.writeContent(t, "movie.mkv", 100)
		ref := cc.FileRef(3, path)
		sealed, _ := cc.Seal(rg.boxPub, "file", ref, []byte(path))
		wire := rg.mint(map[string]any{"fetcher_sid": 7, "fetcher_gen": 2, "owner_sid": 3, "ref": ref, "file": "n." + base64.RawURLEncoding.EncodeToString(sealed)})
		assert404(t, rg.do(rg.req(t, wire, "o=0&n=10", freshNonce(t))))
	})
	t.Run("replayed nonce", func(t *testing.T) {
		rg := newOwnerRig(t)
		path, _ := rg.writeContent(t, "movie.mkv", 100)
		wire, _ := rg.ticketFor(path)
		r := rg.req(t, wire, "o=0&n=10", freshNonce(t))
		if rec := rg.do(r); rec.Code != http.StatusOK {
			t.Fatalf("first request: %d", rec.Code)
		}
		assert404(t, rg.do(r)) // same nonce, replayed
	})
	t.Run("target off by one byte", func(t *testing.T) {
		rg := newOwnerRig(t)
		path, _ := rg.writeContent(t, "movie.mkv", 100)
		wire, _ := rg.ticketFor(path)
		// Sign over n=10 but send n=11: the proof target no longer matches.
		hdr, _ := cc.RelayAuthHeader(rg.fetcher, wire, "GET", "/xfile?o=0&n=10", rg.now*1000, freshNonce(t))
		r := httptest.NewRequest(http.MethodGet, "/xfile?o=0&n=11", nil)
		r.Header.Set("X-XCVM-File", wire)
		r.Header.Set("X-XCVM-File-Auth", hdr)
		assert404(t, rg.do(r))
	})
}

func TestXFileBounds(t *testing.T) {
	rg := newOwnerRig(t)
	path, _ := rg.writeContent(t, "movie.mkv", 1000)
	wire, _ := rg.ticketFor(path)

	code := func(query string) int {
		return rg.do(rg.req(t, wire, query, freshNonce(t))).Code
	}
	if c := code("o=0&n=0"); c != http.StatusBadRequest {
		t.Errorf("n=0 -> %d, want 400", c)
	}
	if c := code("o=0&n=1"); c != http.StatusOK {
		t.Errorf("n=1 -> %d, want 200", c)
	}
	if c := code("o=0&n=4194304"); c != http.StatusOK {
		t.Errorf("n=CHUNK -> %d, want 200", c)
	}
	if c := code("o=0&n=4194305"); c != http.StatusBadRequest {
		t.Errorf("n=CHUNK+1 -> %d, want 400", c)
	}
	if c := code("o=01&n=10"); c != http.StatusBadRequest {
		t.Errorf("o with a leading zero -> %d, want 400", c)
	}
	// offset == total: 200 with an empty body.
	rec := rg.do(rg.req(t, wire, "o=1000&n=10", freshNonce(t)))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Errorf("offset==total -> %d, %d bytes", rec.Code, rec.Body.Len())
	}
	// offset > total: 416 with the file size in Content-Range.
	rec = rg.do(rg.req(t, wire, "o=1001&n=10", freshNonce(t)))
	if rec.Code != http.StatusRequestedRangeNotSatisfiable || rec.Header().Get("Content-Range") != "bytes */1000" {
		t.Errorf("offset>total -> %d, Content-Range %q", rec.Code, rec.Header().Get("Content-Range"))
	}
}

func TestXFilePathConfinement(t *testing.T) {
	// A file that unseals to a real path OUTSIDE the scan roots: resolves and
	// hashes, but is refused by pathAllowed.
	rg := newOwnerRig(t)
	outside := filepath.Join(t.TempDir(), "escape.mkv")
	if err := os.WriteFile(outside, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	wire, _ := rg.ticketFor(outside)
	if rec := rg.do(rg.req(t, wire, "o=0&n=4", freshNonce(t))); rec.Code != http.StatusNotFound {
		t.Fatalf("outside-root path -> %d, want 404", rec.Code)
	}

	// A disallowed extension, even inside a root.
	rg2 := newOwnerRig(t)
	bad, _ := rg2.writeContent(t, "script.php", 10)
	wire2, _ := rg2.ticketFor(bad)
	if rec := rg2.do(rg2.req(t, wire2, "o=0&n=10", freshNonce(t))); rec.Code != http.StatusNotFound {
		t.Fatalf("disallowed extension -> %d, want 404", rec.Code)
	}
}

func TestXFileRefusalMetrics(t *testing.T) {
	rg := newOwnerRig(t)
	path, data := rg.writeContent(t, "movie.mkv", 50)
	wire, _ := rg.ticketFor(path)

	// One good serve, one 400 (bad n), one 404 (wrong owner).
	if rec := rg.do(rg.req(t, wire, "o=0&n=50", freshNonce(t))); rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("serve: %d", rec.Code)
	}
	if rec := rg.do(rg.req(t, wire, "o=0&n=0", freshNonce(t))); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad n: %d", rec.Code)
	}
	badOwner := rg.mint(map[string]any{"fetcher_sid": 7, "fetcher_gen": 1, "owner_sid": 9, "ref": "x", "file": "n.x"})
	if rec := rg.do(rg.req(t, wire, "o=0&n=0", freshNonce(t))); rec.Code != http.StatusBadRequest {
		_ = rec
	}
	rg.do(rg.req(t, badOwner, "o=0&n=10", freshNonce(t)))

	serves, refusals := rg.a.xfileMetrics.snapshot()
	if serves != 1 {
		t.Errorf("serves = %d, want 1", serves)
	}
	if refusals["bad o/n"] < 1 || refusals["ticket fields"] < 1 {
		t.Errorf("refusal reasons not counted: %v", refusals)
	}
}

// End-to-end: the real RelayProxy fetcher (server 3) reads a whole multi-chunk
// file from the real Go owner (server 5, serveXFile), both trusting one panel.
// The fetcher's own chunk-by-chunk verification (digest, ChunkMatches, nonce)
// is the oracle, so a byte wrong anywhere in the owner would fail the read.
func TestXFileGoOwnerServesTheRealFetcher(t *testing.T) {
	fx := newRelayFixture(t) // fetcher: server 3, gen 1, panel = fx.panel

	ownSeed := make([]byte, 32)
	ownSeed[0] = 5
	ownSignPub := ed25519.NewKeyFromSeed(ownSeed).Public().(ed25519.PublicKey)
	boxSk, boxPub, err := cc.NewX25519()
	if err != nil {
		t.Fatal(err)
	}
	ownReplica := t.TempDir()
	ownSt := NewState(filepath.Join(t.TempDir(), "own.json"))
	ownSt.NodeUUID, ownSt.ServerID, ownSt.NodeSignSeed = "0f8fad5b-d9cb-469f-a165-70867728950e", 5, ownSeed
	ownSt.PanelSignPub = fx.panel.Public().(ed25519.PublicKey)
	ownSt.NodeBoxSk = boxSk
	owner := &Agent{Client: NewClient(ownSt, "t"), Logf: t.Logf, ReplicaDir: ownReplica}
	owner.Client.setMainTime(fx.now * 1000)
	// The owner resolves the fetcher (server 3, gen 1) from its own servers.json.
	writeServersJSON(t, ownReplica, 3, 1, "active", fx.a.Client.State.SignKey().Public().(ed25519.PublicKey))
	root := filepath.Join(ownReplica, "content")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writeScanRootsSetting(t, ownReplica, `["`+root+`"]`)

	data := make([]byte, 2*cc.FileChunk+1000)
	for i := range data {
		data[i] = byte(i*7 + i/cc.FileChunk)
	}
	path := filepath.Join(root, "movie.mkv")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	ref := cc.FileRef(5, path)
	sealed, _ := cc.Seal(boxPub, "file", ref, []byte(path))
	file := "n." + base64.RawURLEncoding.EncodeToString(sealed)
	wire := fx.ticket("fil", "f1-3-"+ref, map[string]any{"fetcher_sid": 3, "fetcher_gen": 1, "owner_sid": 5, "ref": ref, "file": file})
	if err := fx.a.tickets().setStreams(map[int64]streamTickets{100: {Files: map[string]string{ref: wire}}}); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xfile" {
			owner.serveXFile(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	fx.route(5, srv)
	fx.routes.nodes[5] = routeNode{pub: ownSignPub, gen: 1, state: "active"}

	res := fx.get("/xfile/k0123456789abcdefghijklm/" + ref + ".mkv")
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("round trip: status %d, %d of %d bytes", res.StatusCode, len(body), len(data))
	}
}
