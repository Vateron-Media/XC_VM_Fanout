package clusteragent

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The panel's RelaySealTest vector: key 01×32, "hello " then "relay".
const relayVector = "00000016d750f4b01a1be970c070804b4898dca2248d84850a1b00000015c9bffce6a50e56800e39e15b2e13e53deb43cdad05"

func TestRelayFramesOpenThePanelsVector(t *testing.T) {
	raw, _ := hex.DecodeString(relayVector)
	var out bytes.Buffer
	if err := copyRelayFrames(&out, bytes.NewReader(raw), bytes.Repeat([]byte{1}, 32)); err != nil || out.String() != "hello relay" {
		t.Fatalf("got %q, %v", out.String(), err)
	}
}

// sealFrames seals data as the panel's RelaySeal does, in frames of at most max bytes.
func sealFrames(key, data []byte, max int) []byte {
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	var out []byte
	nonce := make([]byte, 12)
	for c := uint64(0); len(data) > 0; c++ {
		n := len(data)
		if n > max {
			n = max
		}
		binary.BigEndian.PutUint64(nonce[4:], c)
		ct := gcm.Seal(nil, nonce, data[:n], relayAAD)
		out = binary.BigEndian.AppendUint32(out, uint32(len(ct)))
		out = append(out, ct...)
		data = data[n:]
	}
	return out
}

// sealingParent is a fake parent that seals: it checks the relay as the
// panel does, opens the session key with its box key, and frames body.
func sealingParent(t *testing.T, fx *relayFixture, boxSk []byte, body []byte, answer func(w http.ResponseWriter, key []byte)) *httptest.Server {
	par := &parent{panelPub: fx.panel.Public().(ed25519.PublicKey), childPub: fx.a.Client.State.SignKey().Public().(ed25519.PublicKey), nonces: newNonceCache(), stream: 77}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/live" || !par.check(r.Header, r.URL.RequestURI()) {
			http.NotFound(w, r)
			return
		}
		sealed, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get(relaySealParam))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		key, err := cc.Open(boxSk, relaySealPurpose, relaySealContext(1, 77), sealed)
		if err != nil || len(key) != 32 {
			http.NotFound(w, r)
			return
		}
		answer(w, key)
	}))
}

func newSealingFixture(t *testing.T) (*relayFixture, []byte) {
	fx := newRelayFixture(t)
	boxSk := make([]byte, 32)
	_, _ = rand.Read(boxSk)
	boxPub, _ := cc.X25519Public(boxSk)
	fx.routes.nodes[1] = routeNode{box: boxPub, gen: 1, state: "active"}
	wire := fx.ticket("rly", "r1-3-77", map[string]any{"child_sid": 3, "child_gen": 1, "parent_sid": 1, "stream_id": 77})
	if err := fx.a.tickets().setStreams(map[int64]streamTickets{77: {Relay: wire}}); err != nil {
		t.Fatal(err)
	}
	return fx, boxSk
}

func TestASealingParentsRelayIsReadSealed(t *testing.T) {
	fx, boxSk := newSealingFixture(t)
	body := bytes.Repeat([]byte("TS-BYTES"), 20000) // three frames
	srv := sealingParent(t, fx, boxSk, body, func(w http.ResponseWriter, key []byte) {
		w.Header().Set(relaySealHeader, relaySealVersion)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(sealFrames(key, body, relayFrameMax))
	})
	defer srv.Close()
	fx.route(1, srv)
	fx.routes.byID[1] = serverRoute{ip: fx.routes.byID[1].ip, port: fx.routes.byID[1].port, seal: true}

	res := fx.get("/relay/k0123456789abcdefghijklm/77.ts")
	got, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !bytes.Equal(got, body) {
		t.Fatalf("got %d, %d bytes", res.StatusCode, len(got))
	}
}

func TestASealingParentIsTakenSealedOnly(t *testing.T) {
	fx, boxSk := newSealingFixture(t)
	body := []byte("TS-BYTES")
	answer := func(w http.ResponseWriter, key []byte) { _, _ = w.Write(body) } // an unsealed answer
	srv := sealingParent(t, fx, boxSk, body, func(w http.ResponseWriter, key []byte) { answer(w, key) })
	defer srv.Close()
	fx.route(1, srv)
	fx.routes.byID[1] = serverRoute{ip: fx.routes.byID[1].ip, port: fx.routes.byID[1].port, seal: true}
	if res := fx.get("/relay/k0123456789abcdefghijklm/77.ts"); res.StatusCode != http.StatusBadGateway {
		t.Fatalf("an unsealed answer from a sealing parent was taken: %d", res.StatusCode)
	}

	// A frame changed on the way: the read stops before it.
	answer = func(w http.ResponseWriter, key []byte) {
		w.Header().Set(relaySealHeader, relaySealVersion)
		frames := sealFrames(key, bytes.Repeat([]byte("A"), 3*relayFrameMax), relayFrameMax)
		frames[len(frames)-5] ^= 1
		_, _ = w.Write(frames)
	}
	res := fx.get("/relay/k0123456789abcdefghijklm/77.ts")
	got, _ := io.ReadAll(res.Body)
	if len(got) != 2*relayFrameMax {
		t.Fatalf("got %d bytes, want the two frames before the changed one", len(got))
	}
}

func TestAParentThatDoesNotSealGetsNoKey(t *testing.T) {
	fx, _ := newSealingFixture(t)
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		_, _ = io.WriteString(w, "TS-BYTES")
	}))
	defer srv.Close()
	fx.route(1, srv) // seal false: an older parent
	res := fx.get("/relay/k0123456789abcdefghijklm/77.ts")
	got, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || string(got) != "TS-BYTES" || query != "stream=77&extension=ts" {
		t.Fatalf("got %d %q, query %q", res.StatusCode, got, query)
	}
}

func TestTheServersSectionCarriesSealingAndBoxKeys(t *testing.T) {
	box := bytes.Repeat([]byte{9}, 32)
	doc := `{"data":{"servers":[{"id":1,"is_main":1,"server_ip":"10.0.0.1","http_broadcast_port":80,"relay_seal":1},{"id":2,"server_ip":"10.0.0.2","http_broadcast_port":80}],` +
		`"nodes":[{"sid":2,"gen":1,"state":"active","ed_pub":"` + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)) + `","box_pub":"` + base64.StdEncoding.EncodeToString(box) + `"}]}}`
	rt, err := parseServerRoutes([]byte(doc), 2)
	if err != nil || !rt.byID[1].seal || rt.byID[2].seal || !bytes.Equal(rt.nodes[2].box, box) {
		t.Fatalf("%+v %v", rt, err)
	}
}

func TestASealingOwnersFileIsReadSealedChunkByChunk(t *testing.T) {
	fx, o, path := xfileFixture(t, 2*cc.FileChunk+1000)
	o.boxSk = make([]byte, 32)
	_, _ = rand.Read(o.boxSk)
	boxPub, _ := cc.X25519Public(o.boxSk)
	n := fx.routes.nodes[5]
	n.box = boxPub
	fx.routes.nodes[5] = n
	r := fx.routes.byID[5]
	r.seal = true
	fx.routes.byID[5] = r

	res := fx.get(path)
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !bytes.Equal(body, o.data) {
		t.Fatalf("sealed file: %d, %d bytes", res.StatusCode, len(body))
	}
	// An owner that answers a sealing request in the clear is refused.
	o.unsealed = true
	res = fx.get(path)
	if body, _ := io.ReadAll(res.Body); res.StatusCode == 200 && len(body) == len(o.data) {
		t.Fatal("an unsealed chunk from a sealing owner was taken")
	}
}
