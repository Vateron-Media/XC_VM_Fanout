package clustercrypto

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

// The vector files are copied verbatim from their owners (xcvm_core and the
// panel's tests/Support/). They are the contract: fix the code, never them.

type hexStr []byte

func (h *hexStr) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	d, err := hex.DecodeString(s)
	*h = d
	return err
}

type vectors struct {
	Version int `json:"version"`
	Seal    struct {
		RecipientSk  hexStr `json:"recipient_sk"`
		RecipientPub hexStr `json:"recipient_pub"`
		EphSk        hexStr `json:"eph_sk"`
		Nonce        hexStr `json:"nonce"`
		Sealed       hexStr `json:"sealed"`
		Purpose      string `json:"purpose"`
		Context      string `json:"context"`
		Plaintext    string `json:"plaintext"`
	} `json:"seal_v1"`
	Box struct {
		Key, Nonce, Boxed  hexStr
		Context, Plaintext string
	} `json:"box_v1"`
	Token struct {
		NodeUUID     string `json:"node_uuid"`
		AgentEphSk   hexStr `json:"agent_eph_sk"`
		AgentEphPub  hexStr `json:"agent_eph_pub"`
		PanelSignPub hexStr `json:"panel_sign_pub"`
		Doc          string `json:"doc"`
		DocSig       hexStr `json:"doc_sig"`
		SealEphSk    hexStr `json:"seal_eph_sk"`
		SealNonce    hexStr `json:"seal_nonce"`
		TokenSealed  hexStr `json:"token_sealed"`
		T            hexStr `json:"t"`
		KMacUp       hexStr `json:"k_mac_up"`
		KMacDown     hexStr `json:"k_mac_down"`
		KEncUp       hexStr `json:"k_enc_up"`
		KEncDown     hexStr `json:"k_enc_down"`
		Epoch        uint64 `json:"epoch"`
		Exp          int64  `json:"exp"`
	} `json:"token"`
	Sign struct {
		Seed   hexStr `json:"seed"`
		Public hexStr `json:"public"`
		Cases  []struct {
			Tag     string `json:"tag"`
			Payload string `json:"payload"`
			Sig     hexStr `json:"sig"`
		} `json:"cases"`
	} `json:"sign"`
}

type canonVectors struct {
	Request struct {
		Proto           uint32 `json:"proto"`
		Agent           string `json:"agent"`
		Method          string `json:"method"`
		Path            string `json:"path"`
		Query           string `json:"query"`
		CanonicalQuery  string `json:"canonical_query"`
		ContentType     string `json:"content_type"`
		ContentEncoding string `json:"content_encoding"`
		Node            string `json:"node"`
		Epoch           uint64 `json:"epoch"`
		TsMs            uint64 `json:"ts_ms"`
		Nonce           hexStr `json:"nonce"`
		Context         hexStr `json:"context"`
		MacKey          hexStr `json:"mac_key"`
		Body            string `json:"body"`
		Mac             hexStr `json:"mac"`
	} `json:"request"`
	Response struct {
		Status      uint32 `json:"status"`
		ContentType string `json:"content_type"`
		TsMs        uint64 `json:"ts_ms"`
		Nonce       hexStr `json:"nonce"`
		Context     hexStr `json:"context"`
		MacKey      hexStr `json:"mac_key"`
		Body        string `json:"body"`
		Mac         hexStr `json:"mac"`
	} `json:"response"`
}

func load(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func TestSealVector(t *testing.T) {
	var v vectors
	load(t, "cluster_vectors.json", &v)
	s := v.Seal
	pub, err := X25519Public(s.RecipientSk)
	if err != nil || !bytes.Equal(pub, s.RecipientPub) {
		t.Fatalf("recipient pub: %x %v", pub, err)
	}
	got, err := SealWith(s.EphSk, s.Nonce, s.RecipientPub, s.Purpose, s.Context, []byte(s.Plaintext))
	if err != nil || !bytes.Equal(got, s.Sealed) {
		t.Fatalf("sealed mismatch: %x %v", got, err)
	}
	pt, err := Open(s.RecipientSk, s.Purpose, s.Context, s.Sealed)
	if err != nil || string(pt) != s.Plaintext {
		t.Fatalf("open: %q %v", pt, err)
	}
	for _, c := range []struct{ purpose, context string }{{"relay", s.Context}, {s.Purpose, "other"}} {
		if _, err := Open(s.RecipientSk, c.purpose, c.context, s.Sealed); err == nil {
			t.Fatalf("opened with purpose %q context %q", c.purpose, c.context)
		}
	}
	bad := append([]byte{}, s.Sealed...)
	bad[len(bad)-1] ^= 1
	if _, err := Open(s.RecipientSk, s.Purpose, s.Context, bad); err == nil {
		t.Fatal("tampered blob opened")
	}
	if _, err := SealWith(s.EphSk, s.Nonce, make([]byte, 32), s.Purpose, s.Context, nil); err == nil {
		t.Fatal("sealed to the all-zero point")
	}
}

func TestBoxVector(t *testing.T) {
	var v vectors
	load(t, "cluster_vectors.json", &v)
	b := v.Box
	got, err := BoxWith(b.Key, b.Nonce, []byte(b.Context), []byte(b.Plaintext))
	if err != nil || !bytes.Equal(got, b.Boxed) {
		t.Fatalf("boxed mismatch: %x %v", got, err)
	}
	pt, err := Unbox(b.Key, []byte(b.Context), b.Boxed)
	if err != nil || string(pt) != b.Plaintext {
		t.Fatalf("unbox: %q %v", pt, err)
	}
	if _, err := Unbox(b.Key, []byte("other"), b.Boxed); err == nil {
		t.Fatal("opened under another context")
	}
}

func TestTokenChain(t *testing.T) {
	var v vectors
	load(t, "cluster_vectors.json", &v)
	tv := v.Token
	pub, _ := X25519Public(tv.AgentEphSk)
	if !bytes.Equal(pub, tv.AgentEphPub) {
		t.Fatal("agent eph pub")
	}
	if !VerifyPanel(tv.PanelSignPub, "tok", []byte(tv.Doc), tv.DocSig) {
		t.Fatal("doc signature")
	}
	tok, keys, err := OpenToken(tv.AgentEphSk, tv.PanelSignPub, tv.NodeUUID, tv.TokenSealed)
	if err != nil {
		t.Fatal(err)
	}
	if tok.Epoch != tv.Epoch || tok.Exp != tv.Exp || tok.T != hex.EncodeToString(tv.T) {
		t.Fatalf("token doc: %+v", tok)
	}
	for name, pair := range map[string][2][]byte{
		"mac_up": {keys.MacUp, tv.KMacUp}, "mac_down": {keys.MacDown, tv.KMacDown},
		"enc_up": {keys.EncUp, tv.KEncUp}, "enc_down": {keys.EncDown, tv.KEncDown},
	} {
		if !bytes.Equal(pair[0], pair[1]) {
			t.Fatalf("%s: %x want %x", name, pair[0], pair[1])
		}
	}
	// The same sealed body, rebuilt from the vector's inputs.
	body := append(u32(uint32(len(tv.Doc))), tv.Doc...)
	body = append(body, tv.DocSig...)
	resealed, err := SealWith(tv.SealEphSk, tv.SealNonce, tv.AgentEphPub, "token", tv.NodeUUID, body)
	if err != nil || !bytes.Equal(resealed, tv.TokenSealed) {
		t.Fatalf("token_sealed mismatch: %v", err)
	}
	if _, _, err := OpenToken(tv.AgentEphSk, tv.PanelSignPub, "11111111-1111-4111-a111-111111111111", tv.TokenSealed); err == nil {
		t.Fatal("token opened for another node")
	}
	other := ed25519.NewKeyFromSeed(make([]byte, 32)).Public().(ed25519.PublicKey)
	if _, _, err := OpenToken(tv.AgentEphSk, other, tv.NodeUUID, tv.TokenSealed); err == nil {
		t.Fatal("token accepted under another panel key")
	}
}

func TestPanelSignatureVectors(t *testing.T) {
	var v vectors
	load(t, "cluster_vectors.json", &v)
	pub := ed25519.NewKeyFromSeed(v.Sign.Seed).Public().(ed25519.PublicKey)
	if !bytes.Equal(pub, v.Sign.Public) {
		t.Fatal("public from seed")
	}
	sk := ed25519.NewKeyFromSeed(v.Sign.Seed)
	for _, c := range v.Sign.Cases {
		if got := ed25519.Sign(sk, PanelSigInput(c.Tag, []byte(c.Payload))); !bytes.Equal(got, c.Sig) {
			t.Fatalf("%s: signature mismatch", c.Tag)
		}
		if !VerifyPanel(v.Sign.Public, c.Tag, []byte(c.Payload), c.Sig) {
			t.Fatalf("%s: does not verify", c.Tag)
		}
		if VerifyPanel(v.Sign.Public, "den", []byte(c.Payload), c.Sig) && c.Tag != "den" {
			t.Fatalf("%s: verifies under another tag", c.Tag)
		}
	}
	if VerifyPanel(v.Sign.Public, "zzz", nil, make([]byte, 64)) {
		t.Fatal("unknown tag verified")
	}
}

func TestCanonicalVectors(t *testing.T) {
	var v canonVectors
	load(t, "cluster_canonical_vectors.json", &v)
	r := v.Request
	if got := CanonicalQuery(r.Query); got != r.CanonicalQuery {
		t.Fatalf("query %q want %q", got, r.CanonicalQuery)
	}
	ctx, err := RequestContext(Request{
		Proto: r.Proto, Agent: r.Agent, Method: r.Method, Path: r.Path, Query: r.Query,
		ContentType: r.ContentType, ContentEncoding: r.ContentEncoding, Node: r.Node,
		Epoch: r.Epoch, TsMs: r.TsMs, Nonce: r.Nonce,
	})
	if err != nil || !bytes.Equal(ctx, r.Context) {
		t.Fatalf("request context: %x %v", ctx, err)
	}
	if !bytes.Equal(MAC(r.MacKey, ctx, []byte(r.Body)), r.Mac) || !VerifyMAC(r.MacKey, ctx, []byte(r.Body), r.Mac) {
		t.Fatal("request mac")
	}
	s := v.Response
	rctx, err := ResponseContext(ctx, s.Status, s.ContentType, s.TsMs, s.Nonce)
	if err != nil || !bytes.Equal(rctx, s.Context) {
		t.Fatalf("response context: %x %v", rctx, err)
	}
	if !VerifyMAC(s.MacKey, rctx, []byte(s.Body), s.Mac) {
		t.Fatal("response mac")
	}
	if VerifyMAC(s.MacKey, rctx, []byte(s.Body+"x"), s.Mac) {
		t.Fatal("mac over another body")
	}
}

func TestCanonicalQueryEdges(t *testing.T) {
	for in, want := range map[string]string{
		"":              "",
		"&&":            "",
		"b&a":           "a=&b=",
		"k=%zz":         "k=%25zz",
		"k=%4":          "k=%254",
		"k=a%2Bb+c":     "k=a%2Bb%20c",
		"k=~._-":        "k=~._-",
		"z=1&a=2&a=1":   "a=1&a=2&z=1",
		"%C3%A9=%c3%a9": "%C3%A9=%C3%A9",
	} {
		if got := CanonicalQuery(in); got != want {
			t.Errorf("CanonicalQuery(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNodeSignatureDomain(t *testing.T) {
	_, sk, _ := ed25519.GenerateKey(nil)
	pub := sk.Public().(ed25519.PublicKey)
	sig := SignNode(sk, "request", []byte("ctx"))
	if !VerifyNode(pub, "request", []byte("ctx"), sig) {
		t.Fatal("node sig")
	}
	if VerifyNode(pub, "relay", []byte("ctx"), sig) {
		t.Fatal("purpose not bound")
	}
	// A node signature is never a panel signature, whatever the tag.
	for tag := range PanelTags {
		if VerifyPanel(pub, tag, []byte("ctx"), sig) {
			t.Fatalf("node signature verified as panel tag %s", tag)
		}
	}
}
