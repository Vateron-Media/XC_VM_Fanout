package clustercrypto

import (
	"crypto/ed25519"
	"encoding/json"
	"strings"
	"testing"
)

type dataplaneTicket struct {
	Tag    string                     `json:"tag"`
	Tid    string                     `json:"tid"`
	Iat    int64                      `json:"iat"`
	Exp    int64                      `json:"exp"`
	Fields map[string]json.RawMessage `json:"fields"`
	Doc    string                     `json:"doc"`
	Wire   string                     `json:"wire"`
}

type dataplaneVectors struct {
	Version     int             `json:"version"`
	PanelSeed   hexStr          `json:"panel_seed"`
	PanelPub    hexStr          `json:"panel_pub"`
	NodeSeed    hexStr          `json:"node_seed"`
	NodePub     hexStr          `json:"node_pub"`
	RelayTicket dataplaneTicket `json:"relay_ticket"`
	FileTicket  dataplaneTicket `json:"file_ticket"`
	RelayAuth   struct {
		Method  string `json:"method"`
		Target  string `json:"target"`
		TsMs    int64  `json:"ts_ms"`
		Nonce   hexStr `json:"nonce"`
		Message hexStr `json:"message"`
		Header  string `json:"header"`
	} `json:"relay_auth"`
	FileAuth struct {
		Method string `json:"method"`
		Target string `json:"target"`
		TsMs   int64  `json:"ts_ms"`
		Nonce  hexStr `json:"nonce"`
		Header string `json:"header"`
	} `json:"file_auth"`
	FileDigest struct {
		Tid         string `json:"tid"`
		OwnerSid    int64  `json:"owner_sid"`
		Offset      int64  `json:"offset"`
		Total       int64  `json:"total"`
		Iat         int64  `json:"iat"`
		Body        string `json:"body"`
		Sha256      string `json:"sha256"`
		Doc         string `json:"doc"`
		NodeHeader  string `json:"node_header"`
		PanelHeader string `json:"panel_header"`
	} `json:"file_digest"`
}

func loadDataplane(t *testing.T) dataplaneVectors {
	var v dataplaneVectors
	load(t, "cluster_dataplane_vectors.json", &v)
	if v.Version != 1 {
		t.Fatalf("version %d", v.Version)
	}
	return v
}

func TestTicketVectors(t *testing.T) {
	v := loadDataplane(t)
	if pub := ed25519.NewKeyFromSeed(v.PanelSeed).Public().(ed25519.PublicKey); string(pub) != string(v.PanelPub) {
		t.Fatal("panel pub")
	}
	for _, tc := range []dataplaneTicket{v.RelayTicket, v.FileTicket} {
		// The panel's signature is deterministic: the same doc signs to the same wire.
		sig := ed25519.Sign(ed25519.NewKeyFromSeed(v.PanelSeed), PanelSigInput(tc.Tag, []byte(tc.Doc)))
		if got := JoinSigned([]byte(tc.Doc), sig); got != tc.Wire {
			t.Fatalf("%s wire %s", tc.Tag, got)
		}
		tk, ok := VerifyTicket(v.PanelPub, tc.Tag, tc.Wire, tc.Iat+60)
		if !ok || tk.Tid != tc.Tid || tk.Iat != tc.Iat || tk.Exp != tc.Exp {
			t.Fatalf("%s did not verify", tc.Tag)
		}
		for name, raw := range tc.Fields {
			if string(tk.Fields[name]) != string(raw) {
				t.Fatalf("%s field %s = %s", tc.Tag, name, tk.Fields[name])
			}
		}
		if _, ok := VerifyTicket(v.PanelPub, tc.Tag, tc.Wire, tc.Exp); ok {
			t.Fatalf("%s: expired verified", tc.Tag)
		}
		if _, ok := VerifyTicket(v.PanelPub, tc.Tag, tc.Wire, tc.Iat-TicketSkew-1); ok {
			t.Fatalf("%s: not yet valid verified", tc.Tag)
		}
		if _, ok := VerifyTicket(v.PanelPub, tc.Tag, tc.Wire, tc.Iat-TicketSkew); !ok {
			t.Fatalf("%s: skew refused", tc.Tag)
		}
		if _, ok := VerifyTicket(v.NodePub, tc.Tag, tc.Wire, tc.Iat+60); ok {
			t.Fatalf("%s: another key verified", tc.Tag)
		}
	}
	if _, ok := VerifyTicket(v.PanelPub, "fil", v.RelayTicket.Wire, v.RelayTicket.Iat+60); ok {
		t.Fatal("a relay ticket verified as a file ticket")
	}
	if _, ok := VerifyTicket(v.PanelPub, "rly", v.FileTicket.Wire, v.FileTicket.Iat+60); ok {
		t.Fatal("a file ticket verified as a relay ticket")
	}
	doc, sig, _ := SplitSigned(v.RelayTicket.Wire, TicketMaxWire)
	forged := JoinSigned([]byte(strings.Replace(string(doc), `"stream_id":77`, `"stream_id":78`, 1)), sig)
	if _, ok := VerifyTicket(v.PanelPub, "rly", forged, v.RelayTicket.Iat+60); ok {
		t.Fatal("a tampered ticket verified")
	}
	tk, _ := VerifyTicket(v.PanelPub, "rly", v.RelayTicket.Wire, v.RelayTicket.Iat+60)
	if s, ok := tk.Int("stream_id"); !ok || s != 77 {
		t.Fatal("stream_id")
	}
}

func TestRelayAuthVectors(t *testing.T) {
	v := loadDataplane(t)
	sk := ed25519.NewKeyFromSeed(v.NodeSeed)
	if string(sk.Public().(ed25519.PublicKey)) != string(v.NodePub) {
		t.Fatal("node pub")
	}
	a := v.RelayAuth
	msg, err := RelayAuthMessage(v.RelayTicket.Wire, a.Method, a.Target, a.TsMs, a.Nonce)
	if err != nil || string(msg) != string(a.Message) {
		t.Fatalf("message %x", msg)
	}
	h, err := RelayAuthHeader(sk, v.RelayTicket.Wire, a.Method, a.Target, a.TsMs, a.Nonce)
	if err != nil || h != a.Header {
		t.Fatalf("header %s", h)
	}
	ts, nonce, ok := VerifyRelayAuth(v.NodePub, a.Header, v.RelayTicket.Wire, "GET", a.Target, a.TsMs+RelayWindowMs)
	if !ok || ts != a.TsMs || string(nonce) != string(a.Nonce) {
		t.Fatal("relay auth did not verify")
	}
	for name, bad := range map[string]func() bool{
		"stale": func() bool {
			_, _, ok := VerifyRelayAuth(v.NodePub, a.Header, v.RelayTicket.Wire, "GET", a.Target, a.TsMs+RelayWindowMs+1)
			return ok
		},
		"ahead": func() bool {
			_, _, ok := VerifyRelayAuth(v.NodePub, a.Header, v.RelayTicket.Wire, "GET", a.Target, a.TsMs-RelayWindowMs-1)
			return ok
		},
		"other target": func() bool {
			_, _, ok := VerifyRelayAuth(v.NodePub, a.Header, v.RelayTicket.Wire, "GET", "/admin/live?stream=78&extension=ts", a.TsMs)
			return ok
		},
		"other ticket": func() bool {
			_, _, ok := VerifyRelayAuth(v.NodePub, a.Header, v.FileTicket.Wire, "GET", a.Target, a.TsMs)
			return ok
		},
		"other key": func() bool {
			_, _, ok := VerifyRelayAuth(v.PanelPub, a.Header, v.RelayTicket.Wire, "GET", a.Target, a.TsMs)
			return ok
		},
		"other method": func() bool {
			_, _, ok := VerifyRelayAuth(v.NodePub, a.Header, v.RelayTicket.Wire, "HEAD", a.Target, a.TsMs)
			return ok
		},
	} {
		if bad() {
			t.Errorf("%s verified", name)
		}
	}
	f := v.FileAuth
	if h, _ := RelayAuthHeader(sk, v.FileTicket.Wire, f.Method, f.Target, f.TsMs, f.Nonce); h != f.Header {
		t.Fatalf("file auth header %s", h)
	}
}

func TestFileDigestVectors(t *testing.T) {
	v := loadDataplane(t)
	d := v.FileDigest
	doc, err := FileDigestDoc(d.Tid, d.OwnerSid, int64(len(d.Body)), d.Sha256, d.Iat, &d.Offset, &d.Total)
	if err != nil || string(doc) != d.Doc {
		t.Fatalf("doc %s %v", doc, err)
	}
	if h := JoinSigned(doc, SignNode(ed25519.NewKeyFromSeed(v.NodeSeed), "digest", doc)); h != d.NodeHeader {
		t.Fatalf("node header %s", h)
	}
	got, ok := VerifyFileDigest(d.NodeHeader, d.Tid, nil, v.NodePub)
	if !ok || !got.ChunkMatches(d.Offset, []byte(d.Body)) || *got.Total != d.Total || got.OwnerSid != d.OwnerSid {
		t.Fatal("node digest")
	}
	if _, ok := VerifyFileDigest(d.PanelHeader, d.Tid, v.PanelPub, nil); !ok {
		t.Fatal("panel digest")
	}
	if _, ok := VerifyFileDigest(d.NodeHeader, d.Tid, v.PanelPub, nil); ok {
		t.Fatal("a node's digest verified as MAIN's")
	}
	if _, ok := VerifyFileDigest(d.PanelHeader, d.Tid, nil, v.NodePub); ok {
		t.Fatal("MAIN's digest verified as a node's")
	}
	if _, ok := VerifyFileDigest(d.NodeHeader, "other-tid-0001", nil, v.NodePub); ok {
		t.Fatal("bound to its ticket")
	}
	if got.ChunkMatches(d.Offset+1, []byte(d.Body)) || got.ChunkMatches(d.Offset, []byte(d.Body+"x")) {
		t.Fatal("a moved or altered chunk matched")
	}
	tampered := []byte(d.Body)
	tampered[0] ^= 1
	if got.ChunkMatches(d.Offset, tampered) {
		t.Fatal("a tampered chunk matched")
	}
}
