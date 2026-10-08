package gateway

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// The panel's tests/Support/gateway_token_vectors.json, which its
// GatewayTokenVectorsTest checks against Encryption: the same file both sides pass.
type tokenVectors struct {
	Version int                          `json:"version"`
	Context string                       `json:"context"`
	Keysets map[string]map[string][]vkey `json:"keysets"`
	Seal    []primitive                  `json:"seal"`
	CBC     []primitive                  `json:"cbc"`
	Read    []struct {
		Name   string  `json:"name"`
		Token  string  `json:"token"`
		Keyset string  `json:"keyset"`
		Legacy bool    `json:"legacy"`
		Plain  *string `json:"plain"`
	} `json:"read"`
}

type vkey struct {
	Hex   string `json:"hex"`
	Until *int64 `json:"until"`
}

type primitive struct {
	Kind    string `json:"kind"`
	Key     string `json:"key"`
	Context string `json:"context"`
	Plain   string `json:"plain"`
	Token   string `json:"token"`
}

func loadVectors(t *testing.T) tokenVectors {
	t.Helper()
	b, err := os.ReadFile("testdata/gateway_token_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v tokenVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if v.Version != 1 || len(v.Seal) == 0 || len(v.CBC) == 0 || len(v.Read) == 0 {
		t.Fatalf("unexpected vectors file: version %d, %d/%d/%d cases", v.Version, len(v.Seal), len(v.CBC), len(v.Read))
	}
	return v
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func keysOf(t *testing.T, set map[string][]vkey) Keys {
	conv := func(list []vkey) []Key {
		out := make([]Key, 0, len(list))
		for _, k := range list {
			var until int64
			if k.Until != nil {
				until = *k.Until
			}
			out = append(out, Key{Value: unhex(t, k.Hex), Until: until})
		}
		return out
	}
	return Keys{Viewer: conv(set["viewer"]), Shared: conv(set["shared"]), Context: conv(set["context"])}
}

func TestSealedTokensMatchThePanel(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.Seal {
		key, ctx := unhex(t, c.Key), unhex(t, c.Context)
		raw, ok := decodeToken(c.Token)
		if !ok {
			t.Fatalf("%s: the panel's token does not decode", c.Kind)
		}
		plain, ok := open(raw, key, ctx)
		if !ok || string(plain) != c.Plain {
			t.Fatalf("%s: open = %q, %v; want %q", c.Kind, plain, ok, c.Plain)
		}
		if got := Seal([]byte(c.Plain), key, ctx, raw[:sealNonce]); got != c.Token {
			t.Fatalf("%s: Seal with the token's nonce = %s; want %s", c.Kind, got, c.Token)
		}
	}
}

func TestLegacyTokensMatchThePanel(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.CBC {
		key, ctx := unhex(t, c.Key), unhex(t, c.Context)
		if got := EncryptLegacy([]byte(c.Plain), key, ctx); got != c.Token {
			t.Fatalf("%s: EncryptLegacy = %s; want %s", c.Kind, got, c.Token)
		}
		raw, _ := decodeToken(c.Token)
		if plain, ok := decryptLegacy(raw, key, ctx); !ok || string(plain) != c.Plain {
			t.Fatalf("%s: decryptLegacy = %q, %v", c.Kind, plain, ok)
		}
	}
}

// readToken's try order: the same verdict for every case, in any order of keys the node holds.
func TestReadFollowsReadTokensTryOrder(t *testing.T) {
	v := loadVectors(t)
	now := time.Now().Unix()
	for _, c := range v.Read {
		keys := keysOf(t, v.Keysets[c.Keyset])
		if string(keys.Context[0].Value) != string(unhex(t, v.Context)) {
			t.Fatalf("%s: the keyset's context is not the vectors' one", c.Name)
		}
		plain, verdict := keys.Read(c.Token, c.Legacy, now)
		switch {
		case c.Plain == nil && verdict != Refused:
			t.Errorf("%s: verdict %d, plain %q; the panel refuses it", c.Name, verdict, plain)
		case c.Plain != nil && (verdict != Opened || string(plain) != *c.Plain):
			t.Errorf("%s: verdict %d, plain %q; the panel opens %q", c.Name, verdict, plain, *c.Plain)
		}
	}
}

// A token PHP's lenient base64_decode could read some other way is PHP's to judge.
func TestATokenNotWrittenAsThePanelWritesOneIsUnsure(t *testing.T) {
	v := loadVectors(t)
	keys := keysOf(t, v.Keysets["open"])
	now := time.Now().Unix()
	good := v.Read[4].Token // the stream secret's current token
	if _, verdict := keys.Read(good, false, now); verdict != Opened {
		t.Fatalf("the canonical token: verdict %d", verdict)
	}
	for name, tok := range map[string]string{
		"padding":           good + "=",
		"percent-encoded":   "%41" + good[1:],
		"whitespace":        good[:10] + " " + good[10:],
		"standard alphabet": "+" + good[1:],
		"a length of 4n+1":  "AAAAA",
	} {
		if _, verdict := keys.Read(tok, true, now); verdict != Unsure {
			t.Errorf("%s: verdict %d; want Unsure", name, verdict)
		}
	}
}
