package clusteragent

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/viewertoken"
)

func sealAdminTok(t *testing.T, payload map[string]any, key, ctx []byte) string {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return viewertoken.Seal(b, key, ctx, make([]byte, viewertoken.NonceLen))
}

func testAdminKeys(key, ctx []byte) viewertoken.Keys {
	return viewertoken.Keys{
		Viewer:  []viewertoken.Key{{Value: key}},
		Context: []viewertoken.Key{{Value: ctx}},
	}
}

func TestAdminTokenValid(t *testing.T) {
	key, ctx := bytes.Repeat([]byte{7}, 32), []byte("openssl-extra-ctx")
	keys := testAdminKeys(key, ctx)
	tok := sealAdminTok(t, map[string]any{"stream_id": 42, "ip": "203.0.113.5", "expires": int64(2_000_000_000), "container": "mp4"}, key, ctx)

	at, ok := decodeAdminToken(tok, keys, false, 1000)
	if !ok {
		t.Fatalf("decodeAdminToken ok=false, want a decoded token")
	}
	if at.StreamID != 42 || at.IP != "203.0.113.5" || at.Container != "mp4" {
		t.Fatalf("decoded = %+v", at)
	}
	if !at.valid(false, "203.0.113.5", 1000) {
		t.Fatalf("valid(matching ip, not expired) = false")
	}
}

func TestAdminTokenExpired(t *testing.T) {
	key, ctx := bytes.Repeat([]byte{9}, 32), []byte("ctx")
	keys := testAdminKeys(key, ctx)
	tok := sealAdminTok(t, map[string]any{"stream_id": 1, "ip": "198.51.100.1", "expires": int64(500)}, key, ctx)
	at, ok := decodeAdminToken(tok, keys, false, 1000)
	if !ok {
		t.Fatalf("decode ok=false")
	}
	if at.valid(false, "198.51.100.1", 1000) {
		t.Fatalf("expired token (expires 500 < now 1000) reported valid")
	}
}

func TestAdminTokenWrongIP(t *testing.T) {
	key, ctx := bytes.Repeat([]byte{3}, 32), []byte("ctx")
	keys := testAdminKeys(key, ctx)
	tok := sealAdminTok(t, map[string]any{"stream_id": 1, "ip": "203.0.113.5", "expires": int64(2_000_000_000)}, key, ctx)
	at, ok := decodeAdminToken(tok, keys, false, 1000)
	if !ok {
		t.Fatalf("decode ok=false")
	}
	// subnet match OFF: a different address is rejected.
	if at.valid(false, "203.0.113.200", 1000) {
		t.Fatalf("different ip admitted with subnet match off")
	}
	// subnet match ON: same /24 is admitted, a different /24 is not.
	if !at.valid(true, "203.0.113.200", 1000) {
		t.Fatalf("same /24 rejected with subnet match on")
	}
	if at.valid(true, "203.0.114.5", 1000) {
		t.Fatalf("different /24 admitted with subnet match on")
	}
}

func TestAdminTokenTampered(t *testing.T) {
	key, ctx := bytes.Repeat([]byte{1}, 32), []byte("ctx")
	keys := testAdminKeys(key, ctx)
	tok := sealAdminTok(t, map[string]any{"stream_id": 1, "ip": "203.0.113.5", "expires": int64(2_000_000_000)}, key, ctx)
	// Flip a character in the middle of the token.
	bad := []byte(tok)
	bad[len(bad)/2] ^= 1 << 3
	if _, ok := decodeAdminToken(string(bad), keys, false, 1000); ok {
		t.Fatalf("a tampered token decoded")
	}
}

func TestAdminTokenMissingFields(t *testing.T) {
	key, ctx := bytes.Repeat([]byte{5}, 32), []byte("ctx")
	keys := testAdminKeys(key, ctx)
	// No ip / expires — the panel requires stream_id, ip, expires.
	tok := sealAdminTok(t, map[string]any{"stream_id": 7}, key, ctx)
	if _, ok := decodeAdminToken(tok, keys, false, 1000); ok {
		t.Fatalf("a token missing ip/expires decoded")
	}
}

func TestParseViewerKeysFromPolicy(t *testing.T) {
	// The keys subset of the gateway policy file, as cron:cache writes it.
	js := []byte(`{"ip_subnet_match":true,"keys":{"viewer":[{"hex":"` +
		"aa" + `","until":null}],"shared":[{"hex":"bb","until":123}],"context":[{"hex":"cc","until":null}],"accept_legacy_cbc":true}}`)
	keys, acceptLegacy, subnet, ok := parseViewerKeys(js)
	if !ok {
		t.Fatalf("parseViewerKeys ok=false")
	}
	if !acceptLegacy || !subnet {
		t.Fatalf("acceptLegacy=%v subnet=%v, want both true", acceptLegacy, subnet)
	}
	if len(keys.Viewer) != 1 || keys.Viewer[0].Value[0] != 0xaa || keys.Shared[0].Until != 123 || keys.Context[0].Value[0] != 0xcc {
		t.Fatalf("parsed keys = %+v", keys)
	}
}
