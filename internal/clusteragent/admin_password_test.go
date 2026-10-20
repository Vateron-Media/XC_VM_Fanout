package clusteragent

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeSecrets(t *testing.T, dir, pass string) {
	t.Helper()
	doc := fmt.Sprintf(`{"data":{"live_streaming_pass":%q}}`, pass)
	if err := os.WriteFile(filepath.Join(dir, "secrets.json"), []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
}

// writeServersWithNode writes a servers.json with one server (id/ip) and its node
// at the given dataplane flag.
func writeServersWithNode(t *testing.T, dir string, id int, ip string, dataplane bool) {
	t.Helper()
	doc := fmt.Sprintf(`{"data":{"servers":[{"id":%d,"server_ip":%q,"private_ip":""}],"nodes":[{"sid":%d,"dataplane":%t}]}}`, id, ip, id, dataplane)
	if err := os.WriteFile(filepath.Join(dir, "servers.json"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mintLoopback(key []byte, streamID, exp int64) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("xcvm-loopback-v1"))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(streamID))
	m.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(exp))
	m.Write(buf[:])
	return fmt.Sprintf("lb1-%d-%s", exp, hex.EncodeToString(m.Sum(nil)))
}

func TestAdmitPasswordDataplaneOff(t *testing.T) {
	dir := t.TempDir()
	writeSecrets(t, dir, "s3cr3t")
	writeServersWithNode(t, dir, 5, "203.0.113.5", false) // node dataplane OFF (a legacy child)
	a := &Agent{ReplicaDir: dir, Logf: t.Logf}
	if !a.admitPassword("s3cr3t", "203.0.113.5") {
		t.Fatal("correct password from a dataplane-off server IP was not admitted")
	}
}

func TestAdmitPasswordDataplaneOn(t *testing.T) {
	dir := t.TempDir()
	writeSecrets(t, dir, "s3cr3t")
	writeServersWithNode(t, dir, 5, "203.0.113.5", true) // node dataplane ON → must use a relay ticket
	a := &Agent{ReplicaDir: dir, Logf: t.Logf}
	if a.admitPassword("s3cr3t", "203.0.113.5") {
		t.Fatal("password from a dataplane-ON server was admitted (must be refused)")
	}
}

func TestAdmitPasswordWrongPass(t *testing.T) {
	dir := t.TempDir()
	writeSecrets(t, dir, "s3cr3t")
	writeServersWithNode(t, dir, 5, "203.0.113.5", false)
	a := &Agent{ReplicaDir: dir, Logf: t.Logf}
	if a.admitPassword("nope", "203.0.113.5") {
		t.Fatal("a wrong password was admitted")
	}
}

func TestAdmitPasswordNotAllowedIP(t *testing.T) {
	dir := t.TempDir()
	writeSecrets(t, dir, "s3cr3t")
	writeServersWithNode(t, dir, 5, "203.0.113.5", false)
	a := &Agent{ReplicaDir: dir, Logf: t.Logf}
	if a.admitPassword("s3cr3t", "198.51.100.9") { // not any server IP, not loopback
		t.Fatal("correct password from a non-allowed IP was admitted")
	}
}

func TestLoopbackTokenValidExpiredBadMac(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	if !verifyLoopbackToken(key, 42, mintLoopback(key, 42, 2_000_000_000), 1000) {
		t.Fatal("a valid loopback token was rejected")
	}
	if verifyLoopbackToken(key, 42, mintLoopback(key, 42, 500), 1000) {
		t.Fatal("an expired loopback token was accepted")
	}
	// wrong stream id in the MAC
	if verifyLoopbackToken(key, 99, mintLoopback(key, 42, 2_000_000_000), 1000) {
		t.Fatal("a token minted for another stream was accepted")
	}
	// wrong key
	if verifyLoopbackToken([]byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 42, mintLoopback(key, 42, 2_000_000_000), 1000) {
		t.Fatal("a token under a different key was accepted")
	}
	// malformed
	if verifyLoopbackToken(key, 42, "lb1-notanumber-deadbeef", 1000) {
		t.Fatal("a malformed token was accepted")
	}
}

func TestAdmitLoopbackRequiresLoopbackIP(t *testing.T) {
	dir := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	kf := filepath.Join(dir, "loopback.key")
	if err := os.WriteFile(kf, key, 0o600); err != nil {
		t.Fatal(err)
	}
	a := &Agent{ReplicaDir: dir, LoopbackKeyFile: kf, Logf: t.Logf}
	tok := mintLoopback(key, 42, 2_000_000_000)
	if !a.admitLoopback(42, tok, "127.0.0.1", 1000) {
		t.Fatal("a valid loopback token from 127.0.0.1 was not admitted")
	}
	if a.admitLoopback(42, tok, "203.0.113.5", 1000) {
		t.Fatal("a loopback token from a non-loopback peer was admitted")
	}
}
