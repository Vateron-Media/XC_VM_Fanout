package clusteragent

import (
	"bytes"
	"testing"
	"time"
)

func TestIssueLoopbackTokenRoundTrips(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	now := time.Now().Unix()
	tok := issueLoopbackToken(key, 42, now+300)
	if !verifyLoopbackToken(key, 42, tok, now) {
		t.Fatalf("minted token %q not accepted by verifyLoopbackToken", tok)
	}
	if verifyLoopbackToken(key, 43, tok, now) {
		t.Fatal("token accepted for the wrong stream")
	}
	if verifyLoopbackToken(key, 42, issueLoopbackToken(key, 42, now-1), now) {
		t.Fatal("expired token accepted")
	}
	if issueLoopbackToken(bytes.Repeat([]byte{7}, 16), 42, now+300) != "" {
		t.Fatal("a non-32-byte key must mint an empty token")
	}
}
