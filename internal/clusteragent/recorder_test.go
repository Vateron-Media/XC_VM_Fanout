package clusteragent

import (
	"bytes"
	"net/http"
	"net/http/httptest"
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

func TestRecordSink(t *testing.T) {
	var buf bytes.Buffer
	s := newRecordSink(&buf)
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("TSDATA"))
	}).ServeHTTP(s, httptest.NewRequest(http.MethodGet, "/x", nil))
	if s.status != http.StatusOK || buf.String() != "TSDATA" {
		t.Fatalf("200 body: status=%d body=%q", s.status, buf.String())
	}

	var buf2 bytes.Buffer
	s2 := newRecordSink(&buf2)
	http.NotFound(s2, httptest.NewRequest(http.MethodGet, "/x", nil))
	if s2.status != http.StatusNotFound {
		t.Fatalf("404 handler: status=%d, want 404", s2.status)
	}
}
