package puller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
)

// TestTransportIsBounded pins the transport bounds. A zero-value http.Transport
// has none of these: a source that accepted a connection and then said nothing
// pinned its puller until the stream was stopped, and a connection returned to
// the pool never expired.
func TestTransportIsBounded(t *testing.T) {
	c, err := httpClient(Source{})
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T", c.Transport)
	}
	for _, cse := range []struct {
		name string
		got  interface{ String() string }
		want interface{ String() string }
	}{
		{"TLSHandshakeTimeout", tr.TLSHandshakeTimeout, defaults.PullTLSTimeout},
		{"ResponseHeaderTimeout", tr.ResponseHeaderTimeout, defaults.PullHeaderTimeout},
		{"IdleConnTimeout", tr.IdleConnTimeout, defaults.PullIdleConnTimeout},
	} {
		if cse.got != cse.want {
			t.Errorf("%s = %v, want %v", cse.name, cse.got, cse.want)
		}
	}
	if tr.DialContext == nil {
		t.Error("DialContext unset: the TCP connect has no timeout")
	}
	if c.Timeout != 0 {
		t.Errorf("Client.Timeout = %v, must stay 0 — the body is a live stream", c.Timeout)
	}
}

// TestProbeReusesConnection: one client per puller means the reconnect loop
// reuses its connection instead of paying a fresh TCP (and TLS) handshake per
// attempt — and, more importantly, does not strand the previous transport's
// pooled connection with its reader goroutine for the life of the process.
func TestProbeReusesConnection(t *testing.T) {
	var mu sync.Mutex
	conns := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conns[r.RemoteAddr] = true
		mu.Unlock()
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write([]byte("x"))
	}))
	defer srv.Close()

	c := mustClient(t, Source{})
	for i := 0; i < 5; i++ {
		_, body, err := probe(context.Background(), c, Source{}, srv.URL)
		if err != nil {
			t.Fatalf("probe %d: %v", i, err)
		}
		// Drain to EOF and close, as the direct-mpegts path does when a source ends.
		buf := make([]byte, 32)
		for {
			if _, err := body.Read(buf); err != nil {
				break
			}
		}
		body.Close()
	}

	mu.Lock()
	n := len(conns)
	mu.Unlock()
	if n != 1 {
		t.Errorf("5 sequential probes opened %d connections, want 1 (no reuse ⇒ a stranded conn per reconnect)", n)
	}
}
