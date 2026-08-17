package server

import (
	"bufio"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestServeLiveDropsStalledClient reproduces the ghost-connection bug: a viewer
// that stops draining its socket without cleanly closing the TCP connection
// (channel switch on a mobile app, backgrounded player, network drop) must not
// pin the daemon connection forever. The hub drops the slow subscriber, and
// serveLive must then exit and run removeConn so fanout_sync can close the
// lines_live row. Without a bounded write, serveLive blocks in w.Write and the
// uuid lingers in /connections indefinitely.
func TestServeLiveDropsStalledClient(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.SetWriteTimeout(500 * time.Millisecond) // fast, deterministic drop
	st := mgr.GetOrCreate("5")
	feedStream(st)

	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	addr := strings.TrimPrefix(ts.URL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Issue the live request by hand so we control reading.
	fmt.Fprintf(conn, "GET /live/5?c=ghost&prebuffer=0 HTTP/1.1\r\nHost: x\r\n\r\n")

	// Read just the response headers, then stop reading entirely — the client is
	// now a black hole that never drains the body.
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}

	// Wait for the viewer to be registered on the daemon.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(st.connUUIDs()) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if len(st.connUUIDs()) == 0 {
		t.Fatal("viewer never registered")
	}

	// Flood the stream: overflow both the OS socket buffer (blocking the write)
	// and the 256-slot subscriber queue (so the hub drops the subscriber).
	go func() {
		big := make([]byte, 64*1024)
		for i := 0; i < 2000; i++ {
			st.Publish(big)
		}
	}()

	// The stalled connection must be cleaned up promptly once the hub drops it.
	cleaned := false
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(st.connUUIDs()) == 0 {
			cleaned = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !cleaned {
		t.Fatalf("stalled viewer never dropped: /connections still reports %v (ghost connection)", st.connUUIDs())
	}
}
