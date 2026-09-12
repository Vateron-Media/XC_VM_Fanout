// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// setSockBuf best-effort sets a socket buffer option on a raw connection.
func setSockBuf(c syscall.RawConn, opt, n int) {
	_ = c.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, opt, n)
	})
}

// smallSndListener caps the send buffer of every connection it accepts, so a
// client that stops reading blocks the server's write after a few KB instead of
// after loopback's autotuned megabytes — see TestServeLiveDropsStalledClient.
type smallSndListener struct{ net.Listener }

func (l smallSndListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		if sc, ok := c.(syscall.Conn); ok {
			if raw, rerr := sc.SyscallConn(); rerr == nil {
				setSockBuf(raw, syscall.SO_SNDBUF, 2048)
			}
		}
	}
	return c, err
}

// TestServeLiveDropsStalledClient reproduces the ghost-connection bug: a viewer
// that stops draining its socket without cleanly closing the TCP connection
// (channel switch on a mobile app, backgrounded player, network drop) must not
// pin the daemon connection forever. The follower's write blocks on the full
// socket buffer, the per-run write deadline turns that into an error, and
// serveLive exits and runs removeConn so fanout_sync can close the lines_live
// row. Without a bounded write, serveLive blocks in w.Write and the uuid lingers
// in /connections indefinitely.
//
// The stall is forced by capping BOTH socket buffers to a few KB: loopback's
// default autotuned buffers otherwise absorb megabytes, so a non-reading client
// never actually blocks the server and the deadline never fires — an artifact of
// loopback, not of how a congested or dead real link behaves.
func TestServeLiveDropsStalledClient(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.SetWriteTimeout(500 * time.Millisecond) // fast, deterministic drop
	st := mgr.GetOrCreate("5")
	feedStream(st)

	// Server with a tiny per-connection send buffer.
	ts := httptest.NewUnstartedServer(mgr.ClientHandler())
	ts.Listener = smallSndListener{ts.Listener}
	ts.Start()
	defer ts.Close()

	// Client with a tiny receive buffer, set BEFORE connect so the advertised TCP
	// window is small from the first byte (SO_RCVBUF set after connect may not
	// shrink an already-negotiated window).
	addr := strings.TrimPrefix(ts.URL, "http://")
	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		setSockBuf(c, syscall.SO_RCVBUF, 2048)
		return nil
	}}
	conn, err := d.Dial("tcp", addr)
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

	// Flood the stream CONTINUOUSLY with VALID TS so the follower always has bytes
	// to write. With both socket buffers capped, its write fills them within a few
	// KB and blocks; the per-run write deadline then drops it. It must be sustained
	// — once the flood stops the follower simply parks at the edge, which is not a
	// stall. Valid packets only: raw non-TS bytes are skipped by the ring parser and
	// never delivered (the ring, not a per-viewer channel, is the byte path now).
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		big := bytes.Repeat(tsfixture.Keyframe(0x101, 0), 350) // ~65 KB of valid TS
		for {
			select {
			case <-stop:
				return
			default:
			}
			st.Publish(big)
		}
	}()

	// The stalled connection must be cleaned up promptly once the write deadline
	// fires.
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
