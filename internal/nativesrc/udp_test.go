// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// freeUDPPort returns a port nothing is listening on.
func freeUDPPort(t *testing.T) int {
	t.Helper()
	probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	probe.Close()
	return port
}

// localUDPAddr digs the socket a reader actually bound out of it.
func localUDPAddr(t *testing.T, rc io.ReadCloser) *net.UDPAddr {
	t.Helper()
	r, ok := rc.(*udpReader)
	if !ok {
		t.Fatalf("Open returned %T, not a udp reader", rc)
	}
	return r.conn.LocalAddr().(*net.UDPAddr)
}

// TestUDPWithARemoteHostBindsTheWildcard: `udp://203.0.113.5:1234` names the
// SENDER, which is a form ffmpeg accepts — its udp.c binds INADDR_ANY on that
// port for a read URL. Here net.ListenUDP was handed the remote address, so the
// bind failed with "cannot assign requested address". That is not a format
// refusal, so `xc_fanout remux` exited 1 and the supervisor restart-looped the
// channel forever instead of handing it to the fallback that serves the same
// URL.
func TestUDPWithARemoteHostBindsTheWildcard(t *testing.T) {
	port := freeUDPPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rc, err := Open(ctx, fmt.Sprintf("udp://203.0.113.5:%d", port), Options{})
	if err != nil {
		t.Fatalf("Open: %v (ffmpeg serves this URL; the native reader must too)", err)
	}
	defer rc.Close()
	if ip := localUDPAddr(t, rc).IP; ip != nil && !ip.IsUnspecified() {
		t.Fatalf("bound %s, want the wildcard address", ip)
	}

	send, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer send.Close()
	go func() {
		for i := 0; i < 20 && ctx.Err() == nil; i++ {
			_, _ = send.Write(tsSegment(0)[:188])
			time.Sleep(5 * time.Millisecond)
		}
	}()
	buf := make([]byte, 64<<10)
	n, err := rc.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n < 188 || buf[0] != 0x47 {
		t.Fatalf("read %d bytes starting 0x%02x, want a TS packet", n, buf[0])
	}
}

// TestUDPHonoursLocaladdr: on a multi-homed node the operator names the NIC the
// feed arrives on. The query was never read, so the kernel picked an interface
// from the routing table, no datagrams arrived, and the stall bound reconnected
// forever — while ffmpeg, which honours localaddr, served the same URL.
func TestUDPHonoursLocaladdr(t *testing.T) {
	port := freeUDPPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rc, err := Open(ctx, fmt.Sprintf("udp://203.0.113.5:%d?localaddr=127.0.0.1", port), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	if ip := localUDPAddr(t, rc).IP; !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("bound %s, want the configured localaddr 127.0.0.1", ip)
	}
}

// TestUDPKeepsAnExplicitLocalBind: a URL naming an address that IS local stays
// bound to exactly that address — an operator who pinned the feed to one NIC
// must not be widened to the wildcard.
func TestUDPKeepsAnExplicitLocalBind(t *testing.T) {
	port := freeUDPPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rc, err := Open(ctx, fmt.Sprintf("udp://127.0.0.1:%d", port), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	if ip := localUDPAddr(t, rc).IP; !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("bound %s, want 127.0.0.1 exactly as configured", ip)
	}
}

// TestUDPRefusesFilteringOptionsItCannotHonour: `?sources=` and `?block=` decide
// WHICH senders a receiver accepts. Ignoring them delivers a different stream
// from the one that was configured, so they are a format refusal — which is what
// hands the source to ffmpeg, where they work.
func TestUDPRefusesFilteringOptionsItCannotHonour(t *testing.T) {
	port := freeUDPPort(t)
	for _, q := range []string{"sources=198.51.100.9", "block=198.51.100.9"} {
		rc, err := Open(context.Background(), fmt.Sprintf("udp://@239.1.1.1:%d?%s", port, q), Options{})
		if err == nil {
			rc.Close()
			t.Fatalf("?%s was accepted and silently ignored", q)
		}
		if !IsFormat(err) {
			t.Errorf("?%s: err = %v, want a format refusal so ffmpeg takes the source", q, err)
		}
	}
}

// TestUDPIgnoresBufferKnobs: fifo_size and friends are tuning, not semantics —
// refusing them would push every already-working multicast source onto ffmpeg.
func TestUDPIgnoresBufferKnobs(t *testing.T) {
	port := freeUDPPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rc, err := Open(ctx, fmt.Sprintf("udp://127.0.0.1:%d?fifo_size=1000000&overrun_nonfatal=1&buffer_size=8388608", port), Options{})
	if err != nil {
		t.Fatalf("Open: %v — a tuning option must not take a working source off air", err)
	}
	rc.Close()
}

// TestInterfaceForIPFindsTheLocalNIC: the multicast join needs an *net.Interface
// rather than an address, so localaddr has to be resolved to one.
func TestInterfaceForIPFindsTheLocalNIC(t *testing.T) {
	ifi, err := interfaceForIP("127.0.0.1")
	if err != nil {
		t.Fatalf("interfaceForIP(127.0.0.1): %v", err)
	}
	if ifi == nil || ifi.Name == "" {
		t.Fatal("no interface returned for the loopback address")
	}
	if _, err := interfaceForIP("203.0.113.5"); err == nil {
		t.Error("an address on no interface resolved anyway")
	}
	if _, err := interfaceForIP("not-an-ip"); err == nil {
		t.Error("a localaddr that is not an address resolved anyway")
	}
}
