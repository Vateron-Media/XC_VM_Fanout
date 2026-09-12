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

// rtpDatagram wraps n MPEG-TS packets in an RTP header with cc CSRCs, an
// optional header extension of extWords 32-bit words, and pad bytes of padding.
func rtpDatagram(n, cc, extWords, pad int) []byte {
	b0 := byte(2<<6) | byte(cc)
	if extWords > 0 {
		b0 |= 0x10
	}
	if pad > 0 {
		b0 |= 0x20
	}
	d := []byte{b0, 33, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1} // PT 33 (MP2T), seq, ts, ssrc
	d = append(d, make([]byte, 4*cc)...)
	if extWords > 0 {
		d = append(d, 0xbe, 0xde, 0, byte(extWords))
		d = append(d, make([]byte, 4*extWords)...)
	}
	for i := 0; i < n; i++ {
		p := make([]byte, 188)
		p[0] = 0x47
		d = append(d, p...)
	}
	if pad > 0 {
		d = append(d, make([]byte, pad-1)...)
		d = append(d, byte(pad))
	}
	return d
}

func TestRTPPayloadBounds(t *testing.T) {
	for _, c := range []struct {
		name               string
		cc, extWords, pad  int
		wantStart, wantLen int
	}{
		{"plain header", 0, 0, 0, 12, 7 * 188},
		{"two CSRCs", 2, 0, 0, 20, 7 * 188},
		{"header extension", 0, 3, 0, 12 + 4 + 12, 7 * 188},
		{"padding", 0, 0, 4, 12, 7 * 188},
	} {
		d := rtpDatagram(7, c.cc, c.extWords, c.pad)
		start, end := rtpPayload(d)
		if start != c.wantStart || end-start != c.wantLen || d[start] != 0x47 {
			t.Errorf("%s: payload [%d:%d], want to start at %d with %d bytes of TS", c.name, start, end, c.wantStart, c.wantLen)
		}
	}
	if s, e := rtpPayload([]byte{0x47, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}); s != 0 || e != 13 {
		t.Errorf("a non-RTP datagram is cut to [%d:%d], want it passed through whole", s, e)
	}
}

// TestRTPSourceIsPacketAligned: rtp:// was read with its RTP headers left in,
// 12 bytes per datagram, and a pull path that trusts 188-byte alignment dropped
// nearly every packet. What an rtp:// source reads must be bare, aligned TS.
func TestRTPSourceIsPacketAligned(t *testing.T) {
	probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	probe.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rc, err := Open(ctx, fmt.Sprintf("rtp://127.0.0.1:%d", port), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	send, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer send.Close()
	go func() {
		for i := 0; i < 20 && ctx.Err() == nil; i++ {
			send.Write(rtpDatagram(7, 0, 0, 0))
			time.Sleep(5 * time.Millisecond)
		}
	}()

	got := make([]byte, 0, 10*7*188)
	buf := make([]byte, 64<<10)
	for len(got) < 10*7*188 {
		n, err := rc.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil && err != io.EOF {
			t.Fatalf("read: %v", err)
		}
	}
	for off := 0; off+188 <= len(got); off += 188 {
		if got[off] != 0x47 {
			t.Fatalf("byte %d is 0x%02x, not a sync byte: the RTP header was passed through", off, got[off])
		}
	}
}
