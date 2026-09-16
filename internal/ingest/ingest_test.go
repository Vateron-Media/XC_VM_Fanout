// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package ingest

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"testing/iotest"
)

// collect concatenates every chunk publish receives and records whether any
// chunk was not a whole number of packets.
type collector struct {
	buf        []byte
	misaligned bool
}

func (c *collector) publish(b []byte) {
	if len(b)%PacketSize != 0 {
		c.misaligned = true
	}
	c.buf = append(c.buf, b...)
}

// tsBytes is n bytes of packet-aligned MPEG-TS: the sync byte every 188 bytes,
// as it is on the wire, and a ramp in between so a test can tell the published
// bytes from the input apart.
func tsBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		if i%PacketSize == 0 {
			b[i] = 0x47
		} else {
			b[i] = byte(i)
		}
	}
	return b
}

// boundaries reports how many packet boundaries in b carry the sync byte and
// how many do not — what tsjoin.Update sees, which silently skips any packet
// whose first byte is not 0x47.
func boundaries(b []byte) (synced, lost int) {
	for off := 0; off+PacketSize <= len(b); off += PacketSize {
		if b[off] == 0x47 {
			synced++
		} else {
			lost++
		}
	}
	return synced, lost
}

func TestCopyEmitsWholePacketsAndHoldsRemainder(t *testing.T) {
	// 3 whole packets + 50 trailing bytes that do not complete a 4th.
	data := tsBytes(PacketSize*3 + 50)

	var c collector
	if err := Copy(bytes.NewReader(data), 100, c.publish); err != io.EOF {
		t.Fatalf("Copy err = %v, want io.EOF", err)
	}
	if c.misaligned {
		t.Fatal("publish received a non-188-aligned chunk")
	}
	if len(c.buf) != PacketSize*3 {
		t.Fatalf("published %d bytes, want %d (remainder must be held)", len(c.buf), PacketSize*3)
	}
	if !bytes.Equal(c.buf, data[:PacketSize*3]) {
		t.Fatal("published bytes differ from input")
	}
}

func TestCopyAlignsAcrossTinyReads(t *testing.T) {
	// A reader that yields one byte per Read must still be re-assembled into
	// whole packets.
	data := tsBytes(PacketSize * 2)

	var c collector
	if err := Copy(iotest.OneByteReader(bytes.NewReader(data)), 188, c.publish); err != io.EOF {
		t.Fatalf("Copy err = %v, want io.EOF", err)
	}
	if c.misaligned {
		t.Fatal("publish received a non-188-aligned chunk")
	}
	if !bytes.Equal(c.buf, data) {
		t.Fatalf("published %d bytes, want the full %d", len(c.buf), len(data))
	}
}

// TestCopyResyncsAfterAStrayChunk: one input that is not a whole number of
// packets must not shift the stream for good. A stray datagram on the UDP port,
// an RTP header strip that came out a few bytes off, a truncated or padded HLS
// segment — any of them used to move every later chunk by that many bytes, and
// tsjoin.Update then skipped every packet: no PAT, no PMT, no keyframes, so no
// joins and no HLS. Publish still counted the bytes, so has_data stayed true and
// the idle timeout never fired; the channel was dead until the source
// reconnected.
func TestCopyResyncsAfterAStrayChunk(t *testing.T) {
	const dgram = 7 * PacketSize // 1316 bytes, a typical UDP/RTP payload

	var in, want []byte
	add := func(n int) {
		for i := 0; i < n; i++ {
			d := tsBytes(dgram)
			in = append(in, d...)
			want = append(want, d...)
		}
	}
	add(10)
	in = append(in, bytes.Repeat([]byte{0xAA}, 100)...) // the stray datagram
	add(90)

	var c collector
	if err := Copy(bytes.NewReader(in), 4*PacketSize, c.publish); err != io.EOF {
		t.Fatalf("Copy err = %v, want io.EOF", err)
	}
	if c.misaligned {
		t.Fatal("publish received a non-188-aligned chunk")
	}
	if synced, lost := boundaries(c.buf); lost > 0 {
		t.Fatalf("%d of %d published packets do not start with the sync byte: the stream never re-synced", lost, synced+lost)
	}
	if !bytes.Equal(c.buf, want) {
		t.Fatalf("published %d bytes, want the %d good ones with only the stray dropped", len(c.buf), len(want))
	}
}

// TestCopySurvivesArbitraryGarbage: whatever a source sends — junk of any
// length at any offset, read back at any size — publish only ever sees whole
// packets that open with the sync byte, and the copy neither stalls nor hands
// out more than its buffer holds.
func TestCopySurvivesArbitraryGarbage(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for trial := 0; trial < 60; trial++ {
		var in []byte
		for i := 0; i < 20; i++ {
			in = append(in, tsBytes(PacketSize*(1+rnd.Intn(4)))...)
			if rnd.Intn(3) == 0 {
				g := make([]byte, rnd.Intn(400))
				rnd.Read(g)
				in = append(in, g...)
			}
		}
		chunk := PacketSize * (1 + rnd.Intn(8))
		var c collector
		big := 0
		publish := func(b []byte) {
			if len(b) > big {
				big = len(b)
			}
			c.publish(b)
		}
		var r io.Reader = bytes.NewReader(in)
		if trial%4 == 0 {
			r = iotest.OneByteReader(r)
		}
		if err := Copy(r, chunk, publish); err != io.EOF {
			t.Fatalf("trial %d: Copy err = %v, want io.EOF", trial, err)
		}
		if c.misaligned {
			t.Fatalf("trial %d: publish received a non-188-aligned chunk", trial)
		}
		if _, lost := boundaries(c.buf); lost > 0 {
			t.Fatalf("trial %d: %d published packets do not start with the sync byte", trial, lost)
		}
		if big > chunk+PacketSize {
			t.Fatalf("trial %d: published a chunk of %d bytes, past the %d-byte buffer", trial, big, chunk+PacketSize)
		}
	}
}

// TestCopyFindsTheBoundaryWhenTheBodyStartsMidPacket: an HTTP body handed over
// part-way through a packet is aligned by finding the first sync byte the next
// packet's own confirms — a lone 0x47 inside a payload is not a boundary.
func TestCopyFindsTheBoundaryWhenTheBodyStartsMidPacket(t *testing.T) {
	full := tsBytes(PacketSize * 6)
	const skew = 40

	var c collector
	if err := Copy(bytes.NewReader(full[skew:]), 4*PacketSize, c.publish); err != io.EOF {
		t.Fatalf("Copy err = %v, want io.EOF", err)
	}
	if synced, lost := boundaries(c.buf); lost > 0 {
		t.Fatalf("%d of %d published packets do not start with the sync byte", lost, synced+lost)
	}
	if !bytes.Equal(c.buf, full[PacketSize:]) {
		t.Fatalf("published %d bytes, want the 5 whole packets after the partial one", len(c.buf))
	}
}
