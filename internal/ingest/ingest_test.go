package ingest

import (
	"bytes"
	"io"
	"testing/iotest"
	"testing"
)

// collect concatenates every chunk publish receives and records whether any
// chunk was not a whole number of packets.
type collector struct {
	buf       []byte
	misaligned bool
}

func (c *collector) publish(b []byte) {
	if len(b)%PacketSize != 0 {
		c.misaligned = true
	}
	c.buf = append(c.buf, b...)
}

func TestCopyEmitsWholePacketsAndHoldsRemainder(t *testing.T) {
	// 3 whole packets + 50 trailing bytes that do not complete a 4th.
	data := make([]byte, PacketSize*3+50)
	for i := range data {
		data[i] = byte(i)
	}

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
	data := make([]byte, PacketSize*2)
	for i := range data {
		data[i] = byte(i)
	}

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
