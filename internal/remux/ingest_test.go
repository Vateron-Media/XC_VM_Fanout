// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package remux

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// TestIngestWriterDoesNotRedialOnASlowDaemon: a daemon whose ingest reader is
// merely STALLED (the hub lock held, a long GC, a starved CPU) is not a daemon
// that has gone away, and it must not be dropped for a fresh connection.
//
// The daemon runs one ingest.Copy goroutine per accepted connection and never
// evicts an older producer. A unix socket keeps everything already queued on it
// readable after the writer closes, so a redial left the old goroutine still
// publishing ~200 KB of older chunks into the ring while the new one published
// newer ones: TS packets reach the ring out of order, the PCR steps backwards,
// GOPs are corrupted and HLS cuts a discontinuity. That is not the gap the
// daemon resyncs on.
func TestIngestWriterDoesNotRedialOnASlowDaemon(t *testing.T) {
	path := filepath.Join(t.TempDir(), "9.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- c
		}
	}()

	w := newIngestWriter(path, func(string, ...any) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.run(ctx)
	defer w.close()

	var first net.Conn
	select {
	case first = <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never connected to the ingest socket")
	}
	defer first.Close()

	// 4 MB with nobody reading: far more than the socket's send buffer, so the
	// write blocks well past ingestWriteTimeout. The queue holds 256 chunks, so
	// nothing is dropped on the way in.
	const chunks, chunkSize = 64, 64 << 10
	want := make([]byte, 0, chunks*chunkSize)
	for i := 0; i < chunks; i++ {
		b := bytes.Repeat([]byte{byte(i)}, chunkSize)
		want = append(want, b...)
		w.send(b)
	}

	time.Sleep(ingestWriteTimeout + 500*time.Millisecond)
	select {
	case c := <-conns:
		_ = c.Close()
		t.Fatal("the writer dropped a slow daemon and redialled: the old connection's queued bytes are still on their way in, so the daemon publishes old and new chunks interleaved")
	default:
	}

	// The daemon catches up. Everything sent has to arrive on that one
	// connection, whole and in order.
	got := make([]byte, 0, len(want))
	buf := make([]byte, 64<<10)
	_ = first.SetReadDeadline(time.Now().Add(20 * time.Second))
	for len(got) < len(want) {
		n, err := first.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			break
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the feed is not the chunks in order: got %d of %d bytes", len(got), len(want))
	}
}

// TestIngestWriterRedialsOnAHardError: the reconnect the slow-daemon fix must
// not take away — a daemon that restarts takes its producer connections with
// it, and the feed has to come back without restarting the stream.
func TestIngestWriterRedialsOnAHardError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "9.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				b := make([]byte, 32<<10)
				for {
					if _, err := c.Read(b); err != nil {
						return
					}
				}
			}()
			conns <- c
		}
	}()

	w := newIngestWriter(path, func(string, ...any) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.run(ctx)
	defer w.close()

	first := <-conns
	w.send(bytes.Repeat([]byte{1}, 1<<10))
	time.Sleep(100 * time.Millisecond)
	_ = first.Close() // the daemon goes away

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		w.send(bytes.Repeat([]byte{2}, 1<<10))
		select {
		case c := <-conns:
			_ = c.Close()
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("the writer never reconnected after the daemon dropped the connection")
}
