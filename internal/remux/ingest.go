// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package remux

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ingestWriter feeds the daemon's per-stream ingest socket — the part of the
// ffmpeg tee that went to `unix:<sock>` with onfail=ignore, and the same
// contract: best effort. The daemon being down, restarting or slow must never
// stall the source read or the on-disk segmenting, so chunks go through a
// bounded queue that drops rather than blocks, and a broken connection is
// redialled on its own schedule. The daemon resyncs on PAT/PMT and the next
// keyframe after any gap, as it does for every producer.
//
// Unlike ffmpeg's tee slave — which, once failed, stays failed for the life of
// the process — this reconnects. After a daemon restart the panel re-registers
// the ingest at the same path and the feed resumes without restarting the
// stream.
type ingestWriter struct {
	path    string
	logf    func(string, ...any)
	queue   chan []byte
	done    chan struct{}
	once    sync.Once
	dropped atomic.Int64
}

const (
	ingestQueue        = 256 // chunks, ~3 MB at the 12 KB read size
	ingestWriteTimeout = 2 * time.Second
	ingestRedial       = 500 * time.Millisecond
	// ingestStall is how long a connected-but-unreadable daemon is waited out
	// before the connection is treated as dead after all. A stall that long is
	// not back-pressure any more, and whatever is queued on the socket is far
	// too old to be worth protecting.
	ingestStall = 30 * time.Second
)

func newIngestWriter(path string, logf func(string, ...any)) *ingestWriter {
	return &ingestWriter{path: path, logf: logf, queue: make(chan []byte, ingestQueue), done: make(chan struct{})}
}

// send queues a copy of chunk, or drops it when the daemon is not keeping up.
func (w *ingestWriter) send(chunk []byte) {
	b := make([]byte, len(chunk))
	copy(b, chunk)
	select {
	case w.queue <- b:
	default:
		w.dropped.Add(1)
	}
}

func (w *ingestWriter) close() { w.once.Do(func() { close(w.done) }) }

func (w *ingestWriter) run(ctx context.Context) {
	var (
		conn    net.Conn
		wasUp   bool
		lastLog time.Time
	)
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	for {
		if conn == nil {
			c, err := net.DialTimeout("unix", w.path, time.Second)
			if err != nil {
				// Not connected: what is queued is stale by the time a
				// connection exists, so drop it rather than replay a burst.
				w.drain()
				if wasUp || time.Since(lastLog) > time.Minute {
					w.logf("ingest %s unavailable: %v", w.path, err)
					lastLog, wasUp = time.Now(), false
				}
				select {
				case <-ctx.Done():
					return
				case <-w.done:
					return
				case <-time.After(ingestRedial):
				}
				continue
			}
			conn, wasUp = c, true
			w.logf("ingest %s connected", w.path)
		}
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case b := <-w.queue:
			if err := w.write(ctx, conn, b); err != nil {
				w.logf("ingest %s write: %v; reconnecting", w.path, err)
				_ = conn.Close()
				conn = nil
				// Whatever is queued was produced before the break and is stale
				// by the time a connection exists again.
				w.drain()
			}
		}
	}
}

// write puts one chunk on the connection, keeping the connection across a write
// DEADLINE.
//
// A deadline here means the daemon is not reading fast enough, not that it has
// gone: its socket buffer is full. Reconnecting on it used to hand the daemon a
// second producer while the first connection still had up to a full send buffer
// queued — a unix socket keeps that readable after the writer closes — and the
// daemon runs one publisher goroutine per accepted connection with no rule that
// a newer producer replaces an older one. Old and new chunks then went into the
// ring interleaved: the PCR steps backwards, GOPs are corrupted and HLS cuts a
// discontinuity. So keep pushing the rest of THIS chunk, which is also what
// keeps the feed packet-aligned, and let the bounded queue drop the chunks that
// pile up behind it. Only a hard error (EPIPE, ECONNRESET, a closed socket) or
// a stall past ingestStall is a broken connection.
func (w *ingestWriter) write(ctx context.Context, conn net.Conn, b []byte) error {
	giveUp := time.Now().Add(ingestStall)
	for {
		_ = conn.SetWriteDeadline(time.Now().Add(ingestWriteTimeout))
		n, err := conn.Write(b)
		b = b[n:]
		if err == nil {
			return nil
		}
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			return err
		}
		if len(b) == 0 {
			return nil // the whole chunk went out before the deadline landed
		}
		select {
		case <-ctx.Done():
			return err
		case <-w.done:
			return err
		default:
		}
		if time.Now().After(giveUp) {
			return err
		}
	}
}

func (w *ingestWriter) drain() {
	for {
		select {
		case <-w.queue:
			w.dropped.Add(1)
		default:
			return
		}
	}
}
