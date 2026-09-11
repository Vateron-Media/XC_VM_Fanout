// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package remux

import (
	"context"
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
			_ = conn.SetWriteDeadline(time.Now().Add(ingestWriteTimeout))
			if _, err := conn.Write(b); err != nil {
				w.logf("ingest %s write: %v; reconnecting", w.path, err)
				_ = conn.Close()
				conn = nil
			}
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
