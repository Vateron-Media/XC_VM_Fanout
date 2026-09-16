// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"bufio"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// smallBufListener caps the send buffer of every connection it accepts, like
// smallSndListener, but leaves room for a paced reader to actually take its
// share per read — see TestServeLiveKeepsViewerFasterThanTheStream.
type smallBufListener struct{ net.Listener }

func (l smallBufListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		if sc, ok := c.(syscall.Conn); ok {
			if raw, rerr := sc.SyscallConn(); rerr == nil {
				setSockBuf(raw, syscall.SO_SNDBUF, 16<<10)
			}
		}
	}
	return c, err
}

// TestServeLiveKeepsViewerFasterThanTheStream: the write deadline must drop a
// viewer slower than the stream, not one merely slower than a megabyte.
//
// A Follow run is written under ONE write deadline, and the run was a fixed
// 1 MiB whatever the channel's bitrate — so draining it in time took ~533 kbit/s
// at the default 15 s timeout, however slow the stream itself was. A viewer on a
// low-bitrate channel with a deep prebuffer carries a run-sized backlog for its
// whole session, so a link several times faster than the stream was still
// dropped as "write stalled past timeout", reconnected, and failed the same way.
//
// Scaled down here: a ~100 KB/s stream, a 1 s write timeout and a client reading
// at 400 KB/s — four times the stream, a quarter of what a 1 MiB run demanded.
func TestServeLiveKeepsViewerFasterThanTheStream(t *testing.T) {
	const (
		blocks    = 12       // seconds of history
		pktsPerS  = 532      // ~100 KB per second of stream
		clientBps = 400 << 10 // the client drains four times the stream's rate
		wantBytes = 900 << 10 // well past the 1 MiB a single-deadline run needed
	)

	mgr := NewManager(1<<20, 40000, 6, 6, time.Hour)
	mgr.SetWriteTimeout(time.Second)
	mgr.viewerIdleNS.Store(0) // the only thing that may end this session is a stall
	st := mgr.GetOrCreate("5")
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	for sec := int64(1); sec <= blocks; sec++ {
		publishSecond(st, sec, pktsPerS-1)
	}

	ts := httptest.NewUnstartedServer(mgr.ClientHandler())
	ts.Listener = smallBufListener{ts.Listener}
	ts.Start()
	defer ts.Close()

	// A small receive window so the client's pacing really is backpressure on the
	// daemon's writes rather than something loopback absorbs.
	addr := strings.TrimPrefix(ts.URL, "http://")
	d := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		setSockBuf(c, syscall.SO_RCVBUF, 16<<10)
		return nil
	}}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "GET /live/5?c=slowlink&prebuffer=%d HTTP/1.1\r\nHost: x\r\n\r\n", blocks)
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

	// Drain at a steady clientBps until we have taken more than a fixed run, or
	// the daemon drops us.
	buf := make([]byte, 16<<10)
	got, start := 0, time.Now()
	for got < wantBytes {
		if paced := time.Duration(float64(got) / clientBps * float64(time.Second)); paced > time.Since(start) {
			time.Sleep(paced - time.Since(start))
		}
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("read deadline: %v", err)
		}
		n, err := br.Read(buf)
		got += n
		if err != nil {
			t.Fatalf("viewer dropped after %d KB at %.0f KB/s on a %d KB/s stream (%v): "+
				"a run sized in bytes, not in stream time, times out on a link faster than the source",
				got/1024, clientBps/1024.0, blocks*pktsPerS*188/1024/blocks, err)
		}
	}
}
