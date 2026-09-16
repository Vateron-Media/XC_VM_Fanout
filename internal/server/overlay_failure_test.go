// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"errors"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// fillRing publishes `blocks` random-access blocks of a quarter of a megabyte
// each — about a second of a 2 Mbit/s stream — so the overlay feed has far more
// to write than the pipes between it and ffmpeg can swallow. With a toy ring the
// kernel buffers absorb everything and the feed never blocks, which is precisely
// the state these tests are about.
func fillRing(t *testing.T, st *Stream, blocks int) {
	t.Helper()
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	for i := 0; i < blocks; i++ {
		blk := tsfixture.KeyframePCR(0x101, int64(i)*90000, int64(i)*90000)
		for len(blk) < 256*1024 {
			blk = append(blk, tsfixture.Fill(0x101)...)
		}
		st.Publish(blk)
	}
}

// TestOverlayTSWindowStopsWhenTheViewerGoes: the viewer disconnects (or is
// kicked, which breaks the same write) part-way through the banner window.
//
// The stdout loop then stops draining ffmpeg, so ffmpeg fills its stdout pipe
// and blocks, stops reading its stdin, and the feed goroutine blocks in
// stdin.Write — holding a pinned ring run, which suspends GOP-buffer recycling
// for the whole stream. close(stopFeed) is only looked at between writes, so the
// join waited for CommandContext to kill ffmpeg at overlayTSDuration + 10 s: a
// handler goroutine, an ffmpeg process and a ring pin held for fifteen seconds
// after the viewer was already gone. catFFmpeg models it exactly — one process
// that copies stdin to stdout and blocks when nobody reads.
func TestOverlayTSWindowStopsWhenTheViewerGoes(t *testing.T) {
	shortOverlayWindow(t, 300*time.Millisecond)

	mgr := NewManager(1<<20, 30000, 2, 6, 0)
	mgr.SetOverlay(catFFmpeg(t), "/font.ttf")
	st := mgr.GetOrCreate("ov")
	fillRing(t, st, 8) // ~2 MB, well past every buffer in the path

	_, cur := st.Hub.Join(30000)
	write := func([]byte) error { return errors.New("write tcp: broken pipe") }

	type result struct {
		alive bool
		took  time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		sig := pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
		_, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264")
		done <- result{alive, time.Since(start)}
	}()

	select {
	case r := <-done:
		if r.alive {
			t.Fatal("the viewer's write failed; overlayTSWindow must report the connection gone")
		}
		t.Logf("overlayTSWindow returned %v after the viewer's first failed write", r.took)
	case <-time.After(3 * time.Second):
		t.Fatal("overlayTSWindow is still running 3s after the viewer's write failed: ffmpeg is " +
			"blocked on a stdout nobody drains, the feed is blocked in stdin.Write holding a ring " +
			"pin, and both stay that way until the window's own kill timeout")
	}
}
