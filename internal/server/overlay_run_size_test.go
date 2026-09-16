// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// deafFFmpeg stands in for an ffmpeg that has stopped reading its stdin — the
// normal state of the overlay encoder whenever the viewer downstream of it is
// draining slower than the stream. It emits one byte (so the window counts as
// having delivered something) and then simply does not read, until it exits.
func deafFFmpeg(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "deafffmpeg")
	if err := os.WriteFile(p, []byte("#!/bin/sh\nprintf x\nexec sleep 0.5\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// fillSlowRing publishes `blocks` quarter-megabyte random-access blocks spaced
// `gapS` seconds apart on the ring clock: a low-bitrate channel with a long GOP,
// which is exactly the shape runBytes was introduced for (a stream that produces
// far less than a megabyte in half a write deadline).
func fillSlowRing(st *Stream, blocks int, gapS int64) {
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	for i := int64(0); i < int64(blocks); i++ {
		blk := tsfixture.KeyframePCR(0x101, i*gapS*90000, i*gapS*90000)
		for len(blk) < 256*1024 {
			blk = append(blk, tsfixture.Fill(0x101)...)
		}
		st.Publish(blk)
	}
}

// TestOverlayTSWindowFeedsRunsSizedForTheStream: the banner window's feed reads
// the ring in runs, and it holds each run's PIN across the stdin.Write that
// hands it to ffmpeg. A pin suspends GOP-buffer recycling for the whole stream —
// every viewer of it — so the run size is how long one signalled viewer can
// freeze the ring for everybody else.
//
// serveLive stopped asking for a fixed megabyte for exactly this reason: a run
// is now sized from the stream's own rate (runBytes). The overlay feed was left
// on the fixed joinRunBytes ceiling, so on a low-bitrate channel it asks for a
// megabyte — far more than anything downstream can absorb — blocks part-way
// through writing it, and holds the whole pin there. It also loses its place:
// the feed's cursor only advances on a run it wrote in full, so a window that
// blocked inside its first run hands the raw fan-out back the cursor it started
// with and re-sends everything the viewer just watched.
func TestOverlayTSWindowFeedsRunsSizedForTheStream(t *testing.T) {
	shortOverlayWindow(t, 200*time.Millisecond)

	mgr := NewManager(8<<20, 60000, 2, 6, 0)
	mgr.SetWriteTimeout(time.Second)
	mgr.SetOverlay(deafFFmpeg(t), "/font.ttf")
	st := mgr.GetOrCreate("ov")
	fillSlowRing(st, 4, 10) // ~1 MB over 30s of stream: ~256 kbit/s

	if got, ceiling := mgr.runBytes(st), joinRunBytes; got >= ceiling {
		t.Fatalf("this stream sizes a run at %d bytes, not below the %d ceiling — the fixture no "+
			"longer distinguishes the two", got, ceiling)
	}

	_, cur := st.Hub.Join(60000)
	write := func([]byte) error { return nil }

	sig := pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
	next, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264")
	if !alive {
		t.Fatal("the viewer connection never broke; the window must leave it serving")
	}
	if next == cur {
		t.Fatalf("the feed never completed a single run: it asked the ring for one fixed megabyte, "+
			"more than the encoder could take, and blocked inside that run holding its pin — so the "+
			"window resumes at %+v, the cursor it was given, and re-sends the whole banner window", next)
	}
}
