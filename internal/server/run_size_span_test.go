// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// publishBlock publishes one second of a steady source as a single ring block:
// a keyframe carrying a PCR at `sec` seconds, then padding to pktsPerSecond
// packets. The program info rides inside the FIRST block rather than opening a
// block of its own, so every block in the ring holds the same bytes and the
// stream's rate is exactly pktsPerSecond*188 bytes per second.
func publishBlock(st *Stream, sec int64, pktsPerSecond int) {
	buf := tsfixture.KeyframePCR(0x101, sec*90000, sec*90000)
	if sec == 1 {
		buf = append(buf, tsfixture.PAT(0x100)...)
		buf = append(buf, tsfixture.PMT(0x100, 0x101)...)
	}
	for i := 0; i < pktsPerSecond-1; i++ {
		buf = append(buf, tsfixture.Fill(0x101)...)
	}
	st.Publish(buf)
}

// TestRunBytesDoesNotOverreadAShortRing: the run must be sized from the stream's
// real rate, not from one inflated by how the ring's span is measured.
//
// RingStats sums the bytes of all n blocks but measures the span as
// gops[n-1].t - gops[0].t, which covers only the n-1 intervals BETWEEN their
// start times — the newest block's own duration is not in it. Dividing n blocks'
// bytes by n-1 blocks' time reads the source as n/(n-1) times faster than it is:
// twice as fast on a two-block ring, half again on three. runBytes then sizes the
// run from that, so the "keep up with the stream, with a factor of two to spare"
// its comment promises collapses towards no spare at all — on exactly the rings
// that are short: one that has just refilled after an idle-stop, a zap onto a
// gated channel, a source with long GOPs.
func TestRunBytesDoesNotOverreadAShortRing(t *testing.T) {
	const (
		pktsPerSecond = 532 // 100016 B/s
		timeout       = time.Second
	)
	rate := pktsPerSecond * 188
	want := rate * int(timeout/time.Second) / 2 // half a deadline of the stream itself

	for _, blocks := range []int64{2, 3, 5, 9} {
		mgr := NewManager(2<<20, 40000, 6, 6, time.Hour)
		mgr.SetWriteTimeout(timeout)
		st := mgr.GetOrCreate("kf")
		for sec := int64(1); sec <= blocks; sec++ {
			publishBlock(st, sec, pktsPerSecond)
		}

		ringBytes, spanMS, gops := st.Hub.RingStats()
		if gops != int(blocks) || spanMS != (blocks-1)*1000 {
			t.Fatalf("premise broken: %d published blocks left gops=%d spanMS=%d", blocks, gops, spanMS)
		}

		got := mgr.runBytes(st, time.Now())
		if got < want*19/20 || got > want*21/20 {
			t.Errorf("%d-block ring (%d bytes over %d ms): runBytes = %d, want about %d — "+
				"the source reads as %d B/s instead of %d B/s, so the run is %d%% of what it should be "+
				"and the promised factor of two to spare shrinks to %d%%",
				blocks, ringBytes, spanMS, got, want,
				got*2/int(timeout/time.Second), rate, got*100/want, want*200/got)
		}
	}
}
