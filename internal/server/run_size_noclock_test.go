// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// publishKeyframeless feeds `n` bytes of a source that never signals a
// random-access point — the `nokf` class: a radio channel, a low-resolution
// camera, anything whose encoder leaves random_access_indicator clear. tsjoin
// can only cut its ring on the max_gop_bytes backstop, so the ring ends up
// holding a single enormous block, and a single block spans no stream time at
// all: RingStats reports spanMS 0 for as long as the stream runs.
func publishKeyframeless(st *Stream, n int) {
	var buf []byte
	for sent := 0; sent < n; sent += 188 {
		buf = append(buf, tsfixture.Fill(0x101)...)
		if len(buf) >= 64*188 {
			st.Publish(buf)
			buf = nil
		}
	}
	if len(buf) > 0 {
		st.Publish(buf)
	}
}

// TestRunBytesSizesAKeyframelessStream: the run a live viewer is written must be
// sized from the stream's own rate on a source whose ring has NO clock — which
// is exactly the class the sized run was introduced for.
//
// runBytes derived the rate from the ring alone, and tsjoin can only report a
// span once the ring holds more than one block with a PCR on each end. A source
// that never sets random_access_indicator has its ring cut only by the
// max_gop_bytes backstop, so it holds one block and spans 0 ms forever — and
// runBytes fell through to the 1 MiB ceiling, permanently. A viewer joining such
// a channel with a deep prebuffer was handed a 1 MiB first run under ONE write
// deadline: ~560 kbit/s at the default 15 s timeout, on a channel carrying
// 150 kbit/s. It was dropped as "write stalled past timeout", reconnected, and
// was dropped the same way — the very failure the sized run was meant to end,
// for the very channels it named ("radio, low-res").
//
// The Stream counts every byte it publishes, which needs no PCR and no keyframe:
// difference that against wall time instead.
func TestRunBytesSizesAKeyframelessStream(t *testing.T) {
	const (
		bps     = 18750 // 150 kbit/s
		seconds = 80    // enough history that one fixed run is a real backlog
		timeout = 15 * time.Second
	)

	mgr := NewManager(2<<20, seconds*1000, 6, 6, time.Hour)
	mgr.SetWriteTimeout(timeout)
	st := mgr.GetOrCreate("nokf")
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))

	t0 := time.Now()
	publishKeyframeless(st, bps*seconds)

	// Pin the premise: this really is a ring with no clock of its own.
	ringBytes, spanMS, gops := st.Hub.RingStats()
	if spanMS != 0 || gops != 1 {
		t.Fatalf("premise broken: a keyframe-less ring reported spanMS=%d gops=%d; "+
			"this test no longer exercises the clockless path", spanMS, gops)
	}
	if ringBytes < joinRunBytes {
		t.Fatalf("premise broken: the ring holds %d bytes, less than the %d ceiling, so a "+
			"fixed run could not hurt anyone here", ringBytes, joinRunBytes)
	}

	// Half a write deadline of the stream itself: what a viewer keeping up with
	// the source, twice over, clears inside one deadline.
	want := bps * int(timeout/time.Second) / 2
	got := mgr.runBytes(st, t0.Add(seconds*time.Second))
	if got < want*4/5 || got > want*5/4 {
		t.Errorf("runBytes on a keyframe-less %d B/s stream = %d, want about %d (ceiling %d): "+
			"a viewer must sustain %d kbit/s to clear one run inside the %s deadline, on a %d kbit/s channel",
			bps, got, want, joinRunBytes,
			got*8/int(timeout/time.Second)/1000, timeout, bps*8/1000)
	}
}

// TestRunBytesKeepsTheCeilingWhileNothingHasBeenPublished: a stream with no
// measurement of any kind keeps the ceiling. There is no backlog to drain on a
// stream that has published nothing, so the ceiling costs nobody anything — and
// guessing a rate from no data would.
func TestRunBytesKeepsTheCeilingWhileNothingHasBeenPublished(t *testing.T) {
	mgr := NewManager(2<<20, 40000, 6, 6, time.Hour)
	mgr.SetWriteTimeout(15 * time.Second)
	st := mgr.GetOrCreate("cold")
	if got := mgr.runBytes(st, time.Now()); got != joinRunBytes {
		t.Errorf("runBytes on a stream that has published nothing = %d, want the %d ceiling", got, joinRunBytes)
	}
}
