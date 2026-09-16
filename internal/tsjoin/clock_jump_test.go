// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

const hour90 = 3600 * 90000

// jumpState is a 40 s ring with 6 s HLS segments carrying n one-second GOPs, its
// clock started two hours in (so a test can step an hour either way without
// straddling the 33-bit wrap). It returns the last PCR published.
func jumpState(t *testing.T, n int) (*State, int64) {
	t.Helper()
	s := New(1<<24, 40_000)
	s.Configure(40_000, 6_000, 6)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	last := feedClock(s, 2*hour90, n)
	if len(s.gops) < n || len(s.segs) == 0 || s.HLSPlaylist() == "" {
		t.Fatalf("setup: %d blocks, %d segments, playlist %q", len(s.gops), len(s.segs), s.HLSPlaylist())
	}
	return s, last
}

// TestATimelineJumpDoesNotWipeTheRing: the puller runs ffmpeg with -copyts and
// native passthrough keeps the source's own clock, so failing over to a backup
// encoder lands on an unrelated clock — roughly half the time one that is AHEAD.
// The ring clock took any forward step as that much time passing, so an hour's
// jump aged the whole ring out in one Update.
//
// Under ADR 0004 the ring IS the live tail, so that is not a memory question any
// more: every follower still taking its history is dropped as behind, the
// followers at the live edge are dropped too (the keyframe's chunk carries the
// previous block's last packets, which they have not read), and every HLS segment
// goes with the blocks, so index.m3u8 404s until a new segment closes — long
// enough for hls.js to give up with a fatal level-load error. A backwards jump —
// the same failover, the other half of the time — was always handled this way,
// and both must behave alike.
func TestATimelineJumpDoesNotWipeTheRing(t *testing.T) {
	for _, tc := range []struct {
		name string
		step int64
	}{
		{"a source an hour ahead", hour90},
		{"a source an hour behind", -hour90},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, last := jumpState(t, 41)
			gops, segs := len(s.gops), len(s.segs)

			// One follower still taking its history 20 s back, one parked at the
			// live edge with every byte the ring holds.
			_, back := s.JoinStart(nil, 20_000)
			_, back, _, _, pin := s.ReadFrom(back, 1<<16)
			s.Unpin(pin)
			open := s.gops[len(s.gops)-1]
			edge := Cursor{GOP: open.id, Off: len(open.data)}

			// The failover: the previous block's last packets, then a keyframe on
			// the new source's clock.
			jump := last + tc.step
			s.Update(tsfixture.Concat(tsfixture.Fill(0x101), tsfixture.KeyframePCR(0x101, jump, jump)))

			if n := len(s.gops); n < gops-1 {
				t.Errorf("%d blocks left in the ring after the jump, want the %d s window kept (had %d)", n, 40, gops)
			}
			if n := len(s.segs); n < segs-1 {
				t.Errorf("%d HLS segments left after the jump, want the window kept (had %d)", n, segs)
			}
			if s.HLSPlaylist() == "" {
				t.Error("the HLS playlist went empty after the jump: every viewer gets 404 until a new segment closes")
			}
			if _, _, _, behind, _ := s.ReadFrom(back, 1<<16); behind {
				t.Error("a follower still taking its history was dropped as behind by the jump")
			}
			if _, _, _, behind, _ := s.ReadFrom(edge, 1<<16); behind {
				t.Error("a follower parked at the live edge was dropped as behind by the jump")
			}
		})
	}
}

// TestACorruptAdaptationFieldCannotInjectAPCR: readPCR reads bytes 6..10, which
// only hold a PCR when adaptation_field_length covers them. A packet whose flags
// byte claims a PCR in a shorter field had its own payload read as one — a single
// corrupt packet stamping the ring clock with a random time.
func TestACorruptAdaptationFieldCannotInjectAPCR(t *testing.T) {
	s, last := jumpState(t, 20)

	bad := tsfixture.KeyframePCR(0x101, last, ptsWrap-1) // a wild PCR
	bad[4] = 1                                           // adaptation_field_length: the flags byte alone
	s.Update(bad)

	if s.lastPCR != last {
		t.Fatalf("the ring clock took a PCR (%d) from an adaptation field too short to carry one, want the last real PCR %d", s.lastPCR, last)
	}
}
