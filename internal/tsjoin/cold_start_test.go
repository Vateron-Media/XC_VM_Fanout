// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestAColdJoinSurvivesTheStreamsFirstKeyframe: an on-demand channel starts its
// puller on the first viewer's attach, so that viewer joins an EMPTY ring and
// parks at the edge. The producer's first chunk is PAT, PMT, then a keyframe
// carrying a PCR: the PAT opens a pre-roll block before any PCR has been seen
// (t=-1), and the keyframe starts the ring clock at 0. prune read the pre-roll's
// unknown time as "infinitely old" and dropped it in the SAME Update that had
// just appended the PAT and PMT to it — so the viewer's cursor pointed at a block
// that no longer existed and serveLive dropped it as behind, 0 bytes sent. That
// is every first attempt to watch a cold channel.
//
// The two cases differ only in where the chunk boundary falls; the ring must hold
// the pre-roll either way.
func TestAColdJoinSurvivesTheStreamsFirstKeyframe(t *testing.T) {
	head := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101))
	tail := tsfixture.Concat(tsfixture.KeyframePCR(0x101, 0, 0), tsfixture.Fill(0x101))

	for _, tc := range []struct {
		name   string
		chunks [][]byte
	}{
		{"one chunk", [][]byte{tsfixture.Concat(head, tail)}},
		// The viewer takes the pre-roll, and the keyframe then arrives together
		// with more of that same pre-roll block — so it is not the "took every
		// byte of the pruned block" case either.
		{"the keyframe arrives with the pre-roll's last packets", [][]byte{head, tsfixture.Concat(tsfixture.Fill(0x101), tail)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(1<<20, 40_000)

			// The cold join: nothing retained yet, so the viewer starts at the edge.
			_, c := s.JoinStart(nil, 0)
			if _, _, atEnd, behind, _ := s.ReadFrom(c, 1<<20); !atEnd || behind {
				t.Fatalf("join on an empty ring: atEnd=%v behind=%v", atEnd, behind)
			}

			want := 0
			for _, ch := range tc.chunks {
				s.Update(ch)
				want += len(ch)
				parts, next, _, behind, pin := s.ReadFrom(c, 1<<20)
				s.Unpin(pin)
				if behind {
					t.Fatalf("the viewer was dropped as behind at the stream's first keyframe (cursor %+v, ring starts at %d)", c, s.gops[0].id)
				}
				c = next
				if got := partsLen(parts); got == 0 {
					t.Fatalf("read 0 bytes of the %d published", want)
				}
			}
			if c.Off != len(s.gops[len(s.gops)-1].data) {
				t.Fatalf("the viewer did not reach the live edge: cursor %+v, open block holds %d bytes", c, len(s.gops[len(s.gops)-1].data))
			}
		})
	}
}

// TestTheRingClockBackfillsThePreRollBlock: the mechanism behind the test above.
// A block opened before the stream showed a PCR carries t=-1; once the clock
// starts it must be stamped with the clock's origin, not left as a time prune
// reads as older than any window.
func TestTheRingClockBackfillsThePreRollBlock(t *testing.T) {
	s := New(1<<20, 40_000)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	if n := len(s.gops); n != 1 || s.gops[0].t != -1 {
		t.Fatalf("before the first PCR: %d blocks, t=%d, want one pre-roll block at t=-1", n, s.gops[0].t)
	}
	s.Update(tsfixture.KeyframePCR(0x101, 0, 0))
	if n := len(s.gops); n != 2 {
		t.Fatalf("%d blocks after the first keyframe, want the pre-roll and the keyframe's", n)
	}
	if s.gops[0].t != 0 {
		t.Fatalf("the pre-roll block is stamped t=%d after the clock started, want 0", s.gops[0].t)
	}
}
