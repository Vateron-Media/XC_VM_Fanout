// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package hub

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// The first viewer of a cold channel joins an EMPTY ring: on demand, the puller
// only starts when that viewer attaches. It parks on the wake signal, and the
// stream's very first keyframe must hand it bytes — not drop it.
//
// It did drop it. The pre-roll block (PAT/PMT, opened before the stream had
// shown a PCR) carried "no time yet", the first PCR-bearing keyframe started
// the ring clock, and prune read that block as older than any window and
// dropped it in the same Update that had just appended to it. serveLive saw
// behind and closed the connection with 0 bytes sent.
//
// Every fixture in this package used a keyframe with NO PCR, which is why the
// package missed it; a real source stamps one. Both fixtures are tested here on
// purpose.
func TestAColdViewerIsNotDroppedAtTheStreamsFirstKeyframe(t *testing.T) {
	for _, tc := range []struct {
		name     string
		keyframe []byte
	}{
		{"keyframe carrying a PCR, as a real source sends", tsfixture.KeyframePCR(0x101, 0, 0)},
		{"keyframe with no PCR", tsfixture.Keyframe(0x101, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New(1<<20, 40000) // a 40 s ring, the shipped default
			_, cur := h.Join(0)

			// Nothing published yet: the viewer is at the live edge, waiting.
			b, cur, atEnd, _, behind, _ := h.Follow(cur, 1<<20)
			b.Release()
			if behind || !atEnd {
				t.Fatalf("a join on an empty ring: behind=%v atEnd=%v, want a parked viewer", behind, atEnd)
			}

			h.Publish(tsfixture.Concat(
				tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101),
				tc.keyframe, tsfixture.Fill(0x101),
			))

			b, _, _, _, behind, _ = h.Follow(cur, 1<<20)
			n := 0
			for _, p := range b.Parts {
				n += len(p)
			}
			b.Release()
			if behind {
				t.Error("the first viewer of a cold channel was dropped as behind at the stream's first keyframe")
			}
			if n == 0 {
				t.Error("the first viewer of a cold channel got 0 bytes from the stream's first keyframe")
			}
		})
	}
}
