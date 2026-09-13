// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import "testing"

// TestAJoinAskingForTheWholeRingSurvivesTheNextKeyframe: a viewer asking for as
// much prebuffer as the ring holds (or more — 30 s against a 20 s ring) was
// started in the ring's OLDEST block, the one the next keyframe prunes. It had
// one GOP's time to take that whole block; any slower — a storm of joins, a
// busy node, a modest link — and its next read found the block gone, and it
// was dropped as behind. In the 200-channel zap storm that was 11 viewers in
// 4 s. A join must leave the oldest block alone when there is a later one.
func TestAJoinAskingForTheWholeRingSurvivesTheNextKeyframe(t *testing.T) {
	const tick = 90000
	s := New(16<<20, 20_000)  // a 20 s ring
	for i := 0; i < 15; i++ { // 2 s GOPs: the ring is full
		feedGOP(s, int64(i)*2*tick, 0x11)
	}

	_, c := s.JoinStart(nil, 30_000)        // more than the ring holds
	_, c, _, _, pin := s.ReadFrom(c, 1<<20) // the first run: part of a 1.2 MB block
	s.Unpin(pin)
	feedGOP(s, 15*2*tick, 0x11) // the next keyframe prunes the oldest block

	if _, _, _, behind, _ := s.ReadFrom(c, 1<<20); behind {
		t.Fatal("the join started in the block the next keyframe pruned; the viewer is dropped as behind")
	}
}
