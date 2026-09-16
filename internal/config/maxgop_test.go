// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package config

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

const vpid = 0x101

// TestMaxGOPClampFloorKeepsBlocksWhole: the clamp's promise is that a typo in
// the panel "can never push the daemon into a pathological state". A floor of
// one TS packet broke that promise for max_gop_bytes.
//
// An admin thinking in megabytes types 1, or 10. The value was floored at 188 —
// one packet — and from then on every stream created opened a NEW ring block for
// every packet that is not a keyframe: the open block is already full, so the
// append arm of tsjoin.Update can never be taken. Each new block calls prune(),
// which sums every block in the ring and memmoves the slice, all under the hub
// lock. A 40-second ring at 5 Mbit/s holds ~133k blocks, so the producer spends
// hundreds of microseconds per 188-byte packet and cannot keep up in real time —
// with the lock held, which stalls every viewer on the stream.
//
// So the floor has to be a size that can still hold a GOP, not a packet.
func TestMaxGOPClampFloorKeepsBlocksWhole(t *testing.T) {
	for _, c := range []struct {
		name string
		in   int
	}{
		{"megabytes typed as bytes", 1},
		{"ten megabytes typed as ten", 10},
		{"exactly one packet", 188},
		{"zero", 0},
		{"negative", -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := Defaults()
			v.MaxGOPBytes = c.in
			v.clamp()

			// One keyframe, then a long run of ordinary packets — a plain live
			// source between random-access points.
			s := tsjoin.New(v.MaxGOPBytes, 40000)
			s.Update(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, vpid)))
			s.Update(tsfixture.KeyframePCR(vpid, 0, 0))
			const fill = 2000
			for i := 0; i < fill; i++ {
				s.Update(tsfixture.Fill(vpid))
			}

			// A block cut with no keyframe in it is what the pathology is made of:
			// on a source that DOES give keyframes there should be none at all.
			if cuts := s.NoKeyframeCuts(); cuts > 0 {
				t.Errorf("max_gop_bytes=%d clamped to %d: %d ring blocks were cut with no keyframe in them; the GOP is being split into single packets",
					c.in, v.MaxGOPBytes, cuts)
			}
			if _, _, blocks := s.RingStats(); blocks > 2 {
				t.Errorf("max_gop_bytes=%d clamped to %d: %d packets left a %d-block ring; prune walks all of it under the hub lock, per packet",
					c.in, v.MaxGOPBytes, fill, blocks)
			}
		})
	}
}

// TestMaxGOPClampRange pins the clamped range itself: the floor is the smallest
// block that still holds a GOP rather than a packet, and the ceiling is
// unchanged. A value the panel already sends inside the range passes through.
func TestMaxGOPClampRange(t *testing.T) {
	for _, c := range []struct{ in, want int }{
		{0, defaults.CfgMinMaxGOPBytes},
		{188, defaults.CfgMinMaxGOPBytes},
		{defaults.CfgMinMaxGOPBytes - 1, defaults.CfgMinMaxGOPBytes},
		{defaults.CfgMinMaxGOPBytes, defaults.CfgMinMaxGOPBytes},
		{defaults.CfgMaxGOPBytes, defaults.CfgMaxGOPBytes}, // what the panel writes
		{256 << 20, 256 << 20},
		{1 << 30, 256 << 20},
	} {
		v := Defaults()
		v.MaxGOPBytes = c.in
		v.clamp()
		if v.MaxGOPBytes != c.want {
			t.Errorf("clamp(max_gop_bytes=%d) = %d, want %d", c.in, v.MaxGOPBytes, c.want)
		}
	}
}
