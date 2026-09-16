// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// pcrFill is an ordinary payload packet that carries a PCR and flags NO
// random-access point — what a source that has stopped marking keyframes still
// sends to keep the clock running.
func pcrFill(pid int, pcr int64) []byte {
	p := make([]byte, PacketSize)
	p[0] = 0x47
	p[1] = byte((pid >> 8) & 0x1f)
	p[2] = byte(pid)
	p[3] = 0x30 // adaptation field + payload
	p[4] = 7    // adaptation_field_length: the flags byte plus six PCR bytes
	p[5] = 0x10 // PCR_flag, and no random_access_indicator
	p[6] = byte(pcr >> 25)
	p[7] = byte(pcr >> 17)
	p[8] = byte(pcr >> 9)
	p[9] = byte(pcr >> 1)
	p[10] = byte(pcr << 7)
	return p
}

// The ring clock caps a forward step no cadence explains, so a timeline jump
// cannot age the whole ring out at once. But a cadence is only a cadence while
// the source still produces the blocks it was measured between: when a source
// stops flagging keyframes, its blocks are cut at max_gop_bytes instead and can
// each span minutes of media. Capping those at the old cadence froze the clock
// against real elapsed time, and the ring stopped ageing — bounded only by the
// byte backstop, which at the shipped max_gop_bytes is 120 MB of resident
// memory per affected stream instead of a 40 s window.
func TestABlockCutWithoutAKeyframeStillAgesTheRing(t *testing.T) {
	s := New(2*PacketSize, 40000) // cut every 2 packets, 40 s of ring
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))

	// A 2 s keyframe cadence, learned the ordinary way.
	for i := int64(0); i < 5; i++ {
		s.Update(tsfixture.KeyframePCR(0x101, i*2*90000, i*2*90000))
	}

	// The source stops flagging keyframes. Each block is now a max_gop_bytes cut
	// carrying five minutes of media.
	pcr := int64(5 * 2 * 90000)
	for i := 0; i < 12; i++ {
		pcr += 5 * 60 * 90000
		s.Update(pcrFill(0x101, pcr))
		s.Update(tsfixture.Fill(0x101))
		s.Update(tsfixture.Fill(0x101))
	}

	_, spanMS, gops := s.RingStats()
	if gops > 4 {
		t.Errorf("the ring holds %d blocks (%d ms on its own clock) of five-minute blocks: it is not ageing against a 40 s window", gops, spanMS)
	}
}

// And the source that never had a cadence at all — keyframe-less radio, whose
// blocks are all max_gop_bytes cuts — must keep pruning, which is what the
// uncapped forward step is there for.
func TestAKeyframelessSourceStillPrunes(t *testing.T) {
	s := New(2*PacketSize, 40000)
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))
	pcr := int64(0)
	for i := 0; i < 12; i++ {
		pcr += 5 * 60 * 90000
		s.Update(pcrFill(0x101, pcr))
		s.Update(tsfixture.Fill(0x101))
		s.Update(tsfixture.Fill(0x101))
	}
	if _, _, gops := s.RingStats(); gops > 4 {
		t.Errorf("a keyframe-less source holds %d blocks: the ring is not pruning", gops)
	}
}
