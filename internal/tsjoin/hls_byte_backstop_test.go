// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"strings"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// The byte backstop is sized from an ASSUMED bitrate (~24 Mbit/s). A stream
// that runs faster than that hit it before the window it is meant to hold was
// full, so the ring kept less than the two segments the HLS view needs and the
// playlist rendered empty. internal/server answers an empty playlist with 404,
// and a manifest 404 is fatal to a player where a missing segment is merely
// retried.
func TestAFastStreamStillListsSegments(t *testing.T) {
	s := New(1<<26, 0)
	s.Configure(0, 1000, 6) // no TS prebuffer: the HLS floor is the whole ring
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))

	empty, polls, pts := 0, 0, int64(0)
	for i := 0; i < 60; i++ {
		s.Update(tsfixture.KeyframePCR(0x101, pts, pts))
		for j := 0; j < 2723; j++ { // ~5 MB/s, well past the assumption
			s.Update(tsfixture.Fill(0x101))
		}
		pts += 100 * pcrHz
		if i < 12 {
			continue // the first segments have not closed yet
		}
		polls++
		if !strings.Contains(s.HLSPlaylist(), "#EXTINF") {
			empty++
		}
	}
	if empty > 0 {
		t.Errorf("the playlist rendered empty on %d of %d polls: the ring is pruned below the two segments HLS needs", empty, polls)
	}

	// And the ring is still bounded by its window, not by the raised backstop.
	if _, spanMS, _ := s.RingStats(); spanMS > 3000 {
		t.Errorf("the ring holds %d ms, want its 2 s HLS floor: raising the byte backstop must not disable duration pruning", spanMS)
	}
}

// A stream with no parseable clock has no measured rate, so the assumption
// stands and the backstop still bounds the ring — the unbounded-growth case it
// exists for.
func TestAClocklessStreamIsStillBoundedByBytes(t *testing.T) {
	s := New(4*PacketSize, 0)
	s.Configure(1000, 0, 0) // 1 s of ring, no HLS view
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))
	for i := 0; i < 40000; i++ {
		s.Update(tsfixture.Fill(0x101)) // no PCR anywhere: no ring clock
	}
	// The backstop drops whole blocks, so the ring settles within one block of
	// its limit rather than exactly on it.
	bytes, _, _ := s.RingStats()
	if limit := 1000*3000 + 4*PacketSize; bytes > limit {
		t.Errorf("a clockless stream grew the ring to %d bytes, past its %d-byte backstop", bytes, limit)
	}
}
