// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// playlistSeqs reads the "<seq>.ts" URIs out of a rendered playlist.
func playlistSeqs(t *testing.T, pl string) []int {
	t.Helper()
	var out []int
	for _, l := range strings.Split(pl, "\n") {
		v, ok := strings.CutSuffix(l, ".ts")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("unparseable segment uri %q: %v", l, err)
		}
		out = append(out, n)
	}
	return out
}

// TestHLSNeverListsASegmentItCannotServe: the ring's byte backstop is sized at
// defaults.JoinRingBytesPerMS (≈24 Mbit/s), so a channel above that keeps fewer
// milliseconds than the HLS floor asked for and the ring prunes past a closed
// segment's FIRST GOP while the playlist still lists it. HLSSegmentPin then
// refuses the fetch — a segment served without its own keyframe is a decode
// error, where a 404 only makes the player skip — so the operator sees a
// playlist whose segments mostly 404.
//
// The playlist may be short here; it must never advertise what the ring can no
// longer assemble.
func TestHLSNeverListsASegmentItCannotServe(t *testing.T) {
	const (
		gopMS   = 100                   // keyframes 100 ms apart
		rateBPS = 5120                  // bytes per ms ≈ 41 Mbit/s, past the backstop
		gopPkts = gopMS * rateBPS / 188 // packets in one GOP
		target  = int64(1000)           // 1 s HLS segments = 10 GOPs
	)
	s := New(1<<26, 0)
	s.Configure(0, target, 6) // prebuffer_max_sec=0: the HLS floor is all the ring has
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	bulk := bytes.Repeat(tsfixture.Fill(0x101), 200)

	listed, unservable := 0, 0
	for g := int64(0); g < 60; g++ {
		s.Update(tsfixture.KeyframePCR(0x101, g*gopMS*pcrHz, g*gopMS*pcrHz))
		for n := 0; n < gopPkts; n += 200 {
			s.Update(bulk)
		}
		for _, seq := range playlistSeqs(t, s.HLSPlaylist()) {
			listed++
			if _, _, pin, ok := s.HLSSegmentPin(nil, seq); !ok {
				unservable++
			} else {
				s.Unpin(pin)
			}
		}
	}
	if listed == 0 {
		t.Fatal("fixture listed no segments at all; it proves nothing")
	}
	if unservable > 0 {
		_, span, gops := s.RingStats()
		t.Errorf("%d of %d listed segments could not be assembled (404): ring holds %d ms in %d blocks against a %d ms HLS floor",
			unservable, listed, span, gops, 2*target)
	}
}
