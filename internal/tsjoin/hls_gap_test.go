// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"strconv"
	"strings"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// gapState is a 40 s ring cutting 6 s HLS segments, primed with PAT/PMT. The
// returned gop publishes one keyframe-opened block stamped at the given 90 kHz
// time (PTS and PCR together, as a real video keyframe carries both) plus the
// filler packets that follow it.
func gapState(t *testing.T) (s *State, gop func(t90 int64)) {
	t.Helper()
	s = New(1<<24, 40_000)
	s.Configure(40_000, 6_000, 6)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	return s, func(t90 int64) {
		s.Update(tsfixture.KeyframePCR(0x101, t90, t90))
		for i := 0; i < 20; i++ {
			s.Update(tsfixture.Fill(0x101))
		}
	}
}

// extinfs reads the #EXTINF durations out of a rendered playlist.
func extinfs(t *testing.T, pl string) []float64 {
	t.Helper()
	var out []float64
	for _, l := range strings.Split(pl, "\n") {
		v, ok := strings.CutPrefix(l, "#EXTINF:")
		if !ok {
			continue
		}
		d, err := strconv.ParseFloat(strings.TrimSuffix(v, ","), 64)
		if err != nil {
			t.Fatalf("unparseable #EXTINF %q: %v", l, err)
		}
		out = append(out, d)
	}
	return out
}

// targetDuration reads #EXT-X-TARGETDURATION out of a rendered playlist.
func targetDuration(t *testing.T, pl string) int {
	t.Helper()
	for _, l := range strings.Split(pl, "\n") {
		if v, ok := strings.CutPrefix(l, "#EXT-X-TARGETDURATION:"); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				t.Fatalf("unparseable target duration %q: %v", l, err)
			}
			return n
		}
	}
	t.Fatalf("no #EXT-X-TARGETDURATION in playlist:\n%s", pl)
	return 0
}

// TestHLSGapDoesNotInflateSegmentDuration: a source that reconnects, or an
// upstream HLS source that skips segments, steps its PTS forward by seconds with
// no media in between. The segment span was measured from its first keyframe to
// the first keyframe after the gap, so the segment straddling the gap was listed
// with #EXTINF — and #EXT-X-TARGETDURATION — of media PLUS gap, 16 s for 6 s of
// video after a 10 s gap, and with no #EXT-X-DISCONTINUITY. A player buffers
// against a duration the segment cannot fill, and nothing tells it the timeline
// jumped.
func TestHLSGapDoesNotInflateSegmentDuration(t *testing.T) {
	const sec = int64(90000)
	s, gop := gapState(t)

	base := int64(2 * hour90) // far from the 33-bit wrap
	var last int64
	for g := int64(0); g < 12; g++ { // 24 s of 2 s GOPs
		last = base + g*2*sec
		gop(last)
	}
	for g := int64(1); g <= 10; g++ { // 10 s of stream time with no media
		gop(last + 10*sec + g*2*sec)
	}

	pl := s.HLSPlaylist()
	// A segment holds at most the target plus the GOP that carried it over —
	// 6 s + 2 s here. Anything longer is gap counted as media.
	for _, d := range extinfs(t, pl) {
		if d > 8.001 {
			t.Errorf("#EXTINF:%.3f lists gap as media (6 s target, 2 s GOPs):\n%s", d, pl)
		}
	}
	if td := targetDuration(t, pl); td > 8 {
		t.Errorf("#EXT-X-TARGETDURATION:%d after a 10 s gap, want the real segment length:\n%s", td, pl)
	}
	if !strings.Contains(pl, "#EXT-X-DISCONTINUITY\n") {
		t.Errorf("a 10 s forward gap left no #EXT-X-DISCONTINUITY, so a player never resets its clock:\n%s", pl)
	}

	// The segment closed by the gap must be timed on the media it holds: three
	// 2 s GOPs.
	var closing hlsSeg
	for i, sg := range s.segs {
		if sg.disc && i > 0 {
			closing = s.segs[i-1]
		}
	}
	if closing.durMS == 0 {
		t.Fatalf("no segment was marked as following the gap: %+v", s.segs)
	}
	if closing.durMS != 6000 {
		t.Errorf("the segment closed at the gap lasts %d ms, want the 6000 ms of media it holds", closing.durMS)
	}
}

// TestHLSKeepsLongGOPsWhenTheyAreTheCadence is the other half of the gap rule: a
// source whose keyframes are further apart than the HLS target cuts one GOP per
// segment, and those long segments are real media. They must keep their true
// duration and must NOT be read as a timeline jump.
func TestHLSKeepsLongGOPsWhenTheyAreTheCadence(t *testing.T) {
	const sec = int64(90000)
	s, gop := gapState(t)

	for g := int64(0); g < 8; g++ { // 10 s GOPs against a 6 s target
		gop(2*hour90 + g*10*sec)
	}

	for _, sg := range s.segs {
		if sg.disc {
			t.Fatalf("a 10 s GOP cadence was read as a timeline jump: %+v", s.segs)
		}
	}
	for _, d := range extinfs(t, s.HLSPlaylist()) {
		if d != 10 {
			t.Errorf("#EXTINF:%.3f for a one-GOP segment holding 10 s of media:\n%s", d, s.HLSPlaylist())
		}
	}
}

// TestHLSTargetDurationNeverDrops: #EXT-X-TARGETDURATION is what a player sizes
// its reload timer and its startup buffer from, and RFC 8216 requires it not to
// change for the life of the playlist. Rendering it from the window alone moved
// it every time a longer-than-usual segment slid out — a value the same client
// saw change between two reloads of the same URL.
func TestHLSTargetDurationNeverDrops(t *testing.T) {
	const sec = int64(90000)
	s, gop := gapState(t)

	// One stretch of slower keyframes (5 s apart — still this source's ordinary
	// cadence, not a jump) makes one segment longer than the rest.
	tt := int64(2 * hour90)
	for g := 0; g < 4; g++ {
		gop(tt)
		tt += 2 * sec
	}
	for g := 0; g < 2; g++ {
		gop(tt)
		tt += 5 * sec
	}
	long := targetDuration(t, s.HLSPlaylist())
	if long <= 6 {
		t.Fatalf("fixture produced no segment past the 6 s target (target duration %d)", long)
	}

	// Ordinary 2 s GOPs from here on: the long segment ages out of the window.
	for g := 0; g < 40; g++ {
		gop(tt)
		tt += 2 * sec
	}
	if td := targetDuration(t, s.HLSPlaylist()); td < long {
		t.Errorf("#EXT-X-TARGETDURATION dropped %d → %d as the window slid; it must not change:\n%s",
			long, td, s.HLSPlaylist())
	}
}
