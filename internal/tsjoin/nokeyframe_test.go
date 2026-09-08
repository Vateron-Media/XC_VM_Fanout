package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestNoKeyframeSourceKeepsRolling: some sources never set
// random_access_indicator. Such a stream used to grow ONE block to maxGOP and
// then silently discard every packet after it — the ring froze (pruning needs
// two blocks), the stream held maxGOP forever, and every joining viewer was
// served that same stale block from whenever the cap was hit. Cutting a new
// block at the cap keeps the ring rolling and bounded.
func TestNoKeyframeSourceKeepsRolling(t *testing.T) {
	const maxGOP = 8 * 188 // tiny cap so the cut happens quickly
	s := New(maxGOP, 2000)
	s.Configure(2000, 0, 0) // HLS off; this source can't produce segments anyway

	chunk := tsfixture.Fill(0x100)
	for i := 0; i < 500; i++ {
		s.Update(chunk)
	}

	if len(s.gops) < 2 {
		t.Fatalf("ring froze at %d block(s): the source's data is being discarded", len(s.gops))
	}
	if s.NoKeyframeCuts() == 0 {
		t.Error("forced cuts not counted — nothing surfaces that this source has no keyframes")
	}
	total := 0
	for i := range s.gops {
		if len(s.gops[i].data) > maxGOP {
			t.Errorf("block %d is %d bytes, over the %d cap", i, len(s.gops[i].data), maxGOP)
		}
		total += len(s.gops[i].data)
	}
	if total > s.maxRing {
		t.Errorf("ring holds %d bytes, over its %d ceiling", total, s.maxRing)
	}

	// The last packets fed must be in the ring: a joiner gets CURRENT data, not a
	// block frozen when the cap was first reached.
	last := s.gops[len(s.gops)-1]
	if len(last.data) == 0 {
		t.Error("newest block is empty — recent packets were dropped")
	}
}

// TestPlaylistCacheInvalidates: the playlist is cached because every HLS viewer
// polls it on its own schedule, and re-rendering an identical string per poll
// held the hub lock against the producer hundreds of times a second. It must
// still change the moment the segment list does.
func TestPlaylistCacheInvalidates(t *testing.T) {
	s := New(1<<20, 40000)
	s.Configure(40000, 2000, 6)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	for g := 0; g < 4; g++ {
		s.Update(tsfixture.Keyframe(0x101, int64(g)*3*90000))
	}

	first := s.HLSPlaylist()
	if first == "" {
		t.Fatal("no playlist")
	}
	if again := s.HLSPlaylist(); again != first {
		t.Fatal("cached playlist differs from the first render")
	}

	// A new segment closes → the playlist must reflect it.
	for g := 4; g < 7; g++ {
		s.Update(tsfixture.Keyframe(0x101, int64(g)*3*90000))
	}
	if grown := s.HLSPlaylist(); grown == first {
		t.Fatal("playlist did not change after new segments closed — stale cache")
	}

	// A window change must invalidate it too.
	before := s.HLSPlaylist()
	s.Configure(40000, 2000, 1)
	if after := s.HLSPlaylist(); after == before {
		t.Fatal("playlist did not change after hls_window was reduced — stale cache")
	}
}
