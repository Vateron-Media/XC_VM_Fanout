package hlsseg

import (
	"math"
	"strings"
	"testing"

	"github.com/Vateron-Media/xc_fanout/internal/tsfixture"
)

func TestPtsDiffWraparound(t *testing.T) {
	if got := ptsDiff(0, ptsClock); math.Abs(got-1.0) > 1e-9 {
		t.Fatalf("ptsDiff(0, 90000) = %v, want 1.0", got)
	}
	// Start just below the 33-bit wrap, end just after it: still 1 second.
	if got := ptsDiff(ptsWrap-45000, 45000); math.Abs(got-1.0) > 1e-9 {
		t.Fatalf("wrapped ptsDiff = %v, want 1.0", got)
	}
}

// TestParsePTS crafts a video TS packet whose PES header carries PTS = 90000
// (one second) and checks the 33-bit field is decoded correctly.
func TestParsePTS(t *testing.T) {
	p := tsfixture.Keyframe(0x101, 90000)
	pts, ok := parsePTS(p)
	if !ok {
		t.Fatal("parsePTS returned ok=false")
	}
	if pts != 90000 {
		t.Fatalf("parsePTS = %d, want 90000", pts)
	}
}

func TestIsVideoStreamType(t *testing.T) {
	for _, v := range []byte{0x1b, 0x24, 0x02} {
		if !isVideoStreamType(v) {
			t.Fatalf("stream type 0x%02x should be video", v)
		}
	}
	for _, a := range []byte{0x0f, 0x81, 0x03} { // AAC, AC-3, MP2 audio
		if isVideoStreamType(a) {
			t.Fatalf("stream type 0x%02x should not be video", a)
		}
	}
}

// TestSegmenterCutsAtTarget feeds keyframes exactly targetDur apart and asserts
// one segment is cut per keyframe, each with the PTS-derived duration.
func TestSegmenterCutsAtTarget(t *testing.T) {
	const pmt, vid = 0x100, 0x101
	s := New(2, 6) // 2s target, window 6

	s.Feed(tsfixture.PAT(pmt))
	s.Feed(tsfixture.PMT(pmt, vid))
	for _, sec := range []int64{0, 2, 4, 6} { // keyframes at 0,2,4,6 s
		s.Feed(tsfixture.Keyframe(vid, sec*90000))
	}

	pl := s.Playlist()
	if !strings.Contains(pl, "#EXT-X-MEDIA-SEQUENCE:0") {
		t.Fatalf("playlist media-sequence wrong:\n%s", pl)
	}
	// 4 keyframes 2s apart => 3 finalized segments (the 4th is still open).
	if n := strings.Count(pl, "#EXTINF:"); n != 3 {
		t.Fatalf("got %d segments, want 3\n%s", n, pl)
	}
	if !strings.Contains(pl, "#EXTINF:2.000,") {
		t.Fatalf("segment duration should be 2.000s\n%s", pl)
	}
	if seg := s.Segment(0); seg == nil || seg[0] != 0x47 {
		t.Fatal("Segment(0) must exist and start with a TS sync byte")
	}
	if s.Segment(3) != nil {
		t.Fatal("Segment(3) is still open and must not be served")
	}
	if s.Segment(99) != nil {
		t.Fatal("Segment(99) does not exist")
	}
}

// TestSegmenterWindowRotation asserts the sliding window drops old segments and
// advances the media sequence.
func TestSegmenterWindowRotation(t *testing.T) {
	const pmt, vid = 0x100, 0x101
	s := New(2, 3) // keep only 3 segments

	s.Feed(tsfixture.PAT(pmt))
	s.Feed(tsfixture.PMT(pmt, vid))
	for sec := int64(0); sec <= 14; sec += 2 { // 8 keyframes => 7 finalized segments
		s.Feed(tsfixture.Keyframe(vid, sec*90000))
	}

	pl := s.Playlist()
	if n := strings.Count(pl, "#EXTINF:"); n != 3 {
		t.Fatalf("window should hold 3 segments, got %d\n%s", n, pl)
	}
	// 7 finalized (seq 0..6), window keeps 4,5,6 => media-sequence 4.
	if !strings.Contains(pl, "#EXT-X-MEDIA-SEQUENCE:4") {
		t.Fatalf("media-sequence should be 4 after rotation\n%s", pl)
	}
	if s.Segment(0) != nil {
		t.Fatal("segment 0 should have rotated out")
	}
	if s.Segment(6) == nil {
		t.Fatal("segment 6 should still be in the window")
	}
}
