package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// feedGOPs feeds n GOPs of a source that sets NO random_access_indicator: each
// opens with a keyframe PES (kf) and carries non-key PES starts (nonKey) and fill
// after it, 3 s apart on the video clock.
func feedGOPs(s *State, streamType byte, kf, nonKey []byte, n int) {
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMTType(0x100, 0x101, streamType))
	for g := 0; g < n; g++ {
		base := int64(g) * 3 * 90000
		s.Update(tsfixture.PESStart(0x101, base, kf...))
		for f := 1; f < 4; f++ {
			s.Update(tsfixture.PESStart(0x101, base+int64(f)*20000, nonKey...))
			s.Update(tsfixture.Fill(0x101))
		}
	}
}

// TestUnflaggedKeyframesCutSegments: with nothing but the elementary stream to go
// on, IDR slices (H.264), IRAP pictures (HEVC) and sequence headers (MPEG-2)
// still open segments. Before this, such a source produced no HLS at all — which
// passing a source through without ffmpeg made a common case, not a rare one.
func TestUnflaggedKeyframesCutSegments(t *testing.T) {
	for _, c := range []struct {
		name       string
		streamType byte
		kf, nonKey []byte
	}{
		{"h264 idr", tspes.StreamTypeH264, []byte{0x65}, []byte{0x41}},
		{"h264 aud+idr", tspes.StreamTypeH264, []byte{0x09, 0xf0, 0x00, 0x00, 0x01, 0x65}, []byte{0x09, 0xf0, 0x00, 0x00, 0x01, 0x41}},
		{"hevc idr_w_radl", tspes.StreamTypeHEVC, []byte{19 << 1, 0x01}, []byte{1 << 1, 0x01}},
		{"hevc cra", tspes.StreamTypeHEVC, []byte{21 << 1, 0x01}, []byte{0 << 1, 0x01}},
		{"mpeg2 sequence header", tspes.StreamTypeMPEG2, []byte{0xb3}, []byte{0x00}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := New(1<<20, 40000)
			s.Configure(40000, 2000, 6)
			feedGOPs(s, c.streamType, c.kf, c.nonKey, 5)
			if s.HLSPlaylist() == "" {
				t.Fatal("no HLS segment was cut from a source whose keyframes are only in the ES")
			}
			if len(s.segs) < 3 {
				t.Errorf("%d segments from 5 keyframes, want every keyframe to cut", len(s.segs))
			}
		})
	}
}

// TestNonKeyframesDoNotCut: the fallback must never mistake an ordinary frame for
// a keyframe — a segment opening on a frame that cannot be decoded alone is worse
// than one that runs long.
func TestNonKeyframesDoNotCut(t *testing.T) {
	s := New(1<<20, 40000)
	s.Configure(40000, 2000, 6)
	// Every PES is a non-IDR slice: there is nowhere to cut.
	feedGOPs(s, tspes.StreamTypeH264, []byte{0x41}, []byte{0x01}, 5)
	if pl := s.HLSPlaylist(); pl != "" {
		t.Fatalf("segments were cut on non-key frames:\n%s", pl)
	}
}

// TestFlaggedSourcesUnchanged: a source that DOES flag its keyframes behaves as
// before — the scan only runs where the flag is absent.
func TestFlaggedSourcesUnchanged(t *testing.T) {
	s := New(1<<20, 40000)
	s.Configure(40000, 2000, 6)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	for g := 0; g < 5; g++ {
		s.Update(tsfixture.Keyframe(0x101, int64(g)*3*90000))
	}
	if len(s.segs) < 3 {
		t.Errorf("%d segments from 5 flagged keyframes", len(s.segs))
	}
}
