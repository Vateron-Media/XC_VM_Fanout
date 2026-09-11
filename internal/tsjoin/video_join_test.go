// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestJoinStartsOnVideoKeyframeNotAudioRAI: random_access_indicator is set on the
// AUDIO PID as well (ffmpeg's own muxer does it — every audio frame is a random
// access point), and a block opened there begins with audio packets and a video
// slice from the middle of a GOP. A viewer joining on it decodes pictures whose
// SPS/PPS it never received: "non-existing PPS 0 referenced", a black screen
// until the next real keyframe. With no prebuffer configured — only the open
// block is kept — that was every join on every stream.
func TestJoinStartsOnVideoKeyframeNotAudioRAI(t *testing.T) {
	const videoPID, audioPID = 0x101, 0x102

	s := New(1<<20, 0) // ring90 = 0: the common "no prebuffer" configuration
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, videoPID))
	s.Update(tsfixture.Keyframe(videoPID, 0))
	for i := 0; i < 3; i++ {
		s.Update(tsfixture.Fill(videoPID))
		s.Update(tsfixture.Keyframe(audioPID, int64(i)*3000)) // audio flagged random-access
	}

	snap := s.Snapshot(0)
	if len(snap) < 3*PacketSize {
		t.Fatalf("snapshot is %d bytes, want the tables plus the video GOP", len(snap))
	}
	gop := snap[2*PacketSize:] // after the PAT and PMT header
	if pid := int(gop[1]&0x1f)<<8 | int(gop[2]); pid != videoPID {
		t.Fatalf("join starts on PID 0x%03x, want the video PID 0x%03x", pid, videoPID)
	}
	if gop[3]>>4&0x2 == 0 || gop[5]&0x40 == 0 {
		t.Fatal("join must start on a random-access point")
	}
	// Everything after that keyframe is there too: the audio packets went into
	// the same block instead of cutting one of their own.
	if want := (2 + 7) * PacketSize; len(snap) != want {
		t.Fatalf("snapshot len = %d, want %d (tables + the whole video GOP)", len(snap), want)
	}
}

// TestAudioOnlyStreamStillJoinsOnRAI: a radio channel has no video PID, so the
// plain random-access rule is all there is — it must keep working.
func TestAudioOnlyStreamStillJoinsOnRAI(t *testing.T) {
	const audioPID = 0x101

	s := New(1<<20, 0)
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMTType(0x100, audioPID, 0x0f)) // AAC: no video in the programme
	s.Update(tsfixture.Fill(audioPID))
	s.Update(tsfixture.Keyframe(audioPID, 0))
	s.Update(tsfixture.Fill(audioPID))

	snap := s.Snapshot(0)
	gop := snap[2*PacketSize:]
	if len(gop) != 2*PacketSize {
		t.Fatalf("audio GOP is %d bytes, want the random-access packet and the one after it", len(gop))
	}
	if gop[5]&0x40 == 0 {
		t.Fatal("an audio-only stream must still open its block on a random-access point")
	}
}
