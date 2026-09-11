// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// pmtWithAudio builds a PMT announcing an H.264 video ES and an AC-3 audio ES,
// which is the shape a DVB broadcast channel actually has. tsfixture's PMT
// carries video only.
func pmtWithAudio(pmtPID, videoPID, audioPID int) []byte {
	p := make([]byte, 188)
	p[0] = 0x47
	p[1] = byte(0x40 | (pmtPID>>8)&0x1f)
	p[2] = byte(pmtPID & 0xff)
	p[3] = 0x10
	p[4] = 0x00 // pointer_field
	p[5] = 0x02 // table_id (PMT)
	p[6], p[7] = 0xb0, 0x1c
	p[15], p[16] = 0x00, 0x00 // program_info_length = 0

	es := 17
	p[es] = 0x1b // H.264
	p[es+1] = byte(0xe0 | (videoPID>>8)&0x1f)
	p[es+2] = byte(videoPID & 0xff)
	p[es+3], p[es+4] = 0x00, 0x00

	es += 5
	p[es] = 0x81 // AC-3
	p[es+1] = byte(0xe0 | (audioPID>>8)&0x1f)
	p[es+2] = byte(audioPID & 0xff)
	p[es+3], p[es+4] = 0x00, 0x00
	return p
}

// TestStreamMetadataFromTheRing is M4's claim end to end: the codec names and
// picture size the panel used to run ffprobe for are read out of the bytes the
// daemon is already holding.
func TestStreamMetadataFromTheRing(t *testing.T) {
	m := NewManager(1<<20, 20000, 2, 6, time.Second)
	m.EnableSupervision()
	st := m.GetOrCreate("42")

	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtWithAudio(0x100, 0x101, 0x102))

	meta, ok := m.StreamMetadata("42")
	if !ok {
		t.Fatal("no metadata for a registered stream")
	}
	if meta.VideoCodec != "h264" {
		t.Errorf("VideoCodec = %q, want h264 from the PMT stream_type", meta.VideoCodec)
	}
	if meta.AudioCodec != "ac3" {
		t.Errorf("AudioCodec = %q, want ac3 from the PMT stream_type", meta.AudioCodec)
	}
}

// TestStreamMetadataMeasuresBitrate: the panel took bitrate from ffprobe's
// estimate over one segment. The daemon knows exactly how many bytes it fanned
// out, so it measures instead.
func TestStreamMetadataMeasuresBitrate(t *testing.T) {
	m := NewManager(1<<20, 20000, 2, 6, time.Second)
	m.EnableSupervision()
	st := m.GetOrCreate("42")
	st.Publish(tsfixture.PAT(0x100))

	// First read establishes the baseline sample.
	if _, ok := m.StreamMetadata("42"); !ok {
		t.Fatal("no metadata")
	}

	// Push a known amount, then reach back past the minimum window so the next
	// read computes a rate rather than declining to.
	const packets = 500
	for i := 0; i < packets; i++ {
		st.Publish(tsfixture.Fill(0x101))
	}
	m.meta.mu.Lock()
	m.meta.entries["42"].at = time.Now().Add(-4 * time.Second)
	m.meta.mu.Unlock()

	meta, _ := m.StreamMetadata("42")
	if meta.BitrateKbps <= 0 {
		t.Fatalf("BitrateKbps = %d, want a measured rate", meta.BitrateKbps)
	}
	// 500 * 188 bytes over ~4s is roughly 188 kbit/s; allow a wide band since the
	// window is wall-clock.
	if meta.BitrateKbps < 100 || meta.BitrateKbps > 400 {
		t.Errorf("BitrateKbps = %d, want roughly 188 for %d packets over ~4s", meta.BitrateKbps, packets)
	}
}

// TestStreamMetadataKeepsWhatItLearned: a later snapshot that happens not to
// carry the PMT must not erase codecs already determined. Overwriting a correct
// value with a blank is worse than never having read it.
func TestStreamMetadataKeepsWhatItLearned(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second) // prebuffer 0: the ring keeps only the current GOP
	m.EnableSupervision()
	st := m.GetOrCreate("42")

	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtWithAudio(0x100, 0x101, 0x102))
	first, _ := m.StreamMetadata("42")
	if first.VideoCodec != "h264" {
		t.Fatalf("VideoCodec = %q on the first read", first.VideoCodec)
	}

	// Roll the ring well past the PMT and force a re-parse.
	for i := 0; i < 50; i++ {
		st.Publish(tsfixture.Keyframe(0x101, int64(i)*90000))
	}
	m.meta.mu.Lock()
	m.meta.entries["42"].lastParse = time.Now().Add(-time.Hour)
	m.meta.mu.Unlock()

	again, _ := m.StreamMetadata("42")
	if again.VideoCodec != "h264" {
		t.Errorf("VideoCodec = %q after a re-parse, want the learned h264 to survive", again.VideoCodec)
	}
}

// TestStreamMetadataUnknownStream: a stream this node does not have reports
// nothing rather than an empty-but-present answer.
func TestStreamMetadataUnknownStream(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.EnableSupervision()
	if _, ok := m.StreamMetadata("nope"); ok {
		t.Error("reported metadata for a stream that is not registered")
	}
}
