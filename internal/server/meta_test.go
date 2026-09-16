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
	return pmtCodecs(pmtPID, videoPID, 0x1b, audioPID, 0x81) // H.264 + AC-3
}

// pmtCodecs builds the same PMT with the stream types spelled out, so a test can
// say "this stream is now carrying something else" — which is what a failover to
// a backup source, or a re-encoded source, looks like on the wire.
func pmtCodecs(pmtPID, videoPID int, videoType byte, audioPID int, audioType byte) []byte {
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
	p[es] = videoType
	p[es+1] = byte(0xe0 | (videoPID>>8)&0x1f)
	p[es+2] = byte(videoPID & 0xff)
	p[es+3], p[es+4] = 0x00, 0x00

	es += 5
	p[es] = audioType
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

// TestStreamMetadataRereadsAfterASourceChange: a complete entry used never to be
// read again, for the life of the process. But the stream under the id changes:
// the supervisor fails over to a backup source, an operator forces another one,
// the panel re-registers it. A 1080p H.264 primary that fell back to a 720p HEVC
// backup kept being reported to the panel as h264 1920x1080 — where the ffprobe
// this replaced re-ran every five minutes and would have corrected it. The panel
// writes that straight into streams_servers, so the row stays wrong until
// somebody restarts the daemon.
func TestStreamMetadataRereadsAfterASourceChange(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second) // prebuffer 0: the ring holds the current block only
	m.EnableSupervision()
	st := m.GetOrCreate("42")

	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtWithAudio(0x100, 0x101, 0x102)) // the primary: H.264 + AC-3
	if _, ok := m.StreamMetadata("42"); !ok {
		t.Fatal("no metadata for a registered stream")
	}

	// tsfixture's packets carry no sequence header, so pretend the first parse
	// also got the picture size: Complete() is the state that used to freeze the
	// entry, and it is the state every real A/V stream reaches within seconds.
	m.meta.mu.Lock()
	e := m.meta.entries["42"]
	e.info.Width, e.info.Height = 1920, 1080
	e.lastParse = time.Now().Add(-10 * time.Minute)
	m.meta.mu.Unlock()

	// Failover: the same stream id, a different encoder, HEVC video and MP3 audio.
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtCodecs(0x100, 0x101, 0x24, 0x102, 0x04))

	after, _ := m.StreamMetadata("42")
	if after.VideoCodec != "hevc" || after.AudioCodec != "mp3" {
		t.Fatalf("after a source switch the panel is still told %q/%q; want hevc/mp3 from the new PMT",
			after.VideoCodec, after.AudioCodec)
	}

	// And it must not turn into a per-call re-parse: a snapshot copies the ring,
	// and the panel polls this on a short cadence for every supervised stream.
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtCodecs(0x100, 0x101, 0x02, 0x102, 0x0f)) // mpeg2video + aac
	soon, _ := m.StreamMetadata("42")
	if soon.VideoCodec != "hevc" {
		t.Fatalf("VideoCodec = %q on a read seconds after the last parse; the snapshot must not be "+
			"taken on every call", soon.VideoCodec)
	}
}

// TestStreamMetadataNeverReportsANegativeBitrate: the bitrate is a difference
// between two readings of a counter that belongs to the Stream, not to the id.
// Unregister the stream (a panel teardown, a re-registration) and the next
// viewer creates a fresh one whose counter starts at zero, while the meta cache
// still holds the old sample. The difference went negative and the panel was
// handed a negative bitrate_kbps for streams_servers.
func TestStreamMetadataNeverReportsANegativeBitrate(t *testing.T) {
	m := NewManager(1<<20, 20000, 2, 6, time.Second)
	m.EnableSupervision()
	st := m.GetOrCreate("42")
	st.Publish(tsfixture.PAT(0x100))
	for i := 0; i < 500; i++ {
		st.Publish(tsfixture.Fill(0x101))
	}
	if _, ok := m.StreamMetadata("42"); !ok { // baseline sample at ~94 KB
		t.Fatal("no metadata")
	}

	// The stream is torn down and re-created under the same id: new Stream, new
	// counter, from zero.
	m.Unregister("42")
	st = m.GetOrCreate("42")
	st.Publish(tsfixture.PAT(0x100))

	// Reach back past the minimum window so a rate is computed rather than skipped.
	m.meta.mu.Lock()
	m.meta.entries["42"].at = time.Now().Add(-4 * time.Second)
	m.meta.mu.Unlock()

	meta, _ := m.StreamMetadata("42")
	if meta.BitrateKbps < 0 {
		t.Fatalf("BitrateKbps = %d after the stream was re-created; the panel is being told the "+
			"stream has a negative bitrate", meta.BitrateKbps)
	}

	// And the next window measures the new stream properly.
	for i := 0; i < 500; i++ {
		st.Publish(tsfixture.Fill(0x101))
	}
	m.meta.mu.Lock()
	m.meta.entries["42"].at = time.Now().Add(-4 * time.Second)
	m.meta.mu.Unlock()
	if meta, _ := m.StreamMetadata("42"); meta.BitrateKbps <= 0 {
		t.Fatalf("BitrateKbps = %d one window after the re-create; the measurement never recovered",
			meta.BitrateKbps)
	}
}
