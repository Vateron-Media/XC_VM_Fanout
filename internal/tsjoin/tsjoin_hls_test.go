package tsjoin

import (
	"bytes"
	"strings"
	"testing"
)

// pmtPacket is a PMT on PID 0x100 (matches patPacket(0x100)) that declares one
// H.264 video elementary stream on PID 0x101 — so parseVideoPID locates the PES
// the HLS clock reads. Only the bytes parseVideoPID inspects are set.
func pmtPacket() []byte {
	return pkt(map[int]byte{
		1:  0x41, // PUSI + PID hi → 0x100
		2:  0x00, // PID lo
		3:  0x10, // payload only
		4:  0x00, // pointer_field = 0 → section at offset 5
		15: 0x00, // program_info_length hi (p+10)
		16: 0x00, // program_info_length lo (p+11) → 0
		17: 0x1b, // ES loop @ es=17: stream_type = H.264 (video)
		18: 0xe1, // ES PID hi (masked &0x1f → 0x01)
		19: 0x01, // ES PID lo → video PID 0x101
		20: 0xf0, // ES_info_length hi (masked &0x0f → 0)
		21: 0x00, // ES_info_length lo → 0
	})
}

// keyPTS builds a video keyframe on PID 0x101 (the PMT's video ES) carrying both
// a PCR (RAI + PCR_flag, so the ring ages by duration) and a PES PTS at t90
// (90 kHz), which is the HLS segment clock. Real video keyframes carry both.
func keyPTS(t90 int64) []byte {
	return pkt(map[int]byte{
		1:  0x41, // PUSI + PID hi
		2:  0x01, // PID lo → 0x101 (matches pmtPacket's video ES)
		3:  0x30, // AFC = 3 (adaptation + payload)
		4:  0x07, // adaptation_field_length (flags + 6 PCR bytes)
		5:  0x50, // random_access_indicator + PCR_flag
		6:  byte(t90 >> 25),
		7:  byte(t90 >> 17),
		8:  byte(t90 >> 9),
		9:  byte(t90 >> 1),
		10: byte((t90 & 1) << 7),
		// PES payload starts at 5 + adaptation_field_length = 12.
		12: 0x00, 13: 0x00, 14: 0x01, // PES start code
		15: 0xe0, // stream_id (video)
		16: 0x00, 17: 0x00, // PES packet length (0 = unbounded)
		18: 0x80, // '10' marker
		19: 0x80, // PTS_DTS_flags = PTS only
		20: 0x05, // PES_header_data_length
		// PTS at payloadOffset + 9 = 21.
		21: byte(((t90 >> 30) & 0x07) << 1),
		22: byte(t90 >> 22),
		23: byte(((t90 >> 15) & 0x7f) << 1),
		24: byte(t90 >> 7),
		25: byte((t90 & 0x7f) << 1),
	})
}

// feedHLS primes PAT/PMT then feeds `n` keyframe GOPs `stepSec` apart, and
// returns the State configured for HLS with the given target/window.
func feedHLS(t *testing.T, targetMS int64, window, n, stepSec int) *State {
	t.Helper()
	const tick = 90000 // 1s in 90 kHz ticks
	s := New(1<<20, 0)
	s.Configure(0, targetMS, window)
	s.Update(patPacket(0x100))
	s.Update(pmtPacket())
	for i := 0; i < n; i++ {
		s.Update(keyPTS(int64(i) * int64(stepSec) * tick))
	}
	return s
}

func TestHLSSegmentsCutFromRing(t *testing.T) {
	// 2 s target, window 3, keyframes 2 s apart: each new keyframe closes the
	// previous 2 s segment. 6 keyframes → segs seq0..seq4, oldest aged out of the
	// 4-segment ring → playlist window shows the last 3.
	s := feedHLS(t, 2000, 3, 6, 2)

	pl := s.HLSPlaylist()
	if pl == "" {
		t.Fatal("expected a non-empty playlist")
	}
	if n := strings.Count(pl, "#EXTINF:"); n != 3 {
		t.Fatalf("want 3 segments in the window, got %d\n%s", n, pl)
	}
	if !strings.Contains(pl, "#EXTINF:2.000,") {
		t.Fatalf("want 2.000 s segments:\n%s", pl)
	}
	if !strings.Contains(pl, "#EXT-X-MEDIA-SEQUENCE:2\n") {
		t.Fatalf("want media-sequence 2 (oldest two aged/rotated out):\n%s", pl)
	}
	if !strings.Contains(pl, "#EXT-X-TARGETDURATION:2\n") {
		t.Fatalf("want target-duration 2:\n%s", pl)
	}

	// A segment still in the playlist is assemblable from the ring: PAT + PMT +
	// its GOP, packet-aligned, starting with the PAT.
	seg := s.HLSSegment(4) // newest closed segment
	if seg == nil {
		t.Fatal("HLSSegment(4) should assemble from the ring")
	}
	if len(seg)%PacketSize != 0 {
		t.Fatalf("segment not packet-aligned: %d bytes", len(seg))
	}
	if !bytes.Equal(seg[:PacketSize], patPacket(0x100)) {
		t.Fatal("segment must start with the latest PAT")
	}
	if !bytes.Equal(seg[PacketSize:2*PacketSize], pmtPacket()) {
		t.Fatal("segment must carry PAT then PMT")
	}

	// A segment that has aged out of the ring is gone (nil), never a torn read.
	if s.HLSSegment(0) != nil {
		t.Fatal("aged-out segment 0 should be nil")
	}
	// An unknown seq is nil.
	if s.HLSSegment(999) != nil {
		t.Fatal("unknown segment should be nil")
	}
}

func TestHLSDisabledYieldsNothing(t *testing.T) {
	// targetMS 0 = HLS view off: keyframes still ring for TS, but no segments.
	s := feedHLS(t, 0, 3, 6, 2)
	if pl := s.HLSPlaylist(); pl != "" {
		t.Fatalf("HLS disabled must yield no playlist, got:\n%s", pl)
	}
	if s.HLSSegment(0) != nil {
		t.Fatal("HLS disabled must yield no segments")
	}
}

func TestHLSGrowsWindowAsKeyframesArrive(t *testing.T) {
	// Fewer keyframes than the window: playlist grows, no aging yet.
	s := feedHLS(t, 2000, 10, 4, 2) // kf0..kf3 → seg0,seg1,seg2 closed; kf3 open
	pl := s.HLSPlaylist()
	if n := strings.Count(pl, "#EXTINF:"); n != 3 {
		t.Fatalf("want 3 closed segments, got %d\n%s", n, pl)
	}
	if !strings.Contains(pl, "#EXT-X-MEDIA-SEQUENCE:0\n") {
		t.Fatalf("want media-sequence 0 (nothing aged out):\n%s", pl)
	}
}

// TestHLSConfigureResizesRing verifies the ring is sized to cover the HLS window
// even when the TS prebuffer is 0 (so HLS has history to cut from).
func TestHLSConfigureResizesRing(t *testing.T) {
	s := New(1<<20, 0)
	s.Configure(0, 4000, 3) // window 3 × 4 s → ring must hold ≥ ~16 s, not 0
	if s.ring90 <= 0 {
		t.Fatalf("ring must be sized to the HLS window, ring90=%d", s.ring90)
	}
	// Lowering the window shrinks the ring again.
	prev := s.ring90
	s.Configure(0, 2000, 1)
	if s.ring90 >= prev {
		t.Fatalf("ring should shrink when the HLS window shrinks: %d -> %d", prev, s.ring90)
	}
}
