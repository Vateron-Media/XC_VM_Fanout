// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"bytes"
	"strconv"
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
		15: 0xe0,           // stream_id (video)
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

// feedHLS primes PAT/PMT then feeds `n` keyframe GOPs `stepSec` apart into a
// State whose buffer (ring) is `bufSec` seconds. In the unified model the ring
// size IS the buffer; HLS is cut from whatever it holds (hls_window is a display
// cap). targetMS 0 disables the HLS view.
func feedHLS(t *testing.T, bufSec int, targetMS int64, window, n, stepSec int) *State {
	t.Helper()
	const tick = 90000 // 1s in 90 kHz ticks
	s := New(1<<20, int64(bufSec)*1000)
	s.Configure(int64(bufSec)*1000, targetMS, window)
	s.Update(patPacket(0x100))
	s.Update(pmtPacket())
	for i := 0; i < n; i++ {
		s.Update(keyPTS(int64(i) * int64(stepSec) * tick))
	}
	return s
}

// lastSeq returns the sequence number of the last "<seq>.ts" line in a playlist.
func lastSeq(t *testing.T, pl string) int {
	t.Helper()
	seq, found := 0, false
	for _, line := range strings.Split(pl, "\n") {
		if strings.HasSuffix(line, ".ts") {
			if n, err := strconv.Atoi(strings.TrimSuffix(line, ".ts")); err == nil {
				seq, found = n, true
			}
		}
	}
	if !found {
		t.Fatalf("no segment uri in playlist:\n%s", pl)
	}
	return seq
}

func TestHLSCutsAndServes(t *testing.T) {
	// 30 s buffer holds everything; 8 keyframes 2 s apart → 7 closed segments;
	// window 3 → the last 3 are listed (the single oldest in-ring segment is held
	// back as a fetch margin).
	s := feedHLS(t, 30, 2000, 3, 8, 2)

	pl := s.HLSPlaylist()
	if pl == "" {
		t.Fatal("expected a non-empty playlist")
	}
	if n := strings.Count(pl, "#EXTINF:"); n != 3 {
		t.Fatalf("want 3 listed segments, got %d\n%s", n, pl)
	}
	if !strings.Contains(pl, "#EXT-X-TARGETDURATION:2\n") {
		t.Fatalf("want target-duration 2:\n%s", pl)
	}

	// The newest listed segment assembles from the ring: PAT + PMT + its GOPs,
	// packet-aligned, starting with the PAT.
	seq := lastSeq(t, pl)
	seg := s.HLSSegment(seq)
	if seg == nil {
		t.Fatalf("HLSSegment(%d) should assemble from the ring", seq)
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
	if s.HLSSegment(999) != nil {
		t.Fatal("unknown segment should be nil")
	}
}

func TestHLSAgesOutOnSmallRing(t *testing.T) {
	// Small (6 s) buffer: the oldest segments age out of the ring, but HLS still
	// opens with a short playlist and an aged-out segment reads back nil.
	s := feedHLS(t, 6, 2000, 10, 8, 2)
	if pl := s.HLSPlaylist(); pl == "" {
		t.Fatal("a small ring must still serve a (short) playlist")
	}
	if s.HLSSegment(0) != nil {
		t.Fatal("an aged-out segment must be nil, never a torn read")
	}
	if s.HLSSegment(999) != nil {
		t.Fatal("unknown segment should be nil")
	}
}

func TestHLSDisabledYieldsNothing(t *testing.T) {
	// targetMS 0 = HLS view off: keyframes still ring for TS, but no segments.
	s := feedHLS(t, 6, 0, 3, 6, 2)
	if pl := s.HLSPlaylist(); pl != "" {
		t.Fatalf("HLS disabled must yield no playlist, got:\n%s", pl)
	}
	if s.HLSSegment(0) != nil {
		t.Fatal("HLS disabled must yield no segments")
	}
}

func TestHLSGrowsAsKeyframesArrive(t *testing.T) {
	// Few keyframes, big window: 4 keyframes → 3 closed → 2 listed (oldest reserved).
	s := feedHLS(t, 30, 2000, 10, 4, 2)
	if n := strings.Count(s.HLSPlaylist(), "#EXTINF:"); n != 2 {
		t.Fatalf("want 2 listed segments, got %d\n%s", n, s.HLSPlaylist())
	}
}

// TestGateShrinksRingButKeepsHLS verifies the unified model: gating collapses the
// ring to the idle fraction (freeing memory) yet HLS stays openable, and ungating
// restores the full buffer.
func TestGateShrinksRingButKeepsHLS(t *testing.T) {
	s := feedHLS(t, 30, 2000, 6, 12, 2)
	s.SetIdleRatio(0.5)
	full := s.ring90
	fullSegs := strings.Count(s.HLSPlaylist(), "#EXTINF:")
	if fullSegs == 0 {
		t.Fatal("watched playlist should be non-empty")
	}

	s.SetGated(true)
	if s.ring90 >= full {
		t.Fatalf("gated ring should shrink: %d -> %d", full, s.ring90)
	}
	// Feed more so the shrunk ring settles; HLS must still open.
	for i := 12; i < 20; i++ {
		s.Update(keyPTS(int64(i) * 2 * 90000))
	}
	pl := s.HLSPlaylist()
	if pl == "" {
		t.Fatal("a gated stream must still serve a playlist (HLS stays openable)")
	}
	if n := strings.Count(pl, "#EXTINF:"); n > fullSegs {
		t.Fatalf("gated playlist should not exceed the full one: %d vs %d", n, fullSegs)
	}

	s.SetGated(false)
	if s.ring90 != full {
		t.Fatalf("ungating should restore the ring: %d != %d", s.ring90, full)
	}
}
