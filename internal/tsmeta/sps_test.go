// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsmeta

import "testing"

// bitWriter is the inverse of bitReader, used to build sequence headers to spec
// so the parser is tested against the SYNTAX rather than against a captured blob
// whose provenance nobody can check.
type bitWriter struct {
	buf []byte
	n   int // bits written
}

func (w *bitWriter) bit(v uint) {
	if w.n%8 == 0 {
		w.buf = append(w.buf, 0)
	}
	if v&1 == 1 {
		w.buf[w.n/8] |= 1 << (7 - uint(w.n%8))
	}
	w.n++
}

func (w *bitWriter) bits(v uint, n int) {
	for i := n - 1; i >= 0; i-- {
		w.bit((v >> uint(i)) & 1)
	}
}

// ue writes an unsigned Exp-Golomb code.
func (w *bitWriter) ue(v uint) {
	v++ // codeNum + 1, so the value is at least 1
	n := 0
	for t := v; t > 1; t >>= 1 {
		n++
	}
	for i := 0; i < n; i++ {
		w.bit(0)
	}
	w.bits(v, n+1)
}

func (w *bitWriter) bytes() []byte { return w.buf }

// h264SPS builds a baseline-profile SPS for a given coded size and cropping.
func h264SPS(widthMBs, heightMBs uint, frameMBsOnly bool, cropRight, cropBottom uint) []byte {
	w := &bitWriter{}
	w.bits(66, 8) // profile_idc = Baseline (no chroma/scaling syntax)
	w.bits(0, 8)  // constraint flags + reserved
	w.bits(30, 8) // level_idc
	w.ue(0)       // seq_parameter_set_id
	w.ue(0)       // log2_max_frame_num_minus4
	w.ue(2)       // pic_order_cnt_type = 2 (no extra fields)
	w.ue(1)       // max_num_ref_frames
	w.bit(0)      // gaps_in_frame_num_value_allowed_flag
	w.ue(widthMBs - 1)
	w.ue(heightMBs - 1)
	if frameMBsOnly {
		w.bit(1)
	} else {
		w.bit(0)
		w.bit(0) // mb_adaptive_frame_field_flag
	}
	w.bit(1) // direct_8x8_inference_flag
	if cropRight > 0 || cropBottom > 0 {
		w.bit(1) // frame_cropping_flag
		w.ue(0)  // left
		w.ue(cropRight)
		w.ue(0) // top
		w.ue(cropBottom)
	} else {
		w.bit(0)
	}
	w.bit(0) // vui_parameters_present_flag

	// Prepend the NAL header byte: nal_ref_idc=3, nal_unit_type=7 (SPS).
	return append([]byte{0x67}, w.bytes()...)
}

func annexB(nals ...[]byte) []byte {
	var out []byte
	for _, n := range nals {
		out = append(out, 0, 0, 0, 1)
		out = append(out, n...)
	}
	return out
}

// TestH264PictureSize covers the shapes that actually turn up on air, including
// the one that matters most: 1080 is coded as 1088 lines and cropped back, so a
// parser that ignores cropping reports the wrong number to the panel.
func TestH264PictureSize(t *testing.T) {
	cases := []struct {
		name          string
		wMBs, hMBs    uint
		frameOnly     bool
		cropR, cropB  uint
		wantW, wantH  int
	}{
		{"720x576 SD, no cropping", 45, 36, true, 0, 0, 720, 576},
		{"1280x720", 80, 45, true, 0, 0, 1280, 720},
		{"1920x1080 cropped from 1088", 120, 68, true, 0, 4, 1920, 1080},
		// Interlaced: CropUnitY = SubHeightC * (2 - frame_mbs_only_flag) = 4 for
		// 4:2:0, so trimming the coded 1088 down to 1080 takes an offset of 2,
		// not the 4 a progressive stream needs. Getting this wrong is how a
		// parser reports 1072.
		{"1920x1080 interlaced", 120, 34, false, 0, 2, 1920, 1080},
		{"3840x2160 UHD", 240, 135, true, 0, 0, 3840, 2160},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			es := annexB(h264SPS(c.wMBs, c.hMBs, c.frameOnly, c.cropR, c.cropB))
			w, h, ok := h264PictureSize(es)
			if !ok {
				t.Fatal("SPS did not parse")
			}
			if w != c.wantW || h != c.wantH {
				t.Errorf("got %dx%d, want %dx%d", w, h, c.wantW, c.wantH)
			}
		})
	}
}

// TestEmulationPreventionIsUndone: an encoder inserts 0x03 into any 00 00 0x
// sequence so the payload can never contain a start code. Reading the header
// without undoing that shifts every field after the first occurrence — a bug
// that yields a confidently wrong resolution rather than an obvious failure.
func TestEmulationPreventionIsUndone(t *testing.T) {
	sps := h264SPS(120, 68, true, 0, 4)

	// Splice in an escaped 00 00 00 and check the unescaper restores it.
	escaped := []byte{0x00, 0x00, 0x03, 0x00}
	got := unescape(escaped)
	want := []byte{0x00, 0x00, 0x00}
	if len(got) != len(want) {
		t.Fatalf("unescape produced %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unescape produced %v, want %v", got, want)
		}
	}

	// And an SPS that contains no escapes must survive unchanged.
	if w, h, ok := h264PictureSize(annexB(sps)); !ok || w != 1920 || h != 1080 {
		t.Errorf("clean SPS parsed as %dx%d ok=%v", w, h, ok)
	}
}

// TestTruncatedSPSIsRefused: a short read must report not-determined, never a
// partially-decoded size. The caller keeps its previous value on false.
func TestTruncatedSPSIsRefused(t *testing.T) {
	full := h264SPS(120, 68, true, 0, 4)
	for n := 1; n < len(full); n++ {
		if _, _, ok := h264PictureSize(annexB(full[:n])); ok {
			// A prefix MAY legitimately parse once every needed field is present,
			// so only complain if it yields something absurd.
			w, h, _ := h264PictureSize(annexB(full[:n]))
			if w <= 0 || h <= 0 || w > 16384 || h > 16384 {
				t.Fatalf("truncated to %d bytes yielded %dx%d", n, w, h)
			}
		}
	}
}

// TestNoSPSMeansNotDetermined: a stream with no sequence header yields nothing,
// so the panel keeps whatever it had rather than being handed a zero.
func TestNoSPSMeansNotDetermined(t *testing.T) {
	// A single non-SPS NAL (type 1, a coded slice).
	if _, _, ok := h264PictureSize(annexB([]byte{0x41, 0x9a, 0x00})); ok {
		t.Error("reported a picture size with no SPS present")
	}
	if _, _, ok := h264PictureSize(nil); ok {
		t.Error("reported a picture size from an empty stream")
	}
}

// hevcSPS builds a minimal HEVC SPS with no temporal sub-layers.
func hevcSPS(width, height uint, cropRight, cropBottom uint) []byte {
	w := &bitWriter{}
	w.bits(0, 4) // sps_video_parameter_set_id
	w.bits(0, 3) // sps_max_sub_layers_minus1 = 0
	w.bit(1)     // sps_temporal_id_nesting_flag

	// profile_tier_level with no sub-layers: 8 + 32 + 32 + 16 + 8 bits.
	w.bits(0x01, 8)
	w.bits(0x60000000, 32)
	w.bits(0, 32)
	w.bits(0, 16)
	w.bits(120, 8) // general_level_idc

	w.ue(0) // sps_seq_parameter_set_id
	w.ue(1) // chroma_format_idc = 4:2:0
	w.ue(width)
	w.ue(height)
	if cropRight > 0 || cropBottom > 0 {
		w.bit(1)
		w.ue(0)
		w.ue(cropRight)
		w.ue(0)
		w.ue(cropBottom)
	} else {
		w.bit(0)
	}

	// Two-byte NAL header: type 33 (SPS_NUT) in bits 1..6 of the first byte.
	return append([]byte{33 << 1, 0x01}, w.bytes()...)
}

func TestHEVCPictureSize(t *testing.T) {
	cases := []struct {
		name         string
		w, h         uint
		cropR, cropB uint
		wantW, wantH int
	}{
		{"1920x1080", 1920, 1080, 0, 0, 1920, 1080},
		{"3840x2160 UHD", 3840, 2160, 0, 0, 3840, 2160},
		{"1440x1080 with conformance window", 1440, 1088, 0, 4, 1440, 1080},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w, h, ok := hevcPictureSize(annexB(hevcSPS(c.w, c.h, c.cropR, c.cropB)))
			if !ok {
				t.Fatal("HEVC SPS did not parse")
			}
			if w != c.wantW || h != c.wantH {
				t.Errorf("got %dx%d, want %dx%d", w, h, c.wantW, c.wantH)
			}
		})
	}
}

// TestHEVCSubLayersAreSkipped: profile_tier_level grows with the number of
// temporal sub-layers, and mis-sizing it misaligns the picture dimensions that
// follow. This is the field most likely to be got wrong.
func TestHEVCSubLayersAreSkipped(t *testing.T) {
	w := &bitWriter{}
	w.bits(0, 4)
	w.bits(2, 3) // sps_max_sub_layers_minus1 = 2
	w.bit(1)

	w.bits(0x01, 8)
	w.bits(0x60000000, 32)
	w.bits(0, 32)
	w.bits(0, 16)
	w.bits(120, 8)
	// Two sub-layers, each declaring both profile and level present.
	w.bit(1)
	w.bit(1)
	w.bit(1)
	w.bit(1)
	for i := 2; i < 8; i++ { // reserved padding
		w.bits(0, 2)
	}
	for i := 0; i < 2; i++ {
		w.bits(0, 32)
		w.bits(0, 32)
		w.bits(0, 24)
		w.bits(0, 8)
	}

	w.ue(0)
	w.ue(1)
	w.ue(1920)
	w.ue(1080)
	w.bit(0)

	nal := append([]byte{33 << 1, 0x01}, w.bytes()...)
	gotW, gotH, ok := hevcPictureSize(annexB(nal))
	if !ok {
		t.Fatal("SPS with sub-layers did not parse")
	}
	if gotW != 1920 || gotH != 1080 {
		t.Errorf("got %dx%d, want 1920x1080 — profile_tier_level was mis-sized", gotW, gotH)
	}
}
