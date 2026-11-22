// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsmeta

// Sequence-header parsing: the picture size a stream actually carries.
//
// The panel got this from ffprobe. It is a few dozen bytes at the front of the
// video elementary stream, so reading it here removes the last reason to run a
// subprocess against a segment on disk just to fill in a database column.
//
// Everything below returns ok=false rather than a best guess when the bytes do
// not parse. A wrong resolution in the panel is worse than a missing one: the
// missing one leaves the previous value alone.

// bitReader reads the bit-oriented syntax these headers are written in.
type bitReader struct {
	buf []byte
	pos int // bit position
}

// bit reads one bit; past the end it reports 0 and not-ok, and every composite
// read propagates that, so a truncated header can never be read as valid.
func (r *bitReader) bit() (uint, bool) {
	if r.pos >= len(r.buf)*8 {
		return 0, false
	}
	b := (r.buf[r.pos/8] >> (7 - uint(r.pos%8))) & 1
	r.pos++
	return uint(b), true
}

// bits reads n bits, most significant first.
func (r *bitReader) bits(n int) (uint, bool) {
	var v uint
	for i := 0; i < n; i++ {
		b, ok := r.bit()
		if !ok {
			return 0, false
		}
		v = v<<1 | b
	}
	return v, true
}

// ue reads an unsigned Exp-Golomb code, the variable-length integer these
// headers are full of: a run of N zeros, a one, then N more bits.
func (r *bitReader) ue() (uint, bool) {
	zeros := 0
	for {
		b, ok := r.bit()
		if !ok {
			return 0, false
		}
		if b == 1 {
			break
		}
		zeros++
		if zeros > 32 {
			return 0, false // malformed; a real code is never this long
		}
	}
	if zeros == 0 {
		return 0, true
	}
	rest, ok := r.bits(zeros)
	if !ok {
		return 0, false
	}
	return (1 << uint(zeros)) - 1 + rest, true
}

// se reads a signed Exp-Golomb code.
func (r *bitReader) se() (int, bool) {
	v, ok := r.ue()
	if !ok {
		return 0, false
	}
	if v%2 == 0 {
		return -int(v / 2), true
	}
	return int((v + 1) / 2), true
}

// nalUnits splits an Annex-B byte stream into NAL units, with the start codes
// removed and emulation-prevention bytes stripped.
//
// Emulation prevention is the encoder inserting a 0x03 into any 00 00 00/01/02/03
// so the payload can never contain a start code. Reading the header without
// undoing that silently shifts every field after the first occurrence, which is
// exactly the kind of bug that yields a confidently wrong resolution.
func nalUnits(es []byte) [][]byte {
	var out [][]byte
	i := 0
	for i+3 <= len(es) {
		// Find a start code: 00 00 01, optionally preceded by another 00.
		if es[i] == 0 && es[i+1] == 0 && es[i+2] == 1 {
			start := i + 3
			j := start
			for j+3 <= len(es) && !(es[j] == 0 && es[j+1] == 0 && es[j+2] == 1) {
				j++
			}
			end := j
			if j+3 > len(es) {
				end = len(es)
			}
			// Trim a trailing zero that belongs to the next start code.
			for end > start && es[end-1] == 0 {
				end--
			}
			if end > start {
				out = append(out, unescape(es[start:end]))
			}
			i = end
			continue
		}
		i++
	}
	return out
}

// unescape removes emulation-prevention bytes.
func unescape(b []byte) []byte {
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if i+2 < len(b) && b[i] == 0 && b[i+1] == 0 && b[i+2] == 3 {
			out = append(out, 0, 0)
			i += 2
			continue
		}
		out = append(out, b[i])
	}
	return out
}

// h264PictureSize reads width and height from an H.264 sequence parameter set.
func h264PictureSize(es []byte) (int, int, bool) {
	for _, nal := range nalUnits(es) {
		if len(nal) < 4 || nal[0]&0x1f != 7 { // nal_unit_type 7 = SPS
			continue
		}
		if w, h, ok := parseH264SPS(nal[1:]); ok {
			return w, h, true
		}
	}
	return 0, 0, false
}

func parseH264SPS(b []byte) (int, int, bool) {
	r := &bitReader{buf: b}

	profile, ok := r.bits(8)
	if !ok {
		return 0, 0, false
	}
	if _, ok := r.bits(8); !ok { // constraint flags + reserved
		return 0, 0, false
	}
	if _, ok := r.bits(8); !ok { // level_idc
		return 0, 0, false
	}
	if _, ok := r.ue(); !ok { // seq_parameter_set_id
		return 0, 0, false
	}

	chromaFormat := uint(1)
	// The high profiles carry extra chroma and scaling syntax before the picture
	// size. Skipping it on a High-profile stream (which is most H.264 broadcast)
	// would read the size out of the wrong bits.
	switch profile {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135:
		if chromaFormat, ok = r.ue(); !ok {
			return 0, 0, false
		}
		if chromaFormat == 3 {
			if _, ok := r.bit(); !ok { // separate_colour_plane_flag
				return 0, 0, false
			}
		}
		if _, ok := r.ue(); !ok { // bit_depth_luma_minus8
			return 0, 0, false
		}
		if _, ok := r.ue(); !ok { // bit_depth_chroma_minus8
			return 0, 0, false
		}
		if _, ok := r.bit(); !ok { // qpprime_y_zero_transform_bypass_flag
			return 0, 0, false
		}
		scaling, ok := r.bit()
		if !ok {
			return 0, 0, false
		}
		if scaling == 1 {
			n := 8
			if chromaFormat == 3 {
				n = 12
			}
			for i := 0; i < n; i++ {
				present, ok := r.bit()
				if !ok {
					return 0, 0, false
				}
				if present == 1 {
					size := 16
					if i >= 6 {
						size = 64
					}
					if !skipScalingList(r, size) {
						return 0, 0, false
					}
				}
			}
		}
	}

	if _, ok := r.ue(); !ok { // log2_max_frame_num_minus4
		return 0, 0, false
	}
	pocType, ok := r.ue()
	if !ok {
		return 0, 0, false
	}
	switch pocType {
	case 0:
		if _, ok := r.ue(); !ok { // log2_max_pic_order_cnt_lsb_minus4
			return 0, 0, false
		}
	case 1:
		if _, ok := r.bit(); !ok { // delta_pic_order_always_zero_flag
			return 0, 0, false
		}
		if _, ok := r.se(); !ok { // offset_for_non_ref_pic
			return 0, 0, false
		}
		if _, ok := r.se(); !ok { // offset_for_top_to_bottom_field
			return 0, 0, false
		}
		n, ok := r.ue()
		if !ok || n > 256 {
			return 0, 0, false
		}
		for i := uint(0); i < n; i++ {
			if _, ok := r.se(); !ok {
				return 0, 0, false
			}
		}
	}

	if _, ok := r.ue(); !ok { // max_num_ref_frames
		return 0, 0, false
	}
	if _, ok := r.bit(); !ok { // gaps_in_frame_num_value_allowed_flag
		return 0, 0, false
	}
	widthMBs, ok := r.ue()
	if !ok {
		return 0, 0, false
	}
	heightMapUnits, ok := r.ue()
	if !ok {
		return 0, 0, false
	}
	frameMBsOnly, ok := r.bit()
	if !ok {
		return 0, 0, false
	}
	if frameMBsOnly == 0 {
		if _, ok := r.bit(); !ok { // mb_adaptive_frame_field_flag
			return 0, 0, false
		}
	}
	if _, ok := r.bit(); !ok { // direct_8x8_inference_flag
		return 0, 0, false
	}

	width := int(widthMBs+1) * 16
	// An interlaced stream codes half-height map units, hence the doubling.
	height := (2 - int(frameMBsOnly)) * int(heightMapUnits+1) * 16

	// Cropping is how 1920x1080 is carried in 1088 lines of macroblocks. Ignoring
	// it reports the coded size instead of the displayed one, which is the
	// difference between "1080" and "1088" in the panel.
	cropping, ok := r.bit()
	if !ok {
		return width, height, true // size is already valid; cropping merely refines it
	}
	if cropping == 1 {
		left, ok1 := r.ue()
		right, ok2 := r.ue()
		top, ok3 := r.ue()
		bottom, ok4 := r.ue()
		if !(ok1 && ok2 && ok3 && ok4) {
			return width, height, true
		}
		subW, subH := 2, 2
		switch chromaFormat {
		case 0: // monochrome
			subW, subH = 1, 1
		case 2: // 4:2:2
			subH = 1
		case 3: // 4:4:4
			subW, subH = 1, 1
		}
		if frameMBsOnly == 0 {
			subH *= 2
		}
		width -= int(left+right) * subW
		height -= int(top+bottom) * subH
	}
	if width <= 0 || height <= 0 {
		return 0, 0, false
	}
	return width, height, true
}

// skipScalingList walks past a scaling list without keeping it.
func skipScalingList(r *bitReader, size int) bool {
	last, next := 8, 8
	for i := 0; i < size; i++ {
		if next != 0 {
			delta, ok := r.se()
			if !ok {
				return false
			}
			next = (last + delta + 256) % 256
		}
		if next != 0 {
			last = next
		}
	}
	return true
}

// hevcPictureSize reads width and height from an HEVC sequence parameter set.
func hevcPictureSize(es []byte) (int, int, bool) {
	for _, nal := range nalUnits(es) {
		if len(nal) < 3 {
			continue
		}
		// HEVC has a two-byte NAL header; the type is bits 1..6 of the first.
		if (nal[0]>>1)&0x3f != 33 { // 33 = SPS_NUT
			continue
		}
		if w, h, ok := parseHEVCSPS(nal[2:]); ok {
			return w, h, true
		}
	}
	return 0, 0, false
}

func parseHEVCSPS(b []byte) (int, int, bool) {
	r := &bitReader{buf: b}

	if _, ok := r.bits(4); !ok { // sps_video_parameter_set_id
		return 0, 0, false
	}
	maxSubLayersMinus1, ok := r.bits(3)
	if !ok {
		return 0, 0, false
	}
	if _, ok := r.bit(); !ok { // sps_temporal_id_nesting_flag
		return 0, 0, false
	}
	if !skipProfileTierLevel(r, int(maxSubLayersMinus1)) {
		return 0, 0, false
	}
	if _, ok := r.ue(); !ok { // sps_seq_parameter_set_id
		return 0, 0, false
	}
	chromaFormat, ok := r.ue()
	if !ok {
		return 0, 0, false
	}
	if chromaFormat == 3 {
		if _, ok := r.bit(); !ok { // separate_colour_plane_flag
			return 0, 0, false
		}
	}
	width, ok := r.ue()
	if !ok {
		return 0, 0, false
	}
	height, ok := r.ue()
	if !ok {
		return 0, 0, false
	}
	w, h := int(width), int(height)

	conformance, ok := r.bit()
	if !ok {
		return w, h, w > 0 && h > 0
	}
	if conformance == 1 {
		left, ok1 := r.ue()
		right, ok2 := r.ue()
		top, ok3 := r.ue()
		bottom, ok4 := r.ue()
		if ok1 && ok2 && ok3 && ok4 {
			subW, subH := 2, 2
			switch chromaFormat {
			case 0, 3:
				subW, subH = 1, 1
			case 2:
				subH = 1
			}
			w -= int(left+right) * subW
			h -= int(top+bottom) * subH
		}
	}
	if w <= 0 || h <= 0 {
		return 0, 0, false
	}
	return w, h, true
}

// skipProfileTierLevel walks past the profile_tier_level structure, whose length
// depends on how many temporal sub-layers the stream declares. Getting its size
// wrong misaligns everything after it, including the picture size.
func skipProfileTierLevel(r *bitReader, maxSubLayersMinus1 int) bool {
	// general_profile_space(2) + general_tier_flag(1) + general_profile_idc(5),
	// then 32 compatibility flags, 48 constraint bits, and general_level_idc(8).
	if _, ok := r.bits(8); !ok {
		return false
	}
	if _, ok := r.bits(32); !ok {
		return false
	}
	if _, ok := r.bits(32); !ok {
		return false
	}
	if _, ok := r.bits(16); !ok {
		return false
	}
	if _, ok := r.bits(8); !ok {
		return false
	}

	profilePresent := make([]bool, maxSubLayersMinus1)
	levelPresent := make([]bool, maxSubLayersMinus1)
	for i := 0; i < maxSubLayersMinus1; i++ {
		p, ok1 := r.bit()
		l, ok2 := r.bit()
		if !ok1 || !ok2 {
			return false
		}
		profilePresent[i], levelPresent[i] = p == 1, l == 1
	}
	// The flags are padded out to eight sub-layers' worth of reserved bits.
	if maxSubLayersMinus1 > 0 {
		for i := maxSubLayersMinus1; i < 8; i++ {
			if _, ok := r.bits(2); !ok {
				return false
			}
		}
	}
	for i := 0; i < maxSubLayersMinus1; i++ {
		if profilePresent[i] {
			if _, ok := r.bits(32); !ok {
				return false
			}
			if _, ok := r.bits(32); !ok {
				return false
			}
			if _, ok := r.bits(24); !ok {
				return false
			}
		}
		if levelPresent[i] {
			if _, ok := r.bits(8); !ok {
				return false
			}
		}
	}
	return true
}
