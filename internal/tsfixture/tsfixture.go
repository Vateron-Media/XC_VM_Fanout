// Package tsfixture builds synthetic, spec-shaped MPEG-TS packets so the parsing
// and segmentation logic can be unit-tested deterministically, without ffmpeg or
// captured media.
package tsfixture

// P is the MPEG-TS packet size.
const P = 188

// pkt returns a 188-byte packet with sync byte, PID, PUSI and adaptation-field
// control set. afc: 1=payload only, 2=adaptation only, 3=adaptation+payload.
func pkt(pid int, pusi bool, afc byte) []byte {
	p := make([]byte, P)
	p[0] = 0x47
	p[1] = byte((pid >> 8) & 0x1f)
	if pusi {
		p[1] |= 0x40
	}
	p[2] = byte(pid & 0xff)
	p[3] = (afc & 0x3) << 4
	return p
}

// PAT builds a Program Association Table announcing one program on pmtPID.
func PAT(pmtPID int) []byte {
	p := pkt(0, true, 1)
	p[4] = 0x00 // pointer_field
	p[5] = 0x00 // table_id (PAT)
	p[6] = 0xb0
	p[7] = 0x0d
	p[8], p[9] = 0x00, 0x01 // transport_stream_id
	p[10] = 0xc1
	p[13], p[14] = 0x00, 0x01 // program_number 1
	p[15] = byte(0xe0 | (pmtPID>>8)&0x1f)
	p[16] = byte(pmtPID & 0xff)
	return p
}

// PMT builds a Program Map Table on pmtPID announcing an H.264 video ES on videoPID.
func PMT(pmtPID, videoPID int) []byte {
	p := pkt(pmtPID, true, 1)
	p[4] = 0x00 // pointer_field
	p[5] = 0x02 // table_id (PMT)
	p[6], p[7] = 0xb0, 0x17
	p[15], p[16] = 0x00, 0x00 // program_info_length = 0
	p[17] = 0x1b              // stream_type H.264
	p[18] = byte(0xe0 | (videoPID>>8)&0x1f)
	p[19] = byte(videoPID & 0xff)
	p[20], p[21] = 0x00, 0x00 // ES_info_length = 0
	return p
}

// Keyframe builds a video packet marked as a random-access point, carrying a PES
// header with the given 33-bit PTS (90 kHz units).
func Keyframe(videoPID int, pts int64) []byte {
	p := pkt(videoPID, true, 3)
	p[4] = 0x01 // adaptation_field_length = 1
	p[5] = 0x40 // random_access_indicator
	const ps = 6
	p[ps], p[ps+1], p[ps+2] = 0x00, 0x00, 0x01 // PES start code
	p[ps+3] = 0xE0                              // video stream_id
	p[ps+6] = 0x80                              // marker bits
	p[ps+7] = 0x80                              // PTS_DTS_flags = PTS only
	p[ps+8] = 0x05                              // PES_header_data_length
	e := EncodePTS(pts)
	copy(p[ps+9:ps+14], e[:])
	return p
}

// Fill builds a non-keyframe video payload packet.
func Fill(videoPID int) []byte { return pkt(videoPID, false, 1) }

// EncodePTS is the inverse of the daemon's PTS decoder.
func EncodePTS(pts int64) [5]byte {
	var b [5]byte
	b[0] = 0x01 | byte((pts>>30)&0x07)<<1
	b[1] = byte(pts >> 22)
	b[2] = 0x01 | byte((pts>>15)&0x7f)<<1
	b[3] = byte(pts >> 7)
	b[4] = 0x01 | byte(pts&0x7f)<<1
	return b
}

// Concat joins packets into one buffer.
func Concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
