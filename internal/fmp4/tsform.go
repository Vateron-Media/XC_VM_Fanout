// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package fmp4

import "fmt"

// The two shapes a sample has to change into on its way to MPEG-TS.
//
// fMP4 stores what a TS streams. Video samples are length-prefixed NAL units
// with the parameter sets kept once in the init segment; a TS carries start
// codes and repeats the parameter sets in front of every keyframe, because a
// viewer may join at any of them. Audio samples are bare AAC; a TS carries ADTS,
// whose header repeats the sample rate and channel count for the same reason.

// startCode is the Annex-B delimiter. The four-byte form is used throughout:
// the three-byte form is legal but only saves a byte, and the four-byte one is
// what every encoder writes in front of a parameter set.
var startCode = []byte{0x00, 0x00, 0x00, 0x01}

// AnnexB converts one video sample from AVCC to Annex-B, appending to dst.
//
// On a random-access point the track's SPS and PPS go in front: fMP4 states
// them once in the init segment, which a viewer joining a live TS never saw.
// Without them a decoder has a keyframe it cannot configure itself from, which
// is a channel that stays black until the next time the upstream happens to
// send parameter sets — in fMP4, never.
func AnnexB(dst []byte, s Sample, t *Track) ([]byte, error) {
	if t.NALULengthSize < 1 || t.NALULengthSize > 4 {
		return dst, fmt.Errorf("%w: NAL length size %d", ErrFormat, t.NALULengthSize)
	}
	if s.Sync {
		for _, sps := range t.SPS {
			dst = append(append(dst, startCode...), sps...)
		}
		for _, pps := range t.PPS {
			dst = append(append(dst, startCode...), pps...)
		}
	}
	for off := 0; off < len(s.Data); {
		if off+t.NALULengthSize > len(s.Data) {
			return dst, fmt.Errorf("%w: sample ends inside a NAL length prefix", ErrFormat)
		}
		n := 0
		for i := 0; i < t.NALULengthSize; i++ {
			n = n<<8 | int(s.Data[off+i])
		}
		off += t.NALULengthSize
		if n < 0 || off+n > len(s.Data) {
			return dst, fmt.Errorf("%w: NAL of %d bytes runs past the sample", ErrFormat, n)
		}
		dst = append(append(dst, startCode...), s.Data[off:off+n]...)
		off += n
	}
	return dst, nil
}

// adtsHeaderLen is the ADTS header this package writes: no CRC.
const adtsHeaderLen = 7

// ADTS wraps one raw AAC frame in the header a TS demuxer expects, appending to
// dst. The fields come from the AudioSpecificConfig the init segment carried.
func ADTS(dst []byte, frame []byte, t *Track) ([]byte, error) {
	total := adtsHeaderLen + len(frame)
	if total > 0x1FFF {
		// aac_frame_length is 13 bits. A frame this large is not one.
		return dst, fmt.Errorf("%w: AAC frame of %d bytes cannot be written as ADTS", ErrFormat, len(frame))
	}
	dst = append(dst,
		0xFF,
		0xF1, // MPEG-4, layer 00, no CRC
		(t.AACProfile<<6)|(t.SampleRateIdx<<2)|((t.Channels>>2)&0x01),
		byte((t.Channels&0x03)<<6)|byte(total>>11),
		byte(total>>3),
		byte((total&0x07)<<5)|0x1F,
		0xFC,
	)
	return append(dst, frame...), nil
}

// Scale converts a timestamp from a track's own timescale to the 90 kHz clock
// MPEG-TS runs on, which is the only clock anything downstream of the puller
// understands.
func Scale(t int64, timescale uint32) int64 {
	if timescale == 0 {
		return 0
	}
	// Multiply first, in 64 bits: a track at 48 kHz over a day is ~4e9 ticks, so
	// the product stays far inside the range while the division stays exact for
	// the usual timescales.
	return t * 90000 / int64(timescale)
}
