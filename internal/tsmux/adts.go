// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package tsmux wraps packed-audio HLS segments — a bare ADTS AAC stream, with
// no transport layer at all — in MPEG-TS, so the rest of the daemon can treat
// such a source exactly like any other.
//
// It exists because the ring, the join, the HLS cutter and every viewer path
// downstream speak MPEG-TS and nothing else. An upstream serving .aac segments
// was therefore refused and handed to ffmpeg, which costs a process per radio
// channel to do what is, for this one format, a few hundred lines of framing:
// AAC is already the elementary stream, so there is nothing to decode, only to
// packetise.
//
// What it is NOT: a general muxer. It takes one audio elementary stream and
// emits one programme. Anything else — video, several tracks, a format whose
// frame boundaries are not self-describing — belongs to ffmpeg.
package tsmux

import (
	"errors"
	"fmt"
)

// ErrNotADTS reports a body that does not parse as ADTS AAC. Packed-audio
// playlists are recognised by their segment extension, so the first body is
// also the proof: a source that names .aac and serves something else is a
// source this package must refuse rather than emit noise for.
var ErrNotADTS = errors.New("tsmux: not an ADTS AAC stream")

// adtsHeaderLen is the fixed part of an ADTS header; a frame with CRC carries
// two more bytes, which sit between the header and the payload and matter only
// for the length arithmetic.
const adtsHeaderLen = 7

// sampleRates is the ADTS sampling_frequency_index table.
var sampleRates = [16]int{
	96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050,
	16000, 12000, 11025, 8000, 7350, 0, 0, 0,
}

// aacSamplesPerFrame is fixed for AAC-LC: every frame carries 1024 samples,
// which is what makes a frame's duration derivable from the sample rate alone
// and so lets this package build a clock with nothing but the bitstream.
const aacSamplesPerFrame = 1024

// frame is one ADTS access unit: where it sits in the buffer, and what the
// header says about it.
type frame struct {
	// data is the WHOLE frame including its ADTS header. A TS demuxer expects
	// ADTS in the PES payload — that is what stream_type 0x0F means — so the
	// header travels with the audio rather than being stripped.
	data       []byte
	sampleRate int
}

// duration90k is the frame's length on the 90 kHz MPEG clock.
func (f frame) duration90k() int64 {
	if f.sampleRate <= 0 {
		return 0
	}
	return int64(aacSamplesPerFrame) * 90000 / int64(f.sampleRate)
}

// skipID3 steps over the ID3v2 tag an RFC 8216 packed-audio segment carries in
// front of its frames. The tag holds the upstream's own timestamp, which this
// package deliberately ignores: the muxer keeps a continuous clock of its own,
// and adopting a per-segment timestamp would put the upstream's gaps and resets
// on the wire as jumps the ring would then have to absorb.
func skipID3(b []byte) []byte {
	for len(b) >= 10 && b[0] == 'I' && b[1] == 'D' && b[2] == '3' {
		// A syncsafe 28-bit size: seven bits per byte, high bit always clear.
		if b[6]&0x80 != 0 || b[7]&0x80 != 0 || b[8]&0x80 != 0 || b[9]&0x80 != 0 {
			return b
		}
		// Parenthesised deliberately: + and | share a precedence level in Go, so
		// without them the header length is OR-ed into the size instead of added,
		// which happens to be right only when their bits do not overlap.
		n := 10 + (int(b[6])<<21 | int(b[7])<<14 | int(b[8])<<7 | int(b[9]))
		if n <= 0 || n > len(b) {
			return b
		}
		b = b[n:]
	}
	return b
}

// parseADTS walks a packed-audio body into frames. It is strict on purpose:
// every frame must carry the sync word and a length that lands exactly on the
// next frame, so a body that is not ADTS — an error page, a TS segment, an
// origin's HTML — fails here rather than reaching a viewer as noise.
func parseADTS(b []byte) ([]frame, error) {
	b = skipID3(b)
	var out []frame
	for i := 0; i < len(b); {
		if len(b)-i < adtsHeaderLen {
			return nil, fmt.Errorf("%w: %d trailing bytes, short of a header", ErrNotADTS, len(b)-i)
		}
		h := b[i:]
		// syncword: 12 bits of 1, then layer must be 00 (MPEG-4/2 AAC).
		if h[0] != 0xFF || h[1]&0xF0 != 0xF0 || h[1]&0x06 != 0 {
			return nil, fmt.Errorf("%w: no sync at offset %d", ErrNotADTS, i)
		}
		rate := sampleRates[(h[2]>>2)&0x0F]
		if rate == 0 {
			return nil, fmt.Errorf("%w: reserved sampling frequency index at offset %d", ErrNotADTS, i)
		}
		// aac_frame_length spans the header, the optional CRC and the payload.
		n := int(h[3]&0x03)<<11 | int(h[4])<<3 | int(h[5])>>5
		if n < adtsHeaderLen || i+n > len(b) {
			return nil, fmt.Errorf("%w: frame length %d at offset %d does not fit the body (%d bytes)",
				ErrNotADTS, n, i, len(b))
		}
		out = append(out, frame{data: b[i : i+n], sampleRate: rate})
		i += n
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: no frames", ErrNotADTS)
	}
	return out, nil
}

// LooksLikeADTS reports whether b opens with something this package could
// parse. Used to tell a genuine packed-audio segment from whatever else an
// origin answered with, before any of it is muxed.
func LooksLikeADTS(b []byte) bool {
	b = skipID3(b)
	if len(b) < adtsHeaderLen {
		return false
	}
	return b[0] == 0xFF && b[1]&0xF0 == 0xF0 && b[1]&0x06 == 0 && sampleRates[(b[2]>>2)&0x0F] != 0
}
