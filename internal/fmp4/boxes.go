// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package fmp4 reads fragmented MP4 (CMAF) segments — the other way HLS ships —
// and hands their samples to a caller that turns them back into MPEG-TS.
//
// It exists for the same reason internal/tsmux does: everything downstream of
// the puller speaks MPEG-TS, so an upstream serving .m4s cost a permanent
// ffmpeg child per channel. Unlike packed audio, fMP4 is not just a framing
// difference — the samples are stored, not streamed, and their timing lives in
// tables rather than in the bitstream — so this package parses the box tree
// that holds them.
//
// What it reads is the CMAF subset an HLS packager writes: one init segment
// (ftyp + moov) naming the tracks, then media segments of moof + mdat. It does
// not attempt general MP4: no edit lists, no seeking, no progressive files.
// Anything it cannot read is refused, and the caller falls back to ffmpeg,
// exactly as before.
package fmp4

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrFormat reports a box tree this package cannot read. It is a format
// problem, not a transport one: the next fetch of the same source will be the
// same, so the caller moves the stream to ffmpeg rather than retrying.
var ErrFormat = errors.New("fmp4: unreadable")

// maxBoxSize bounds what a single box may claim. A segment is megabytes; a
// header claiming gigabytes is a corrupt or hostile body, and the point of the
// bound is that the claim is read from the wire before any of it is trusted.
const maxBoxSize = 256 << 20

// box is one MP4 box: its four-character type and its payload, which for a
// container box is the concatenation of its children.
type box struct {
	typ     string
	payload []byte
}

// walk calls fn for each box at this level. It stops at the first malformed
// header rather than resynchronising: a box tree whose sizes do not add up is
// not something to guess at.
func walk(b []byte, fn func(box) error) error {
	for off := 0; off < len(b); {
		if len(b)-off < 8 {
			return fmt.Errorf("%w: %d trailing bytes, short of a box header", ErrFormat, len(b)-off)
		}
		size := int64(binary.BigEndian.Uint32(b[off:]))
		typ := string(b[off+4 : off+8])
		hdr := int64(8)
		switch {
		case size == 1:
			// 64-bit size, in the eight bytes after the type.
			if len(b)-off < 16 {
				return fmt.Errorf("%w: %s claims a 64-bit size it has no room for", ErrFormat, typ)
			}
			size = int64(binary.BigEndian.Uint64(b[off+8:]))
			hdr = 16
		case size == 0:
			// "to the end of the file", which for a segment is the rest of it.
			size = int64(len(b) - off)
		}
		if size < hdr || size > maxBoxSize || int64(off)+size > int64(len(b)) {
			return fmt.Errorf("%w: box %s claims %d bytes, %d remain", ErrFormat, typ, size, int64(len(b))-int64(off))
		}
		if err := fn(box{typ: typ, payload: b[int64(off)+hdr : int64(off)+size]}); err != nil {
			return err
		}
		off += int(size)
	}
	return nil
}

// find returns the first child box of the given type, or nil.
func find(b []byte, typ string) []byte {
	var out []byte
	_ = walk(b, func(c box) error {
		if out == nil && c.typ == typ {
			out = c.payload
		}
		return nil
	})
	return out
}

// findAll returns every child box of the given type.
func findAll(b []byte, typ string) [][]byte {
	var out [][]byte
	_ = walk(b, func(c box) error {
		if c.typ == typ {
			out = append(out, c.payload)
		}
		return nil
	})
	return out
}

// fullBox splits a version-and-flags header off a box payload.
func fullBox(b []byte) (version byte, flags uint32, rest []byte, ok bool) {
	if len(b) < 4 {
		return 0, 0, nil, false
	}
	return b[0], uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), b[4:], true
}

// be32 and be64 read the fixed-width fields the box formats are made of, after
// the caller has checked there are enough bytes.
func be32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }
func be64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
