// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package tspes reads the few things a byte-for-byte MPEG-TS consumer needs out
// of individual packets: which PIDs carry the program's tables and video, the
// PCR and PTS clocks, and whether a packet opens a keyframe. It never rewrites a
// packet.
//
// Ported from the P2PTV project's native passthrough segmenter (pkg/remux,
// tspassthrough_psi.go — same author), where it runs in production against
// sources served with no ffmpeg in between. The bounds checks on every
// wire-supplied length are the part to keep: pointer_field,
// adaptation_field_length and PES_header_data_length all come straight off the
// network and a corrupt packet must never index past its 188 bytes.
package tspes

import "fmt"

// PacketSize is the MPEG-TS packet length.
const PacketSize = 188

// Video stream types whose keyframes StartsKeyframe can recognise.
const (
	StreamTypeMPEG1 = 0x01
	StreamTypeMPEG2 = 0x02
	StreamTypeH264  = 0x1b
	StreamTypeHEVC  = 0x24
)

// PID returns a packet's 13-bit PID.
func PID(pkt []byte) uint16 { return uint16(pkt[1]&0x1f)<<8 | uint16(pkt[2]) }

// PUSI reports payload_unit_start_indicator: the packet opens a PES or a section.
func PUSI(pkt []byte) bool { return pkt[1]&0x40 != 0 }

// afc is the adaptation_field_control bits.
func afc(pkt []byte) byte { return (pkt[3] >> 4) & 0x3 }

// PayloadOffset returns where a packet's payload begins, or -1 when it carries
// none (adaptation only) or its adaptation field claims more than the packet.
func PayloadOffset(pkt []byte) int {
	c := afc(pkt)
	off := 4
	if c&0x2 != 0 { // adaptation field present
		if len(pkt) < 5 {
			return -1
		}
		off = 5 + int(pkt[4])
	}
	if c&0x1 == 0 || off >= len(pkt) { // no payload
		return -1
	}
	return off
}

// section returns a PSI section's bytes from a PUSI packet (skipping the
// pointer_field), or nil.
func section(pkt []byte) []byte {
	off := PayloadOffset(pkt)
	if off < 0 {
		return nil
	}
	if PUSI(pkt) {
		off += 1 + int(pkt[off]) // pointer_field
		if off >= len(pkt) {
			return nil
		}
	}
	return pkt[off:]
}

// PMTPID returns the first program's PMT PID from a PAT packet, or 0.
func PMTPID(pkt []byte) uint16 {
	sec := section(pkt)
	if len(sec) < 12 || sec[0] != 0x00 {
		return 0
	}
	secLen := int(sec[1]&0x0f)<<8 | int(sec[2])
	end := 3 + secLen - 4 // programs run to the CRC
	if end > len(sec) {
		end = len(sec)
	}
	for i := 8; i+4 <= end; i += 4 {
		prog := int(sec[i])<<8 | int(sec[i+1])
		if prog != 0 { // program_number 0 is the NIT, not a program
			return uint16(sec[i+2]&0x1f)<<8 | uint16(sec[i+3])
		}
	}
	return 0
}

// PMT is what a byte-for-byte consumer needs from a Program Map Table.
type PMT struct {
	VideoPID  uint16
	VideoType byte   // stream_type of VideoPID
	PCRPID    uint16 // falls back to VideoPID when the table declares none
}

// ParsePMT reads a PMT packet; ok is false when it is not one or names no video.
func ParsePMT(pkt []byte) (PMT, bool) {
	var out PMT
	sec := section(pkt)
	if len(sec) < 12 || sec[0] != 0x02 {
		return out, false
	}
	secLen := int(sec[1]&0x0f)<<8 | int(sec[2])
	out.PCRPID = uint16(sec[8]&0x1f)<<8 | uint16(sec[9])
	progInfoLen := int(sec[10]&0x0f)<<8 | int(sec[11])
	end := 3 + secLen - 4
	if end > len(sec) {
		end = len(sec)
	}
	for i := 12 + progInfoLen; i+5 <= end; {
		st := sec[i]
		pid := uint16(sec[i+1]&0x1f)<<8 | uint16(sec[i+2])
		esInfoLen := int(sec[i+3]&0x0f)<<8 | int(sec[i+4])
		if out.VideoPID == 0 && isVideo(st) {
			out.VideoPID, out.VideoType = pid, st
		}
		i += 5 + esInfoLen
	}
	if out.VideoPID == 0 {
		return out, false
	}
	if out.PCRPID == 0x1fff {
		out.PCRPID = out.VideoPID
	}
	return out, true
}

func isVideo(t byte) bool {
	switch t {
	case 0x01, 0x02, 0x10, 0x1b, 0x24, 0xea: // MPEG-1/2, MPEG-4, H.264, HEVC, VC-1
		return true
	}
	return false
}

// RAI reports random_access_indicator in the adaptation field.
func RAI(pkt []byte) bool {
	if afc(pkt)&0x2 == 0 || len(pkt) < 6 || pkt[4] == 0 {
		return false
	}
	return pkt[5]&0x40 != 0
}

// PCR reads the 33-bit PCR base (90 kHz) from the adaptation field.
func PCR(pkt []byte) (int64, bool) {
	if afc(pkt)&0x2 == 0 || len(pkt) < 12 || int(pkt[4]) < 7 || pkt[5]&0x10 == 0 {
		return 0, false
	}
	b := pkt[6:12]
	base := int64(b[0])<<25 | int64(b[1])<<17 | int64(b[2])<<9 | int64(b[3])<<1 | int64(b[4])>>7
	return base & 0x1ffffffff, true
}

// PTS reads the 33-bit PTS (90 kHz) from the PES header a PUSI packet opens.
func PTS(pkt []byte) (int64, bool) {
	off := PayloadOffset(pkt)
	if off < 0 {
		return 0, false
	}
	p := pkt[off:]
	if len(p) < 14 || p[0] != 0 || p[1] != 0 || p[2] != 1 || (p[7]>>6)&0x3 == 0 {
		return 0, false
	}
	v := int64(p[9]&0x0e)<<29 | int64(p[10])<<22 | int64(p[11]&0xfe)<<14 | int64(p[12])<<7 | int64(p[13])>>1
	return v & 0x1ffffffff, true
}

// StartsKeyframe reports whether a PES-starting packet on the video PID opens a
// keyframe, judged from the elementary stream itself: an H.264 IDR slice
// (nal_unit_type 5), an HEVC IRAP picture (types 16–23), or an MPEG-1/2
// sequence header (0xB3), which broadcast encoders place only in front of an
// I-frame. It is the fallback for a source that never sets
// random_access_indicator.
//
// Like the original it reads only THIS packet — a keyframe whose NAL starts in a
// later packet (a long SEI in front of it) is simply not a cut point, and the
// segment runs on to the next keyframe that is. A missed cut makes one segment
// longer; a false one would start a segment on a frame that cannot be decoded
// alone, which is why nothing weaker than these is accepted.
func StartsKeyframe(pkt []byte, streamType byte) bool {
	switch streamType {
	case StreamTypeMPEG1, StreamTypeMPEG2, StreamTypeH264, StreamTypeHEVC:
	default:
		return false
	}
	off := PayloadOffset(pkt)
	if off < 0 || off+9 > len(pkt) {
		return false
	}
	p := pkt[off:]
	if p[0] != 0 || p[1] != 0 || p[2] != 1 {
		return false // no PES start code
	}
	esStart := 9 + int(p[8]) // PES_header_data_length can overrun a corrupt packet
	if esStart >= len(p) {
		return false
	}
	es := p[esStart:]

	// Walk the Annex-B start codes (00 00 01, or 00 00 00 01) in what is left of
	// the packet, reading the header byte that follows each.
	for i := 0; i+3 < len(es); {
		if es[i] != 0 || es[i+1] != 0 {
			i++
			continue
		}
		k := -1
		switch {
		case es[i+2] == 1:
			k = i + 3
		case es[i+2] == 0 && i+4 < len(es) && es[i+3] == 1:
			k = i + 4
		}
		if k < 0 {
			i++
			continue
		}
		if k >= len(es) {
			return false
		}
		switch streamType {
		case StreamTypeH264:
			if es[k]&0x1f == 5 { // IDR slice
				return true
			}
		case StreamTypeHEVC:
			if t := (es[k] >> 1) & 0x3f; t >= 16 && t <= 23 { // IRAP
				return true
			}
		default: // MPEG-1/2 video: start codes are 00 00 01 <code>
			if es[k] == 0xb3 { // sequence_header_code
				return true
			}
		}
		i = k
	}
	return false
}

// ES is one elementary stream a PMT declares.
type ES struct {
	PID  uint16
	Type byte
}

// PATPrograms returns the program numbers a PAT packet declares (excluding the
// NIT's program 0). More than one means the source is a multi-programme
// transport stream, which a byte-for-byte copy hands to the player whole.
func PATPrograms(pkt []byte) []uint16 {
	sec := section(pkt)
	if len(sec) < 12 || sec[0] != 0x00 {
		return nil
	}
	secLen := int(sec[1]&0x0f)<<8 | int(sec[2])
	end := 3 + secLen - 4
	if end > len(sec) {
		end = len(sec)
	}
	var out []uint16
	for i := 8; i+4 <= end; i += 4 {
		if prog := uint16(sec[i])<<8 | uint16(sec[i+1]); prog != 0 {
			out = append(out, prog)
		}
	}
	return out
}

// ParsePMTStreams lists every elementary stream in a PMT packet, in table order.
// ParsePMT answers what the segmenter needs; this answers what an operator reading
// the log needs — the whole programme, video, audio, subtitles and data alike.
func ParsePMTStreams(pkt []byte) ([]ES, bool) {
	sec := section(pkt)
	if len(sec) < 12 || sec[0] != 0x02 {
		return nil, false
	}
	secLen := int(sec[1]&0x0f)<<8 | int(sec[2])
	progInfoLen := int(sec[10]&0x0f)<<8 | int(sec[11])
	end := 3 + secLen - 4
	if end > len(sec) {
		end = len(sec)
	}
	var out []ES
	for i := 12 + progInfoLen; i+5 <= end; {
		out = append(out, ES{PID: uint16(sec[i+1]&0x1f)<<8 | uint16(sec[i+2]), Type: sec[i]})
		i += 5 + int(sec[i+3]&0x0f)<<8 + int(sec[i+4])
	}
	return out, len(out) > 0
}

// Kind classifies a stream_type the way an operator reads it: "Video", "Audio",
// "Subtitle" or "Data".
func (e ES) Kind() string {
	switch {
	case isVideo(e.Type):
		return "Video"
	case isAudio(e.Type):
		return "Audio"
	case e.Type == 0x06: // private PES: DVB subtitles/teletext live here
		return "Data"
	}
	return "Data"
}

func isAudio(t byte) bool {
	switch t {
	case 0x03, 0x04, 0x0f, 0x11, 0x1c, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x91:
		return true
	}
	return false
}

// StreamTypeName is the ffmpeg-ish codec name for a PMT stream_type, for logs.
func StreamTypeName(t byte) string {
	switch t {
	case 0x01:
		return "mpeg1video"
	case 0x02:
		return "mpeg2video"
	case 0x03:
		return "mp2"
	case 0x04:
		return "mp3"
	case 0x06:
		return "private_pes"
	case 0x0f:
		return "aac"
	case 0x10:
		return "mpeg4"
	case 0x11:
		return "aac_latm"
	case 0x1b:
		return "h264"
	case 0x1c:
		return "aac_raw"
	case 0x24:
		return "hevc"
	case 0x33:
		return "vvc"
	case 0x51:
		return "av1"
	case 0x81, 0x87:
		return "ac3"
	case 0x82:
		return "dts"
	case 0x84, 0x85, 0x86:
		return "eac3"
	case 0x91:
		return "ac3_bluray"
	case 0xea:
		return "vc1"
	}
	return fmt.Sprintf("stream_type_0x%02x", t)
}
