// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsmux

const (
	packetSize = 188
	syncByte   = 0x47

	// The programme this package emits. The PIDs are arbitrary but must be
	// stable for the life of a stream, since a viewer that joined mid-stream
	// reads the tables the ring replays to it.
	pmtPID   = 0x1000
	videoPID = 0x0100
	audioPID = 0x0101

	// streamTypeAAC is ADTS AAC in a PES payload (ISO 13818-1 table 2-29). It is
	// what tells a demuxer the payload still has its ADTS headers on.
	streamTypeAAC = 0x0F
	// streamTypeH264 is Annex-B H.264, which is how a TS carries it: start
	// codes, and the parameter sets repeated in front of every keyframe.
	streamTypeH264 = 0x1B

	// tablePeriod90k is how often the PAT and PMT are re-emitted: every 100 ms
	// of media, the usual broadcast cadence. A viewer joining mid-stream gets
	// its tables within that, and the daemon's own join replays the last of
	// each anyway.
	tablePeriod90k = 9000
)

// Muxer turns packed-audio segments into MPEG-TS. One per stream: it carries
// the clock and the continuity counters, which must advance across segments,
// not restart with each.
//
// Not safe for concurrent use; the puller drives it from one goroutine.
type Muxer struct {
	pts       int64 // next presentation time for packed audio, 90 kHz
	ccPAT     byte
	ccPMT     byte
	ccAudio   byte
	ccVideo   byte
	nextTable int64 // pts at which the tables are due again
	started   bool

	// hasVideo says the programme carries video, which changes the PMT and
	// moves the clock: the video stream carries the PCR when there is one,
	// because that is the stream a decoder times itself against.
	hasVideo bool
}

// New returns a muxer for an audio-only programme, whose clock starts at zero.
func New() *Muxer { return &Muxer{} }

// NewAV returns a muxer for a programme with both video and audio — what a
// fragmented-MP4 source carries. Timestamps come from the caller here rather
// than from the bitstream: fMP4 keeps them in tables, and they are the one
// thing that must survive the conversion exactly.
func NewAV() *Muxer { return &Muxer{hasVideo: true} }

// pcrPID is the stream a decoder times itself against: the video when there is
// video, else the audio, which is then the only stream there is.
func (m *Muxer) pcrPID() uint16 {
	if m.hasVideo {
		return videoPID
	}
	return audioPID
}

// Tables emits one PAT and PMT. A caller muxing sample by sample writes them at
// the head of every segment, so a viewer joining mid-stream has the programme
// within one segment rather than whenever the upstream felt like repeating it.
func (m *Muxer) Tables(dst []byte) []byte { return m.writeTables(dst) }

// WriteVideo appends one Annex-B access unit as PES packets. rap marks a
// random-access point, which is what the ring cuts a block on and what a
// joining viewer starts at; the parameter sets must already be in front of the
// picture, since a TS carries them inline.
func (m *Muxer) WriteVideo(dst, annexB []byte, pts, dts int64, rap bool) []byte {
	m.started = true
	return m.writePESFull(dst, videoPID, &m.ccVideo, 0xE0, annexB, pts, dts, rap, true)
}

// WriteAudio appends one ADTS frame as a PES packet. Audio carries a PTS only:
// it is never stored out of order, so a DTS would say nothing a PTS does not.
func (m *Muxer) WriteAudio(dst, adts []byte, pts int64) []byte {
	m.started = true
	return m.writePESFull(dst, audioPID, &m.ccAudio, 0xC0, adts, pts, pts, !m.hasVideo, !m.hasVideo)
}

// Segment wraps one packed-audio segment in MPEG-TS and appends it to dst.
//
// Every segment opens with a PAT and PMT and flags its first audio packet as a
// random-access point, so it decodes on its own: that is what lets the ring cut
// a block at a segment boundary and a viewer join there. The clock continues
// from the previous segment — restarting it per segment would make time run
// backwards on the wire every few seconds.
func (m *Muxer) Segment(dst, body []byte) ([]byte, error) {
	frames, err := parseADTS(body)
	if err != nil {
		return dst, err
	}
	dst = m.writeTables(dst)
	m.nextTable = m.pts + tablePeriod90k

	for i, f := range frames {
		if m.pts >= m.nextTable && i > 0 {
			dst = m.writeTables(dst)
			m.nextTable = m.pts + tablePeriod90k
		}
		// The first frame of a segment opens it: the random-access indicator is
		// what tsjoin cuts a block on, and every audio frame is a valid entry
		// point anyway.
		dst = m.writePES(dst, f, i == 0)
		m.pts += f.duration90k()
	}
	m.started = true
	return dst, nil
}

// Started reports whether any segment has been muxed yet. The caller uses it to
// decide whether a refusal can still be a clean format refusal.
func (m *Muxer) Started() bool { return m.started }

// writeTables emits one PAT and one PMT, each in its own packet.
func (m *Muxer) writeTables(dst []byte) []byte {
	dst = append(dst, m.section(0x0000, &m.ccPAT, patSection())...)
	return append(dst, m.section(pmtPID, &m.ccPMT, pmtSection(m.hasVideo))...)
}

// section wraps one PSI section in a single TS packet, padded to 188 bytes.
// Both tables this package emits are far smaller than one payload, so the
// multi-packet case never arises.
func (m *Muxer) section(pid uint16, cc *byte, sec []byte) []byte {
	p := make([]byte, packetSize)
	for i := range p {
		p[i] = 0xFF // stuffing after the section
	}
	p[0] = syncByte
	p[1] = byte(pid>>8) | 0x40 // payload_unit_start_indicator
	p[2] = byte(pid)
	p[3] = 0x10 | (*cc & 0x0F) // payload only
	*cc = (*cc + 1) & 0x0F
	p[4] = 0x00 // pointer_field
	copy(p[5:], sec)
	return p
}

// patSection is a PAT declaring one programme, whose map is at pmtPID.
func patSection() []byte {
	body := []byte{
		0x00, 0x01, // transport_stream_id
		0xC1,       // version 0, current_next_indicator
		0x00, 0x00, // section_number, last_section_number
		0x00, 0x01, // program_number 1
		byte(0xE0 | pmtPID>>8), byte(pmtPID & 0xFF),
	}
	return section(0x00, body)
}

// pmtSection is a PMT declaring one AAC elementary stream, which also carries
// the PCR.
func pmtSection(hasVideo bool) []byte {
	pcr := uint16(audioPID)
	if hasVideo {
		pcr = videoPID
	}
	body := []byte{
		0x00, 0x01, // program_number
		0xC1,       // version 0, current_next_indicator
		0x00, 0x00, // section_number, last_section_number
		byte(0xE0 | pcr>>8), byte(pcr & 0xFF),
		0xF0, 0x00, // program_info_length = 0
	}
	if hasVideo {
		body = append(body,
			streamTypeH264,
			byte(0xE0|videoPID>>8), byte(videoPID&0xFF),
			0xF0, 0x00,
		)
	}
	body = append(body,
		streamTypeAAC,
		byte(0xE0|audioPID>>8), byte(audioPID&0xFF),
		0xF0, 0x00,
	)
	return section(0x02, body)
}

// section prefixes a table body with its table_id and section_length and
// appends a CRC field.
//
// The CRC is written as zero rather than computed. Nothing in this daemon
// verifies it — internal/tspes reads section_length and stops, as every parser
// here does — and a real demuxer downstream of the daemon reads the tables the
// SOURCE sent for a normal stream. For this one synthetic case a correct CRC
// would be better manners; it is noted here so the next reader knows it is a
// deliberate omission rather than an oversight.
func section(tableID byte, body []byte) []byte {
	const crcLen = 4
	secLen := len(body) + crcLen
	out := make([]byte, 0, 3+secLen)
	out = append(out, tableID, byte(0xB0|secLen>>8), byte(secLen))
	out = append(out, body...)
	return append(out, 0x00, 0x00, 0x00, 0x00)
}

// writePES packages one audio frame as a PES packet and cuts it into TS
// packets. The first carries the PCR (and, when rap is set, the random-access
// indicator); the rest are payload only.
func (m *Muxer) writePES(dst []byte, f frame, rap bool) []byte {
	return m.writePESFull(dst, audioPID, &m.ccAudio, 0xC0, f.data, m.pts, m.pts, rap, true)
}

// writePESFull packages one access unit as a PES packet and cuts it into TS
// packets on pid.
//
// withDTS says whether the header carries a decode time as well as a
// presentation one: video stored out of display order needs both, and audio,
// which is never reordered, carries a PTS alone. withPCR puts the clock on this
// stream — true for the PID the PMT names as the PCR carrier.
func (m *Muxer) writePESFull(dst []byte, pid uint16, cc *byte, streamID byte, payload []byte, pts, dts int64, rap, withPCR bool) []byte {
	withDTS := pts != dts
	hdrLen := 5
	ptsFlags := byte(0x80) // PTS only
	if withDTS {
		hdrLen = 10
		ptsFlags = 0xC0 // PTS and DTS
	}
	pes := make([]byte, 0, len(payload)+14+hdrLen)
	pes = append(pes, 0x00, 0x00, 0x01, streamID)
	// PES_packet_length: zero means "unbounded, to the next start code", which
	// is what a unit longer than 65535 bytes must use. An ordinary picture at
	// broadcast bitrates exceeds it; an audio frame never comes close.
	plen := len(payload) + 3 + hdrLen
	if plen > 0xFFFF {
		plen = 0
	}
	pes = append(pes, byte(plen>>8), byte(plen))
	pes = append(pes, 0x80, ptsFlags, byte(hdrLen))
	// The four-bit prefix says which field this is: 0010 for a PTS on its own,
	// 0011 and 0001 for the two halves of a PTS/DTS pair.
	if withDTS {
		pes = append(pes, encodeTime(0x30, pts)...)
		pes = append(pes, encodeTime(0x10, dts)...)
	} else {
		pes = append(pes, encodeTime(0x20, pts)...)
	}
	pes = append(pes, payload...)

	first := true
	for len(pes) > 0 {
		p := make([]byte, packetSize)
		p[0] = syncByte
		p[1] = byte(pid >> 8)
		if first {
			p[1] |= 0x40
		}
		p[2] = byte(pid & 0xFF)

		off := 4
		needAF := first && (withPCR || rap)
		room := packetSize - off
		var afLen int
		if needAF {
			afLen = 1 // flags
			if withPCR {
				afLen += 6
			}
			room = packetSize - off - 1 - afLen
		}
		if len(pes) < room {
			// Pad with the adaptation field so the unit ends exactly on a packet
			// boundary: a TS carries no other way to say "nothing more here".
			extra := room - len(pes)
			if !needAF {
				needAF = true
				afLen = 0
				extra--
			}
			afLen += extra
			room = packetSize - off - 1 - afLen
		}

		p[3] = *cc & 0x0F
		if needAF {
			p[3] |= 0x30
		} else {
			p[3] |= 0x10
		}
		*cc = (*cc + 1) & 0x0F

		if needAF {
			p[off] = byte(afLen)
			off++
			if afLen > 0 {
				flags := byte(0)
				if first && rap {
					flags |= 0x40 // random_access_indicator
				}
				if first && withPCR {
					flags |= 0x10 // PCR_flag
				}
				p[off] = flags
				n := 1
				if first && withPCR {
					putPCR(p[off+1:off+7], dts)
					n = 7
				}
				for i := off + n; i < off+afLen; i++ {
					p[i] = 0xFF // stuffing
				}
				off += afLen
			}
		}
		n := copy(p[off:], pes)
		pes = pes[n:]
		dst = append(dst, p...)
		first = false
	}
	return dst
}

// encodeTime renders a 33-bit timestamp in the five bytes a PES header wants,
// under the four-bit prefix the field's position calls for (0010 for a PTS
// alone, 0011 and 0001 for a PTS/DTS pair).
func encodeTime(prefix byte, t int64) []byte {
	return []byte{
		byte(prefix&0xF0 | byte((t>>29)&0x0E) | 0x01),
		byte(t >> 22),
		byte(0x01 | (t>>14)&0xFE),
		byte(t >> 7),
		byte(0x01 | (t<<1)&0xFE),
	}
}

// putPCR writes the 33-bit base and a zero extension into an adaptation field's
// six PCR bytes.
func putPCR(dst []byte, pcr int64) {
	dst[0] = byte(pcr >> 25)
	dst[1] = byte(pcr >> 17)
	dst[2] = byte(pcr >> 9)
	dst[3] = byte(pcr >> 1)
	dst[4] = byte(pcr<<7) | 0x7E
	dst[5] = 0x00
}
