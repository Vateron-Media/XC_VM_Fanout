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
	audioPID = 0x0101
	pcrPID   = audioPID // the audio stream carries the clock: there is no other

	// streamTypeAAC is ADTS AAC in a PES payload (ISO 13818-1 table 2-29). It is
	// what tells a demuxer the payload still has its ADTS headers on.
	streamTypeAAC = 0x0F

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
	pts       int64 // next presentation time, 90 kHz
	ccPAT     byte
	ccPMT     byte
	ccAudio   byte
	nextTable int64 // pts at which the tables are due again
	started   bool
}

// New returns a muxer whose clock starts at zero.
func New() *Muxer { return &Muxer{} }

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
	return append(dst, m.section(pmtPID, &m.ccPMT, pmtSection())...)
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
func pmtSection() []byte {
	body := []byte{
		0x00, 0x01, // program_number
		0xC1,       // version 0, current_next_indicator
		0x00, 0x00, // section_number, last_section_number
		byte(0xE0 | pcrPID>>8), byte(pcrPID & 0xFF),
		0xF0, 0x00, // program_info_length = 0
		streamTypeAAC,
		byte(0xE0 | audioPID>>8), byte(audioPID & 0xFF),
		0xF0, 0x00, // ES_info_length = 0
	}
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
	pes := make([]byte, 0, len(f.data)+14)
	pes = append(pes,
		0x00, 0x00, 0x01, // packet_start_code_prefix
		0xC0, // stream_id: audio stream 0
	)
	// PES_packet_length covers the optional header (3 + 5) plus the payload. It
	// is 16 bits; an audio frame never approaches that, and a frame that did
	// would be rejected by parseADTS long before here.
	plen := len(f.data) + 8
	pes = append(pes, byte(plen>>8), byte(plen))
	pes = append(pes,
		0x80, // marker bits, no scrambling
		0x80, // PTS_DTS_flags = PTS only
		0x05, // PES_header_data_length
	)
	pes = append(pes, encodePTS(m.pts)...)
	pes = append(pes, f.data...)

	first := true
	for len(pes) > 0 {
		p := make([]byte, packetSize)
		p[0] = syncByte
		p[1] = byte(audioPID >> 8)
		if first {
			p[1] |= 0x40 // payload_unit_start_indicator
		}
		p[2] = byte(audioPID & 0xFF)

		// Room for the payload, after whatever adaptation field this packet needs.
		off := 4
		needAF := first // the first packet carries the PCR
		room := packetSize - off
		var afLen int
		if needAF {
			afLen = 7 // flags + 6 PCR bytes
			room = packetSize - off - 1 - afLen
		}
		// The LAST packet of a frame is padded with an adaptation field so the
		// PES ends exactly on a packet boundary — a TS stream carries no other
		// way to say "nothing more here".
		if !needAF && len(pes) < room {
			afLen = room - len(pes) - 1
			if afLen < 0 {
				afLen = 0
			}
			needAF = true
			room = packetSize - off - 1 - afLen
		} else if needAF && len(pes) < room {
			afLen += room - len(pes)
			room = packetSize - off - 1 - afLen
		}

		p[3] = m.ccAudio & 0x0F
		if needAF {
			p[3] |= 0x30 // adaptation field + payload
		} else {
			p[3] |= 0x10 // payload only
		}
		m.ccAudio = (m.ccAudio + 1) & 0x0F

		if needAF {
			p[off] = byte(afLen)
			off++
			if afLen > 0 {
				flags := byte(0)
				if first {
					flags |= 0x10 // PCR_flag
					if rap {
						flags |= 0x40 // random_access_indicator
					}
				}
				p[off] = flags
				if first {
					putPCR(p[off+1:off+7], m.pts)
					for i := off + 7; i < off+afLen; i++ {
						p[i] = 0xFF // stuffing
					}
				} else {
					for i := off + 1; i < off+afLen; i++ {
						p[i] = 0xFF
					}
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

// encodePTS renders a 33-bit presentation time in the five bytes a PES header
// wants, with the '0010' prefix a PTS-only header carries.
func encodePTS(pts int64) []byte {
	return []byte{
		byte(0x21 | (pts>>29)&0x0E),
		byte(pts >> 22),
		byte(0x01 | (pts>>14)&0xFE),
		byte(pts >> 7),
		byte(0x01 | (pts<<1)&0xFE),
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
