// Package tsmeta derives a stream's descriptive metadata — codec names, picture
// size — from the MPEG-TS bytes the daemon already holds.
//
// XC_VM's panel stores this per stream (streams_servers.video_codec,
// audio_codec, resolution) and its watchdog obtained it by shelling out to
// ffprobe against a segment on disk: once at start-up, and again every five
// minutes while the stream ran. Every one of those answers is already present in
// the bytes passing through the fan-out — the codec in the PMT, the picture size
// in the video sequence header — so this reads them there instead.
//
// The contract is that an unknown answer is reported as unknown. A zero Height
// or an empty VideoCodec means "this could not be determined from these bytes",
// never a guess, so the caller can keep whatever it already had rather than
// overwrite a correct value with a plausible one.
package tsmeta

const packetSize = 188

// Info is what could be determined about a stream. Every field is optional: the
// zero value means "not determined".
type Info struct {
	VideoCodec string // ffprobe's codec_name, e.g. "h264", "hevc", "mpeg2video"
	AudioCodec string // e.g. "aac", "ac3", "eac3", "mp2"
	Width      int
	Height     int
}

// Complete reports whether everything worth knowing was determined, so a caller
// can stop re-reading a stream whose metadata has settled.
func (i Info) Complete() bool {
	return i.VideoCodec != "" && i.AudioCodec != "" && i.Height > 0
}

// Parse reads what it can out of a packet-aligned MPEG-TS buffer — in practice a
// clean-join snapshot, which by construction opens with the PAT and PMT and then
// carries a keyframe, so the sequence header is present.
//
// It never fails: a buffer it cannot make sense of yields a zero Info.
func Parse(ts []byte) Info {
	var info Info

	pmtPID, videoPID, audioPID := -1, -1, -1
	var videoType byte

	// First pass: program structure. PAT names the PMT; the PMT names the
	// elementary streams and their types, which IS the codec.
	for off := 0; off+packetSize <= len(ts); off += packetSize {
		pkt := ts[off : off+packetSize]
		if pkt[0] != 0x47 {
			continue
		}
		pid := (int(pkt[1]&0x1f) << 8) | int(pkt[2])
		if pkt[1]&0x40 == 0 {
			continue // only a payload-unit start carries a section header
		}
		switch {
		case pid == 0:
			if p := parsePMTPID(pkt); p >= 0 {
				pmtPID = p
			}
		case pmtPID >= 0 && pid == pmtPID:
			for _, es := range parseES(pkt) {
				if name, ok := videoCodecName(es.streamType); ok && videoPID < 0 {
					videoPID, videoType = es.pid, es.streamType
					info.VideoCodec = name
				}
				if name, ok := audioCodecName(es.streamType, es.descriptors); ok && audioPID < 0 {
					audioPID = es.pid
					info.AudioCodec = name
				}
			}
		}
	}

	if videoPID >= 0 {
		if w, h, ok := pictureSize(ts, videoPID, videoType); ok {
			info.Width, info.Height = w, h
		}
	}
	return info
}

// esInfo is one elementary stream from a PMT.
type esInfo struct {
	streamType  byte
	pid         int
	descriptors []byte
}

// parsePMTPID extracts the first program's PMT PID from a PAT packet.
func parsePMTPID(pkt []byte) int {
	ps := payloadOffset(pkt)
	if ps < 0 || ps >= len(pkt) {
		return -1
	}
	p := ps + 1 + int(pkt[ps]) // skip pointer_field
	prog := p + 8              // past the section header
	for prog+4 <= len(pkt) {
		programNumber := (int(pkt[prog]) << 8) | int(pkt[prog+1])
		pid := ((int(pkt[prog+2]) & 0x1f) << 8) | int(pkt[prog+3])
		if programNumber != 0 {
			return pid
		}
		prog += 4
	}
	return -1
}

// parseES walks a PMT's elementary-stream loop.
func parseES(pkt []byte) []esInfo {
	ps := payloadOffset(pkt)
	if ps < 0 || ps >= len(pkt) {
		return nil
	}
	p := ps + 1 + int(pkt[ps])
	if p+12 > len(pkt) {
		return nil
	}
	pil := ((int(pkt[p+10]) & 0x0f) << 8) | int(pkt[p+11]) // program_info_length
	es := p + 12 + pil

	var out []esInfo
	for es+5 <= len(pkt) {
		infoLen := ((int(pkt[es+3]) & 0x0f) << 8) | int(pkt[es+4])
		e := esInfo{
			streamType: pkt[es],
			pid:        ((int(pkt[es+1]) & 0x1f) << 8) | int(pkt[es+2]),
		}
		if start := es + 5; start+infoLen <= len(pkt) {
			e.descriptors = pkt[start : start+infoLen]
		}
		// A stream_type of 0 is the padding a fixture or a short section leaves
		// behind, not a real stream.
		if e.streamType == 0 && e.pid == 0 {
			break
		}
		out = append(out, e)
		es += 5 + infoLen
	}
	return out
}

// videoCodecName maps a PMT stream_type to the codec name ffprobe reports, which
// is what the panel stores.
func videoCodecName(t byte) (string, bool) {
	switch t {
	case 0x01:
		return "mpeg1video", true
	case 0x02:
		return "mpeg2video", true
	case 0x10:
		return "mpeg4", true
	case 0x1b:
		return "h264", true
	case 0x24:
		return "hevc", true
	case 0xea:
		return "vc1", true
	}
	return "", false
}

// audioCodecName maps a PMT stream_type to a codec name. Type 0x06 is "PES
// carrying private data", which DVB uses for AC-3 and E-AC-3 and identifies by a
// descriptor rather than by the type — so the descriptors decide it, and a 0x06
// with no recognised descriptor is left unknown rather than guessed at (it is
// just as likely to be subtitles or teletext).
func audioCodecName(t byte, descriptors []byte) (string, bool) {
	switch t {
	case 0x03:
		return "mp2", true
	case 0x04:
		return "mp3", true
	case 0x0f:
		return "aac", true
	case 0x11:
		return "aac_latm", true
	case 0x81:
		return "ac3", true
	case 0x87:
		return "eac3", true
	case 0x06:
		for _, tag := range descriptorTags(descriptors) {
			switch tag {
			case 0x6a: // AC-3_descriptor
				return "ac3", true
			case 0x7a: // enhanced_AC-3_descriptor
				return "eac3", true
			case 0x7b: // DTS_descriptor
				return "dts", true
			case 0x7c: // AAC_descriptor
				return "aac", true
			}
		}
	}
	return "", false
}

// descriptorTags lists the tags in a descriptor loop.
func descriptorTags(d []byte) []byte {
	var out []byte
	for i := 0; i+2 <= len(d); {
		tag, length := d[i], int(d[i+1])
		out = append(out, tag)
		i += 2 + length
	}
	return out
}

// payloadOffset returns where a packet's payload begins, or -1 if it has none.
func payloadOffset(pkt []byte) int {
	switch (pkt[3] >> 4) & 0x3 {
	case 2: // adaptation only
		return -1
	case 3: // adaptation + payload
		return 5 + int(pkt[4])
	default:
		return 4
	}
}

// pictureSize finds the video sequence header and reads the picture dimensions
// from it. Only H.264 and HEVC are read; anything else reports not-determined,
// which the caller treats as "keep what you had".
func pictureSize(ts []byte, videoPID int, streamType byte) (int, int, bool) {
	es := elementaryStream(ts, videoPID)
	if len(es) == 0 {
		return 0, 0, false
	}
	switch streamType {
	case 0x1b:
		return h264PictureSize(es)
	case 0x24:
		return hevcPictureSize(es)
	}
	return 0, 0, false
}

// elementaryStream concatenates the PES payload of one PID, dropping the PES
// headers so the result is the raw elementary stream (a NAL byte-stream, for the
// codecs handled here).
func elementaryStream(ts []byte, pid int) []byte {
	var out []byte
	for off := 0; off+packetSize <= len(ts); off += packetSize {
		pkt := ts[off : off+packetSize]
		if pkt[0] != 0x47 {
			continue
		}
		if p := (int(pkt[1]&0x1f) << 8) | int(pkt[2]); p != pid {
			continue
		}
		ps := payloadOffset(pkt)
		if ps < 0 || ps >= len(pkt) {
			continue
		}
		payload := pkt[ps:]
		// A payload-unit start begins a PES packet; skip its header to reach the
		// elementary stream. Continuation packets are already raw.
		if pkt[1]&0x40 != 0 {
			if len(payload) < 9 || payload[0] != 0 || payload[1] != 0 || payload[2] != 1 {
				continue // not a PES start after all
			}
			hdrLen := int(payload[8])
			if 9+hdrLen > len(payload) {
				continue
			}
			payload = payload[9+hdrLen:]
		}
		out = append(out, payload...)
		// A sequence header lives at the very front of a stream; a few packets is
		// plenty and stops a long snapshot being copied for nothing.
		if len(out) > 64*1024 {
			break
		}
	}
	return out
}
