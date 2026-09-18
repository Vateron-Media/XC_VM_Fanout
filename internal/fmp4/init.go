// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package fmp4

import "fmt"

// Kind is what a track carries. Anything else in the file — subtitles, data,
// a second video angle — is ignored: this package builds one audio/video
// programme, which is what an IPTV channel is.
type Kind int

const (
	KindOther Kind = iota
	KindVideo
	KindAudio
)

// Track is one elementary stream as the init segment declares it.
type Track struct {
	ID        uint32
	Kind      Kind
	TimeScale uint32 // ticks per second for this track's timestamps

	// SPS and PPS come from an avcC box and are what a TS demuxer needs in
	// front of a keyframe: fMP4 carries them once, in the init segment, while
	// MPEG-TS repeats them in the stream.
	SPS [][]byte
	PPS [][]byte
	// NALULengthSize is how many bytes prefix each NAL unit in a sample (avcC
	// says 1, 2 or 4). Converting to Annex-B means replacing exactly these.
	NALULengthSize int

	// ASC is the AudioSpecificConfig from an esds box, from which an ADTS
	// header is built for every audio sample.
	ASC []byte
	// Derived from ASC, and what the ADTS header actually carries.
	AACProfile    byte // 0 = Main, 1 = LC, 2 = SSR (ADTS stores profile-1)
	SampleRateIdx byte
	Channels      byte
}

// Init is a parsed init segment: the tracks a media segment will carry.
type Init struct {
	Tracks []Track
}

// Video and Audio return the first track of each kind, or nil.
func (i *Init) Video() *Track { return i.first(KindVideo) }
func (i *Init) Audio() *Track { return i.first(KindAudio) }

func (i *Init) first(k Kind) *Track {
	for idx := range i.Tracks {
		if i.Tracks[idx].Kind == k {
			return &i.Tracks[idx]
		}
	}
	return nil
}

// ParseInit reads an HLS EXT-X-MAP init segment: ftyp, then moov holding one
// trak per elementary stream.
func ParseInit(b []byte) (*Init, error) {
	moov := find(b, "moov")
	if moov == nil {
		return nil, fmt.Errorf("%w: no moov in the init segment", ErrFormat)
	}
	out := &Init{}
	for _, trak := range findAll(moov, "trak") {
		t, err := parseTrak(trak)
		if err != nil {
			return nil, err
		}
		if t.Kind != KindOther {
			out.Tracks = append(out.Tracks, t)
		}
	}
	if len(out.Tracks) == 0 {
		return nil, fmt.Errorf("%w: the init segment declares no audio or video track", ErrFormat)
	}
	return out, nil
}

// parseTrak reads one track: its id and timescale from the media header, its
// kind from the handler, and its codec configuration from the sample entry.
func parseTrak(trak []byte) (Track, error) {
	var t Track
	tkhd := find(trak, "tkhd")
	if _, _, rest, ok := fullBox(tkhd); ok {
		// version 0: creation(4) modification(4) track_id(4); version 1 widens
		// the two times to 8 bytes each.
		switch tkhd[0] {
		case 0:
			if len(rest) >= 12 {
				t.ID = be32(rest[8:])
			}
		case 1:
			if len(rest) >= 20 {
				t.ID = be32(rest[16:])
			}
		}
	}
	mdia := find(trak, "mdia")
	if mdia == nil {
		return t, fmt.Errorf("%w: track %d has no mdia", ErrFormat, t.ID)
	}
	mdhd := find(mdia, "mdhd")
	if v, _, rest, ok := fullBox(mdhd); ok {
		switch v {
		case 0:
			if len(rest) >= 12 {
				t.TimeScale = be32(rest[8:])
			}
		case 1:
			// version 1 widens creation and modification to 8 bytes each, so the
			// timescale sits at 16 rather than 8.
			if len(rest) >= 20 {
				t.TimeScale = be32(rest[16:])
			}
		}
	}
	if hdlr := find(mdia, "hdlr"); len(hdlr) >= 12 {
		switch string(hdlr[8:12]) {
		case "vide":
			t.Kind = KindVideo
		case "soun":
			t.Kind = KindAudio
		}
	}
	if t.Kind == KindOther {
		return t, nil // not a track this package carries; the caller drops it
	}
	if t.TimeScale == 0 {
		return t, fmt.Errorf("%w: track %d has no timescale", ErrFormat, t.ID)
	}
	stsd := find(find(find(mdia, "minf"), "stbl"), "stsd")
	if stsd == nil {
		return t, fmt.Errorf("%w: track %d has no sample description", ErrFormat, t.ID)
	}
	if err := parseSampleEntry(&t, stsd); err != nil {
		return t, err
	}
	return t, nil
}

// parseSampleEntry finds the codec configuration inside a sample description.
func parseSampleEntry(t *Track, stsd []byte) error {
	if len(stsd) < 8 {
		return fmt.Errorf("%w: short stsd on track %d", ErrFormat, t.ID)
	}
	// version/flags(4) entry_count(4), then the entries.
	return walk(stsd[8:], func(e box) error {
		switch e.typ {
		case "avc1", "avc3":
			// A visual sample entry: 78 bytes of fixed fields, then child boxes.
			if len(e.payload) <= 78 {
				return fmt.Errorf("%w: short %s entry", ErrFormat, e.typ)
			}
			if avcC := find(e.payload[78:], "avcC"); avcC != nil {
				return parseAVCC(t, avcC)
			}
			return fmt.Errorf("%w: %s entry with no avcC", ErrFormat, e.typ)
		case "mp4a":
			// An audio sample entry: 28 bytes of fixed fields, then child boxes.
			if len(e.payload) <= 28 {
				return fmt.Errorf("%w: short mp4a entry", ErrFormat)
			}
			if esds := find(e.payload[28:], "esds"); esds != nil {
				return parseESDS(t, esds)
			}
			return fmt.Errorf("%w: mp4a entry with no esds", ErrFormat)
		}
		return nil
	})
}

// parseAVCC reads the H.264 decoder configuration: the NAL length prefix size
// and the parameter sets a TS stream has to carry inline.
func parseAVCC(t *Track, b []byte) error {
	if len(b) < 6 {
		return fmt.Errorf("%w: short avcC", ErrFormat)
	}
	t.NALULengthSize = int(b[4]&0x03) + 1
	n := int(b[5] & 0x1F)
	off := 6
	for i := 0; i < n; i++ {
		if off+2 > len(b) {
			return fmt.Errorf("%w: avcC ends inside its SPS list", ErrFormat)
		}
		l := int(b[off])<<8 | int(b[off+1])
		off += 2
		if off+l > len(b) {
			return fmt.Errorf("%w: avcC SPS runs past the box", ErrFormat)
		}
		t.SPS = append(t.SPS, b[off:off+l])
		off += l
	}
	if off >= len(b) {
		return fmt.Errorf("%w: avcC has no PPS list", ErrFormat)
	}
	n = int(b[off])
	off++
	for i := 0; i < n; i++ {
		if off+2 > len(b) {
			return fmt.Errorf("%w: avcC ends inside its PPS list", ErrFormat)
		}
		l := int(b[off])<<8 | int(b[off+1])
		off += 2
		if off+l > len(b) {
			return fmt.Errorf("%w: avcC PPS runs past the box", ErrFormat)
		}
		t.PPS = append(t.PPS, b[off:off+l])
		off += l
	}
	if len(t.SPS) == 0 || len(t.PPS) == 0 {
		return fmt.Errorf("%w: avcC carries no parameter sets", ErrFormat)
	}
	return nil
}

// parseESDS digs the AudioSpecificConfig out of an esds descriptor tree and
// reads the three fields an ADTS header needs from it.
func parseESDS(t *Track, b []byte) error {
	_, _, rest, ok := fullBox(b)
	if !ok {
		return fmt.Errorf("%w: short esds", ErrFormat)
	}
	asc := findDescriptor(rest)
	if len(asc) < 2 {
		return fmt.Errorf("%w: esds carries no AudioSpecificConfig", ErrFormat)
	}
	t.ASC = asc
	// AudioSpecificConfig: 5 bits object type, 4 bits sampling index, 4 bits
	// channel configuration.
	objType := asc[0] >> 3
	if objType == 0 || objType > 4 {
		// ADTS can only name profiles 1..4 (Main, LC, SSR, LTP). An upstream
		// using anything else — HE-AAC signalled explicitly, say — needs the
		// decoder ffmpeg has.
		return fmt.Errorf("%w: AAC object type %d cannot be written as ADTS", ErrFormat, objType)
	}
	t.AACProfile = objType - 1
	t.SampleRateIdx = (asc[0]&0x07)<<1 | asc[1]>>7
	t.Channels = (asc[1] >> 3) & 0x0F
	if t.SampleRateIdx > 12 || t.Channels == 0 || t.Channels > 7 {
		return fmt.Errorf("%w: AAC config names sample rate index %d and %d channels",
			ErrFormat, t.SampleRateIdx, t.Channels)
	}
	return nil
}

// findDescriptor walks the MPEG-4 descriptor chain inside an esds for the
// DecoderSpecificInfo (tag 0x05), which is the AudioSpecificConfig.
func findDescriptor(b []byte) []byte {
	for off := 0; off+2 <= len(b); {
		tag := b[off]
		off++
		// Descriptor lengths are 7 bits a byte, high bit continuing.
		size := 0
		for i := 0; i < 4 && off < len(b); i++ {
			c := b[off]
			off++
			size = size<<7 | int(c&0x7F)
			if c&0x80 == 0 {
				break
			}
		}
		if size < 0 || off+size > len(b) {
			return nil
		}
		switch tag {
		case 0x03: // ES_Descriptor: ES_ID(2) flags(1), then children
			if off+3 > len(b) {
				return nil
			}
			return findDescriptor(b[off+3 : off+size])
		case 0x04: // DecoderConfigDescriptor: 13 fixed bytes, then children
			if off+13 > len(b) {
				return nil
			}
			return findDescriptor(b[off+13 : off+size])
		case 0x05: // DecoderSpecificInfo
			return b[off : off+size]
		}
		off += size
	}
	return nil
}
