// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package fmp4

import "fmt"

// Sample is one access unit: a frame of video or audio, with the timing the
// fragment's tables give it.
//
// DTS and PTS are in the TRACK's timescale; the caller scales them. They are
// separate because video may be stored out of display order, which is exactly
// what the composition offset in a trun expresses and what a TS demuxer needs
// told — a stream whose PTS and DTS are collapsed together plays, but reorders
// B-frames wrongly.
type Sample struct {
	Data []byte
	DTS  int64
	PTS  int64
	Sync bool // an IDR / random-access point
}

// Fragment is one media segment's samples, per track.
type Fragment struct {
	Video []Sample
	Audio []Sample
}

// trackDefaults are the tfhd's per-track defaults, which a trun may override
// per sample and which the init segment's trex may set for the whole file.
type trackDefaults struct {
	duration uint32
	size     uint32
	flags    uint32
}

// ParseFragment reads one media segment (moof + mdat) against the tracks the
// init segment declared.
//
// A segment may carry several moof/mdat pairs; each is read in order, so the
// samples come back in the order they are meant to be played.
func ParseFragment(b []byte, in *Init) (*Fragment, error) {
	out := &Fragment{}
	var pending []byte // the moof waiting for its mdat
	err := walk(b, func(c box) error {
		switch c.typ {
		case "moof":
			pending = c.payload
		case "mdat":
			if pending == nil {
				// An mdat with no moof in front of it: a progressive file, or a
				// segment cut mid-stream. Either way there are no tables to read
				// its samples with.
				return fmt.Errorf("%w: mdat with no moof", ErrFormat)
			}
			if err := parseMoof(pending, c.payload, in, out); err != nil {
				return err
			}
			pending = nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(out.Video) == 0 && len(out.Audio) == 0 {
		return nil, fmt.Errorf("%w: segment carries no samples", ErrFormat)
	}
	return out, nil
}

// parseMoof reads every traf in one fragment and cuts its samples out of the
// mdat that follows.
//
// The samples of every traf and every trun come out of ONE mdat, laid end to
// end in the order the tables appear — so the cursor into it runs across the
// whole fragment rather than restarting per table. (A trun's data_offset says
// the same thing in absolute terms, relative to the start of the moof; a
// packager that disagreed with the sequential reading would have to leave gaps,
// which is why an explicit base-data-offset is refused below rather than
// guessed at.)
func parseMoof(moof, mdat []byte, in *Init, out *Fragment) error {
	mdatOff := 0
	for _, traf := range findAll(moof, "traf") {
		tfhd := find(traf, "tfhd")
		if tfhd == nil {
			return fmt.Errorf("%w: traf with no tfhd", ErrFormat)
		}
		_, flags, rest, ok := fullBox(tfhd)
		if !ok || len(rest) < 4 {
			return fmt.Errorf("%w: short tfhd", ErrFormat)
		}
		trackID := be32(rest)
		off := 4
		var def trackDefaults
		var baseDataOffset int64 = -1
		if flags&0x000001 != 0 { // base-data-offset-present
			if len(rest) < off+8 {
				return fmt.Errorf("%w: tfhd claims a base data offset it has no room for", ErrFormat)
			}
			baseDataOffset = int64(be64(rest[off:]))
			off += 8
		}
		if flags&0x000002 != 0 { // sample-description-index-present
			off += 4
		}
		if flags&0x000008 != 0 { // default-sample-duration-present
			if len(rest) < off+4 {
				return fmt.Errorf("%w: short tfhd defaults", ErrFormat)
			}
			def.duration = be32(rest[off:])
			off += 4
		}
		if flags&0x000010 != 0 { // default-sample-size-present
			if len(rest) < off+4 {
				return fmt.Errorf("%w: short tfhd defaults", ErrFormat)
			}
			def.size = be32(rest[off:])
			off += 4
		}
		if flags&0x000020 != 0 { // default-sample-flags-present
			if len(rest) < off+4 {
				return fmt.Errorf("%w: short tfhd defaults", ErrFormat)
			}
			def.flags = be32(rest[off:])
		}

		track := trackByID(in, trackID)
		if track == nil {
			continue // a track this package does not carry
		}

		// The decode time of the fragment's first sample. Without a tfdt there
		// is nothing to anchor the segment to, and guessing would put the
		// segment at the wrong time on a wire whose clock everything else
		// follows.
		baseTime, ok := baseMediaDecodeTime(traf)
		if !ok {
			return fmt.Errorf("%w: traf for track %d has no tfdt", ErrFormat, trackID)
		}

		for _, trun := range findAll(traf, "trun") {
			var err error
			baseTime, err = parseTrun(trun, mdat, &mdatOff, baseDataOffset, baseTime, def, track, out)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// baseMediaDecodeTime reads the tfdt, which anchors the fragment on the track's
// own clock.
func baseMediaDecodeTime(traf []byte) (int64, bool) {
	v, _, rest, ok := fullBox(find(traf, "tfdt"))
	if !ok {
		return 0, false
	}
	switch v {
	case 0:
		if len(rest) < 4 {
			return 0, false
		}
		return int64(be32(rest)), true
	default:
		if len(rest) < 8 {
			return 0, false
		}
		return int64(be64(rest)), true
	}
}

// parseTrun walks one sample-run table, cutting each sample out of mdat, and
// returns the decode time the next run starts at.
func parseTrun(trun, mdat []byte, mdatOff *int, baseDataOffset, dts int64, def trackDefaults, track *Track, out *Fragment) (int64, error) {
	v, flags, rest, ok := fullBox(trun)
	if !ok || len(rest) < 4 {
		return dts, fmt.Errorf("%w: short trun", ErrFormat)
	}
	count := int(be32(rest))
	off := 4
	// data_offset is relative to the moof, but this package cuts samples out of
	// the mdat PAYLOAD sequentially instead: an offset into the whole segment
	// would need the moof's own position, and every packager writes the samples
	// in trun order anyway. A base-data-offset that says otherwise is refused
	// rather than silently mis-cut.
	if flags&0x000001 != 0 {
		off += 4
	}
	if baseDataOffset > 0 {
		return dts, fmt.Errorf("%w: trun with an explicit base data offset", ErrFormat)
	}
	var firstFlags uint32
	hasFirstFlags := flags&0x000004 != 0
	if hasFirstFlags {
		if len(rest) < off+4 {
			return dts, fmt.Errorf("%w: short trun first-sample flags", ErrFormat)
		}
		firstFlags = be32(rest[off:])
		off += 4
	}

	for i := 0; i < count; i++ {
		dur, size, sflags := def.duration, def.size, def.flags
		var cts int32
		if flags&0x000100 != 0 { // sample-duration-present
			if len(rest) < off+4 {
				return dts, fmt.Errorf("%w: trun ends inside its table", ErrFormat)
			}
			dur = be32(rest[off:])
			off += 4
		}
		if flags&0x000200 != 0 { // sample-size-present
			if len(rest) < off+4 {
				return dts, fmt.Errorf("%w: trun ends inside its table", ErrFormat)
			}
			size = be32(rest[off:])
			off += 4
		}
		if flags&0x000400 != 0 { // sample-flags-present
			if len(rest) < off+4 {
				return dts, fmt.Errorf("%w: trun ends inside its table", ErrFormat)
			}
			sflags = be32(rest[off:])
			off += 4
		}
		if flags&0x000800 != 0 { // sample-composition-time-offset-present
			if len(rest) < off+4 {
				return dts, fmt.Errorf("%w: trun ends inside its table", ErrFormat)
			}
			// Signed from version 1: a frame may be displayed BEFORE the frame
			// it is decoded after.
			if v == 0 {
				cts = int32(be32(rest[off:]))
			} else {
				cts = int32(int32(be32(rest[off:])))
			}
			off += 4
		}
		if i == 0 && hasFirstFlags {
			sflags = firstFlags
		}
		if int(size) < 0 || *mdatOff+int(size) > len(mdat) {
			return dts, fmt.Errorf("%w: sample %d of %d wants %d bytes, %d remain in mdat",
				ErrFormat, i, count, size, len(mdat)-*mdatOff)
		}
		s := Sample{
			Data: mdat[*mdatOff : *mdatOff+int(size)],
			DTS:  dts,
			PTS:  dts + int64(cts),
			// sample_is_non_sync_sample is bit 16; a sample that is not
			// non-sync is a random-access point. Audio has no such flag in
			// practice and every frame is an entry point.
			Sync: sflags&0x00010000 == 0,
		}
		*mdatOff += int(size)
		dts += int64(dur)
		switch track.Kind {
		case KindVideo:
			out.Video = append(out.Video, s)
		case KindAudio:
			s.Sync = true
			out.Audio = append(out.Audio, s)
		}
	}
	return dts, nil
}

func trackByID(in *Init, id uint32) *Track {
	for i := range in.Tracks {
		if in.Tracks[i].ID == id {
			return &in.Tracks[i]
		}
	}
	return nil
}
