// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package fmp4

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// ── fixture builders: the CMAF subset an HLS packager writes ────────────────

func mkBox(typ string, payload ...[]byte) []byte {
	var body []byte
	for _, p := range payload {
		body = append(body, p...)
	}
	out := make([]byte, 8, 8+len(body))
	binary.BigEndian.PutUint32(out, uint32(8+len(body)))
	copy(out[4:], typ)
	return append(out, body...)
}

func u32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func u64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

// avcC with one SPS and one PPS, 4-byte NAL length prefixes.
func avcC(sps, pps []byte) []byte {
	b := []byte{0x01, 0x64, 0x00, 0x1F, 0xFF | 0x00} // configurationVersion, profile…, lengthSizeMinusOne=3
	b[4] = 0xFC | 0x03
	b = append(b, 0xE0|0x01)                         // one SPS
	b = append(b, byte(len(sps)>>8), byte(len(sps))) //
	b = append(b, sps...)
	b = append(b, 0x01)                              // one PPS
	b = append(b, byte(len(pps)>>8), byte(len(pps))) //
	b = append(b, pps...)
	return mkBox("avcC", b)
}

// esds carrying an AudioSpecificConfig: AAC-LC, 48 kHz, stereo.
func esds(asc []byte) []byte {
	dsi := append([]byte{0x05, byte(len(asc))}, asc...)
	dcd := append([]byte{0x04, byte(13 + len(dsi)), 0x40, 0x15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, dsi...)
	esd := append([]byte{0x03, byte(3 + len(dcd)), 0x00, 0x01, 0x00}, dcd...)
	return mkBox("esds", append([]byte{0, 0, 0, 0}, esd...))
}

func videoTrak(id, timescale uint32, sps, pps []byte) []byte {
	tkhd := mkBox("tkhd", []byte{0, 0, 0, 1}, u32(0), u32(0), u32(id), make([]byte, 60))
	mdhd := mkBox("mdhd", []byte{0, 0, 0, 0}, u32(0), u32(0), u32(timescale), u32(0), make([]byte, 4))
	hdlr := mkBox("hdlr", []byte{0, 0, 0, 0}, u32(0), []byte("vide"), make([]byte, 12))
	entry := mkBox("avc1", append(make([]byte, 78), avcC(sps, pps)...))
	stsd := mkBox("stsd", []byte{0, 0, 0, 0}, u32(1), entry)
	stbl := mkBox("stbl", stsd)
	minf := mkBox("minf", stbl)
	mdia := mkBox("mdia", mdhd, hdlr, minf)
	return mkBox("trak", tkhd, mdia)
}

func audioTrak(id, timescale uint32, asc []byte) []byte {
	tkhd := mkBox("tkhd", []byte{0, 0, 0, 1}, u32(0), u32(0), u32(id), make([]byte, 60))
	mdhd := mkBox("mdhd", []byte{0, 0, 0, 0}, u32(0), u32(0), u32(timescale), u32(0), make([]byte, 4))
	hdlr := mkBox("hdlr", []byte{0, 0, 0, 0}, u32(0), []byte("soun"), make([]byte, 12))
	entry := mkBox("mp4a", append(make([]byte, 28), esds(asc)...))
	stsd := mkBox("stsd", []byte{0, 0, 0, 0}, u32(1), entry)
	stbl := mkBox("stbl", stsd)
	minf := mkBox("minf", stbl)
	mdia := mkBox("mdia", mdhd, hdlr, minf)
	return mkBox("trak", tkhd, mdia)
}

var (
	testSPS = []byte{0x67, 0x64, 0x00, 0x1F, 0xAC, 0xD9}
	testPPS = []byte{0x68, 0xEB, 0xE3, 0xCB}
	testASC = []byte{0x11, 0x90} // AAC-LC (2), index 3 (48 kHz), 2 channels
)

func initSegment() []byte {
	ftyp := mkBox("ftyp", []byte("isom"), u32(512), []byte("isomiso2avc1mp41"))
	moov := mkBox("moov",
		mkBox("mvhd", []byte{0, 0, 0, 0}, make([]byte, 96)),
		videoTrak(1, 90000, testSPS, testPPS),
		audioTrak(2, 48000, testASC),
	)
	return append(ftyp, moov...)
}

// mediaSegment builds a moof+mdat carrying the given video and audio samples.
// Video samples are AVCC: a 4-byte length then the NAL.
func mediaSegment(baseDTS int64, video [][]byte, videoDur uint32, audio [][]byte, audioDur uint32) []byte {
	var mdat []byte

	trunFor := func(samples [][]byte, dur uint32, sync func(int) bool, cts int32) []byte {
		// flags: data-offset(0x1) + sample-duration(0x100) + size(0x200) +
		// flags(0x400) + composition offset(0x800)
		const flags = 0x000001 | 0x000100 | 0x000200 | 0x000400 | 0x000800
		body := []byte{1, byte(flags >> 16), byte(flags >> 8), byte(flags & 0xFF)}
		body = append(body, u32(uint32(len(samples)))...)
		body = append(body, u32(0)...) // data_offset
		for i, s := range samples {
			var sflags uint32
			if !sync(i) {
				sflags = 0x00010000 // sample_is_non_sync_sample
			}
			body = append(body, u32(dur)...)
			body = append(body, u32(uint32(len(s)))...)
			body = append(body, u32(sflags)...)
			body = append(body, u32(uint32(cts))...)
			mdat = append(mdat, s...)
		}
		return mkBox("trun", body)
	}

	var trafs [][]byte
	if len(video) > 0 {
		tfhd := mkBox("tfhd", []byte{0, 0, 0, 0}, u32(1))
		tfdt := mkBox("tfdt", []byte{1, 0, 0, 0}, u64(uint64(baseDTS)))
		trafs = append(trafs, mkBox("traf", tfhd, tfdt,
			trunFor(video, videoDur, func(i int) bool { return i == 0 }, 0)))
	}
	if len(audio) > 0 {
		tfhd := mkBox("tfhd", []byte{0, 0, 0, 0}, u32(2))
		tfdt := mkBox("tfdt", []byte{1, 0, 0, 0}, u64(uint64(baseDTS)))
		trafs = append(trafs, mkBox("traf", tfhd, tfdt,
			trunFor(audio, audioDur, func(int) bool { return true }, 0)))
	}
	moof := mkBox("moof", append([][]byte{mkBox("mfhd", []byte{0, 0, 0, 0}, u32(1))}, trafs...)...)
	return append(moof, mkBox("mdat", mdat)...)
}

func avcc(nal []byte) []byte {
	return append(u32(uint32(len(nal))), nal...)
}

// ── tests ───────────────────────────────────────────────────────────────────

// The init segment is where fMP4 keeps everything MPEG-TS repeats inline: the
// timescales, and the parameter sets a decoder needs before a keyframe.
func TestParseInitReadsBothTracks(t *testing.T) {
	in, err := ParseInit(initSegment())
	if err != nil {
		t.Fatalf("ParseInit: %v", err)
	}
	v := in.Video()
	if v == nil {
		t.Fatal("no video track")
	}
	if v.ID != 1 || v.TimeScale != 90000 {
		t.Errorf("video track id=%d timescale=%d, want 1/90000", v.ID, v.TimeScale)
	}
	if v.NALULengthSize != 4 {
		t.Errorf("NAL length size = %d, want 4", v.NALULengthSize)
	}
	if len(v.SPS) != 1 || !bytes.Equal(v.SPS[0], testSPS) {
		t.Errorf("SPS = %x, want %x", v.SPS, testSPS)
	}
	if len(v.PPS) != 1 || !bytes.Equal(v.PPS[0], testPPS) {
		t.Errorf("PPS = %x, want %x", v.PPS, testPPS)
	}

	a := in.Audio()
	if a == nil {
		t.Fatal("no audio track")
	}
	if a.ID != 2 || a.TimeScale != 48000 {
		t.Errorf("audio track id=%d timescale=%d, want 2/48000", a.ID, a.TimeScale)
	}
	// AAC-LC at 48 kHz, stereo: the three fields an ADTS header carries.
	if a.AACProfile != 1 || a.SampleRateIdx != 3 || a.Channels != 2 {
		t.Errorf("AAC profile=%d rateIdx=%d channels=%d, want 1/3/2", a.AACProfile, a.SampleRateIdx, a.Channels)
	}
}

// A media segment's samples come back in order, with the timing its tables
// give them — including a composition offset, which is the whole reason PTS
// and DTS are carried separately.
func TestParseFragmentReadsSamplesAndTiming(t *testing.T) {
	in, err := ParseInit(initSegment())
	if err != nil {
		t.Fatal(err)
	}
	video := [][]byte{avcc([]byte{0x65, 1, 2, 3}), avcc([]byte{0x41, 4, 5}), avcc([]byte{0x41, 6})}
	audio := [][]byte{{0xDE, 0xAD}, {0xBE, 0xEF}}
	seg := mediaSegment(9000, video, 3000, audio, 1024)

	f, err := ParseFragment(seg, in)
	if err != nil {
		t.Fatalf("ParseFragment: %v", err)
	}
	if len(f.Video) != 3 || len(f.Audio) != 2 {
		t.Fatalf("got %d video and %d audio samples, want 3 and 2", len(f.Video), len(f.Audio))
	}
	for i, s := range f.Video {
		if want := int64(9000 + 3000*i); s.DTS != want {
			t.Errorf("video sample %d DTS = %d, want %d", i, s.DTS, want)
		}
		if s.PTS != s.DTS {
			t.Errorf("video sample %d PTS = %d, want %d with a zero composition offset", i, s.PTS, s.DTS)
		}
	}
	if !f.Video[0].Sync {
		t.Error("the first video sample is the keyframe and must be marked sync")
	}
	if f.Video[1].Sync || f.Video[2].Sync {
		t.Error("non-sync samples were marked as random-access points")
	}
	if !bytes.Equal(f.Video[0].Data, video[0]) {
		t.Errorf("first video sample = %x, want %x", f.Video[0].Data, video[0])
	}
	if !bytes.Equal(f.Audio[1].Data, audio[1]) {
		t.Errorf("second audio sample = %x, want %x", f.Audio[1].Data, audio[1])
	}
	if want := int64(9000 + 1024); f.Audio[1].DTS != want {
		t.Errorf("second audio sample DTS = %d, want %d", f.Audio[1].DTS, want)
	}
}

// Several fragments in one segment are read in order: a packager may write one
// moof+mdat pair per track, or per group of frames.
func TestSeveralFragmentsInOneSegment(t *testing.T) {
	in, err := ParseInit(initSegment())
	if err != nil {
		t.Fatal(err)
	}
	a := mediaSegment(0, [][]byte{avcc([]byte{0x65, 1})}, 3000, nil, 0)
	b := mediaSegment(3000, [][]byte{avcc([]byte{0x41, 2})}, 3000, nil, 0)
	f, err := ParseFragment(append(a, b...), in)
	if err != nil {
		t.Fatalf("ParseFragment: %v", err)
	}
	if len(f.Video) != 2 {
		t.Fatalf("got %d samples, want 2", len(f.Video))
	}
	if f.Video[0].DTS != 0 || f.Video[1].DTS != 3000 {
		t.Errorf("DTS = %d,%d, want 0,3000", f.Video[0].DTS, f.Video[1].DTS)
	}
}

// Everything this package cannot read must come back as a format refusal, so
// the caller hands the source to ffmpeg rather than retrying it forever.
func TestUnreadableInputIsRefused(t *testing.T) {
	in, _ := ParseInit(initSegment())
	for _, tc := range []struct {
		name string
		body []byte
		init bool
	}{
		{"not MP4 at all", []byte("<html>error</html>"), true},
		{"a box claiming more than it has", append(u32(1<<20), []byte("moov")...), true},
		{"an init with no moov", mkBox("ftyp", []byte("isom")), true},
		{"a truncated header", []byte{0, 0, 0}, true},
		{"an mdat with no moof", mkBox("mdat", []byte{1, 2, 3}), false},
		{"a segment with no samples", mkBox("moof", mkBox("mfhd", []byte{0, 0, 0, 0}, u32(1))), false},
		{"a trun wanting more bytes than the mdat holds", func() []byte {
			tfhd := mkBox("tfhd", []byte{0, 0, 0, 0}, u32(1))
			tfdt := mkBox("tfdt", []byte{1, 0, 0, 0}, u64(0))
			const flags = 0x000001 | 0x000100 | 0x000200
			body := append([]byte{1, byte(flags >> 16), byte(flags >> 8), byte(flags & 0xFF)}, u32(1)...)
			body = append(body, u32(0)...)
			body = append(body, u32(3000)...)
			body = append(body, u32(9999)...) // far more than the mdat below
			moof := mkBox("moof", mkBox("mfhd", []byte{0, 0, 0, 0}, u32(1)),
				mkBox("traf", tfhd, tfdt, mkBox("trun", body)))
			return append(moof, mkBox("mdat", []byte{1, 2, 3})...)
		}(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.init {
				_, err = ParseInit(tc.body)
			} else {
				_, err = ParseFragment(tc.body, in)
			}
			if err == nil {
				t.Fatal("accepted something unreadable")
			}
			if !errors.Is(err, ErrFormat) {
				t.Errorf("err = %v, want it to wrap ErrFormat", err)
			}
		})
	}
}

// A traf with no tfdt has nothing anchoring it to the track's clock. Guessing
// would put the segment at the wrong time on a wire everything else follows.
func TestAFragmentWithoutATFDTIsRefused(t *testing.T) {
	in, _ := ParseInit(initSegment())
	tfhd := mkBox("tfhd", []byte{0, 0, 0, 0}, u32(1))
	const flags = 0x000001 | 0x000100 | 0x000200
	body := append([]byte{1, byte(flags >> 16), byte(flags >> 8), byte(flags & 0xFF)}, u32(1)...)
	body = append(body, u32(0)...)
	body = append(body, u32(3000)...)
	body = append(body, u32(2)...)
	moof := mkBox("moof", mkBox("mfhd", []byte{0, 0, 0, 0}, u32(1)),
		mkBox("traf", tfhd, mkBox("trun", body)))
	seg := append(moof, mkBox("mdat", []byte{1, 2})...)

	if _, err := ParseFragment(seg, in); !errors.Is(err, ErrFormat) {
		t.Errorf("err = %v, want a format refusal", err)
	}
}
