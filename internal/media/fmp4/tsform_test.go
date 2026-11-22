// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package fmp4

import (
	"bytes"
	"errors"
	"testing"
)

// A keyframe has to carry the parameter sets a live viewer never saw: fMP4
// states them once, in an init segment that arrived before the viewer did.
func TestAnnexBPutsParameterSetsBeforeAKeyframe(t *testing.T) {
	in, err := ParseInit(initSegment())
	if err != nil {
		t.Fatal(err)
	}
	v := in.Video()

	idr := Sample{Data: avcc([]byte{0x65, 0xAA}), Sync: true}
	got, err := AnnexB(nil, idr, v)
	if err != nil {
		t.Fatalf("AnnexB: %v", err)
	}
	want := bytes.Join([][]byte{
		startCode, testSPS,
		startCode, testPPS,
		startCode, {0x65, 0xAA},
	}, nil)
	if !bytes.Equal(got, want) {
		t.Errorf("keyframe =\n %x\nwant\n %x", got, want)
	}

	// A non-sync picture carries no parameter sets: repeating them on every
	// frame would be bytes on the wire for nothing.
	inter := Sample{Data: avcc([]byte{0x41, 0xBB})}
	got, err = AnnexB(nil, inter, v)
	if err != nil {
		t.Fatalf("AnnexB: %v", err)
	}
	if !bytes.Equal(got, append(append([]byte{}, startCode...), 0x41, 0xBB)) {
		t.Errorf("inter frame = %x", got)
	}
}

// Several NALs in one sample all become start-code units, in order.
func TestAnnexBSplitsEveryNAL(t *testing.T) {
	in, _ := ParseInit(initSegment())
	v := in.Video()
	s := Sample{Data: append(avcc([]byte{0x41, 1}), avcc([]byte{0x41, 2, 3})...)}
	got, err := AnnexB(nil, s, v)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join([][]byte{startCode, {0x41, 1}, startCode, {0x41, 2, 3}}, nil)
	if !bytes.Equal(got, want) {
		t.Errorf("got %x, want %x", got, want)
	}
}

// A length prefix that runs past the sample is a refusal, not a slice out of
// range: the bytes come off the network.
func TestAnnexBRefusesAMalformedSample(t *testing.T) {
	in, _ := ParseInit(initSegment())
	v := in.Video()
	for _, data := range [][]byte{
		{0x00, 0x00, 0x00, 0x10, 0x41}, // says 16 bytes, has 1
		{0x00, 0x00},                   // ends inside the prefix
	} {
		if _, err := AnnexB(nil, Sample{Data: data}, v); !errors.Is(err, ErrFormat) {
			t.Errorf("data %x: err = %v, want a format refusal", data, err)
		}
	}
}

// The ADTS header has to say what the init segment said, or a decoder reads the
// audio at the wrong rate — which sounds like the channel is broken.
func TestADTSHeaderCarriesTheTrackConfig(t *testing.T) {
	in, _ := ParseInit(initSegment())
	a := in.Audio()
	frame := []byte{1, 2, 3, 4, 5}
	got, err := ADTS(nil, frame, a)
	if err != nil {
		t.Fatalf("ADTS: %v", err)
	}
	if len(got) != adtsHeaderLen+len(frame) {
		t.Fatalf("header + frame = %d bytes, want %d", len(got), adtsHeaderLen+len(frame))
	}
	if got[0] != 0xFF || got[1]&0xF0 != 0xF0 {
		t.Errorf("no sync word: %x", got[:2])
	}
	if p := (got[2] >> 6) & 0x03; p != a.AACProfile {
		t.Errorf("profile = %d, want %d", p, a.AACProfile)
	}
	if r := (got[2] >> 2) & 0x0F; r != a.SampleRateIdx {
		t.Errorf("sample rate index = %d, want %d", r, a.SampleRateIdx)
	}
	if c := (got[2]&0x01)<<2 | (got[3]>>6)&0x03; c != a.Channels {
		t.Errorf("channels = %d, want %d", c, a.Channels)
	}
	if n := int(got[3]&0x03)<<11 | int(got[4])<<3 | int(got[5])>>5; n != len(got) {
		t.Errorf("frame length field = %d, want %d", n, len(got))
	}
	if !bytes.Equal(got[adtsHeaderLen:], frame) {
		t.Error("the frame was changed")
	}
}

// Timestamps have to land on the 90 kHz clock, which is the only one anything
// downstream of the puller understands.
func TestScaleToThe90kHzClock(t *testing.T) {
	for _, tc := range []struct {
		t         int64
		timescale uint32
		want      int64
	}{
		{0, 90000, 0},
		{9000, 90000, 9000},   // already 90 kHz
		{48000, 48000, 90000}, // one second of audio
		{1024, 48000, 1920},   // one AAC frame
		{30000, 30000, 90000}, // one second at a 30 kHz video timescale
		{5, 0, 0},             // a track with no timescale says nothing
	} {
		if got := Scale(tc.t, tc.timescale); got != tc.want {
			t.Errorf("Scale(%d, %d) = %d, want %d", tc.t, tc.timescale, got, tc.want)
		}
	}
}
