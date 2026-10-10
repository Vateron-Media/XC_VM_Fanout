// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tspes

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// h264AU builds an H.264 access unit as an open-GOP encoder starts one: AUD,
// then the parts asked for, then a non-IDR slice whose header's first byte is
// slice (first_mb_in_slice 0 and the slice_type).
func h264AU(sps bool, sei []byte, slice byte) []byte {
	sc := []byte{0x00, 0x00, 0x01}
	au := []byte{0x09, 0xf0} // the fixture writes the first start code
	if sps {
		au = append(append(au, sc...), 0x67, 0x64, 0x00, 0x28, 0xac)
		au = append(append(au, sc...), 0x68, 0xee, 0x3c, 0x80)
	}
	if sei != nil {
		au = append(append(append(au, sc...), 0x06), sei...)
		au = append(au, 0x80) // rbsp trailing bits
	}
	return append(append(au, sc...), 0x41, slice, 0x84, 0x21)
}

func TestStartsKeyframe(t *testing.T) {
	const iSlice7, iSlice2, pSlice5 = 0x88, 0xb0, 0x98 // 1|0001000, 1|011, 1|00110
	// A buffering period message, then the recovery point, as a broadcast
	// encoder orders them; recovery_frame_cnt is the payload's first ue(v).
	recovery := func(cnt byte) []byte { return []byte{0x00, 0x02, 0x80, 0x00, 0x06, 0x01, cnt} }
	for _, c := range []struct {
		name string
		st   byte
		es   []byte
		want bool
	}{
		{"h264 idr", StreamTypeH264, []byte{0x65}, true},
		{"h264 aud then idr", StreamTypeH264, []byte{0x09, 0xf0, 0x00, 0x00, 0x01, 0x65}, true},
		{"h264 non-idr", StreamTypeH264, []byte{0x41}, false},
		{"h264 sps alone", StreamTypeH264, []byte{0x67}, false},
		{"h264 recovery point, I slice (type 7)", StreamTypeH264, h264AU(true, recovery(0x84), iSlice7), true},
		{"h264 recovery point, I slice (type 2)", StreamTypeH264, h264AU(true, recovery(0x84), iSlice2), true},
		{"h264 recovery some frames later", StreamTypeH264, h264AU(true, recovery(0x44), iSlice7), false},
		{"h264 I slice without a recovery point", StreamTypeH264, h264AU(true, []byte{0x01, 0x01, 0x10}, iSlice7), false},
		{"h264 I slice with no SEI", StreamTypeH264, h264AU(true, nil, iSlice7), false},
		{"h264 recovery point without an SPS", StreamTypeH264, h264AU(false, recovery(0x84), iSlice7), false},
		{"h264 recovery point on a P slice", StreamTypeH264, h264AU(true, recovery(0x84), pSlice5), false},
		{"h264 recovery point cut short", StreamTypeH264, h264AU(true, []byte{0x00, 0x02, 0x80, 0x00, 0x06}, iSlice7)[:22], false},
		{"hevc idr_w_radl", StreamTypeHEVC, []byte{19 << 1, 0x01}, true},
		{"hevc cra", StreamTypeHEVC, []byte{21 << 1, 0x01}, true},
		{"hevc trail", StreamTypeHEVC, []byte{1 << 1, 0x01}, false},
		{"mpeg2 sequence header", StreamTypeMPEG2, []byte{0xb3}, true},
		{"mpeg2 picture", StreamTypeMPEG2, []byte{0x00}, false},
		{"vc-1 never read", 0xea, []byte{0x65}, false},
	} {
		if got := StartsKeyframe(tsfixture.PESStart(0x101, 0, c.es...), c.st); got != c.want {
			t.Errorf("%s: StartsKeyframe = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestCorruptPacketsDoNotPanic: every length these read comes off the wire.
func TestCorruptPacketsDoNotPanic(t *testing.T) {
	pkt := tsfixture.PESStart(0x101, 0, 0x65)
	pkt[4+8] = 0xff // PES_header_data_length far past the packet
	if StartsKeyframe(pkt, StreamTypeH264) {
		t.Error("a PES header that overruns the packet was read as a keyframe")
	}
	tail := tsfixture.PESStart(0x101, 0)
	for i := len(tail) - 3; i < len(tail); i++ {
		tail[i] = 0 // a start code cut off by the end of the packet
	}
	for _, st := range []byte{StreamTypeH264, StreamTypeHEVC, StreamTypeMPEG2} {
		_ = StartsKeyframe(tail, st)
	}

	af := tsfixture.PAT(0x100)
	af[3] = 0x30 // adaptation + payload …
	af[4] = 0xff // … whose adaptation field claims more than the packet
	if PayloadOffset(af) != -1 || PMTPID(af) != 0 {
		t.Error("an adaptation field that overruns the packet was read through")
	}
	pmt := tsfixture.PMT(0x100, 0x101)
	pmt[4] = 0xff // pointer_field past the packet
	if _, ok := ParsePMT(pmt); ok {
		t.Error("a pointer_field that overruns the packet was read through")
	}
}

func TestPSI(t *testing.T) {
	if got := PMTPID(tsfixture.PAT(0x100)); got != 0x100 {
		t.Errorf("PMTPID = %#x, want 0x100", got)
	}
	m, ok := ParsePMT(tsfixture.PMTType(0x100, 0x101, StreamTypeHEVC))
	if !ok || m.VideoPID != 0x101 || m.VideoType != StreamTypeHEVC {
		t.Errorf("ParsePMT = %+v %v", m, ok)
	}
	if pts, ok := PTS(tsfixture.PESStart(0x101, 123456, 0x65)); !ok || pts != 123456 {
		t.Errorf("PTS = %d %v, want 123456", pts, ok)
	}
	if pcr, ok := PCR(tsfixture.KeyframePCR(0x101, 0, 900000)); !ok || pcr != 900000 {
		t.Errorf("PCR = %d %v, want 900000", pcr, ok)
	}
	if !RAI(tsfixture.Keyframe(0x101, 0)) || RAI(tsfixture.PESStart(0x101, 0, 0x65)) {
		t.Error("RAI misread")
	}
}
