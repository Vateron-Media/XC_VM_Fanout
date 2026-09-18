// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package fmp4

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsmux"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// dts decodes the decode timestamp out of a PES header that carries both
// times. internal/tspes reads the PTS, which is all the daemon needs of a
// normal stream; here the point IS the pair.
func dts(pkt []byte) (int64, bool) {
	o := tspes.PayloadOffset(pkt)
	if o < 0 || o+19 > len(pkt) || pkt[o] != 0 || pkt[o+1] != 0 || pkt[o+2] != 1 {
		return 0, false
	}
	if pkt[o+7]&0xC0 != 0xC0 {
		return 0, false
	}
	b := pkt[o+14 : o+19]
	return int64(b[0]&0x0E)<<29 | int64(b[1])<<22 | int64(b[2]&0xFE)<<14 |
		int64(b[3])<<7 | int64(b[4])>>1, true
}

// toTS is the conversion a caller performs: parse the fragment, turn each
// sample into the shape a TS carries, and mux it on the daemon's clock. It
// lives in the test because it is the composition under test, not API.
func toTS(t *testing.T, m *tsmux.Muxer, in *Init, seg []byte) []byte {
	t.Helper()
	f, err := ParseFragment(seg, in)
	if err != nil {
		t.Fatalf("ParseFragment: %v", err)
	}
	out := m.Tables(nil)
	v, a := in.Video(), in.Audio()
	for _, s := range f.Video {
		annexB, err := AnnexB(nil, s, v)
		if err != nil {
			t.Fatalf("AnnexB: %v", err)
		}
		out = m.WriteVideo(out, annexB,
			Scale(s.PTS, v.TimeScale), Scale(s.DTS, v.TimeScale), s.Sync)
	}
	for _, s := range f.Audio {
		adts, err := ADTS(nil, s.Data, a)
		if err != nil {
			t.Fatalf("ADTS: %v", err)
		}
		out = m.WriteAudio(out, adts, Scale(s.PTS, a.TimeScale))
	}
	return out
}

// The whole point, end to end: a CMAF segment comes out as MPEG-TS that the
// daemon's own parsers read as a programme — tables, both streams, a clock —
// and that the ring cuts blocks from. Those parsers are the right judge,
// because they are what every path downstream of the puller uses.
func TestAnFMP4SegmentBecomesUsableMPEGTS(t *testing.T) {
	in, err := ParseInit(initSegment())
	if err != nil {
		t.Fatalf("ParseInit: %v", err)
	}
	m := tsmux.NewAV()

	var ts []byte
	for i := 0; i < 4; i++ {
		ts = append(ts, toTS(t, m, in, reorderableSegment(int64(i)*9000, false))...)
	}
	if len(ts)%188 != 0 {
		t.Fatalf("output is %d bytes, not whole TS packets", len(ts))
	}

	var sawPAT, sawPMT, sawVideo, sawAudio, sawPCR, sawRAP bool
	var videoStream, audioStream bool
	for off := 0; off+188 <= len(ts); off += 188 {
		pkt := ts[off : off+188]
		switch pid := tspes.PID(pkt); {
		case pid == 0x0000:
			sawPAT = true
		case pid == 0x1000:
			sawPMT = true
			es, ok := tspes.ParsePMTStreams(pkt)
			if ok {
				for _, e := range es {
					switch e.Type {
					case 0x1B:
						videoStream = true
					case 0x0F:
						audioStream = true
					}
				}
			}
		case pid == 0x0100:
			sawVideo = true
			if _, ok := tspes.PCR(pkt); ok {
				sawPCR = true
			}
			if pkt[3]&0x20 != 0 && pkt[4] > 0 && pkt[5]&0x40 != 0 {
				sawRAP = true
			}
		case pid == 0x0101:
			sawAudio = true
		}
	}
	for name, ok := range map[string]bool{
		"PAT": sawPAT, "PMT": sawPMT, "video packets": sawVideo, "audio packets": sawAudio,
		"a PCR": sawPCR, "a random-access point": sawRAP,
		"video declared in the PMT": videoStream, "audio declared in the PMT": audioStream,
	} {
		if !ok {
			t.Errorf("the muxed stream carries no %s", name)
		}
	}

	// And the ring reads it as a programme it can serve.
	s := tsjoin.New(4<<20, 40000)
	s.Update(ts)
	if _, _, hasAudio := s.Counters(); !hasAudio {
		t.Error("the ring found no audio stream")
	}
	if _, _, gops := s.RingStats(); gops < 2 {
		t.Errorf("the ring cut %d blocks: keyframes are not being recognised", gops)
	}
}

// A picture stored out of display order must keep its two timestamps apart: a
// TS that collapses them plays, but reorders B-frames wrongly.
func TestCompositionOffsetSurvivesAsAPTSDTSPair(t *testing.T) {
	in, err := ParseInit(initSegment())
	if err != nil {
		t.Fatal(err)
	}
	m := tsmux.NewAV()
	ts := toTS(t, m, in, reorderableSegment(0, true))

	var pairs int
	for off := 0; off+188 <= len(ts); off += 188 {
		pkt := ts[off : off+188]
		if tspes.PID(pkt) != 0x0100 {
			continue
		}
		pts, ok := tspes.PTS(pkt)
		if !ok {
			continue
		}
		// A PES header carrying both times has PTS_DTS_flags = 11.
		o := tspes.PayloadOffset(pkt)
		if o < 0 || o+9 > len(pkt) {
			continue
		}
		if pkt[o+7]&0xC0 == 0xC0 {
			pairs++
			d, _ := dts(pkt)
			if pts <= d {
				t.Errorf("a reordered picture has PTS %d and DTS %d: the offset was lost", pts, d)
			}
		}
	}
	if pairs == 0 {
		t.Error("no picture carried both a PTS and a DTS")
	}
}
