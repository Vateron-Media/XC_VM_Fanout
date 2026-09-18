// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsmux

import (
	"errors"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// adtsFrame builds one ADTS AAC frame of n payload bytes at 48 kHz, stereo.
func adtsFrame(n int) []byte {
	total := adtsHeaderLen + n
	h := make([]byte, total)
	h[0] = 0xFF
	h[1] = 0xF1 // MPEG-4, layer 00, no CRC
	h[2] = 0x40 | (3 << 2) | 0x00
	h[3] = byte(0x80 | (total>>11)&0x03)
	h[4] = byte(total >> 3)
	h[5] = byte((total&0x07)<<5) | 0x1F
	h[6] = 0xFC
	for i := adtsHeaderLen; i < total; i++ {
		h[i] = byte(i)
	}
	return h
}

// adtsSegment is a packed-audio segment of count frames.
func adtsSegment(count, payload int) []byte {
	var out []byte
	for i := 0; i < count; i++ {
		out = append(out, adtsFrame(payload)...)
	}
	return out
}

// The output has to be MPEG-TS by the daemon's OWN reading of it, not by this
// package's opinion: internal/tspes is what every downstream path uses, so it
// is the right judge of whether the tables, the PES and the clock are real.
func TestMuxedSegmentParsesAsMPEGTS(t *testing.T) {
	m := New()
	ts, err := m.Segment(nil, adtsSegment(40, 380))
	if err != nil {
		t.Fatalf("Segment: %v", err)
	}
	if len(ts)%packetSize != 0 {
		t.Fatalf("output is %d bytes, not a whole number of %d-byte packets", len(ts), packetSize)
	}

	var sawPAT, sawPMT, sawPCR, sawPTS, sawRAP bool
	var foundPMTPID, foundVideoless uint16
	var pcr, firstPTS int64
	for off := 0; off+packetSize <= len(ts); off += packetSize {
		pkt := ts[off : off+packetSize]
		if pkt[0] != syncByte {
			t.Fatalf("packet at %d does not start with the sync byte", off)
		}
		switch pid := tspes.PID(pkt); pid {
		case 0x0000:
			sawPAT = true
			if p := tspes.PMTPID(pkt); p != 0 {
				foundPMTPID = p
			}
		case pmtPID:
			sawPMT = true
			es, ok := tspes.ParsePMTStreams(pkt)
			if !ok || len(es) != 1 {
				t.Fatalf("PMT declared %v (ok=%v), want one stream", es, ok)
			}
			if es[0].PID != audioPID || es[0].Type != streamTypeAAC {
				t.Errorf("PMT declares pid=%#x type=%#x, want %#x/%#x", es[0].PID, es[0].Type, uint16(audioPID), byte(streamTypeAAC))
			}
			foundVideoless = es[0].PID
		case audioPID:
			if v, ok := tspes.PCR(pkt); ok && !sawPCR {
				sawPCR, pcr = true, v
			}
			if v, ok := tspes.PTS(pkt); ok && !sawPTS {
				sawPTS, firstPTS = true, v
			}
			// random_access_indicator on the segment's first audio packet.
			if pkt[3]&0x20 != 0 && pkt[4] > 0 && pkt[5]&0x40 != 0 {
				sawRAP = true
			}
		default:
			t.Fatalf("unexpected PID %#x in the output", pid)
		}
	}
	if !sawPAT || !sawPMT {
		t.Fatalf("tables missing: PAT=%v PMT=%v", sawPAT, sawPMT)
	}
	if foundPMTPID != pmtPID {
		t.Errorf("PAT points at PMT pid %#x, want %#x", foundPMTPID, uint16(pmtPID))
	}
	if foundVideoless != audioPID {
		t.Errorf("PMT audio pid %#x, want %#x", foundVideoless, uint16(audioPID))
	}
	if !sawPCR || !sawPTS {
		t.Fatalf("clock missing: PCR=%v PTS=%v", sawPCR, sawPTS)
	}
	if firstPTS != 0 || pcr != 0 {
		t.Errorf("first PTS=%d, first PCR=%d, want the stream to start at 0", firstPTS, pcr)
	}
	if !sawRAP {
		t.Error("no random-access indicator: the ring cannot cut a block at a segment boundary")
	}
}

// The clock must run forward across segments at the rate the frames imply —
// 1024 samples per frame — or the ring's own clock reads a source that jumps.
func TestTheClockAdvancesWithTheAudio(t *testing.T) {
	const frames = 50
	m := New()
	if _, err := m.Segment(nil, adtsSegment(frames, 200)); err != nil {
		t.Fatalf("first segment: %v", err)
	}
	afterFirst := m.pts
	want := int64(frames) * aacSamplesPerFrame * 90000 / 48000
	if afterFirst != want {
		t.Errorf("clock advanced %d ticks over %d frames at 48 kHz, want %d", afterFirst, frames, want)
	}

	ts, err := m.Segment(nil, adtsSegment(frames, 200))
	if err != nil {
		t.Fatalf("second segment: %v", err)
	}
	// The second segment's first PTS continues from the first.
	for off := 0; off+packetSize <= len(ts); off += packetSize {
		pkt := ts[off : off+packetSize]
		if tspes.PID(pkt) != audioPID {
			continue
		}
		if v, ok := tspes.PTS(pkt); ok {
			if v != afterFirst {
				t.Errorf("second segment opens at PTS %d, want %d: the wire jumps between segments", v, afterFirst)
			}
			return
		}
	}
	t.Fatal("no PTS in the second segment")
}

// End to end through the ring: what the muxer emits must be something tsjoin
// can cut blocks from and hand to a viewer, which is the only reason this
// package exists.
func TestTheRingAcceptsMuxedAudio(t *testing.T) {
	m := New()
	s := tsjoin.New(1<<20, 40000)
	for i := 0; i < 6; i++ {
		ts, err := m.Segment(nil, adtsSegment(45, 300))
		if err != nil {
			t.Fatalf("segment %d: %v", i, err)
		}
		s.Update(ts)
	}
	audioPkts, _, hasAudio := s.Counters()
	if !hasAudio {
		t.Error("the ring did not see an audio stream in the muxed output")
	}
	if audioPkts == 0 {
		t.Error("the ring counted no audio packets")
	}
	if _, span, gops := s.RingStats(); gops < 2 || span <= 0 {
		t.Errorf("ring holds %d blocks spanning %d ms: the muxed stream is not being cut into blocks", gops, span)
	}
}

// A body that is not ADTS must be refused rather than muxed into noise: an
// origin answering a .aac URL with an error page is the ordinary way to get
// here.
func TestNonADTSBodiesAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"an HTML error page", []byte("<html>over connection limit</html>")},
		{"MPEG-TS, not packed audio", func() []byte { b := make([]byte, 188); b[0] = 0x47; return b }()},
		{"empty", nil},
		{"a truncated frame", adtsFrame(300)[:20]},
		{"a reserved sampling frequency", func() []byte { f := adtsFrame(100); f[2] |= 0x34; return f }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := New()
			if _, err := m.Segment(nil, tc.body); !errors.Is(err, ErrNotADTS) {
				t.Errorf("err = %v, want ErrNotADTS", err)
			}
		})
	}
}

func TestLooksLikeADTS(t *testing.T) {
	if !LooksLikeADTS(adtsFrame(100)) {
		t.Error("a real ADTS frame was not recognised")
	}
	for _, b := range [][]byte{nil, []byte("<html>"), {0x47, 0x40, 0x11, 0x10}} {
		if LooksLikeADTS(b) {
			t.Errorf("%q was taken for ADTS", b)
		}
	}
}

// RFC 8216 packed audio carries an ID3v2 tag in front of the frames, holding
// the upstream's timestamp. It must be stepped over: parsed as audio it is not
// ADTS, and the segment would be refused.
func TestAnID3TagIsSteppedOver(t *testing.T) {
	// A 30-byte ID3v2 tag: "ID3", version, flags, then a syncsafe size of 20.
	tag := append([]byte{'I', 'D', '3', 0x04, 0x00, 0x00, 0, 0, 0, 20}, make([]byte, 20)...)
	body := append(append([]byte{}, tag...), adtsSegment(5, 100)...)

	if !LooksLikeADTS(body) {
		t.Error("a packed-audio segment with an ID3 tag was not recognised")
	}
	m := New()
	ts, err := m.Segment(nil, body)
	if err != nil {
		t.Fatalf("Segment: %v", err)
	}
	if len(ts) == 0 || len(ts)%packetSize != 0 {
		t.Fatalf("output is %d bytes", len(ts))
	}
	// The tag's bytes must not have reached the wire as audio.
	if m.pts != 5*aacSamplesPerFrame*90000/48000 {
		t.Errorf("clock advanced %d ticks, want exactly the five frames' worth", m.pts)
	}
}

// The ID3 size is a syncsafe integer that the 10-byte header length is ADDED
// to. A size whose bits overlap the header length is what tells the two apart:
// size 10 must skip 20 bytes, not 10.
func TestID3SizeIsAddedToTheHeaderLength(t *testing.T) {
	for _, size := range []int{1, 10, 11, 26, 127} {
		tag := append([]byte{'I', 'D', '3', 0x04, 0x00, 0x00, 0, 0, 0, byte(size)}, make([]byte, size)...)
		body := append(append([]byte{}, tag...), adtsSegment(3, 100)...)
		if got := skipID3(body); len(got) != len(body)-10-size {
			t.Errorf("size %d: skipped %d bytes, want %d", size, len(body)-len(got), 10+size)
		}
		if !LooksLikeADTS(body) {
			t.Errorf("size %d: the frames after the tag were not found", size)
		}
	}
}

// Save/Restore make a segment transactional on the muxer: a caller that cannot
// finish a segment rolls the counters and clock back, so the next segment does
// not skip continuity and a first failed segment still reads as not-started.
func TestSaveRestoreRollsBackTheMuxer(t *testing.T) {
	m := NewAV()
	if m.Started() {
		t.Fatal("a fresh muxer should not be started")
	}
	saved := m.Save()

	// Write a keyframe and some audio — the counters and clock advance and the
	// muxer is now started.
	var dst []byte
	dst = m.Tables(dst)
	dst = m.WriteVideo(dst, []byte{0, 0, 0, 1, 0x65, 1}, 9000, 9000, true)
	dst = m.WriteAudio(dst, []byte{0xFF, 0xF1, 0x40, 0x80, 0x00, 0x1F, 0xFC, 1}, 9000)
	if !m.Started() {
		t.Fatal("the muxer did not register the writes")
	}
	after := m.Save()
	if after == saved {
		t.Fatal("Save captured no change after writing a segment")
	}

	m.Restore(saved)
	if m.Started() {
		t.Error("Restore did not clear started")
	}
	if m.Save() != saved {
		t.Error("Restore did not return the muxer to the saved state")
	}

	// And a segment written after a rollback opens exactly as the rolled-back one
	// would have — same continuity, same clock.
	var a, b []byte
	m.Restore(saved)
	a = m.WriteVideo(m.Tables(a), []byte{0, 0, 0, 1, 0x65, 2}, 9000, 9000, true)
	m.Restore(saved)
	b = m.WriteVideo(m.Tables(b), []byte{0, 0, 0, 1, 0x65, 2}, 9000, 9000, true)
	if string(a) != string(b) {
		t.Error("two segments from the same saved state differ: rollback is not clean")
	}
}

func TestCRC32MPEGCheckValue(t *testing.T) {
	// The published CRC-32/MPEG-2 check value for ASCII "123456789". A wrong CRC
	// is a section a strict set-top box drops, so this is pinned against the
	// standard rather than against our own output.
	if got := crc32MPEG([]byte("123456789")); got != 0x0376E6E7 {
		t.Fatalf("crc32MPEG = %#08x, want 0x0376E6E7", got)
	}
}

// The packed-audio clock must not drift: 1024*90000/rate does not divide evenly
// at most sample rates, and truncating each frame lost ~0.4% — minutes of
// radio drift over an hour. Over many frames the emitted PTS must stay within a
// tick of the exact time.
func TestPackedAudioClockDoesNotDrift(t *testing.T) {
	const rate = 44100 // the awkward one: 1024*90000/44100 = 2089.79…
	m := New()
	// 4000 frames ≈ 93 s of audio.
	seg := make([]byte, 0)
	for i := 0; i < 4000; i++ {
		seg = append(seg, adtsFrameRate(200, rate)...)
	}
	if _, err := m.Segment(nil, seg); err != nil {
		t.Fatalf("Segment: %v", err)
	}
	exact := int64(4000) * 1024 * 90000 / rate
	if diff := m.pts - exact; diff < -1 || diff > 1 {
		t.Errorf("after 4000 frames the clock is %d, exact is %d (drift %d ticks = %d ms)",
			m.pts, exact, diff, diff/90)
	}
}

// adtsFrameRate is adtsFrame at a chosen sample rate.
func adtsFrameRate(n, rate int) []byte {
	idx := map[int]byte{96000: 0, 88200: 1, 64000: 2, 48000: 3, 44100: 4, 32000: 5, 24000: 6, 22050: 7, 16000: 8, 12000: 9, 11025: 10, 8000: 11}[rate]
	total := 7 + n
	h := make([]byte, total)
	h[0], h[1] = 0xFF, 0xF1
	h[2] = 0x40 | (idx << 2)
	h[3] = byte(0x80 | (total>>11)&0x03)
	h[4] = byte(total >> 3)
	h[5] = byte((total&0x07)<<5) | 0x1F
	h[6] = 0xFC
	return h
}
