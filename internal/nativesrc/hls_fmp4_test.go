// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// ── a minimal CMAF upstream ─────────────────────────────────────────────────

func mp4Box(typ string, parts ...[]byte) []byte {
	var body []byte
	for _, p := range parts {
		body = append(body, p...)
	}
	out := make([]byte, 8, 8+len(body))
	binary.BigEndian.PutUint32(out, uint32(8+len(body)))
	copy(out[4:], typ)
	return append(out, body...)
}

func b32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func b64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

func fmp4Init() []byte {
	sps := []byte{0x67, 0x64, 0x00, 0x1F, 0xAC, 0xD9}
	pps := []byte{0x68, 0xEB, 0xE3, 0xCB}
	avcC := mp4Box("avcC", []byte{0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1},
		[]byte{byte(len(sps) >> 8), byte(len(sps))}, sps,
		[]byte{0x01, byte(len(pps) >> 8), byte(len(pps))}, pps)
	vEntry := mp4Box("avc1", make([]byte, 78), avcC)
	vTrak := mp4Box("trak",
		mp4Box("tkhd", []byte{0, 0, 0, 1}, b32(0), b32(0), b32(1), make([]byte, 60)),
		mp4Box("mdia",
			mp4Box("mdhd", []byte{0, 0, 0, 0}, b32(0), b32(0), b32(90000), b32(0), make([]byte, 4)),
			mp4Box("hdlr", []byte{0, 0, 0, 0}, b32(0), []byte("vide"), make([]byte, 12)),
			mp4Box("minf", mp4Box("stbl", mp4Box("stsd", []byte{0, 0, 0, 0}, b32(1), vEntry)))))

	asc := []byte{0x11, 0x90}
	dsi := append([]byte{0x05, byte(len(asc))}, asc...)
	dcd := append([]byte{0x04, byte(13 + len(dsi)), 0x40, 0x15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, dsi...)
	esd := append([]byte{0x03, byte(3 + len(dcd)), 0x00, 0x01, 0x00}, dcd...)
	aEntry := mp4Box("mp4a", make([]byte, 28), mp4Box("esds", []byte{0, 0, 0, 0}, esd))
	aTrak := mp4Box("trak",
		mp4Box("tkhd", []byte{0, 0, 0, 1}, b32(0), b32(0), b32(2), make([]byte, 60)),
		mp4Box("mdia",
			mp4Box("mdhd", []byte{0, 0, 0, 0}, b32(0), b32(0), b32(48000), b32(0), make([]byte, 4)),
			mp4Box("hdlr", []byte{0, 0, 0, 0}, b32(0), []byte("soun"), make([]byte, 12)),
			mp4Box("minf", mp4Box("stbl", mp4Box("stsd", []byte{0, 0, 0, 0}, b32(1), aEntry)))))

	return append(mp4Box("ftyp", []byte("isom"), b32(512), []byte("iso6mp41")),
		mp4Box("moov", mp4Box("mvhd", []byte{0, 0, 0, 0}, make([]byte, 96)), vTrak, aTrak)...)
}

// fmp4Segment: one IDR and two inter pictures, plus two audio frames.
func fmp4Segment(baseDTS int64) []byte {
	var mdat []byte
	nal := func(b []byte) []byte { return append(b32(uint32(len(b))), b...) }
	video := [][]byte{nal([]byte{0x65, 1, 2}), nal([]byte{0x41, 3}), nal([]byte{0x41, 4})}
	audio := [][]byte{{0xDE, 0xAD, 0xBE}, {0xCA, 0xFE}}

	trun := func(samples [][]byte, dur uint32, keyFirst bool) []byte {
		const fl = 0x000001 | 0x000100 | 0x000200 | 0x000400
		body := []byte{1, byte(fl >> 16), byte(fl >> 8), byte(fl & 0xFF)}
		body = append(body, b32(uint32(len(samples)))...)
		body = append(body, b32(0)...)
		for i, s := range samples {
			var sf uint32
			if keyFirst && i > 0 {
				sf = 0x00010000
			}
			body = append(body, b32(dur)...)
			body = append(body, b32(uint32(len(s)))...)
			body = append(body, b32(sf)...)
			mdat = append(mdat, s...)
		}
		return mp4Box("trun", body)
	}
	vTraf := mp4Box("traf",
		mp4Box("tfhd", []byte{0, 0, 0, 0}, b32(1)),
		mp4Box("tfdt", []byte{1, 0, 0, 0}, b64(uint64(baseDTS))),
		trun(video, 3000, true))
	aTraf := mp4Box("traf",
		mp4Box("tfhd", []byte{0, 0, 0, 0}, b32(2)),
		mp4Box("tfdt", []byte{1, 0, 0, 0}, b64(uint64(baseDTS*48000/90000))),
		trun(audio, 1024, false))
	moof := mp4Box("moof", mp4Box("mfhd", []byte{0, 0, 0, 0}, b32(1)), vTraf, aTraf)
	return append(moof, mp4Box("mdat", mdat)...)
}

// An fMP4/CMAF playlist used to be refused on sight, so every such channel cost
// a permanent ffmpeg child. Its segments are now read against the init segment
// the playlist names and rebuilt as MPEG-TS in process.
func TestFMP4SourceIsRemuxedToMPEGTS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch name := strings.TrimPrefix(r.URL.Path, "/"); {
		case name == "init.mp4":
			_, _ = w.Write(fmp4Init())
		case strings.HasSuffix(name, ".m4s"):
			n := int64(0)
			if name == "s1.m4s" {
				n = 9000
			}
			_, _ = w.Write(fmp4Segment(n))
		default:
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n" +
				"#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:0.1,\ns0.m4s\n#EXTINF:0.1,\ns1.m4s\n"))
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/cmaf.m3u8", Options{})
	if err != nil {
		t.Fatalf("an fMP4 playlist was refused: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, 48*188)
	n, _ := io.ReadFull(rc, buf)
	if n == 0 {
		t.Fatal("no bytes from an fMP4 source")
	}
	got := buf[:n]
	if got[0] != 0x47 {
		t.Fatalf("first byte %#x, want the TS sync byte", got[0])
	}

	var video, audio, pat, pmt bool
	for off := 0; off+188 <= len(got); off += 188 {
		switch tspes.PID(got[off : off+188]) {
		case 0x0000:
			pat = true
		case 0x1000:
			pmt = true
		case 0x0100:
			video = true
		case 0x0101:
			audio = true
		}
	}
	if !pat || !pmt || !video || !audio {
		t.Errorf("stream carries PAT=%v PMT=%v video=%v audio=%v", pat, pmt, video, audio)
	}

	s := tsjoin.New(1<<20, 40000)
	s.Update(got)
	if _, _, hasAudio := s.Counters(); !hasAudio {
		t.Error("the ring found no audio in the remuxed stream")
	}
}

// An fMP4 playlist with no #EXT-X-MAP has no init segment, so there is nothing
// to read its media segments against: it must still reach ffmpeg.
func TestFMP4WithoutAnInitSegmentIsRefused(t *testing.T) {
	srv := newHLSServer(t, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\ns0.m4s\n", nil)
	rc, err := Open(context.Background(), srv.URL+"/index.m3u8", Options{})
	if err == nil {
		rc.Close()
		t.Fatal("an fMP4 playlist with no init segment was accepted")
	}
	if !IsFormat(err) {
		t.Errorf("err = %v, want a format refusal", err)
	}
}

// An init segment that cannot be read is a refusal at open, before a byte of
// media goes out.
func TestAnUnreadableInitSegmentIsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "init.mp4") {
			_, _ = w.Write([]byte("<html>not an init segment</html>"))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:0.1,\ns0.m4s\n"))
	}))
	defer srv.Close()

	rc, err := Open(context.Background(), srv.URL+"/cmaf.m3u8", Options{})
	if err == nil {
		rc.Close()
		t.Fatal("a source with an unreadable init segment was accepted")
	}
	if !IsFormat(err) {
		t.Errorf("err = %v, want a format refusal", err)
	}
}
