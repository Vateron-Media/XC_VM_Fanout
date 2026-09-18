// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// adtsAAC builds a packed-audio segment: an ID3 tag, then n ADTS frames at
// 48 kHz — what a radio channel's packager actually writes.
func adtsAAC(frames int) []byte {
	out := append([]byte{'I', 'D', '3', 0x04, 0x00, 0x00, 0, 0, 0, 10}, make([]byte, 10)...)
	for i := 0; i < frames; i++ {
		const payload = 200
		total := 7 + payload
		f := make([]byte, total)
		f[0], f[1] = 0xFF, 0xF1
		f[2] = 0x40 | (3 << 2)
		f[3] = byte(0x80 | (total>>11)&0x03)
		f[4] = byte(total >> 3)
		f[5] = byte((total&0x07)<<5) | 0x1F
		f[6] = 0xFC
		out = append(out, f...)
	}
	return out
}

// A packed-audio (.aac) playlist is how radio channels ship, and every one of
// them used to cost a permanent ffmpeg child: the segments carry no transport
// layer, so the puller refused them on the name. They are now framed as MPEG-TS
// in process, and what reaches the ring is an ordinary audio programme.
func TestPackedAACIsMuxedIntoMPEGTS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".aac") {
			_, _ = w.Write(adtsAAC(45))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n" +
			"#EXTINF:0.96,\ns0.aac\n#EXTINF:0.96,\ns1.aac\n"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/radio.m3u8", Options{})
	if err != nil {
		t.Fatalf("a packed-audio playlist was refused: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, 64*188)
	n, err := io.ReadFull(rc, buf)
	if err != nil && n == 0 {
		t.Fatalf("no bytes from a packed-audio source: %v", err)
	}
	got := buf[:n]
	if n%188 != 0 {
		t.Fatalf("read %d bytes, not a whole number of TS packets", n)
	}
	if got[0] != 0x47 {
		t.Fatalf("first byte %#x, want the TS sync byte", got[0])
	}
	// The ring must find a programme in it: tables, an audio stream, a clock.
	s := tsjoin.New(1<<20, 40000)
	s.Update(got)
	if _, _, hasAudio := s.Counters(); !hasAudio {
		t.Error("the ring found no audio stream in the muxed output")
	}
	var sawPCR bool
	for off := 0; off+188 <= len(got); off += 188 {
		if _, ok := tspes.PCR(got[off : off+188]); ok {
			sawPCR = true
			break
		}
	}
	if !sawPCR {
		t.Error("no PCR: the stream carries no clock")
	}
}

// The formats this package still cannot frame must stay refused, so those
// sources keep reaching ffmpeg.
func TestOtherPackedFormatsAreStillRefused(t *testing.T) {
	for _, ext := range []string{"mp3", "ac3", "ec3", "vtt"} {
		t.Run(ext, func(t *testing.T) {
			srv := newHLSServer(t, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\ns0."+ext+"\n", nil)
			rc, err := Open(context.Background(), srv.URL+"/index.m3u8", Options{})
			if err == nil {
				rc.Close()
				t.Fatalf(".%s segments were accepted", ext)
			}
			if !IsFormat(err) {
				t.Errorf("err = %v, want a format refusal", err)
			}
		})
	}
}

// An origin answering a .aac URL with something that is not ADTS — an error
// page is the usual way — must not be muxed into noise.
func TestANonADTSBodyOnAnAACURLIsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".aac") {
			_, _ = w.Write([]byte("<html>over connection limit</html>"))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:0.96,\ns0.aac\n"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/radio.m3u8", Options{})
	if err != nil {
		return // refused at open, which is fine
	}
	defer rc.Close()
	b, rerr := io.ReadAll(rc)
	if len(b) > 0 {
		t.Errorf("%d bytes of a non-ADTS body reached the wire", len(b))
	}
	if rerr == nil {
		t.Error("the pull ended cleanly on a body that is not audio")
	}
}
