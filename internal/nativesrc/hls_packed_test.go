// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// packedAudio is one ID3-prefixed ADTS body, the shape RFC 8216 §3.4 packed
// audio actually arrives in: a 10-byte ID3v2 header then an AAC ADTS frame.
func packedAudio() []byte {
	var b bytes.Buffer
	b.WriteString("ID3")
	b.Write([]byte{0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a}) // version, flags, size
	b.Write(make([]byte, 10))
	for i := 0; i < 64; i++ {
		b.Write([]byte{0xff, 0xf1, 0x4c, 0x80, 0x1f, 0xfc}) // ADTS frame header
		b.Write(make([]byte, 26))
	}
	return b.Bytes()
}

// TestPackedAudioPlaylistIsRefused: a playlist of packed-audio segments is not
// MPEG-TS. servable() checked only for fMP4 and encryption, so the source was
// accepted and each body copied into the pipe; ingest.Copy then sliced it into
// 188-byte chunks and published it as TS. The ring never saw a PAT, a PMT or a
// keyframe, so viewers got nothing.
//
// .aac is no longer in this list: packed AAC is now framed as MPEG-TS by
// internal/tsmux (see TestPackedAACIsMuxedIntoMPEGTS). The formats left here
// are the ones that still need a decoder or a different framing, and they must
// still reach ffmpeg.
func TestPackedAudioPlaylistIsRefused(t *testing.T) {
	for _, ext := range []string{".ac3", ".mp3"} {
		t.Run(ext, func(t *testing.T) {
			pl := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nseg1" + ext + "\n"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, ext) {
					_, _ = w.Write(packedAudio())
					return
				}
				w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
				_, _ = io.WriteString(w, pl)
			}))
			defer srv.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
			if err == nil {
				_ = rc.Close()
				t.Fatalf("a %s (packed audio) playlist was accepted; its bytes would be "+
					"fanned out as if they were MPEG-TS", ext)
			}
			if !IsFormat(err) {
				t.Fatalf("err = %v, want a format refusal so the caller falls back to ffmpeg", err)
			}
		})
	}
}

// TestNonTSSegmentBodyEndsThePull covers the case the filename cannot catch: a
// provider serving packed audio (or an HTML error page) from a .ts URL. Segment
// bytes are passed through unread, so the only place left to notice is the body
// itself — and a segment that is not MPEG-TS must end the pull with a format
// refusal rather than put unplayable bytes on the wire.
func TestNonTSSegmentBodyEndsThePull(t *testing.T) {
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nseg1.ts\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t") // says TS, is not
			_, _ = w.Write(packedAudio())
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, pl)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, rerr := io.ReadAll(rc)
	if len(got) != 0 {
		t.Fatalf("%d bytes of non-TS reached the wire; the ring would publish them as "+
			"188-byte TS chunks and viewers would get garbage", len(got))
	}
	if !IsFormat(rerr) {
		t.Fatalf("pull ended with %v, want a format refusal so the stream falls back to ffmpeg", rerr)
	}
}

// TestShortSegmentIsATransportFailureNotAFormatRefusal draws the line on the
// other side of the body check. A body too short to hold even one TS packet is
// a truncated fetch: it says nothing about what the upstream IS, so it must
// stay on the retry-and-fail-over path rather than permanently condemning the
// source to ffmpeg. It must also not be passed through, which is what used to
// happen — a fraction of a packet went on the wire, the segment was marked
// delivered, and the pull then polled a playlist it had nothing left to take
// from, so the stream stalled without ever erroring or failing over.
func TestShortSegmentIsATransportFailureNotAFormatRefusal(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the consecutive-failure threshold")
	}
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nseg1.ts\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0)[:100]) // less than one whole packet
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, pl)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	done := make(chan error, 1)
	go func() {
		_, e := io.ReadAll(rc)
		done <- e
	}()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("a source serving truncated segments ended cleanly")
		}
		if IsFormat(e) {
			t.Fatalf("a truncated segment read as a format refusal (%v): that moves a "+
				"source onto ffmpeg permanently for what is a transport fault", e)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("truncated segments never tripped the consecutive-failure threshold: the " +
			"partial packet was passed through as a delivered segment, so the pull stalled " +
			"silently instead of failing over")
	}
}
