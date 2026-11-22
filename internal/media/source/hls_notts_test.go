// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestOneNonTSSegmentBodyIsNotAFormatRefusal: an IPTV origin over its connection
// limit answers a segment with 200 and an HTML page ("Session limit reached"),
// then serves the same segment correctly a second later. The body check refused
// on the FIRST such body with ErrHLSNotTS, which is a format refusal — remux
// exits ExitUnsupported and the supervisor pins the source to its ffmpeg
// fallback for the life of the spec. One bad minute upstream moved a healthy
// channel onto ffmpeg for good, which is exactly what a format refusal must
// never do for a source that is merely down.
func TestOneNonTSSegmentBodyIsNotAFormatRefusal(t *testing.T) {
	const blip = 5
	var polls atomic.Int64
	var served atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/s"), ".ts"))
			if n == blip && served.CompareAndSwap(false, true) {
				// 200 with an error page, the shape an origin returns during an
				// auth or connection-limit blip.
				w.Header().Set("Content-Type", "text/html")
				_, _ = io.WriteString(w, "<html><body>Session limit reached. Please try again.</body></html>"+
					strings.Repeat(" ", 1024))
				return
			}
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(int64(n) * 90000))
			return
		}
		start := int(polls.Add(1)) - 1
		var b strings.Builder
		fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n", start)
		for i := start; i < start+6; i++ {
			fmt.Fprintf(&b, "#EXTINF:2.0,\ns%d.ts\n", i)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	one := len(tsSegment(0))
	want := []int{3, 4, 5, 6}
	buf := make([]byte, len(want)*one)
	if _, err := io.ReadFull(rc, buf); err != nil {
		if IsFormat(err) {
			t.Fatalf("one bad segment body ended the pull with a format refusal (%v): that "+
				"pins the channel onto its ffmpeg fallback for the life of the spec, for a "+
				"source that recovered by itself a second later", err)
		}
		t.Fatalf("read four segments: %v", err)
	}
	for i, n := range want {
		if got := buf[i*one : (i+1)*one]; string(got) != string(tsSegment(int64(n)*90000)) {
			t.Fatalf("segment %d of the stream is not s%d: the retry of a bad body must "+
				"keep its place in the stream", i, n)
		}
	}
}

// TestSegmentBodiesThatStayNonTSStillRefuse is the other half: a provider that
// really does serve packed audio (or anything else) from its .ts URLs must
// still reach ffmpeg, which reads it. The refusal is now the SECOND kind of
// evidence — enough bodies in a row to rule out a blip — rather than the first
// one seen.
func TestSegmentBodiesThatStayNonTSStillRefuse(t *testing.T) {
	var polls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t") // says TS, is not
			_, _ = w.Write(packedAudio())
			return
		}
		start := int(polls.Add(1)) - 1
		var b strings.Builder
		fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n", start)
		for i := start; i < start+6; i++ {
			fmt.Fprintf(&b, "#EXTINF:2.0,\ns%d.ts\n", i)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, rerr := io.ReadAll(rc)
	if len(got) != 0 {
		t.Fatalf("%d bytes of non-TS reached the wire", len(got))
	}
	if !IsFormat(rerr) {
		t.Fatalf("pull ended with %v, want a format refusal so the stream falls back to "+
			"ffmpeg, which reads packed audio", rerr)
	}
}
