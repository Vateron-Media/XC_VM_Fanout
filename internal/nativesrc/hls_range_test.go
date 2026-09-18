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
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rangeUpstream serves one file of three concatenated TS segments, listed as
// three #EXT-X-BYTERANGE slices of it — the shape a packager writes when it
// appends to a single file instead of cutting one per segment. honourRange
// picks whether the origin narrows the body itself (206) or ignores the header
// and sends the whole file (200), which origins are free to do.
func rangeUpstream(t *testing.T, honourRange bool, offsets bool) (*httptest.Server, [][]byte, *atomic.Int64) {
	t.Helper()
	segs := [][]byte{tsSegment(0), tsSegment(90000), tsSegment(180000)}
	var file []byte
	var lines []string
	off := 0
	for _, s := range segs {
		if offsets {
			lines = append(lines, fmt.Sprintf("#EXT-X-BYTERANGE:%d@%d\n#EXTINF:2.0,\nall.ts\n", len(s), off))
		} else {
			// No offset: each slice continues from the previous one.
			lines = append(lines, fmt.Sprintf("#EXT-X-BYTERANGE:%d\n#EXTINF:2.0,\nall.ts\n", len(s)))
		}
		file = append(file, s...)
		off += len(s)
	}
	// Without an offset anywhere, the first slice needs one to be fetchable.
	if !offsets {
		lines[0] = fmt.Sprintf("#EXT-X-BYTERANGE:%d@0\n#EXTINF:2.0,\nall.ts\n", len(segs[0]))
	}

	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "all.ts") {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" + strings.Join(lines, "")))
			return
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			t.Errorf("segment fetched with no Range header: the whole file would be served as one segment")
		}
		if !honourRange {
			served.Add(int64(len(file)))
			_, _ = w.Write(file)
			return
		}
		var lo, hi int
		if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &lo, &hi); err != nil || lo < 0 || hi >= len(file) || lo > hi {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", "bytes "+strconv.Itoa(lo)+"-"+strconv.Itoa(hi)+"/"+strconv.Itoa(len(file)))
		w.WriteHeader(http.StatusPartialContent)
		served.Add(int64(hi - lo + 1))
		_, _ = w.Write(file[lo : hi+1])
	}))
	t.Cleanup(srv.Close)
	return srv, segs, &served
}

// A byte-range playlist used to be refused outright: the puller fetched a
// segment URI whole and de-duped on that URI, so serving one would have fetched
// the entire file once and dropped every later slice of it. Each slice is now
// fetched with a Range request, in order.
func TestByteRangeSegmentsAreFetchedAsRanges(t *testing.T) {
	for _, tc := range []struct {
		name     string
		explicit bool
	}{
		{"explicit offsets", true},
		{"offsets implied by the previous slice", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, segs, served := rangeUpstream(t, true, tc.explicit)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			rc, err := Open(ctx, srv.URL+"/live.m3u8", Options{})
			if err != nil {
				t.Fatalf("a byte-range playlist was refused: %v", err)
			}
			defer rc.Close()

			var want []byte
			for _, s := range segs {
				want = append(want, s...)
			}
			got := make([]byte, len(want))
			if _, err := io.ReadFull(rc, got); err != nil {
				t.Fatalf("reading: %v", err)
			}
			if string(got) != string(want) {
				t.Error("the slices did not arrive in order, or were not the slices asked for")
			}
			// Each slice fetched exactly once: the whole file transferred once in
			// total, not once per segment.
			if n := served.Load(); n != int64(len(want)) {
				t.Errorf("origin served %d bytes for %d bytes of segments: slices are being re-fetched", n, len(want))
			}
		})
	}
}

// An origin is free to ignore a Range header and answer 200 with the whole
// file. The slice has to be cut locally then, or every segment would be the
// entire file — a time jump on the wire and three times the bytes.
func TestAnOriginIgnoringRangeStillYieldsTheSlice(t *testing.T) {
	srv, segs, _ := rangeUpstream(t, false, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/live.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got := make([]byte, len(segs[0])+len(segs[1]))
	if _, err := io.ReadFull(rc, got); err != nil {
		t.Fatalf("reading: %v", err)
	}
	want := append(append([]byte{}, segs[0]...), segs[1]...)
	if string(got) != string(want) {
		t.Error("a 200 answer was not narrowed to the playlist's byte range")
	}
}

// Several slices share one URI, so the de-dup identity has to include the
// range. Keyed on the URI alone, the first slice would mark the whole file
// streamed and the rest of the window would be dropped.
func TestSlicesOfOneURIAreDistinctSegments(t *testing.T) {
	base, err := url.Parse("http://h/live.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	pl, err := parseHLSPlaylist([]byte(
		"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n"+
			"#EXT-X-BYTERANGE:100@0\n#EXTINF:2.0,\nall.ts\n"+
			"#EXT-X-BYTERANGE:100@100\n#EXTINF:2.0,\nall.ts\n"), base)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(pl.Segments) != 2 {
		t.Fatalf("parsed %d segments, want 2", len(pl.Segments))
	}
	if a, b := segIdent(pl.Segments[0]), segIdent(pl.Segments[1]); a == b {
		t.Errorf("both slices have identity %q: the second would be read as already streamed", a)
	}
	if pl.Segments[1].Range == nil || pl.Segments[1].Range.Offset != 100 {
		t.Errorf("second slice range = %+v, want offset 100", pl.Segments[1].Range)
	}
}

// A byte-range tag this package cannot turn into a Range request must still be
// refused, rather than fetched as the whole resource and served as a segment.
func TestUnusableByteRangesAreStillRefused(t *testing.T) {
	base, err := url.Parse("http://h/live.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, body string }{
		{
			"first slice has no offset to continue from",
			"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-BYTERANGE:100\n#EXTINF:2.0,\nall.ts\n",
		},
		{
			"length does not parse",
			"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-BYTERANGE:notanumber@0\n#EXTINF:2.0,\nall.ts\n",
		},
		{
			"a mix: one slice tagged, one not",
			"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-BYTERANGE:100@0\n#EXTINF:2.0,\nall.ts\n#EXTINF:2.0,\nwhole.ts\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pl, err := parseHLSPlaylist([]byte(tc.body), base)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if err := servable(pl); err == nil {
				t.Error("accepted a byte-range playlist it cannot fetch correctly")
			}
		})
	}
}
