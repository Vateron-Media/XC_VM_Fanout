// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"errors"
	"testing"
)

// byteRangePlaylist addresses one resource by ranges, as RFC 8216 allows.
const byteRangePlaylist = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
	"#EXTINF:2.0,\n#EXT-X-BYTERANGE:1316000@0\nstream.ts\n" +
	"#EXTINF:2.0,\n#EXT-X-BYTERANGE:1316000@1316000\nstream.ts\n"

// TestByteRangePlaylistIsRefused: #EXT-X-BYTERANGE fell into the "tag we don't
// model" branch, so a playlist carrying one was accepted and then served wrong
// in every direction. No Range header is ever sent, so the first entry fetches
// the WHOLE resource and the rest are skipped as duplicates of it; once that
// resource passes 64 MiB every fetch fails as a runaway upstream, and the pipe
// closes with a non-format error, so the pull retries natively forever and never
// reaches the fallback. Refusing says what is actually wrong and hands the
// source to ffmpeg, which reads byte ranges.
func TestByteRangePlaylistIsRefused(t *testing.T) {
	pl, err := parseHLSPlaylist([]byte(byteRangePlaylist), nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !pl.HasByteRange {
		t.Fatal("#EXT-X-BYTERANGE was not noticed at all")
	}
	serr := servable(pl)
	if !errors.Is(serr, ErrHLSByteRange) {
		t.Fatalf("servable = %v, want the byte-range refusal", serr)
	}
	if !IsFormat(serr) {
		t.Fatalf("servable = %v, want a format refusal so the source moves to ffmpeg", serr)
	}
}

// TestByteRangeSourceIsRefusedOnOpen: end to end, through the pull, and without
// a single segment byte going out.
func TestByteRangeSourceIsRefusedOnOpen(t *testing.T) {
	srv := newHLSServer(t, byteRangePlaylist, nil)
	rc, err := Open(context.Background(), srv.URL+"/index.m3u8", Options{})
	if err == nil {
		rc.Close()
		t.Fatal("a byte-range playlist was accepted")
	}
	if !errors.Is(err, ErrHLSByteRange) {
		t.Fatalf("err = %v, want the byte-range refusal", err)
	}
	for _, r := range srv.seen() {
		if r.URL.Path == "/stream.ts" {
			t.Fatal("a byte-range segment was fetched: the whole resource would go out as one segment")
		}
	}
}

// TestAPlainPlaylistIsStillServable: the guard must key on the tag, not on
// anything a normal live playlist carries.
func TestAPlainPlaylistIsStillServable(t *testing.T) {
	pl, err := parseHLSPlaylist([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\ns0.ts\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pl.HasByteRange {
		t.Fatal("a playlist with no byte ranges was flagged as having them")
	}
	if err := servable(pl); err != nil {
		t.Fatalf("servable = %v, want nil", err)
	}
}
