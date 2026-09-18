// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"errors"
	"testing"
)

// byteRangePlaylist addresses one resource by ranges, as RFC 8216 allows.
const byteRangePlaylist = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
	"#EXTINF:2.0,\n#EXT-X-BYTERANGE:1316000@0\nstream.ts\n" +
	"#EXTINF:2.0,\n#EXT-X-BYTERANGE:1316000@1316000\nstream.ts\n"

// TestByteRangePlaylistIsServed: #EXT-X-BYTERANGE used to be refused outright,
// because the puller fetched a segment URI whole and de-duped on that URI — so
// the first entry would have fetched the WHOLE resource and the rest been
// skipped as duplicates of it. Each slice is now fetched with a Range request
// and identified by its offset, so the playlist is served rather than handed to
// ffmpeg. The tag is honoured wherever it sits before the URI line: this
// fixture writes it AFTER #EXTINF, which is equally legal.
func TestByteRangePlaylistIsServed(t *testing.T) {
	pl, err := parseHLSPlaylist([]byte(byteRangePlaylist), nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !pl.HasByteRange {
		t.Fatal("#EXT-X-BYTERANGE was not noticed at all")
	}
	if err := servable(pl); err != nil {
		t.Fatalf("servable = %v, want the playlist accepted now that ranges are fetched", err)
	}
	if len(pl.Segments) != 2 {
		t.Fatalf("parsed %d segments, want 2", len(pl.Segments))
	}
	for i, want := range []hlsRange{{Offset: 0, Length: 1316000}, {Offset: 1316000, Length: 1316000}} {
		got := pl.Segments[i].Range
		if got == nil || *got != want {
			t.Errorf("segment %d range = %+v, want %+v", i, got, want)
		}
	}
	if pl.Segments[0].Range.header() != "bytes=0-1315999" {
		t.Errorf("Range header = %q", pl.Segments[0].Range.header())
	}
}

// A byte-range playlist whose slices cannot be turned into Range requests is
// still refused as a format problem, so such a source still reaches ffmpeg.
func TestUnfetchableByteRangeIsStillRefused(t *testing.T) {
	// A first slice with no offset has nothing to continue from.
	pl, err := parseHLSPlaylist([]byte(
		"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\n#EXT-X-BYTERANGE:1316000\nstream.ts\n"), nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	serr := servable(pl)
	if !errors.Is(serr, ErrHLSByteRange) {
		t.Fatalf("servable = %v, want the byte-range refusal", serr)
	}
	if !IsFormat(serr) {
		t.Fatalf("servable = %v, want a format refusal so the source moves to ffmpeg", serr)
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
