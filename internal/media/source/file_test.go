// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// writeFixture drops b in a temp dir and returns its path.
func writeFixture(t *testing.T, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestOpenRefusesANonTSFile: a local file must PROVE it is MPEG-TS, exactly as
// an HTTP body does. The daemon routes a bare path and file:// straight past the
// HTTP probe into Open, which handed back whatever os.Open returned — so a
// stream registered with /home/xc_vm/content/movie.mp4 reported itself as
// running natively while fanning out MP4 boxes as if they were MPEG-TS. The
// channel was dead, and the ffmpeg fallback, which remuxes that file correctly,
// was never tried because nothing ever refused.
func TestOpenRefusesANonTSFile(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{"movie.mp4", []byte("\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2avc1mp41")},
		{"ch.mkv", []byte("\x1a\x45\xdf\xa3\x01\x00\x00\x00\x00\x00\x00\x23matroska")},
		{"42_.m3u8", []byte("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\n42_0.ts\n")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := writeFixture(t, c.name, c.body)
			for _, raw := range []string{p, "file://" + p} {
				rc, err := Open(context.Background(), raw, Options{})
				if err == nil {
					rc.Close()
					t.Fatalf("Open(%s) accepted a %s as MPEG-TS: those bytes go straight to viewers", raw, c.name)
				}
				if !IsFormat(err) {
					t.Fatalf("Open(%s): err = %v, want a format refusal so the stream falls back to ffmpeg", raw, err)
				}
			}
		})
	}
}

// TestOpenReadsATSFileWhole: the sniff must not eat the bytes it read. A real TS
// file is still served, from its first packet.
func TestOpenReadsATSFileWhole(t *testing.T) {
	body := tsSegment(0)
	p := writeFixture(t, "ch.ts", body)
	for _, raw := range []string{p, "file://" + p} {
		rc, err := Open(context.Background(), raw, Options{})
		if err != nil {
			t.Fatalf("Open(%s): %v", raw, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %s: %v", raw, err)
		}
		if len(got) != len(body) {
			t.Fatalf("read %d bytes of %s, want %d — the sniffed prefix was not replayed", len(got), raw, len(body))
		}
		if got[0] != 0x47 {
			t.Fatalf("first byte 0x%02x, want the sync byte", got[0])
		}
	}
}

// TestOpenOnAnEmptyFileIsNotAFormatRefusal: an empty file says nothing about
// WHAT it is — an encoder that has not written yet looks exactly like this — and
// a format refusal is what pins a stream to ffmpeg for the life of its spec.
func TestOpenOnAnEmptyFileIsNotAFormatRefusal(t *testing.T) {
	p := writeFixture(t, "ch.ts", nil)
	rc, err := Open(context.Background(), p, Options{})
	if err == nil {
		rc.Close()
		t.Fatal("Open accepted an empty file")
	}
	if !errors.Is(err, ErrUnsupportedSource) {
		t.Fatalf("err = %v, want ErrUnsupportedSource", err)
	}
	if IsFormat(err) {
		t.Fatalf("err = %v reads as a format refusal", err)
	}
}

// TestOpenOnAFileShorterThanOnePacketIsNotAFormatRefusal: a file with fewer
// than 188 bytes in it has not said what it is. An encoder that has just created
// its output — the local-file case this package exists to serve — looks exactly
// like this for a moment, and a format refusal is what pins the stream to ffmpeg
// for the life of its spec. The segment body check already draws the line here;
// a file on disk is no different.
func TestOpenOnAFileShorterThanOnePacketIsNotAFormatRefusal(t *testing.T) {
	p := writeFixture(t, "ch.ts", tsSegment(0)[:100])
	rc, err := Open(context.Background(), p, Options{})
	if err == nil {
		rc.Close()
		t.Fatal("Open accepted a file too short to hold one TS packet")
	}
	if !errors.Is(err, ErrUnsupportedSource) {
		t.Fatalf("err = %v, want ErrUnsupportedSource", err)
	}
	if IsFormat(err) {
		t.Fatalf("err = %v reads as a format refusal: the file is a producer mid-write, "+
			"not another container", err)
	}
	// The same file, once a whole packet has been written, opens.
	full := writeFixture(t, "full.ts", tsSegment(0))
	rc, err = Open(context.Background(), full, Options{})
	if err != nil {
		t.Fatalf("Open on a whole segment: %v", err)
	}
	rc.Close()
}

// TestOpenOnAMissingFileStillReportsTheOSError: refusing unreadable content must
// not swallow "no such file", which is the one thing that tells an operator the
// path in the panel is wrong.
func TestOpenOnAMissingFileStillReportsTheOSError(t *testing.T) {
	_, err := Open(context.Background(), filepath.Join(t.TempDir(), "nope.ts"), Options{})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want it to still read as os.ErrNotExist", err)
	}
}
