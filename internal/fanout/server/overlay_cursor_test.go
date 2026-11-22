// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mutedFFmpeg stands in for an ffmpeg that cannot set its output up: a -vcodec
// this (static) build does not carry, a font colour sanitizeColor let through
// that drawtext then rejects. Like the real thing it probes its input first,
// reading megabytes of it, and only then dies — without producing a byte.
func mutedFFmpeg(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "mutedffmpeg")
	if err := os.WriteFile(p, []byte("#!/bin/sh\nhead -c 2000000 >/dev/null 2>&1\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestOverlayTSWindowKeepsTheCursorWhenFFmpegProducesNothing: a failed overlay
// must be a no-op, which is what overlayTSWindow's own doc promises — "(cur,
// true) so the caller simply continues raw from where it was". Only a failed
// Start honoured that. An ffmpeg that started and then died at encoder setup had
// already swallowed whatever the feed managed to push at it, and that position
// was handed back as the resume cursor: the raw fan-out carried on from there
// and the viewer silently lost every byte in between — seconds of video it was
// never shown, for a banner it never saw. Reachable from the panel with a vc the
// static ffmpeg lacks, or a font_color drawtext rejects.
func TestOverlayTSWindowKeepsTheCursorWhenFFmpegProducesNothing(t *testing.T) {
	shortOverlayWindow(t, 2*time.Second) // a backstop; the stand-in dies in milliseconds

	mgr := NewManager(1<<20, 30000, 2, 6, 0)
	mgr.SetOverlay(mutedFFmpeg(t), "/font.ttf")
	st := mgr.GetOrCreate("ov")
	fillRing(t, st, 16) // ~4 MB, so the probe reads its fill without waiting on the ring

	_, cur := st.Hub.Join(30000)
	written := 0
	write := func(b []byte) error { written += len(b); return nil }

	sig := pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
	next, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264")
	if !alive {
		t.Fatal("the viewer connection never broke; a failed overlay must leave it serving")
	}
	if written != 0 {
		t.Fatalf("the stand-in produced no output, yet %d bytes reached the viewer", written)
	}
	if next != cur {
		t.Fatalf("ffmpeg died without producing a byte, but the resume cursor moved from %+v to %+v — "+
			"the viewer silently loses everything in between", cur, next)
	}
}
