// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// systemFFmpeg returns the path to the machine's real ffmpeg, or skips the test
// when it is not installed. We log which binary is used so a CI/dev run makes
// clear whether the real-encode path was actually exercised or skipped.
func systemFFmpeg(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg not installed; skipping real-encode overlay test")
	}
	t.Logf("using system ffmpeg: %s", p)
	return p
}

// systemFont returns a real TTF present on the machine, or skips. drawtext needs
// a font file, so without one the real overlay cannot be exercised.
func systemFont(t *testing.T) string {
	t.Helper()
	for _, p := range []string{
		"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
		"/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
		"/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
	} {
		if _, err := os.Stat(p); err == nil {
			t.Logf("using system font: %s", p)
			return p
		}
	}
	t.Skip("no known TTF font found; skipping drawtext overlay test")
	return ""
}

// realMpegTS uses the system ffmpeg to synthesize a short, self-contained H.264
// MPEG-TS segment — a realistic input for the overlay re-encode (the tsfixture
// packets are structurally valid but carry no decodable video).
func realMpegTS(t *testing.T, ffmpeg string) []byte {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=size=128x72:rate=10:duration=1",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", "-f", "mpegts", "pipe:1")
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Skipf("could not synthesize a test TS with ffmpeg (%v): %s", err, errb.String())
	}
	if out.Len() == 0 || out.Bytes()[0] != 0x47 {
		t.Fatalf("synthesized TS is not a valid mpegts stream (%d bytes)", out.Len())
	}
	return out.Bytes()
}

// TestOverlaySegmentRealFFmpeg exercises the drawtext re-encode end to end with
// the real system ffmpeg and a real font: a genuine banner is burned into a
// genuine H.264 segment, and the result must be a valid, changed mpegts stream.
func TestOverlaySegmentRealFFmpeg(t *testing.T) {
	ffmpeg := systemFFmpeg(t)
	font := systemFont(t)
	seg := realMpegTS(t, ffmpeg)

	mgr := NewManager(1<<20, 0, 2, 6, 0)
	mgr.SetOverlay(ffmpeg, font)

	out := mgr.overlaySegment(seg, pendingSignal{text: "HELLO", fontSize: 20, color: "white", x: 10, y: 10}, "h264")
	if len(out) == 0 || out[0] != 0x47 {
		t.Fatalf("overlay produced an invalid TS (%d bytes)", len(out))
	}
	if bytes.Equal(out, seg) {
		t.Fatal("overlay returned the input unchanged — the re-encode did not run")
	}
}

// TestOverlaySegmentSpecialChars proves the drawtext escaping actually parses
// under real ffmpeg for a message carrying the characters that are special to the
// filtergraph — an apostrophe and a colon. Before the '\'' fix the apostrophe
// broke the filter parse, so the re-encode failed and the overlay silently served
// the plain segment: exactly the "returned unchanged" state that would pass the
// other tests while the feature was dead for any message with a quote in it.
func TestOverlaySegmentSpecialChars(t *testing.T) {
	ffmpeg := systemFFmpeg(t)
	font := systemFont(t)
	seg := realMpegTS(t, ffmpeg)

	mgr := NewManager(1<<20, 0, 2, 6, 0)
	mgr.SetOverlay(ffmpeg, font)

	for _, msg := range []string{"it's back", "on at 3:00", "it's on at 3:00!"} {
		out := mgr.overlaySegment(seg, pendingSignal{text: msg, fontSize: 20, color: "white", x: 10, y: 10}, "h264")
		if len(out) == 0 || out[0] != 0x47 {
			t.Fatalf("msg %q: overlay produced an invalid TS (%d bytes)", msg, len(out))
		}
		if bytes.Equal(out, seg) {
			t.Fatalf("msg %q: overlay returned the input unchanged — the filter failed to parse", msg)
		}
	}
}

// TestOverlaySegmentGracefulOnFailure: a failing ffmpeg must never break
// playback — the plain segment is returned unchanged.
func TestOverlaySegmentGracefulOnFailure(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "fakeffmpeg")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\necho 'encode failed' >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(1<<20, 0, 2, 6, 0)
	mgr.SetOverlay(fake, filepath.Join(dir, "font.ttf")) // font path need not exist for the fake

	seg := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	out := mgr.overlaySegment(seg, pendingSignal{text: "x", fontSize: 20}, "h264")
	if !bytes.Equal(out, seg) {
		t.Fatal("on ffmpeg failure the original segment must be served unchanged")
	}
}

// TestOverlaySegmentDisabled: with no ffmpeg or no font configured, the overlay
// is a no-op that returns the segment verbatim.
func TestOverlaySegmentDisabled(t *testing.T) {
	seg := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	sig := pendingSignal{text: "x", fontSize: 20}

	mgr := NewManager(1<<20, 0, 2, 6, 0) // no SetOverlay → ffmpegBin=="" and font==""
	if out := mgr.overlaySegment(seg, sig, "h264"); !bytes.Equal(out, seg) {
		t.Fatal("overlay with no ffmpeg configured must return the segment unchanged")
	}

	mgr.SetOverlay("ffmpeg", "") // bin but no font → still disabled
	if out := mgr.overlaySegment(seg, sig, "h264"); !bytes.Equal(out, seg) {
		t.Fatal("overlay with no font must return the segment unchanged")
	}
}

// TestOverlayTSWindowContinuesRawOnStartFailure: when the overlay ffmpeg is
// disabled or cannot start, overlayTSWindow returns alive=true (keep serving the
// raw fan-out) and the cursor unchanged, rather than dropping the viewer.
func TestOverlayTSWindowContinuesRawOnStartFailure(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, 0)
	st := mgr.GetOrCreate("w")
	feedStream(st)
	write := func([]byte) error { return nil }
	sig := pendingSignal{text: "x", fontSize: 20}
	_, cur := st.Hub.Join(0)

	// Disabled (no overlay configured) → continue raw from the same cursor.
	if next, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264"); !alive || next != cur {
		t.Fatalf("disabled overlay must return (cur, true); got next=%+v alive=%v", next, alive)
	}

	// Configured but the binary does not exist → Start fails → continue raw.
	mgr.SetOverlay(filepath.Join(t.TempDir(), "nonexistent-ffmpeg"), "/font.ttf")
	if next, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264"); !alive || next != cur {
		t.Fatalf("unstartable overlay ffmpeg must return (cur, true); got next=%+v alive=%v", next, alive)
	}
}
