// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tspes"
)

// ptsTolerance is how far an overlaid stream's first timestamp may drift from the
// source's. A re-encode can legitimately start a frame or two in; a rebase to
// zero moves it by the whole elapsed stream time, which is what this catches.
const ptsTolerance = 90000 / 4 // a quarter second, in 90 kHz ticks

// firstPTS reads the first presentation timestamp on the stream's video (or
// audio) PID, in 90 kHz ticks — the number a player puts on its timeline. Read
// out of the TS the way the daemon reads it, rather than shelled out to ffprobe.
func firstPTS(t *testing.T, ts []byte, kind string) (int64, bool) {
	t.Helper()
	var pmtPID uint16
	want := map[uint16]bool{}
	for off := 0; off+tsfixture.P <= len(ts); off += tsfixture.P {
		p := ts[off : off+tsfixture.P]
		if p[0] != 0x47 {
			t.Fatalf("byte %d is not on a TS packet boundary (0x%02x)", off, p[0])
		}
		if !tspes.PUSI(p) {
			continue
		}
		pid := tspes.PID(p)
		switch {
		case pid == 0:
			if v := tspes.PMTPID(p); v != 0 {
				pmtPID = v
			}
		case pmtPID != 0 && pid == pmtPID:
			if es, ok := tspes.ParsePMTStreams(p); ok {
				for _, e := range es {
					if e.Kind() == kind {
						want[e.PID] = true
					}
				}
			}
		case want[pid]:
			if pts, ok := tspes.PTS(p); ok {
				return pts, true
			}
		}
	}
	return 0, false
}

// offsetTS synthesizes a short TS whose timeline starts well past zero, the way
// a segment cut out of a stream that has been on air for a while does.
func offsetTS(t *testing.T, ffmpeg string, offsetSec float64) []byte {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=size=192x108:rate=25:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac",
		"-output_ts_offset", fmt.Sprintf("%.3f", offsetSec),
		"-muxdelay", "0", "-muxpreload", "0", "-f", "mpegts", "pipe:1")
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Skipf("could not synthesize an offset TS (%v): %s", err, errb.String())
	}
	return out.Bytes()
}

// TestOverlaySegmentKeepsTheSourceTimeline: an overlaid HLS segment must sit on
// the same clock as the segments either side of it.
//
// Neither overlay command used -copyts, so ffmpeg rebased the output to start at
// zero plus its mux delay: a segment whose first video PTS was 19.4 s came back
// at 1.4 s. The playlist is cut from the ring and shared by every viewer, so it
// cannot carry an #EXT-X-DISCONTINUITY for one viewer's overlaid sequence — which
// RFC 8216 requires for a timestamp change — and a player that places segments by
// PTS (hls.js, AVPlayer) misplaces or stalls on the very segment carrying the
// message.
func TestOverlaySegmentKeepsTheSourceTimeline(t *testing.T) {
	ffmpeg := systemFFmpeg(t)
	font := systemFont(t)
	seg := offsetTS(t, ffmpeg, 19.4)

	mgr := NewManager(1<<20, 0, 2, 6, 0)
	mgr.SetOverlay(ffmpeg, font)
	out := mgr.overlaySegment(seg, pendingSignal{text: "HELLO", fontSize: 20, color: "white", x: 10, y: 10}, "h264")
	if bytes.Equal(out, seg) {
		t.Fatal("the overlay returned the segment unchanged — the re-encode never ran")
	}

	for _, kind := range []string{"Video", "Audio"} {
		in, ok := firstPTS(t, seg, kind)
		if !ok {
			t.Fatalf("the synthesized segment carries no %s PTS", kind)
		}
		got, ok := firstPTS(t, out, kind)
		if !ok {
			t.Fatalf("the overlaid segment carries no %s PTS", kind)
		}
		if drift := got - in; drift > ptsTolerance || drift < -ptsTolerance {
			t.Errorf("%s: the overlaid segment starts at %.3fs but the source segment starts at %.3fs "+
				"— a %.3fs jump the playlist cannot mark as a discontinuity",
				kind, float64(got)/90000, float64(in)/90000, float64(drift)/90000)
		}
	}
}

// TestOverlayTSWindowKeepsTheSourceTimeline: the live-TS banner window must stay
// on the stream's own clock too, because the raw fan-out resumes on that clock
// the instant the window ends — with nothing to tell the player the timeline
// moved and then moved back.
func TestOverlayTSWindowKeepsTheSourceTimeline(t *testing.T) {
	ffmpeg := systemFFmpeg(t)
	font := systemFont(t)
	shortOverlayWindow(t, time.Second)

	src := offsetTS(t, ffmpeg, 19.4)
	dir := t.TempDir()
	fed := filepath.Join(dir, "fed.ts")

	// The real encoder, with a copy of everything it was fed kept on the side: the
	// question is what the window's output does to the timeline it was handed.
	wrapper := filepath.Join(dir, "teeffmpeg")
	if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec tee "+fed+" | "+ffmpeg+" \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(1<<20, 30000, 2, 6, 0)
	mgr.SetOverlay(wrapper, font)
	st := mgr.GetOrCreate("clock")
	for off := 0; off < len(src)/tsfixture.P*tsfixture.P; off += 64 * tsfixture.P {
		end := off + 64*tsfixture.P
		if end > len(src)/tsfixture.P*tsfixture.P {
			end = len(src) / tsfixture.P * tsfixture.P
		}
		st.Publish(src[off:end])
	}

	_, cur := st.Hub.Join(30000)
	var out []byte
	sig := pendingSignal{text: "HELLO", fontSize: 20, color: "white", x: 10, y: 10}
	if _, alive := mgr.overlayTSWindow(st, cur, func(b []byte) error { out = append(out, b...); return nil }, sig, "h264"); !alive {
		t.Fatal("the viewer connection never broke; overlayTSWindow must report it alive")
	}
	if len(out) == 0 {
		t.Fatal("the overlay window produced no output at all")
	}

	in, ok := firstPTS(t, mustRead(t, fed), "Video")
	if !ok {
		t.Fatal("the bytes fed to the encoder carry no video PTS")
	}
	got, ok := firstPTS(t, out, "Video")
	if !ok {
		t.Fatal("the overlay window's output carries no video PTS")
	}
	if drift := got - in; drift > ptsTolerance || drift < -ptsTolerance {
		t.Fatalf("the banner window starts at %.3fs but it was fed video starting at %.3fs — "+
			"a %.3fs jump, and the raw fan-out then jumps straight back when the window ends",
			float64(got)/90000, float64(in)/90000, float64(drift)/90000)
	}

	// Preserving the clock must not cost the output its own timing: -muxdelay 0
	// and -muxpreload 0 leave the muxer no headroom, so check what the viewer
	// actually receives still plays.
	outPath := filepath.Join(dir, "out.ts")
	if err := os.WriteFile(outPath, out, 0o644); err != nil {
		t.Fatal(err)
	}
	if bad := decodeComplaints(t, ffmpeg, outPath); len(bad) != 0 {
		t.Fatalf("the banner window's own output does not play cleanly:\n  %s", bad[0])
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
