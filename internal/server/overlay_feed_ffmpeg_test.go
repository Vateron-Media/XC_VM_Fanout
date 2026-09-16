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
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// realLiveTS synthesizes a few seconds of realistic live TS: 25 fps H.264 with a
// keyframe every second (so the ring cuts real GOPs), AAC audio and a PCR — the
// shape whose inter-frame references and DTS order a splice actually breaks.
func realLiveTS(t *testing.T, ffmpeg string, seconds int) []byte {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", fmt.Sprintf("testsrc=size=192x108:rate=25:duration=%d", seconds),
		"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=440:duration=%d", seconds),
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-g", "25", "-keyint_min", "25", "-sc_threshold", "0",
		"-c:a", "aac", "-f", "mpegts", "pipe:1")
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Skipf("could not synthesize a live TS with ffmpeg (%v): %s", err, errb.String())
	}
	if out.Len() == 0 || out.Bytes()[0] != 0x47 {
		t.Fatalf("synthesized TS is not a valid mpegts stream (%d bytes)", out.Len())
	}
	return out.Bytes()
}

// spliceSymptoms are what real ffmpeg says about a TS that was cut together from
// two different places in a stream: a truncated access unit, timestamps that go
// backwards, and pictures referencing data the decoder never got. A stream a
// decoder can follow says none of them — the macroblocking a signalled viewer
// sees under the banner is exactly this list.
var spliceSymptoms = []string{
	"out of order",   // DTS jumped backwards at the join
	"Packet corrupt", // an access unit cut in half by the splice
	"corrupt input packet",
	"timestamp discontinuity",
	"non monotonically increasing dts",
	"error while decoding MB", // a picture referenced data it never got
	"mmco: unref short failure",
	"co located POCs unavailable",
	"illegal short term buffer state",
	"reference picture missing",
}

// decodeComplaints decodes a TS with the real ffmpeg and returns the splice
// symptoms it reports. Warning level, not error: ffmpeg files a spliced stream's
// corrupt packets and backwards DTS as warnings, and they are the earliest and
// clearest evidence that the bytes were cut together wrong.
func decodeComplaints(t *testing.T, ffmpeg, path string) []string {
	t.Helper()
	var errb bytes.Buffer
	cmd := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "warning",
		"-i", path, "-f", "null", "-")
	cmd.Stderr = &errb
	_ = cmd.Run() // a decode failure is reported on stderr, not through the exit code
	var bad []string
	for _, line := range strings.Split(errb.String(), "\n") {
		line = strings.TrimSpace(line)
		for _, s := range spliceSymptoms {
			if strings.Contains(line, s) {
				bad = append(bad, line)
				break
			}
		}
	}
	return bad
}

// TestOverlayTSWindowFeedsADecodableStream is the real-ffmpeg half of the
// contiguous-feed contract: the bytes the banner encoder is fed must decode as
// cleanly as the source they were cut from.
//
// The window used to seed ffmpeg with a live Snapshot(0) and then follow the
// viewer's older cursor, so the encoder got the live GOP and then bytes from
// before it. On a real 25 fps H.264 + AAC stream ffmpeg reports that splice as
// corrupt packets and out-of-order DTS, and the viewer sees macroblocking under
// the message until the next keyframe — on a long GOP, the whole window.
func TestOverlayTSWindowFeedsADecodableStream(t *testing.T) {
	ffmpeg := systemFFmpeg(t)

	cases := []struct {
		name   string
		cursor func(join, edge tsjoin.Cursor) tsjoin.Cursor
	}{
		{"parked at the live edge", func(_, edge tsjoin.Cursor) tsjoin.Cursor { return edge }},
		{"seconds behind the edge", func(join, _ tsjoin.Cursor) tsjoin.Cursor { return join }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shortOverlayWindow(t, 400*time.Millisecond)

			src := realLiveTS(t, ffmpeg, 6)
			dir := t.TempDir()
			srcPath := filepath.Join(dir, "src.ts")
			if err := os.WriteFile(srcPath, src, 0o644); err != nil {
				t.Fatal(err)
			}
			if bad := decodeComplaints(t, ffmpeg, srcPath); len(bad) != 0 {
				t.Skipf("the synthesized source does not decode cleanly here, so it cannot judge a splice: %v", bad)
			}

			// A stand-in for ffmpeg that keeps a copy of everything it is fed. What
			// the encoder makes of a corrupt input is a re-encode — syntactically
			// valid and visually wrong — so the input is the thing worth judging.
			capture := filepath.Join(dir, "fed.ts")
			fake := filepath.Join(dir, "teeffmpeg")
			if err := os.WriteFile(fake, []byte("#!/bin/sh\nexec tee "+capture+"\n"), 0o755); err != nil {
				t.Fatal(err)
			}

			mgr := NewManager(1<<20, 30000, 2, 6, 0)
			mgr.SetOverlay(fake, "/font.ttf")
			st := mgr.GetOrCreate("live")

			// Publish the source the way a puller does: packet-aligned chunks,
			// holding the last stretch back so it lands after the viewer caught up.
			const chunk = 64 * tsfixture.P
			packets := len(src) / tsfixture.P
			split := packets * 5 / 6 * tsfixture.P
			publish := func(from, to int) {
				for off := from; off < to; off += chunk {
					end := off + chunk
					if end > to {
						end = to
					}
					st.Publish(src[off:end])
				}
			}
			publish(0, split)

			_, join := st.Hub.Join(30000)
			edge := followToEdge(t, st, join)
			if _, _, gops := st.Hub.RingStats(); gops < 3 {
				t.Skipf("the ring cut only %d block(s) from the synthesized source; there is nothing to splice", gops)
			}

			// The wake that brings serveLive back to its signal check in the first place.
			publish(split, packets*tsfixture.P)

			sig := pendingSignal{text: "HELLO", fontSize: 20, color: "white", x: 10, y: 10}
			if _, alive := mgr.overlayTSWindow(st, tc.cursor(join, edge), func([]byte) error { return nil }, sig, "h264"); !alive {
				t.Fatal("the viewer connection never broke; overlayTSWindow must report it alive")
			}

			fed, err := os.ReadFile(capture)
			if err != nil || len(fed) == 0 {
				t.Fatalf("the overlay fed the encoder nothing (%v)", err)
			}
			t.Logf("the overlay fed %d KB to the encoder", len(fed)/1024)
			bad := decodeComplaints(t, ffmpeg, capture)
			if len(bad) == 0 {
				return
			}
			if len(bad) > 6 {
				bad = bad[:6]
			}
			t.Fatalf("the encoder was fed a stream a decoder cannot follow — this is the macroblocking "+
				"under the banner:\n  %s", strings.Join(bad, "\n  "))
		})
	}
}
