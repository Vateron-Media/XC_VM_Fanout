// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// noOutputLine is the window's "the encoder gave me nothing" diagnosis. It is
// the one line that sends an operator looking at ffmpeg — a codec this build
// lacks, a colour drawtext rejects, a filtergraph that would not parse — so it
// must only be written when that is what happened.
const noOutputLine = "ffmpeg produced no output"

// TestOverlayTSWindowBlamesFFmpegOnlyWhenFFmpegIsAtFault: the window counted
// bytes DELIVERED to the viewer, and reported "ffmpeg produced no output"
// whenever that count was zero. But the counter is only incremented after a
// successful write, so a viewer that went away (or was kicked) before the very
// first 32 KB chunk landed left it at zero with the encoder working perfectly —
// and the operator, debugging a signal that never appeared, was pointed at a
// broken ffmpeg instead of a viewer that had already gone.
func TestOverlayTSWindowBlamesFFmpegOnlyWhenFFmpegIsAtFault(t *testing.T) {
	shortOverlayWindow(t, 300*time.Millisecond)

	t.Run("the viewer goes away on the first chunk", func(t *testing.T) {
		buf := captureDebugLog(t)

		mgr := NewManager(1<<20, 30000, 2, 6, 0)
		mgr.SetOverlay(catFFmpeg(t), "/font.ttf") // a perfectly healthy encoder
		st := mgr.GetOrCreate("ov")
		fillRing(t, st, 4)

		_, cur := st.Hub.Join(30000)
		write := func([]byte) error { return errors.New("write tcp: broken pipe") }

		sig := pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
		if _, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264"); alive {
			t.Fatal("the viewer's write failed; the window must report the connection gone")
		}
		if out := buf.String(); strings.Contains(out, noOutputLine) {
			t.Errorf("the encoder produced plenty and the VIEWER went away, but the log accuses "+
				"ffmpeg of producing nothing:\n%s", out)
		}
	})

	t.Run("ffmpeg really produces nothing", func(t *testing.T) {
		buf := captureDebugLog(t)

		mgr := NewManager(1<<20, 30000, 2, 6, 0)
		mgr.SetOverlay(mutedFFmpeg(t), "/font.ttf") // reads its input, dies at encoder setup
		st := mgr.GetOrCreate("ov")
		fillRing(t, st, 4)

		_, cur := st.Hub.Join(30000)
		write := func([]byte) error { return nil }

		sig := pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
		if _, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264"); !alive {
			t.Fatal("the viewer connection never broke; a failed overlay must leave it serving")
		}
		if out := buf.String(); !strings.Contains(out, noOutputLine) {
			t.Errorf("ffmpeg produced nothing and the signal silently did not appear, with no line "+
				"saying so:\n%s", out)
		}
	})
}
