// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// stubFFmpeg stands in for the overlay ffmpeg: it occupies the window for `d`
// and then writes `out` bytes to stdout. With out == 0 it is the failed overlay
// the window already knows how to survive — a -vcodec this build does not carry,
// a colour drawtext rejects, an off-air stream with no video to burn the banner
// onto — which sits on the window, produces nothing, and dies.
func stubFFmpeg(t *testing.T, d time.Duration, out int) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "stubffmpeg")
	script := fmt.Sprintf("#!/bin/sh\nsleep %.2f\n", d.Seconds())
	if out > 0 {
		script += fmt.Sprintf("head -c %d /dev/zero\n", out)
	}
	script += "exit 1\n"
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// overlayIdleRun serves one ghost viewer of an off-air stream that has an admin
// "send message" waiting for it, and reports how long the daemon held the
// connection and how many bytes it ever delivered.
func overlayIdleRun(t *testing.T, idle, window time.Duration, out int) (time.Duration, int64) {
	t.Helper()
	shortOverlayWindow(t, 3*time.Second) // a backstop; the stand-in exits on its own

	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.SetOverlay(stubFFmpeg(t, window, out), "/font.ttf")
	mgr.viewerIdleNS.Store(int64(idle))
	mgr.GetOrCreate("5") // off-air: registered, never published
	mgr.signals.set("ghost", pendingSignal{text: "hello", fontSize: 20, color: "white", x: 10, y: 10})

	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	start := time.Now()
	resp, err := http.Get(ts.URL + "/live/5?c=ghost&prebuffer=0&vc=h264")
	if err != nil {
		t.Fatalf("GET /live/5: %v", err)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return time.Since(start), n
}

// TestOverlayDeliveringNothingDoesNotResetTheIdleTimer: the idle timer exists to
// notice a viewer that is RECEIVING nothing, and an overlay window is not an
// exception to that.
//
// serveLive reset it unconditionally after an overlay, on the strength of a
// comment — "the overlay delivered a window of video, not silence" — that stopped
// being true when the window learned to deliver nothing and keep the viewer's
// cursor. A ghost connection on an off-air stream, addressed by an admin "send
// message", then bought itself the whole window plus a fresh idle period before
// the reaper could close it: exactly the connection the idle timer was added for,
// kept alive by a banner nobody ever saw.
func TestOverlayDeliveringNothingDoesNotResetTheIdleTimer(t *testing.T) {
	const (
		idle   = 800 * time.Millisecond
		window = 600 * time.Millisecond
	)
	elapsed, n := overlayIdleRun(t, idle, window, 0)
	if n != 0 {
		t.Fatalf("premise broken: %d bytes reached the viewer; the overlay was supposed to deliver nothing", n)
	}
	if elapsed > idle+window/2 {
		t.Errorf("a viewer that received nothing was held for %s, one idle window being %s: "+
			"the failed overlay reset the idle timer, so the ghost got the whole %s window and then a full fresh period",
			elapsed.Round(10*time.Millisecond), idle, window)
	}
}

// TestOverlayDeliveringVideoResetsTheIdleTimer is the other half: a window that
// DID reach the viewer must reset the timer, or an overlay long enough to outlast
// one idle period would drop the viewer it had just served.
func TestOverlayDeliveringVideoResetsTheIdleTimer(t *testing.T) {
	const (
		idle   = 800 * time.Millisecond
		window = 600 * time.Millisecond
	)
	elapsed, n := overlayIdleRun(t, idle, window, 4096)
	if n == 0 {
		t.Fatalf("premise broken: the stand-in wrote its output but nothing reached the viewer")
	}
	if elapsed < idle+window/2 {
		t.Errorf("a viewer served %d bytes of overlay was dropped after %s, one idle window being %s: "+
			"the idle timer was not reset by a window that did deliver", n, elapsed.Round(10*time.Millisecond), idle)
	}
}
