// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"
)

// TestSignalCarriesTheViewerItIsAddressedTo: the window can only watch the
// kicked viewer's channel if the signal it was handed still says whose it is.
// serveSignal builds the pendingSignal and the store keys it by uuid, so
// stamping it in set() is the only place that link is made — drop it and the
// kick watch below silently turns into a no-op for every real signal.
func TestSignalCarriesTheViewerItIsAddressedTo(t *testing.T) {
	s := newSignalStore()
	s.set("v-9", pendingSignal{text: "hello"})
	sig, ok := s.take("v-9")
	if !ok {
		t.Fatal("the signal just queued was not returned")
	}
	if sig.uuid != "v-9" {
		t.Fatalf("taken signal is addressed to %q, want %q", sig.uuid, "v-9")
	}
}

// TestOverlayTSWindowEndsWhenThePanelKicksTheViewer: DELETE /connections/<uuid>
// is the panel's only way to end a daemon-served session — an admin "kill
// connection", a connection-limit eviction, a line that has just been banned or
// has expired. It closes the viewer's kill channel and nothing else: the socket
// stays open, so every write to it keeps succeeding.
//
// serveLive checks that channel once per loop iteration, and the banner window
// is a whole iteration. A viewer kicked while an admin "send message" was being
// burned onto its stream therefore went on receiving video for the rest of the
// window — the full overlayTSDuration, five seconds in production — after the
// panel had already decided it must stop. A banned line kept watching, and a
// connection-limit eviction did not free the slot when it said it had.
func TestOverlayTSWindowEndsWhenThePanelKicksTheViewer(t *testing.T) {
	const window = 4 * time.Second
	shortOverlayWindow(t, window)

	mgr := NewManager(1<<20, 30000, 2, 6, 0)
	mgr.SetOverlay(catFFmpeg(t), "/font.ttf")
	st := mgr.GetOrCreate("ov")
	fillRing(t, st, 4)

	// The viewer, as serveLive registers it: a uuid from live.php's X-Accel URL.
	st.addConn("v-1")
	defer st.removeConn("v-1")

	_, cur := st.Hub.Join(30000)
	write := func([]byte) error { return nil } // a kicked socket still accepts writes

	type result struct {
		alive bool
		took  time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		sig := pendingSignal{uuid: "v-1", text: "hello", fontSize: 20, color: "white", x: 10, y: 10}
		_, alive := mgr.overlayTSWindow(st, cur, write, sig, "h264")
		done <- result{alive, time.Since(start)}
	}()

	time.Sleep(100 * time.Millisecond) // the banner is mid-window
	if !st.dropConn("v-1") {
		t.Fatal("the viewer was not registered on the stream")
	}

	select {
	case r := <-done:
		t.Logf("overlayTSWindow returned %v after the kick", r.took)
		if !r.alive {
			t.Error("a kick is not a broken connection: the window must hand the session back so " +
				"serveLive returns on its own killC, with the reason the operator kicked it for")
		}
	case <-time.After(window / 2):
		t.Fatalf("overlayTSWindow is still serving a viewer the panel kicked %v ago; it runs to the "+
			"end of the banner window (%v in this test, %v in production) because dropConn only "+
			"closes the kill channel — the socket stays writable and nothing in the window looks "+
			"at it", 100*time.Millisecond, window, 5*time.Second)
	}
}
