// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/nativesrc"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// idleWatchers counts the live nativesrc idle-timeout watcher goroutines. Each
// one holds a ticker and the reader it watches until it is Closed.
func idleWatchers(t *testing.T) int {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
		t.Fatalf("goroutine profile: %v", err)
	}
	return strings.Count(buf.String(), "nativesrc.(*idleTimeoutReader).watch")
}

// TestFfmpegStdoutWatcherStopsWithTheChild: the stall bound around ffmpeg's
// stdout is a goroutine plus a ticker, and it only stops when the wrapper is
// Closed or the bound fires. runFfmpeg passed the wrapper to ingest.Copy
// inline and never closed it, so every attempt — including the ordinary clean
// end — left a watcher sitting on the dead pipe for the whole stall bound (8s
// here, 24s for an HLS source). A node reconnecting hundreds of streams on
// backoff carries a standing population of them for no purpose.
func TestFfmpegStdoutWatcherStopsWithTheChild(t *testing.T) {
	dir := t.TempDir()
	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	payloadPath := filepath.Join(dir, "p.ts")
	if err := os.WriteFile(payloadPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	// A stand-in ffmpeg that emits the payload and exits 0: the normal end of a
	// source, the path that must not leave anything behind.
	bin := filepath.Join(dir, "fakeffmpeg")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\ncat "+payloadPath+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	before := idleWatchers(t)
	err := runFfmpeg(context.Background(), Source{FfmpegBin: bin, Label: "t"},
		"http://origin.invalid/live.ts", nativesrc.DefaultSourceIdleTimeout, 12032, func([]byte) {})
	if err != nil && err != io.EOF {
		t.Fatalf("runFfmpeg: %v", err)
	}

	// The watcher stops on Close, so it is gone as soon as runFfmpeg returns;
	// poll only to absorb scheduling, not the stall bound.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && idleWatchers(t) > before {
		time.Sleep(10 * time.Millisecond)
	}
	if n := idleWatchers(t); n > before {
		t.Fatalf("%d stdout idle-timeout watcher(s) outlived runFfmpeg; they hold a ticker and the dead pipe until the %s stall bound fires",
			n-before, nativesrc.DefaultSourceIdleTimeout)
	}
}
