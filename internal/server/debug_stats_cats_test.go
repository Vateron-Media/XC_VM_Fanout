// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
)

// statsLoopRunning reports whether the periodic stats goroutine exists: it is the
// only thing that can be parked in StartDebugStats' closure.
func statsLoopRunning() bool {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Contains(string(buf[:n]), "StartDebugStats.func1")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// TestStartDebugStatsHonoursTheCategoryFilter: the per-stream snapshot loop was
// gated on dlog.On(), which is true for ANY enabled category. An operator who
// narrowed debug to one subsystem (-debug-cats=puller — the whole reason the
// filter exists) still had the loop wake every few seconds on a 500-channel node
// to take each stream's mu and connMu and its hub lock (NoKeyframeCuts) and
// format a line per stream, every one of which Logf then dropped. It must run
// only for the categories it writes to.
func TestStartDebugStatsHonoursTheCategoryFilter(t *testing.T) {
	captureDebugLog(t) // restores the log writer and disables debug afterwards

	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	feedStream(mgr.GetOrCreate("42"))

	// Sanity: with its own category on, the loop must be running — otherwise the
	// negative case below would prove nothing.
	dlog.EnableCats("stats")
	statsCtx, statsCancel := context.WithCancel(context.Background())
	mgr.StartDebugStats(statsCtx, 10*time.Millisecond)
	if !waitFor(statsLoopRunning) {
		t.Fatal("no stats loop with -debug-cats=stats: the snapshot would never be written")
	}
	statsCancel()
	if !waitFor(func() bool { return !statsLoopRunning() }) {
		t.Fatal("the stats loop outlived its context")
	}

	// Another subsystem selected: nothing this loop writes is enabled, so it must
	// not run at all.
	dlog.EnableCats("puller")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.StartDebugStats(ctx, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if statsLoopRunning() {
		t.Fatal("the stats loop walks every stream under -debug-cats=puller, and Logf drops every line it produces")
	}
}
