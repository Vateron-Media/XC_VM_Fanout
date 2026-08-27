// Package dlog is the daemon's optional debug log: a thin wrapper over the
// standard logger that stays silent unless debug mode is enabled (-debug or
// XC_FANOUT_DEBUG=1). When off, every call is a single atomic load and returns
// before formatting, so the hot paths (per-chunk publish, per-viewer write) pay
// almost nothing for the instrumentation left in place.
//
// The point is operational visibility: with -debug on, the daemon narrates what
// it is doing right now — streams registering, pullers starting/reconnecting,
// viewers attaching and dropping, HLS segments rolling, ingest producers
// connecting — plus a periodic per-stream state snapshot, so an operator can see
// both the live activity and where it is stuck.
package dlog

import (
	"log"
	"sync/atomic"
)

var on atomic.Bool

// Enable turns debug logging on or off. Off by default. Call once at startup.
func Enable(v bool) { on.Store(v) }

// On reports whether debug logging is enabled. Use it to guard log calls whose
// arguments are themselves expensive to compute (e.g. walking a map), so that
// work is skipped when debug is off.
func On() bool { return on.Load() }

// Logf emits one debug line tagged with cat (e.g. "stream", "puller", "viewer")
// when debug mode is on, and does nothing otherwise. Keep cat short and stable —
// operators grep by it.
func Logf(cat, format string, args ...any) {
	if !on.Load() {
		return
	}
	log.Printf("[dbg %-7s] "+format, append([]any{cat}, args...)...)
}
