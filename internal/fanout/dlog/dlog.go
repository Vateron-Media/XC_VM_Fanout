// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package dlog is the daemon's optional debug log: a thin wrapper over the
// standard logger that stays silent unless debug mode is enabled (-debug,
// -debug-cats or XC_FANOUT_DEBUG). When off, every call is a single atomic load
// and returns before formatting, so the hot paths (per-chunk publish, per-viewer
// write) pay almost nothing for the instrumentation left in place.
//
// The point is operational visibility: with debug on, the daemon narrates what
// it is doing right now — streams registering, pullers starting/reconnecting and
// WHICH source path they settled on, viewers attaching and dropping, HLS
// segments rolling, ingest producers connecting, the encoder supervisor ticking
// — plus a periodic per-stream state snapshot, so an operator can see both the
// live activity and where it is stuck.
//
// # Categories
//
// Every line carries a category ("puller", "monitor", …). Verbose narration is
// only useful if it can be narrowed, so debug mode is selectable: Enable(true)
// turns on everything, and EnableCats("puller,monitor") turns on just those. A
// node running 500 channels can then watch one subsystem without the other 499
// streams' chatter burying it.
package dlog

import (
	"log"
	"sort"
	"strings"
	"sync/atomic"
)

// filter is the active selection. A nil pointer means debug is off entirely; a
// non-nil one with all=true means every category. Held in a single atomic
// pointer so the check on a disabled log stays one load.
type filter struct {
	all bool
	set map[string]bool
}

var active atomic.Pointer[filter]

// Enable turns debug logging on (every category) or off. Off by default. Call
// once at startup.
func Enable(v bool) {
	if !v {
		active.Store(nil)
		return
	}
	active.Store(&filter{all: true})
}

// EnableCats turns debug logging on for a comma-separated set of categories
// ("puller,monitor"). An empty or whitespace-only spec disables debug entirely;
// "all" (or "1"/"true") enables every category, so the environment variable and
// the flag can share one spelling. Names are matched case-insensitively after
// trimming, because these arrive from a systemd drop-in as often as from a
// shell.
func EnableCats(spec string) {
	set := make(map[string]bool)
	for _, raw := range strings.Split(spec, ",") {
		name := strings.ToLower(strings.TrimSpace(raw))
		switch name {
		case "":
			continue
		case "all", "1", "true", "yes", "on":
			active.Store(&filter{all: true})
			return
		}
		set[name] = true
	}
	if len(set) == 0 {
		active.Store(nil)
		return
	}
	active.Store(&filter{set: set})
}

// On reports whether debug logging is enabled for ANY category. Use it to guard
// work that feeds several categories at once; prefer OnCat when the work serves
// one.
func On() bool { return active.Load() != nil }

// OnCat reports whether cat is being logged. Use it to guard log calls whose
// arguments are themselves expensive to compute (e.g. walking every stream), so
// that work is skipped when the category is off.
func OnCat(cat string) bool {
	f := active.Load()
	if f == nil {
		return false
	}
	return f.all || f.set[strings.ToLower(cat)]
}

// Cats lists the enabled categories, or nil when every one is on (or debug is
// off). For the startup banner, so an operator can see what they actually asked
// for rather than discovering it from the absence of lines.
func Cats() []string {
	f := active.Load()
	if f == nil || f.all {
		return nil
	}
	out := make([]string, 0, len(f.set))
	for c := range f.set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Logf emits one debug line tagged with cat (e.g. "stream", "puller", "monitor")
// when that category is enabled, and does nothing otherwise. Keep cat short and
// stable — operators grep by it, and select by it.
func Logf(cat string, format string, args ...any) {
	f := active.Load()
	if f == nil {
		return
	}
	if !f.all && !f.set[strings.ToLower(cat)] {
		return
	}
	log.Printf("[dbg %-7s] "+format, append([]any{cat}, args...)...)
}
