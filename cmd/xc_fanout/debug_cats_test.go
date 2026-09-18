// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"strings"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
)

// The panel drives debug live through config.json's debug_cats, so
// applyDebugCats must turn narration on, narrow it, and turn it off again as the
// value changes — and must NOT touch it when the CLI pinned debug.
func TestApplyDebugCatsLiveToggle(t *testing.T) {
	dlog.EnableCats("") // start from a known-off state
	defer dlog.EnableCats("")

	applyDebugCats("puller,hls", false)
	if !dlog.OnCat("puller") || !dlog.OnCat("hls") || dlog.OnCat("monitor") {
		t.Fatalf("after 'puller,hls': cats = %v, want just those two", dlog.Cats())
	}

	applyDebugCats("monitor", false)
	if !dlog.OnCat("monitor") || dlog.OnCat("puller") {
		t.Fatalf("after 'monitor': cats = %v, want just monitor", dlog.Cats())
	}

	applyDebugCats("all", false)
	if !dlog.OnCat("anything") || len(dlog.Cats()) != 0 {
		t.Fatalf("after 'all': every category should be on; Cats()=%v", dlog.Cats())
	}

	applyDebugCats("", false)
	if dlog.On() {
		t.Fatalf("after '': debug should be off; Cats()=%v", dlog.Cats())
	}
}

// A daemon started with a -debug flag pins the selection: the panel's config
// must not override it, or a developer's override would vanish on the first poll.
func TestApplyDebugCatsRespectsThePin(t *testing.T) {
	dlog.EnableCats("puller") // stands in for the flag having set it
	defer dlog.EnableCats("")

	applyDebugCats("", true) // the config says off, but debug is pinned
	if !dlog.OnCat("puller") {
		t.Error("a pinned debug selection was cleared by the config")
	}
	applyDebugCats("monitor", true) // and the config cannot widen or change it either
	if dlog.OnCat("monitor") || !dlog.OnCat("puller") {
		t.Errorf("a pinned debug selection was changed by the config: cats=%v", dlog.Cats())
	}
}

// The category names the config comment lists must be the ones the code
// actually logs under, or an operator selecting from the panel gets silence.
func TestDocumentedCategoriesMatchTheComment(t *testing.T) {
	// Named in internal/config Values.DebugCats.
	documented := "boot config stream puller hls viewer ingest ctl signal monitor stats buffer reaper mem"
	for _, c := range strings.Fields(documented) {
		dlog.EnableCats(c)
		if !dlog.OnCat(c) {
			t.Errorf("category %q does not select", c)
		}
	}
	dlog.EnableCats("")
}
