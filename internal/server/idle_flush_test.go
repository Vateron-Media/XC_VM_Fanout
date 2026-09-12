// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"context"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestIdleStopReleasesTheRing pins what was the daemon's largest idle memory
// term.
//
// The reaper stopped an unwatched stream's puller and left the ring exactly as
// it was. prune only ever runs from Update, so with no producer nothing touched
// those bytes again: a registered-but-unwatched channel sat on
// idle_buffer_ratio × prebuffer_max_sec of video — ~20 s at the defaults — for as
// long as it stayed registered. Across a few hundred channels that is gigabytes
// of frozen video that no viewer could ever be served (it is minutes or hours
// stale by the time anyone returns).
//
// Idle-stop now flushes it, and reports that it did so the memory scavenger can
// hand the pages back rather than waiting out its rate floor.
func TestIdleStopReleasesTheRing(t *testing.T) {
	m := NewManager(1<<20, 30000, 2, 6, 10*time.Millisecond)
	st := m.GetOrCreate("flush-me")

	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st.mu.Lock()
	c := puller.Source{URLs: []string{"http://src/live.ts"}}
	st.cfg, st.running, st.cancel = &c, true, cancel
	st.mu.Unlock()

	// Fill the ring and cut at least one HLS segment from it.
	feedStream(st)
	for sec := int64(6); sec < 16; sec += 2 {
		st.Publish(tsfixture.KeyframePCR(0x101, sec*90000, sec*90000))
	}
	if st.Hub.HLSPlaylist() == "" {
		t.Fatal("fixture cut no HLS segments; nothing to prove about releasing them")
	}
	// Warm the segment cache too — an idle channel should not hold copies of
	// segments on top of the ring they were cut from.
	seq := 0
	for ; seq < 8; seq++ {
		if st.hlsSegment(seq) != nil {
			break
		}
	}
	if seq == 8 {
		t.Fatal("no segment was servable before the idle-stop")
	}

	// No viewers and stale access: the reaper's idle-stop fires.
	st.lastAccess.Store(time.Now().Add(-time.Minute).UnixNano())
	st.mu.Lock()
	freed := st.idleStopLocked(time.Now())
	running := st.running
	st.mu.Unlock()

	if running {
		t.Fatal("puller did not idle-stop")
	}
	if !freed {
		t.Error("idleStopLocked must report that it released memory, so the scavenger skips its rate floor")
	}
	if pl := st.Hub.HLSPlaylist(); pl != "" {
		t.Errorf("a stopped stream still advertises segments it cut before the source died:\n%s", pl)
	}
	if b := st.Hub.HLSSegment(seq); b != nil {
		t.Errorf("segment %d still assembles from a ring whose producer is gone (%d bytes retained)", seq, len(b))
	}
	st.segMu.Lock()
	cached := len(st.segCache)
	st.segMu.Unlock()
	if cached != 0 {
		t.Errorf("%d encrypted segment copies survived the idle-stop", cached)
	}

	// And the stream must come back cleanly: a viewer returning restarts the
	// puller and the ring refills, exactly as for a channel never started.
	st.touch()
	st.mu.Lock()
	restarted := st.running
	st.mu.Unlock()
	if !restarted {
		t.Fatal("a returning viewer did not restart the flushed stream")
	}
	feedStream(st)
	// A joining viewer gets a clean-entry snapshot again — proof the ring refilled
	// after the flush (viewers follow the ring now; Snapshot reads it the same way).
	if snap := st.Hub.Snapshot(0); len(snap) == 0 {
		t.Error("the ring did not refill after a flush: a joiner got no clean-entry snapshot")
	}
}
