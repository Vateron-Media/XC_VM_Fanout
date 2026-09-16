// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"context"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/config"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// idleRunningStream registers a stream that looks like a channel whose last
// viewer left idleFor ago: control-managed, puller running, no refs, stale
// access.
func idleRunningStream(t *testing.T, m *Manager, id string, idleFor time.Duration) *Stream {
	t.Helper()
	st := m.GetOrCreate(id)
	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st.mu.Lock()
	src := puller.Source{URLs: []string{"http://src/live.ts"}}
	st.cfg, st.running, st.cancel = &src, true, cancel
	st.mu.Unlock()
	st.lastAccess.Store(time.Now().Add(-idleFor).UnixNano())
	return st
}

// TestApplyConfigLowersGraceOnExistingStreams: grace_sec is one of the keys the
// panel applies live, and the config contract is that a change reaches streams
// that are already registered within one poll. Stream.grace was copied at
// creation and never updated, so a node booted with grace_sec=3600 (channels
// kept warm) went on holding every registered channel's puller open for an hour
// after the operator lowered grace_sec to 10 — until the stream was deleted and
// recreated.
func TestApplyConfigLowersGraceOnExistingStreams(t *testing.T) {
	m := NewManager(1<<20, 20000, 6, 6, time.Hour)
	st := idleRunningStream(t, m, "warm", 5*time.Minute)

	v := config.Defaults()
	v.GraceSec = 10 // the operator lowers it in the panel
	m.ApplyConfig(v)

	st.mu.Lock()
	st.idleStopLocked(time.Now())
	running := st.running
	grace := st.grace
	st.mu.Unlock()

	if running {
		t.Fatalf("a stream registered under grace_sec=3600 still uses grace=%s after the "+
			"panel lowered grace_sec to 10 s: the change never reached it", grace)
	}
}

// TestReaperIntervalFollowsTheLiveWindows: the sweep cadence is derived from the
// windows it enforces, re-read on every tick, not frozen at boot.
func TestReaperIntervalFollowsTheLiveWindows(t *testing.T) {
	m := NewManager(1<<20, 20000, 6, 6, 10*time.Second)
	m.idleBufferGraceNS.Store(int64(30 * time.Second))
	if got := m.reapInterval(); got != 5*time.Second {
		t.Errorf("default grace_sec=10 → interval %s, want 5s (half the shorter window)", got)
	}

	// A node kept warm: grace_sec=3600 must not put the 30 s idle-buffer gate on a
	// 30-minute sweep.
	m.mu.Lock()
	m.grace = time.Hour
	m.mu.Unlock()
	if got := m.reapInterval(); got > reapMaxInterval {
		t.Errorf("grace_sec=3600 → interval %s, want at most %s: the idle-buffer gate is checked on this tick", got, reapMaxInterval)
	}

	// And a tiny grace must not spin the sweep.
	m.mu.Lock()
	m.grace = time.Millisecond
	m.mu.Unlock()
	if got := m.reapInterval(); got < reapMinInterval {
		t.Errorf("grace_sec≈0 → interval %s, want at least %s", got, reapMinInterval)
	}
}

// TestReaperTickFollowsLoweredGrace drives the reaper goroutine end-to-end: the
// tick was computed once, from the boot grace_sec, so a node booted warm swept
// every half hour and neither the operator's later grace_sec change nor the
// idle-buffer gate it also drives could be seen for that long. The sweep must
// re-read its cadence instead.
func TestReaperTickFollowsLoweredGrace(t *testing.T) {
	m := NewManager(1<<20, 20000, 6, 6, time.Hour) // booted warm: grace_sec=3600
	st := idleRunningStream(t, m, "warm", 5*time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartReaper(ctx)

	v := config.Defaults()
	v.GraceSec = 1 // the operator lowers it right after boot
	m.ApplyConfig(v)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		st.mu.Lock()
		running := st.running
		st.mu.Unlock()
		if !running {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the reaper never swept an idle stream after grace_sec was lowered: the tick is still the boot grace_sec/2")
}
