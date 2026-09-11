// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"
)

// TestViewerGateIdleBuffer exercises the viewer-gated buffer decision directly:
// an unwatched stream collapses to the idle floor only when it has no live
// viewers AND nothing has touched it within the idle-buffer grace window, and it
// is restored to the full buffer the instant a viewer returns.
func TestViewerGateIdleBuffer(t *testing.T) {
	m := NewManager(1<<20, 20000, 6, 6, 100*time.Millisecond)
	m.idleBufferGraceNS.Store(int64(100 * time.Millisecond))

	st := m.GetOrCreate("1")
	if !st.buffered {
		t.Fatal("a new stream must start fully buffered")
	}

	// Recent access → must NOT gate.
	st.lastAccess.Store(time.Now().UnixNano())
	st.mu.Lock()
	st.gateIdleBufferLocked(time.Now())
	st.mu.Unlock()
	if !st.buffered {
		t.Fatal("gated despite recent access")
	}

	// Live viewer present with stale access → must NOT gate.
	st.refs = 1
	st.lastAccess.Store(time.Now().Add(-time.Second).UnixNano())
	st.mu.Lock()
	st.gateIdleBufferLocked(time.Now())
	st.mu.Unlock()
	if !st.buffered {
		t.Fatal("gated despite a live viewer")
	}

	// No viewers, stale access → gate fires.
	st.refs = 0
	st.mu.Lock()
	st.gateIdleBufferLocked(time.Now())
	st.mu.Unlock()
	if st.buffered {
		t.Fatal("must gate an idle stream past the grace window")
	}

	// A viewer returns (HLS touch) → ring restored to full.
	st.touch()
	if !st.buffered {
		t.Fatal("a returning viewer must restore the full buffer")
	}

	// Gate disabled (grace 0) → never gates, however stale.
	m.idleBufferGraceNS.Store(0)
	st.refs = 0
	st.lastAccess.Store(time.Now().Add(-time.Hour).UnixNano())
	st.mu.Lock()
	st.gateIdleBufferLocked(time.Now())
	st.mu.Unlock()
	if !st.buffered {
		t.Fatal("a disabled gate must keep the stream buffered")
	}
}

// TestResolvePrebufMS: the panel is authoritative — a passed ?prebuffer= is
// honored as-is (including 0); a blank param falls back to the daemon default;
// everything is clamped to the ring.
func TestResolvePrebufMS(t *testing.T) {
	m := NewManager(1<<20, 40000, 6, 6, time.Second) // ring ceiling 40 s
	m.defaultPrebufMS.Store(8000)                    // fallback 8 s

	cases := []struct {
		name  string
		param string
		want  int64
	}{
		{"absent → default", "", 8000},
		{"explicit 0 honored (not overridden)", "0", 0},
		{"explicit value", "15", 15000},
		{"clamped to ring", "999", 40000},
		{"garbage → default", "abc", 8000},
		{"negative → default", "-5", 8000},
	}
	for _, c := range cases {
		if got := m.resolvePrebufMS(c.param); got != c.want {
			t.Errorf("%s: resolvePrebufMS(%q) = %d, want %d", c.name, c.param, got, c.want)
		}
	}
}
