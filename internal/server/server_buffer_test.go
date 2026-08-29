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
	m.idleBufferMS.Store(2000)
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
