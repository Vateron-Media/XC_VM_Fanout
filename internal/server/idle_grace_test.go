package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// TestGraceStartsWhenTheViewerLeaves: attach() stamps lastAccess and nothing
// moves it during a session, so detach() must re-stamp it. Without that the mark
// is as old as the session was long, and the moment the last viewer left it had
// ALREADY outrun both grace_sec and idle_buffer_grace_sec — the puller was killed
// and the ring collapsed on the very next reaper tick, for every session longer
// than the grace itself. A channel change then tore down the source ffmpeg and
// cold-started it seconds later.
func TestGraceStartsWhenTheViewerLeaves(t *testing.T) {
	const grace = 10 * time.Second
	m := NewManager(1<<20, 20000, 6, 6, grace)
	m.idleBufferGraceNS.Store(int64(30 * time.Second))

	st := m.GetOrCreate("1")
	st.mu.Lock()
	c := puller.Source{URLs: []string{"http://src/live.ts"}}
	st.cfg, st.running = &c, true
	st.mu.Unlock()

	st.attach()
	// The viewer watches for a minute: attach() stamped lastAccess on arrival and
	// nothing on the serve path moves it again, so rewind it to model the session.
	st.lastAccess.Store(time.Now().Add(-time.Minute).UnixNano())
	st.detach() // the viewer leaves now...
	now := time.Now()

	st.mu.Lock()
	st.idleStopLocked(now)
	running := st.running
	gated := st.gateIdleBufferLocked(now)
	st.mu.Unlock()

	if !running {
		t.Errorf("puller stopped immediately after a 60s session ended; grace_sec=%v must be measured from the departure", grace)
	}
	if gated {
		t.Error("ring collapsed immediately after the session ended; idle_buffer_grace_sec must be measured from the departure")
	}

	// ...and once the grace really has elapsed since the departure, both fire.
	late := now.Add(31 * time.Second)
	st.mu.Lock()
	st.idleStopLocked(late)
	running = st.running
	gated = st.gateIdleBufferLocked(late)
	st.mu.Unlock()

	if running {
		t.Error("puller must idle-stop once grace_sec has elapsed since the last viewer left")
	}
	if !gated {
		t.Error("ring must gate once idle_buffer_grace_sec has elapsed since the last viewer left")
	}
}
