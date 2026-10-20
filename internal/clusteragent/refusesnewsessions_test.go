package clusteragent

import (
	"testing"
	"time"
)

// refusesNewSessions is the lease verdict RtmpOffline consults: a node whose
// lease is in or past its drain window (DRAINING or FENCED on MAIN's clock,
// with lb_lease_fence on) refuses to START a new viewer, even though a viewer
// already playing is only dropped once the drain is over (leaseFenced). Mirrors
// the panel's NodeLease::refusesNewSessions (state != SERVING, from exp onward).
func TestRefusesNewSessions(t *testing.T) {
	now := time.Now().UnixMilli()
	on := map[string]any{"lb_lease_fence": "1", "lb_fence_drain_min": "10"}

	if a := leaseFenceAgent(t, "", now/1000+300, now, on); a.refusesNewSessions() {
		t.Fatal("SERVING (lease exp in the future) must not refuse new sessions")
	}
	if a := leaseFenceAgent(t, "", now/1000-60, now, on); !a.refusesNewSessions() {
		t.Fatal("DRAINING (past exp, within drain) must refuse new sessions")
	}
	if a := leaseFenceAgent(t, "", now/1000-20*60, now, on); !a.refusesNewSessions() {
		t.Fatal("FENCED (past exp + drain) must refuse new sessions")
	}
	if a := leaseFenceAgent(t, "", now/1000-60, now, map[string]any{"lb_lease_fence": "0"}); a.refusesNewSessions() {
		t.Fatal("lease fence off must not refuse")
	}
	a := leaseFenceAgent(t, "", now/1000-60, now, on)
	a.mode.Store(0)
	if a.refusesNewSessions() {
		t.Fatal("a non-cluster node must not refuse")
	}
	a = leaseFenceAgent(t, "", now/1000-60, now, on)
	a.Client.State.Lease = nil
	if a.refusesNewSessions() {
		t.Fatal("no lease must not refuse")
	}
}
