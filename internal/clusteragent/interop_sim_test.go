package clusteragent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// newClusterSim is xc_cluster_sim: MAIN's real PHP ClusterApi (the interop
// harness) with n agents enrolled by code on servers 7, 8, …, so what a fleet
// does can be tested without one (ADR 0004, "xc_cluster_sim").
func newClusterSim(t *testing.T, n int) (*interopRig, []*Agent) {
	t.Helper()
	var others []string
	for sid := 8; sid < 7+n; sid++ {
		others = append(others, strconv.Itoa(sid))
	}
	rig := interopMain(t, "XCVM_INTEROP_SERVERS="+strings.Join(others, ","))
	agents := make([]*Agent, n)
	for i := range agents {
		agents[i] = rig.enrol(t, 7+i)
	}
	return rig, agents
}

// licence takes MAIN's licence away (false) or gives it back: without it the
// harness issues no token and no lease, and sessions go on (graceful mode).
func (rig *interopRig) licence(t *testing.T, on bool) {
	t.Helper()
	flag := filepath.Join(rig.dir, "main.sqlite.unlicensed")
	if on {
		os.Remove(flag)
	} else if err := os.WriteFile(flag, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// runFleet runs every agent's loop until the test ends.
func runFleet(t *testing.T, rig *interopRig, agents []*Agent) {
	ctx, cancel := context.WithCancel(rig.ctx)
	var wg sync.WaitGroup
	for _, a := range agents {
		wg.Add(1)
		go func(a *Agent) {
			defer wg.Done()
			a.Run(ctx)
		}(a)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
}

// A fleet whose MAIN lost its licence past the nodes' refresh_at: every
// refresh is refused, and the first tick after the licence returns brings
// each node a token and a fresh lease (ADR 0004, "Re-licensing a fleet").
func TestInteropSimARelicensedFleetGetsItsLeasesBack(t *testing.T) {
	rig, agents := newClusterSim(t, 3)
	rig.licence(t, false)
	epochs := func() (out []uint64) {
		for _, a := range agents {
			tok, _ := a.Client.Current()
			out = append(out, tok.Epoch)
		}
		return out
	}
	before := epochs()
	for _, a := range agents {
		a.Interval = 200 * time.Millisecond
		s, _ := a.Client.current()
		s.tok.RefreshAt = 0 // the lapse outlasted refresh_at
	}
	runFleet(t, rig, agents)

	time.Sleep(time.Second)
	if got := epochs(); fmt.Sprint(got) != fmt.Sprint(before) {
		t.Fatalf("an unlicensed MAIN issued tokens: epochs %v, were %v", got, before)
	}

	back := time.Now().Unix()
	rig.licence(t, true)
	// Well under a minute even on a loaded machine: one php -S serves the fleet.
	deadline := time.Now().Add(20 * time.Second)
	for {
		fresh := 0
		for _, a := range agents {
			st := a.Client.State
			st.mu.Lock()
			if st.Lease != nil && st.Lease.Iat >= back-1 {
				fresh++
			}
			st.mu.Unlock()
		}
		if fresh == len(agents) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d nodes hold a fresh lease 20 s after the licence came back", fresh, len(agents))
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i, e := range epochs() {
		if e <= before[i] {
			t.Fatalf("node %d still on epoch %d", 7+i, e)
		}
	}
}
