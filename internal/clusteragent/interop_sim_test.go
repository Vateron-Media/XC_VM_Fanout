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

// clock runs MAIN's clock d ahead of this machine's from now on (0 puts it
// back): the harness fixes MAIN's ClusterClock at the time plus d for each
// request, so tokens and leases are issued, and stamps checked, in that time.
func (rig *interopRig) clock(t *testing.T, d time.Duration) {
	t.Helper()
	flag := filepath.Join(rig.dir, "main.sqlite.clock")
	if d == 0 {
		os.Remove(flag)
	} else if err := os.WriteFile(flag, []byte(strconv.FormatInt(d.Milliseconds(), 10)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// waitLeases waits until every node has completed its enrolment and holds a
// lease issued at iat or later, failing the test past within.
func waitLeases(t *testing.T, agents []*Agent, iat int64, within time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		n := 0
		for _, a := range agents {
			st := a.Client.State
			st.mu.Lock()
			if st.Enrolled && st.Lease != nil && st.Lease.Iat >= iat {
				n++
			}
			st.mu.Unlock()
		}
		if n == len(agents) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d nodes hold a lease %s, after %s", n, len(agents), what, within)
		}
		time.Sleep(50 * time.Millisecond)
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

	// MAIN's clock is this machine's: a lease issued once the licence is back
	// has an iat of at least back, and the enrolment's lease, a second or more
	// before it, never has (a second's slack let the last node's pass).
	back := time.Now().Unix()
	rig.licence(t, true)
	// Well under a minute even on a loaded machine: one php -S serves the fleet.
	waitLeases(t, agents, back, 20*time.Second, "fresh, after the licence came back")
	for i, e := range epochs() {
		if e <= before[i] {
			t.Fatalf("node %d still on epoch %d", 7+i, e)
		}
	}
}

// MAIN's clock two hours ahead, past every token's expiry: each node takes
// MAIN's time from the CLOCK_SKEW refusal, re-keys, and holds a token and a
// lease issued at MAIN's new time.
func TestInteropSimAFleetFollowsMainsClockPastItsTokens(t *testing.T) {
	rig, agents := newClusterSim(t, 3)
	for _, a := range agents {
		a.Interval = 200 * time.Millisecond
	}
	runFleet(t, rig, agents)
	waitLeases(t, agents, 0, 30*time.Second, "once enrolled")
	var before []uint64
	for _, a := range agents {
		tok, _ := a.Client.Current()
		before = append(before, tok.Epoch)
	}

	ahead := 2 * time.Hour
	rig.clock(t, ahead)
	waitLeases(t, agents, time.Now().Add(ahead).Unix()-1, 30*time.Second, "issued at MAIN's new time")
	for i, a := range agents {
		if tok, _ := a.Client.Current(); tok.Epoch <= before[i] {
			t.Fatalf("node %d still on epoch %d: its expired token was not replaced", 7+i, tok.Epoch)
		}
	}
}
