package clusteragent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Rebuilding the registry after an agent restart (plan, section 8, "Global
// max_connections and kills": "the registry is rebuilt from registry.snap,
// fanout GET /connections, the HLS markers and FPM pids"). registry.snap is
// what the registry held when the agent last saved it (every second); while
// the agent was down, the node's viewers kept coming and going, and their
// ends reached MAIN's store directly or not at all. Once, after the first
// reply that says CONNECTIONS is on, the agent checks the records it
// restored against what still serves them:
//
//   - daemon-served TS viewers (pid 0): the fanout's GET /connections?detail=1
//     (or the bare uuid list of an older daemon) says which are still
//     connected. One the fanout no longer holds left while the agent was
//     down: it ends with a P0 conn.close, as the fanout's conn_close would
//     have. A fanout that cannot be reached is asked again (RebuildTries);
//     without an answer nothing ends here, and fanout_sync still reconciles.
//   - PHP-served viewers (pid > 0: VOD, timeshift, the pre-fanout TS loop):
//     one whose FPM worker is gone (no /proc/<pid>, or a pid reused by
//     something that is not PHP) ended while the agent was down, and ends
//     with a P0 conn.close. MAIN applies a close of a viewer it already closed
//     as a no-op.
//   - HLS viewers have no worker and no fanout connection; their marker is
//     their own hls_last_read. Each restored one gets a full HLSReapAfter to
//     ask for its playlist again (NewRegistry), and the reaper ends the rest.
//
// A viewer the fanout serves that registry.snap does not hold (it opened
// while the agent was down) cannot be rebuilt: the fanout knows its uuid and
// stream, not its line. It is counted and logged; its PHP registered it in
// MAIN's store, and the next heartbeat's digest decides whether MAIN needs a
// snapshot.

// RebuildTries is how often the rebuild asks the fanout before it gives up
// on it; RebuildRetry is the wait between tries.
var (
	RebuildTries = 12
	RebuildRetry = 5 * time.Second
)

// ProcDir is where the workers' pids are looked up.
var ProcDir = "/proc"

// workerAlive reports whether pid is a live PHP process (an FPM worker); ok is
// false when it cannot tell: no /proc, or one that hides other processes
// (hidepid), where a live worker would look gone.
func workerAlive(pid int64) (alive, ok bool) {
	if _, err := os.Stat(filepath.Join(ProcDir, "1")); err != nil {
		return false, false
	}
	comm, err := os.ReadFile(filepath.Join(ProcDir, strconv.FormatInt(pid, 10), "comm"))
	if errors.Is(err, os.ErrNotExist) {
		return false, true
	}
	if err != nil {
		return true, false
	}
	return strings.HasPrefix(strings.TrimSpace(string(comm)), "php"), true
}

// workerless returns the open, non-HLS records whose PHP worker is gone.
func (r *Registry) workerless(alive func(int64) (bool, bool)) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for uuid, c := range r.conns {
		pid := intOf(c["pid"])
		if pid <= 0 || fmt.Sprint(c["container"]) == "hls" || num(c["hls_end"]) != 0 {
			continue
		}
		if on, known := alive(pid); known && !on {
			out = append(out, uuid)
		}
	}
	sort.Strings(out)
	return out
}

// daemonGone splits the registry's open daemon-served viewers by what the
// fanout holds: the uuids it no longer serves, and how many it serves that
// the registry does not hold.
func (r *Registry) daemonGone(live map[string]fanoutConn) (gone []string, unknown int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for uuid, c := range r.conns {
		if intOf(c["pid"]) != 0 || fmt.Sprint(c["container"]) == "hls" || num(c["hls_end"]) != 0 {
			continue
		}
		if _, on := live[uuid]; !on {
			gone = append(gone, uuid)
		}
	}
	for uuid := range live {
		if _, held := r.conns[uuid]; !held {
			unknown++
		}
	}
	sort.Strings(gone)
	return gone, unknown
}

// RebuildRegistry checks the restored registry once CONNECTIONS is on.
func (a *Agent) RebuildRegistry(ctx context.Context) {
	for a.flows.Load()&FlowConnections == 0 {
		if !sleep(ctx, time.Second) {
			return
		}
	}
	if a.Registry == nil {
		return
	}
	if gone := a.Registry.workerless(workerAlive); len(gone) > 0 {
		n, err := a.Registry.closeWhere(gone, func(map[string]any) bool { return true })
		if err != nil {
			a.logf("cluster: rebuilding the registry: %v", err)
		} else if n > 0 {
			a.logf("cluster: rebuilding the registry: %d viewer(s) whose PHP worker ended while the agent was down", n)
		}
	}
	if a.FanoutCtl == "" {
		return
	}
	for try := 0; try < RebuildTries; try++ {
		live, err := fanoutConnections(a.FanoutCtl)
		if err != nil {
			if !sleep(ctx, RebuildRetry) {
				return
			}
			continue
		}
		gone, unknown := a.Registry.daemonGone(live)
		n, err := a.Registry.FanoutClosed(gone)
		if err != nil {
			a.logf("cluster: rebuilding the registry from the fanout: %v", err)
			return
		}
		if n > 0 || unknown > 0 {
			a.logf("cluster: rebuilding the registry: %d daemon viewer(s) left while the agent was down; %d served that it does not hold", n, unknown)
		}
		return
	}
	a.logf("cluster: rebuilding the registry: the fanout did not answer; fanout_sync reconciles")
}
