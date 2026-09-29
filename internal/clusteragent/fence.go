package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

// The agent's own control commands (plan, section 7, "MAIN → LB commands";
// ADR 0004, Phase 9): the fence, its lifting, the quarantine, a resync and a
// policy push. They act on what the agent holds — its state, its replica,
// its registry — so the node's PHP never sees them (cluster:exec would answer
// "unknown command type").
//
//   - node.fence {reason, drain_min} (restrictive): no new viewer starts on
//     this node from now on; after drain_min minutes the viewers still
//     watching are dropped too. The agent keeps the fence in its state file
//     and rewrites FenceFile every heartbeat tick, which the node's PHP reads
//     (Core\Cluster\NodeLease) exactly as it reads the lease's verdict; past
//     the drain it writes one SIGNALS_PATH entry {"type": "drop"} per viewer
//     its registry holds, which ends a live TS request at its next segment
//     (Public/stream/live.php), and drops the fanout's own viewers through
//     its control socket. A fence whose reason is "licence" is MAIN's hard
//     revocation mode: it rides a LICENCE_INVALID denial (sealed.go) and ends
//     on its own once MAIN accepts the node's session again; one handed out
//     by the long-poll — the session is accepted — is acked and not taken.
//   - node.unfence (granting): the fence ends, the file goes.
//   - node.quarantine {reason} (restrictive): the replica sync stops and only
//     restrictive commands run, until MAIN's replies say the node is active
//     again (an admin's "Trust again").
//   - resync {sections}: "config" fetches every replica section again from
//     scratch, "streams" walks the stream records' hashes now, "connections"
//     sends the registry's whole set (conn_snapshot).
//   - policy.update: a hello now, which adopts MAIN's current policy.

// Fence is a fence MAIN commanded, as the state file keeps it. SinceMs is
// this machine's wall clock when it was taken: the drain is the node's own
// business once MAIN has spoken, and a restart must not start it again.
type Fence struct {
	Reason   string `json:"reason"`
	DrainSec int64  `json:"drain_sec"`
	SinceMs  int64  `json:"since_ms"`
	CmdID    string `json:"cmd_id,omitempty"`
}

// LicenceFence is the reason of the fence MAIN queues for a lapsed licence
// in the hard revocation mode (ClusterRoute::LICENCE_FENCE).
const LicenceFence = "licence"

// Fence states, as FenceFile names them (NodeLease::DRAINING, FENCED).
const (
	FenceDraining = "draining"
	FenceFenced   = "fenced"
)

// MaxFenceDrainMin is the longest drain a fence may ask for
// (lb_fence_drain_min's bound on MAIN).
const MaxFenceDrainMin = 60

// Command types the agent runs itself (besides token.rotate_now).
const (
	TypeFence      = "node.fence"
	TypeUnfence    = "node.unfence"
	TypeQuarantine = "node.quarantine"
	TypeResync     = "resync"
	TypePolicy     = "policy.update"
)

// Restrictive lists the command types xcvm_core classes restrictive
// (clustercrypto/testdata/cluster_commands.json): the only ones a
// quarantined node runs.
var Restrictive = map[string]bool{
	"conn.drop": true, "conn.drop_line": true, "conn.kill_worker": true, "conn.close": true,
	"stream.stop": true, "vod.stop": true, TypeRotateNow: true, TypeQuarantine: true,
	TypeFence: true, TypeResync: true, "config.changed": true, "node.purge": true,
}

var fenceReason = regexp.MustCompile(`^[a-z0-9_.-]{1,32}$`)

// fenceDrops remembers the viewers a fence already dropped, so each gets one
// signal file and one fanout drop.
type fenceDrops struct {
	mu   sync.Mutex
	done map[string]bool
}

// controlExec runs the agent's own control commands; ok reports whether cmd
// was one of them.
func (a *Agent) controlExec(ctx context.Context, cmd *Command) (handled, ok bool, result []byte) {
	if a.quarantined() && !Restrictive[cmd.Type] {
		return true, false, []byte("refused: the node is quarantined; only restrictive commands run")
	}
	switch cmd.Type {
	case TypeFence:
		ok, result = a.takeFence(cmd)
	case TypeUnfence:
		ok, result = a.liftFence("unfenced by MAIN")
	case TypeQuarantine:
		reason, _ := cmd.Args["reason"].(string)
		ok, result = a.setQuarantine(reason)
	case TypeResync:
		ok, result = a.resync(ctx, cmd.Args["sections"])
	case TypePolicy:
		a.helloLater(ctx, "policy.update")
		ok, result = true, []byte(`{"result":true}`)
	default:
		return false, false, nil
	}
	return true, ok, result
}

// takeFence records node.fence. A fence already held keeps its start (a
// redelivery or a licence fence queued again must not restart the drain);
// its reason and drain follow the newest command.
func (a *Agent) takeFence(cmd *Command) (bool, []byte) {
	reason, _ := cmd.Args["reason"].(string)
	if !fenceReason.MatchString(reason) {
		return false, []byte("refused: bad reason")
	}
	drain := intOf(cmd.Args["drain_min"])
	if drain < 0 || drain > MaxFenceDrainMin {
		return false, []byte("refused: bad drain_min")
	}
	if reason == LicenceFence && !a.fenced.Load() {
		// Handed out by the long-poll, so MAIN accepts this node's session:
		// the licence it was queued for is back.
		return true, []byte(`{"result":false,"detail":"the session is accepted; licence fence not taken"}`)
	}
	st := a.Client.State
	st.mu.Lock()
	f := &Fence{Reason: reason, DrainSec: drain * 60, SinceMs: time.Now().UnixMilli(), CmdID: cmd.CmdID}
	if st.Fence != nil {
		f.SinceMs = st.Fence.SinceMs
	}
	st.Fence = f
	err := st.saveLocked()
	st.mu.Unlock()
	if err != nil {
		return false, []byte("refused: saving the fence: " + err.Error())
	}
	a.logf("cluster: fenced by MAIN (%s), drain %d min", reason, drain)
	state := a.fenceTick(time.Now())
	out, _ := json.Marshal(map[string]any{"result": true, "state": state})
	return true, out
}

// liftFence ends a fence (node.unfence, or MAIN accepting the session again
// after a licence fence).
func (a *Agent) liftFence(why string) (bool, []byte) {
	st := a.Client.State
	st.mu.Lock()
	had := st.Fence != nil
	st.Fence = nil
	err := st.saveLocked()
	st.mu.Unlock()
	if err != nil {
		return false, []byte("refused: saving the state: " + err.Error())
	}
	if a.FenceFile != "" {
		if rerr := os.Remove(a.FenceFile); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
			return false, []byte("refused: removing the fence file: " + rerr.Error())
		}
	}
	a.drops.mu.Lock()
	a.drops.done = nil
	a.drops.mu.Unlock()
	if had {
		a.logf("cluster: fence lifted (%s)", why)
	}
	return true, []byte(`{"result":true}`)
}

// licenceBack lifts a licence fence once MAIN accepts the session again.
func (a *Agent) licenceBack() {
	st := a.Client.State
	st.mu.Lock()
	lic := st.Fence != nil && st.Fence.Reason == LicenceFence
	st.mu.Unlock()
	if lic {
		a.liftFence("MAIN accepts the session again")
	}
}

// fenceTick publishes the fence's state for the node's PHP and, past the
// drain, drops the viewers still held. It runs on every heartbeat tick,
// whether MAIN answers or not, so the file stays fresh (NodeLease serves on a
// stale one). It returns the state, "" without a fence.
func (a *Agent) fenceTick(now time.Time) string {
	st := a.Client.State
	st.mu.Lock()
	var f Fence
	held := st.Fence != nil
	if held {
		f = *st.Fence
	}
	st.mu.Unlock()
	if !held {
		return ""
	}
	until := f.SinceMs + f.DrainSec*1000
	state := FenceDraining
	if now.UnixMilli() >= until {
		state = FenceFenced
	}
	if a.FenceFile != "" {
		doc, _ := json.Marshal(map[string]any{"state": state, "reason": f.Reason, "since_ms": f.SinceMs, "drain_until_ms": until, "wrote_at_ms": now.UnixMilli()})
		if err := writeFile(a.FenceFile, doc, fileWrite{perm: 0o640, noSync: true}); err != nil {
			a.logf("cluster: writing the fence: %v", err)
		}
	}
	if state == FenceFenced {
		a.dropFenced()
		if a.FanoutCtl != "" {
			// The fanout's viewers the registry does not hold (its
			// CONNECTIONS flow off): the fanout lists them itself.
			a.dropFanoutViewers(&a.fanoutDrops, "commanded")
		}
	}
	return state
}

// dropFenced ends every viewer the registry holds that this fence has not
// dropped yet: a SIGNALS_PATH entry for a PHP worker (live TS, read at its
// next segment) and a DELETE on the fanout's control socket for its own.
func (a *Agent) dropFenced() {
	if a.Registry == nil {
		return
	}
	a.drops.mu.Lock()
	defer a.drops.mu.Unlock()
	if a.drops.done == nil {
		a.drops.done = map[string]bool{}
	}
	seen := map[string]bool{}
	n := 0
	for _, rec := range a.Registry.Records() {
		uuid, _ := rec["uuid"].(string)
		if !connUUID.MatchString(uuid) || intOf(rec["hls_end"]) != 0 {
			continue
		}
		seen[uuid] = true
		if a.drops.done[uuid] {
			continue
		}
		a.drops.done[uuid] = true
		n++
		if a.SignalsDir != "" {
			doc, _ := json.Marshal(map[string]string{"type": "drop", "uuid": uuid, "reason": "fenced"})
			if err := writeFile(filepath.Join(a.SignalsDir, uuid), doc, fileWrite{perm: 0o644, noSync: true}); err != nil {
				a.logf("cluster: fence: writing the drop signal: %v", err)
			}
		}
		if a.FanoutCtl != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, "http://fanout/connections/"+uuid, nil)
			if res, err := fanoutClient(a.FanoutCtl).Do(req); err == nil {
				res.Body.Close()
			}
			cancel()
		}
	}
	for uuid := range a.drops.done {
		if !seen[uuid] {
			delete(a.drops.done, uuid) // gone from the registry: forgotten
		}
	}
	if n > 0 {
		a.logf("cluster: fenced: dropped %d viewer(s)", n)
	}
}

// FenceEvery is how often RunFence rewrites the fence file and, past the
// drain, looks for viewers to drop: well inside NodeLease's 60 s staleness.
var FenceEvery = time.Second

// RunFence keeps the fence published until ctx ends, whether MAIN answers
// or not, and drops the fanout's viewers under a lease past its drain
// (leasefence.go).
func (a *Agent) RunFence(ctx context.Context) {
	t := time.NewTicker(FenceEvery)
	defer t.Stop()
	for {
		if a.fenceTick(time.Now()) == "" {
			a.leaseFenceTick()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// quarantined reports whether MAIN has quarantined this node (node.quarantine),
// until a reply says it is active again.
func (a *Agent) quarantined() bool {
	if a.Client == nil || a.Client.State == nil {
		return false
	}
	st := a.Client.State
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.Quarantine != ""
}

func (a *Agent) setQuarantine(reason string) (bool, []byte) {
	if !fenceReason.MatchString(reason) {
		reason = "main"
	}
	st := a.Client.State
	st.mu.Lock()
	st.Quarantine = reason
	err := st.saveLocked()
	st.mu.Unlock()
	if err != nil {
		return false, []byte("refused: saving the state: " + err.Error())
	}
	a.logf("cluster: quarantined by MAIN (%s): no replica and only restrictive commands until an admin trusts this node again", reason)
	return true, []byte(`{"result":true}`)
}

// followState ends the quarantine once a reply says the node is active.
func (a *Agent) followState(state string) {
	if state != "active" || !a.quarantined() {
		return
	}
	st := a.Client.State
	st.mu.Lock()
	st.Quarantine = ""
	err := st.saveLocked()
	st.mu.Unlock()
	if err != nil {
		a.logf("cluster: saving the state: %v", err)
	}
	a.logf("cluster: MAIN trusts this node again")
}

// resync runs the resync command's sections.
func (a *Agent) resync(ctx context.Context, raw any) (bool, []byte) {
	list, _ := raw.([]any)
	var done []string
	for _, v := range list {
		switch v {
		case "config":
			if a.ReplicaDir == "" {
				continue
			}
			a.replicaResync.Store(true)
			a.ConfigChanged()
		case "streams":
			if a.ReplicaDir == "" {
				continue
			}
			a.streamsResyncWanted.Store(true)
			a.kick(a.kickChans().streams)
		case "connections":
			if a.Registry == nil || a.flows.Load()&FlowConnections == 0 {
				continue
			}
			go a.snapshot(ctx)
		default:
			continue
		}
		done = append(done, v.(string))
	}
	out, _ := json.Marshal(map[string]any{"result": true, "sections": done})
	return true, out
}
