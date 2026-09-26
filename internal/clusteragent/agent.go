package clusteragent

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	mrand "math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

// Agent runs a node's control loop: finish enrolment, say hello, heartbeat,
// and refresh the token before it expires.
type Agent struct {
	Client    *Client
	Version   string
	Interval  time.Duration
	Telemetry func() map[string]any
	Logf      func(format string, args ...any)
	// FlowsFile receives the node's mode and flow bits from MAIN's replies,
	// for the LB's PHP (Core\Cluster\NodeFlows); "" writes nothing.
	FlowsFile string
	// Exec runs MAIN's commands (commands.go); nil leaves the commands lane off.
	Exec Executor
	// SpoolDir is where the node's PHP spools events for MAIN (events.go);
	// "" leaves the event lanes off.
	SpoolDir string
	// SocketPath is the local socket the node's PHP calls MAIN through
	// (socket.go); "" leaves it off.
	SocketPath string
	// FanoutCtl is xc_fanout's control socket, whose GET /events the agent
	// follows while STREAMS is on (fanout.go); "" leaves it off.
	FanoutCtl string
	// Registry holds the node's viewers while CONNECTIONS is on (registry.go);
	// Run makes it when SpoolDir is set.
	Registry *Registry

	flowsSeen string
	flows     atomic.Int64 // the flow bits from MAIN's latest reply
	// MAIN's event cursors from the latest hello, plus one (0: not known yet).
	cursorP0, cursorP1 atomic.Int64
	fanoutLive         atomic.Bool // the fanout's /events feed is being followed
	snapshotting       atomic.Bool // a conn_snapshot is being sent
}

// Reply is what MAIN returns to enrol_complete, hello and heartbeat.
type Reply struct {
	State   string `json:"state"`
	Mode    int    `json:"mode"`
	Flows   int    `json:"flows"`
	Gen     int    `json:"gen"`
	Pending int    `json:"pending"`
	// PolicyVer, in heartbeat replies, is MAIN's current transport policy;
	// a newer one than the node holds makes it say hello again to fetch it.
	PolicyVer int `json:"policy_ver"`
	// WantConnSnapshot, in heartbeat replies: MAIN's store for this node
	// drifted from the digest the heartbeat carried; send the registry.
	WantConnSnapshot bool `json:"want_conn_snapshot"`
	// Cursors, in hello replies, are the last event numbers MAIN applied per lane.
	Cursors *struct {
		P0 int64 `json:"p0"`
		P1 int64 `json:"p1"`
	} `json:"cursors"`
	Policy *struct {
		PolicyVer int      `json:"policy_ver"`
		Transport string   `json:"transport"`
		MainURLs  []string `json:"main_urls"`
	} `json:"policy"`
}

// Features are what this agent tells MAIN at hello that it does, so MAIN
// stands down its own copy: "hls_reaper" (Registry.Reap).
var Features = []string{"hls_reaper"}

// ErrStop is returned when MAIN has told the node to stop: it was revoked or
// is unknown, or its enrolment was never completed in time. An expired token
// is not a stop: the node re-keys.
var ErrStop = errors.New("clusteragent: stopped by MAIN")

// RekeyPoll is how often a node that cannot re-key yet (licence withdrawn,
// quarantined) asks again; MAIN allows one attempt a minute.
var RekeyPoll = 60 * time.Second

func (a *Agent) logf(format string, args ...any) {
	if a.Logf != nil {
		a.Logf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func bootID() string {
	b, _ := os.ReadFile("/proc/sys/kernel/random/boot_id")
	return strings.TrimSpace(string(b))
}

func (a *Agent) identity() map[string]any {
	return map[string]any{"instance_id": a.Client.State.InstanceID, "boot_id": bootID(), "agent_version": a.Version}
}

// fatal reports whether a refusal means the loop must stop.
func fatal(err error) bool {
	var d *Denial
	if !errors.As(err, &d) {
		return false
	}
	switch d.Reason {
	case "NODE_REVOKED", "UNKNOWN_NODE", "ENROL_EXPIRED":
		return true
	}
	return false
}

func (a *Agent) enrolled() bool {
	st := a.Client.State
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.Enrolled
}

// recover re-keys until it works, MAIN stops the node, or ctx ends. Only an
// enrolled node can: MAIN re-keys active nodes only.
func (a *Agent) recover(ctx context.Context) error {
	if !a.enrolled() {
		return errors.Join(ErrStop, errors.New("clusteragent: the first token expired before enrolment completed"))
	}
	backoff := 5 * time.Second
	for {
		tok, err := a.Client.Rekey(ctx, a.identity())
		if err == nil {
			a.logf("cluster: re-keyed (epoch %d)", tok.Epoch)
			return nil
		}
		if fatal(err) {
			return errors.Join(ErrStop, err)
		}
		wait := backoff
		var d *Denial
		switch {
		case errors.Is(err, ErrUnlicensed):
			wait = RekeyPoll
		case errors.As(err, &d) && d.Reason == "RATE_LIMITED":
			wait = max(time.Second, time.Duration(retryAfterMs(d))*time.Millisecond)
		case errors.As(err, &d) && (d.Reason == "LICENCE_INVALID" || d.Reason == "NOT_ACTIVE"):
			wait = RekeyPoll
		default:
			backoff = min(backoff*2, 5*time.Minute)
		}
		a.logf("cluster: re-key: %v (retry in %s)", err, wait)
		if !sleep(ctx, jitter(wait)) {
			return ctx.Err()
		}
	}
}

// publish writes the mode and flows MAIN just sent, when they changed. The
// file is replaced atomically; PHP reads it at request or loop start.
func (a *Agent) publish(r *Reply) {
	if r == nil || r.State == "" {
		return
	}
	a.flows.Store(int64(r.Flows))
	if a.FlowsFile == "" {
		return
	}
	doc := map[string]any{"mode": r.Mode, "flows": r.Flows, "state": r.State}
	var features []string
	if a.fanoutLive.Load() {
		// PHP's reconcile leaves the supervised streams' state to these events.
		features = append(features, "fanout_events")
	}
	if a.Registry != nil {
		// The node's own reaper (UsersCronJob, MySQL mode) leaves idle HLS
		// viewers to the registry's.
		features = append(features, "hls_reaper")
	}
	if features != nil {
		doc["features"] = features
	}
	b, _ := json.Marshal(doc)
	if string(b) == a.flowsSeen {
		// Unchanged: touch it, so the PHP side knows the agent is alive and
		// keeps spooling events (EventSpool::STALE_AFTER).
		now := time.Now()
		os.Chtimes(a.FlowsFile, now, now)
		return
	}
	tmp := a.FlowsFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
		a.logf("cluster: writing flows: %v", err)
		return
	}
	if err := os.Rename(tmp, a.FlowsFile); err != nil {
		a.logf("cluster: writing flows: %v", err)
		return
	}
	a.flowsSeen = string(b)
	a.logf("cluster: mode %d, flows %d (%s)", r.Mode, r.Flows, r.State)
}

// Unpublish removes the flows file, so the LB's PHP falls back to the legacy
// paths: called when MAIN stops the node.
func (a *Agent) Unpublish() {
	if a.FlowsFile != "" {
		os.Remove(a.FlowsFile)
	}
}

func (a *Agent) apply(r *Reply) {
	a.publish(r)
	if r != nil && r.Cursors != nil {
		a.cursorP0.Store(r.Cursors.P0 + 1)
		a.cursorP1.Store(r.Cursors.P1 + 1)
	}
	if r == nil || r.Policy == nil || r.Policy.PolicyVer < a.Client.State.PolicyVer || len(r.Policy.MainURLs) == 0 {
		return
	}
	st := a.Client.State
	st.mu.Lock()
	changed := r.Policy.PolicyVer != st.PolicyVer || strings.Join(r.Policy.MainURLs, " ") != strings.Join(st.MainURLs, " ")
	st.PolicyVer, st.MainURLs = r.Policy.PolicyVer, append([]string{}, r.Policy.MainURLs...)
	var err error
	if changed {
		err = st.saveLocked()
	}
	st.mu.Unlock()
	if err != nil {
		a.logf("cluster: saving policy: %v", err)
	}
}

// Start completes enrolment when needed and says hello.
func (a *Agent) Start(ctx context.Context) (*Reply, error) {
	st := a.Client.State
	if !st.Enrolled {
		var r Reply
		if err := a.Client.Call(ctx, "enrol_complete", a.identity(), &r, true); err != nil {
			return nil, err
		}
		st.mu.Lock()
		st.Enrolled = true
		err := st.saveLocked()
		st.mu.Unlock()
		if err != nil {
			return nil, err
		}
		a.apply(&r)
		a.logf("cluster: enrolled (state %s, mode %d)", r.State, r.Mode)
	}
	var r Reply
	hello := a.identity()
	hello["features"] = Features
	if err := a.Client.Call(ctx, "hello", hello, &r, false); err != nil {
		return nil, err
	}
	a.apply(&r)
	return &r, nil
}

// Run loops until ctx ends or MAIN stops the node. Transport failures back off
// and retry; they never change the node's state. A node whose tokens are gone
// re-keys and carries on.
func (a *Agent) Run(ctx context.Context) error {
	interval := a.Interval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	backoff := interval
	for {
		_, err := a.Start(ctx)
		if err == nil {
			break
		}
		if fatal(err) {
			return errors.Join(ErrStop, err)
		}
		if needsRekey(err) {
			if err := a.recover(ctx); err != nil {
				return err
			}
			continue
		}
		a.logf("cluster: start: %v (retry in %s)", err, backoff)
		if !sleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = min(backoff*2, time.Minute)
	}
	if a.Exec != nil {
		cctx, stopCommands := context.WithCancel(ctx)
		defer stopCommands()
		go a.RunCommands(cctx, a.localExec(a.Exec))
	}
	if a.Registry == nil && a.SpoolDir != "" {
		spool := a.SpoolDir
		a.Registry = NewRegistry(filepath.Join(filepath.Dir(spool), "registry.snap"), func(ev []map[string]any) error { return spoolP0(spool, ev) }, a.logf)
	}
	if a.Registry != nil {
		go func() {
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for tick := 1; ; tick++ {
				select {
				case <-ctx.Done():
					a.Registry.Save()
					return
				case <-t.C:
					if tick%5 == 0 && a.flows.Load()&FlowConnections != 0 {
						if n := a.Registry.Reap(HLSReapAfter); n > 0 {
							a.logf("cluster: ended %d idle HLS viewer(s)", n)
						}
					}
					if err := a.Registry.Save(); err != nil {
						a.logf("cluster: saving the connection registry: %v", err)
					}
				}
			}
		}()
	}
	if a.SocketPath != "" {
		sctx, stopSocket := context.WithCancel(ctx)
		defer stopSocket()
		go func() {
			if err := a.ServeSocket(sctx, a.SocketPath); err != nil {
				a.logf("cluster: local socket: %v", err)
			}
		}()
	}
	if a.FanoutCtl != "" && a.SpoolDir != "" {
		fctx, stopFanout := context.WithCancel(ctx)
		defer stopFanout()
		go a.RunFanoutEvents(fctx)
	}
	if a.SpoolDir != "" {
		ectx, stopEvents := context.WithCancel(ctx)
		defer stopEvents()
		for _, lane := range Lanes {
			go a.RunEvents(ectx, lane)
		}
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		if tok, ok := a.Client.Current(); ok && a.Client.MainNowMs()/1000 >= tok.RefreshAt {
			if _, err := a.Client.Refresh(ctx); err != nil {
				if fatal(err) {
					return errors.Join(ErrStop, err)
				}
				// The current token lasts to exp; a later heartbeat re-keys if it must.
				a.logf("cluster: token refresh: %v", err)
			}
		}
		r, err := a.Heartbeat(ctx)
		if err != nil {
			if fatal(err) {
				return errors.Join(ErrStop, err)
			}
			if needsRekey(err) {
				a.logf("cluster: heartbeat: %v; re-keying", err)
				if err := a.recover(ctx); err != nil {
					return err
				}
				if _, err := a.Start(ctx); err != nil && fatal(err) {
					return errors.Join(ErrStop, err)
				}
				continue
			}
			a.logf("cluster: heartbeat: %v", err)
			continue
		}
		if r.WantConnSnapshot {
			go func() {
				if err := a.SendSnapshot(ctx); err != nil {
					a.logf("cluster: connection snapshot: %v", err)
				}
			}()
		}
		if r.PolicyVer > a.Client.State.PolicyVer {
			if _, err := a.Start(ctx); err != nil {
				a.logf("cluster: fetching policy %d: %v", r.PolicyVer, err)
			}
		}
		if r.State == "quarantined" {
			a.logf("cluster: MAIN has quarantined this node; an admin must decide")
		}
	}
}

// Heartbeat sends one heartbeat and publishes the reply's mode and flows. A
// node whose CONNECTIONS flow is on adds its registry's digest.
func (a *Agent) Heartbeat(ctx context.Context) (*Reply, error) {
	payload := map[string]any{"root_ready": RootReady(a.Client.State)}
	if a.Telemetry != nil {
		payload["telemetry"] = a.Telemetry()
	}
	if a.Registry != nil && a.flows.Load()&FlowConnections != 0 {
		payload["conn_digest"] = a.Registry.Digest()
	}
	var r Reply
	if err := a.Client.Call(ctx, "heartbeat", payload, &r, false); err != nil {
		return nil, err
	}
	a.publish(&r)
	return &r, nil
}

// jitter spreads d by ±10 %, so a fleet recovering together does not arrive at once.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d - d/10 + time.Duration(mrand.Int63n(int64(d/5)+1))
}

// RootPinDir is where root pins the panel key for root commands.
var RootPinDir = "/etc/xc_vm/cluster"

// RootReady reports whether root's pin of the panel key matches the one this
// agent holds, so MAIN may send this node root commands (cluster:root checks
// them against that pin).
func RootReady(st *State) bool {
	pub, err1 := os.ReadFile(filepath.Join(RootPinDir, "main_sign.pub"))
	node, err2 := os.ReadFile(filepath.Join(RootPinDir, "node"))
	st.mu.Lock()
	defer st.mu.Unlock()
	return err1 == nil && err2 == nil && strings.TrimSpace(string(pub)) == hex.EncodeToString(st.PanelSignPub) &&
		strings.TrimSpace(string(node)) == st.NodeUUID
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
