package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	mrand "math/rand"
	"os"
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

	flowsSeen string
	flows     atomic.Int64 // the flow bits from MAIN's latest reply
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
	Policy    *struct {
		PolicyVer int      `json:"policy_ver"`
		Transport string   `json:"transport"`
		MainURLs  []string `json:"main_urls"`
	} `json:"policy"`
}

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
	b, _ := json.Marshal(map[string]any{"mode": r.Mode, "flows": r.Flows, "state": r.State})
	if string(b) == a.flowsSeen {
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
	if err := a.Client.Call(ctx, "hello", a.identity(), &r, false); err != nil {
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
		go a.RunCommands(cctx, a.Exec)
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
		payload := map[string]any{}
		if a.Telemetry != nil {
			payload["telemetry"] = a.Telemetry()
		}
		var r Reply
		if err := a.Client.Call(ctx, "heartbeat", payload, &r, false); err != nil {
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
		a.publish(&r)
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

// jitter spreads d by ±10 %, so a fleet recovering together does not arrive at once.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d - d/10 + time.Duration(mrand.Int63n(int64(d/5)+1))
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
