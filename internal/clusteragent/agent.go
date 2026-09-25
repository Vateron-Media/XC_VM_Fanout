package clusteragent

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"
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
}

// Reply is what MAIN returns to enrol_complete, hello and heartbeat.
type Reply struct {
	State   string `json:"state"`
	Mode    int    `json:"mode"`
	Flows   int    `json:"flows"`
	Gen     int    `json:"gen"`
	Pending int    `json:"pending"`
	Policy  *struct {
		PolicyVer int      `json:"policy_ver"`
		Transport string   `json:"transport"`
		MainURLs  []string `json:"main_urls"`
	} `json:"policy"`
}

// ErrStop is returned when MAIN has told the node to stop (revoked, or its
// token can no longer be renewed without re-enrolment).
var ErrStop = errors.New("clusteragent: stopped by MAIN")

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
	case "NODE_REVOKED", "UNKNOWN_NODE", "ENROL_EXPIRED", "TOKEN_EXPIRED", "LICENCE_INVALID":
		return true
	}
	return false
}

func (a *Agent) apply(r *Reply) {
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
// and retry; they never change the node's state.
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
		a.logf("cluster: start: %v (retry in %s)", err, backoff)
		if !sleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = min(backoff*2, time.Minute)
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
				a.logf("cluster: token refresh: %v", err)
			}
		}
		payload := map[string]any{}
		if a.Telemetry != nil {
			payload["telemetry"] = a.Telemetry()
		}
		var r Reply
		if err := a.Client.Call(ctx, "heartbeat", payload, &r, false); err != nil {
			if fatal(err) || errors.Is(err, ErrNoEpoch) {
				return errors.Join(ErrStop, err)
			}
			a.logf("cluster: heartbeat: %v", err)
			continue
		}
		if r.State == "quarantined" {
			a.logf("cluster: MAIN has quarantined this node; an admin must decide")
		}
	}
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
