package clusteragent

import (
	"context"
	"errors"
	"strings"
	"time"
)

// The transport policy (plan, section 3, "Endpoints and HTTPS"; ADR 0004,
// "Acceptance tests that found gaps", "https_required over plain HTTP"):
// MAIN's URLs in order, and whether HTTPS is tried, preferred or required.
// It comes in hello and enrol_complete replies (MAC'd) and in the signed
// challenge document, and every change raises policy_ver.
//
//   - Versions never go back: a policy is adopted only if its policy_ver is
//     not lower than the one the node holds. The challenge is signed but not
//     bound to a request, so a replayed one could carry an older policy; from
//     it the agent adopts only a strictly newer version.
//   - Under https_required MAIN serves nothing but the challenge (and health)
//     over plain HTTP, and refuses every other op there with a signed 403
//     HTTPS_REQUIRED. While HTTPS fails (heartbeats fail over the policy's
//     HTTPS URLs, or MAIN answers HTTPS_REQUIRED), the agent fetches the
//     challenge over plain HTTP every PolicyPoll and adopts the policy it
//     carries by the rule above, so an admin who switches back to auto
//     recovers the fleet without SSH.
//   - The plain-HTTP URLs to ask are the ones the node has known: the http://
//     URLs of every policy it held (HTTPURLs in the state file), since an
//     https_required policy lists none.

// PolicyPoll is how often the challenge is fetched over plain HTTP while
// HTTPS fails under https_required.
var PolicyPoll = 60 * time.Second

// MaxHTTPURLs bounds the plain-HTTP URLs the node remembers.
const MaxHTTPURLs = 8

// Policy is MAIN's transport policy.
type Policy struct {
	PolicyVer int      `json:"policy_ver"`
	Transport string   `json:"transport"`
	MainURLs  []string `json:"main_urls"`
	// HeartbeatSec is the fleet's heartbeat in seconds
	// (lb_telemetry_interval_sec). Zero means MAIN did not say, and the node
	// keeps the pace it has.
	HeartbeatSec int `json:"heartbeat_sec,omitempty"`
}

// MinHeartbeat and MaxHeartbeat bound the pace a policy may ask for, as MAIN's
// own setting is bounded: a node never beats faster than MAIN's liveness needs
// nor slower than it tolerates (MaxHeartbeatGap).
const (
	MinHeartbeat = time.Second
	MaxHeartbeat = 3 * time.Second
)

func isHTTP(u string) bool { return strings.HasPrefix(strings.ToLower(u), "http://") }

// adoptPolicy makes p the node's policy when its version is not lower than
// the one held (strictly newer when newer is set), and remembers its
// plain-HTTP URLs. It reports whether p was adopted.
func (st *State) adoptPolicy(p *Policy, newer bool) (bool, error) {
	if p == nil || len(p.MainURLs) == 0 {
		return false, nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if p.PolicyVer < st.PolicyVer || (newer && p.PolicyVer == st.PolicyVer) {
		return false, nil
	}
	beat := heartbeatOf(p.HeartbeatSec)
	changed := p.PolicyVer != st.PolicyVer || p.Transport != st.Transport || strings.Join(p.MainURLs, " ") != strings.Join(st.MainURLs, " ")
	if beat != 0 && beat != st.HeartbeatSec {
		st.HeartbeatSec, changed = beat, true
	}
	st.PolicyVer, st.Transport, st.MainURLs = p.PolicyVer, p.Transport, append([]string{}, p.MainURLs...)
	known := map[string]bool{}
	var urls []string
	for _, u := range append(append([]string{}, p.MainURLs...), st.HTTPURLs...) {
		if isHTTP(u) && !known[u] && len(urls) < MaxHTTPURLs {
			known[u] = true
			urls = append(urls, u)
		}
	}
	if strings.Join(urls, " ") != strings.Join(st.HTTPURLs, " ") {
		st.HTTPURLs, changed = urls, true
	}
	if !changed {
		return true, nil
	}
	return true, st.saveLocked()
}

// heartbeatOf is a policy's heartbeat in its bounds, or 0 when it says none.
func heartbeatOf(sec int) int {
	if sec <= 0 {
		return 0
	}
	if d := time.Duration(sec) * time.Second; d < MinHeartbeat {
		return int(MinHeartbeat / time.Second)
	} else if d > MaxHeartbeat {
		return int(MaxHeartbeat / time.Second)
	}
	return sec
}

// heartbeatSec is the pace the node holds, or 0 before any policy set one.
func (st *State) heartbeatSec() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.HeartbeatSec
}

func (st *State) policyVer() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.PolicyVer
}

// plainURLs is every plain-HTTP MAIN URL the node knows, the policy's first.
func (st *State) plainURLs() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	known := map[string]bool{}
	var out []string
	for _, u := range append(append([]string{}, st.MainURLs...), st.HTTPURLs...) {
		if isHTTP(u) && !known[u] {
			known[u] = true
			out = append(out, u)
		}
	}
	return out
}

// httpsTrouble notes a failure that says HTTPS does not work for this node:
// MAIN's HTTPS_REQUIRED, or a transport error under https_required.
func (a *Agent) httpsTrouble(err error) {
	var d *Denial
	if errors.As(err, &d) {
		if d.Reason == "HTTPS_REQUIRED" {
			a.httpsFailing.Store(true)
		}
		return
	}
	st := a.Client.State
	st.mu.Lock()
	required := st.Transport == "https_required"
	st.mu.Unlock()
	if required && !errors.Is(err, ErrNoEpoch) && !errors.Is(err, context.Canceled) {
		a.httpsFailing.Store(true)
	}
}

// RunPolicyRecovery fetches the signed challenge over plain HTTP every
// PolicyPoll while HTTPS fails, until ctx ends.
func (a *Agent) RunPolicyRecovery(ctx context.Context) {
	for sleep(ctx, PolicyPoll) {
		if !a.httpsFailing.Load() {
			continue
		}
		if err := a.PolicyOverHTTP(ctx); err != nil && ctx.Err() == nil {
			a.logf("cluster: HTTPS fails; the challenge over HTTP: %v", err)
		}
	}
}

// PolicyOverHTTP fetches the challenge from the plain-HTTP URLs the node
// knows and adopts a strictly newer policy from it.
func (a *Agent) PolicyOverHTTP(ctx context.Context) error {
	urls := a.Client.State.plainURLs()
	if len(urls) == 0 {
		return errors.New("no plain-HTTP MAIN URL known")
	}
	var lastErr error = ErrTransport
	for _, base := range urls {
		ch, err := a.Client.challenge(ctx, base)
		if err != nil {
			lastErr = err
			continue
		}
		if ch.Policy == nil {
			return errors.New("the challenge carries no policy")
		}
		ok, err := a.Client.State.adoptPolicy(ch.Policy, true)
		if err != nil {
			return err
		}
		if ok {
			a.logf("cluster: adopted policy %d (%s) from the signed challenge over HTTP", ch.Policy.PolicyVer, ch.Policy.Transport)
			a.httpsFailing.Store(false)
		}
		return nil
	}
	return lastErr
}
