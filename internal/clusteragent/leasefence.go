package clusteragent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// The lease fence's drop of the fanout's own viewers (ADR 0004, Phase 9's
// eighth increment).
//
// Past a lease's exp, on MAIN's clock, with lb_lease_fence on, the node's PHP
// (Core\Cluster\NodeLease) refuses new viewers, and past lb_fence_drain_min
// more it ends the sessions it serves itself: live.php's TS loop and
// segment.php/key.php check it at every segment. A viewer the fanout serves
// under X-Accel has no PHP worker left to check anything, and until now only
// a fence MAIN commanded (node.fence) dropped it.
//
// So the agent judges the lease's drain itself, from exactly what NodeLease's
// fallback reads — the lease it holds, its estimate of MAIN's clock
// (mainclock.go), and the two settings from the replica's settings section —
// and past it drops every connection the fanout lists (GET /connections,
// then DELETE /connections/<uuid>), each once. Every uncertainty serves, as
// in NodeLease: no fanout socket, no replica settings, the switch off, mode
// 0, no lease, or MAIN's time never seen. The compiled verdict (the node's
// xcvm_core) does not reach Go; it anchors on the same MAIN time within
// seconds, and it outranks this one only where MAIN's clock went back, when
// this judgement is the later of the two.

// Lease fence settings as MAIN keeps them (ClusterSettings: default, min, max).
const (
	LeaseFenceDrainMinDefault = 10
	LeaseFenceDrainMinMax     = 60
)

// replicaSettings is the replica's settings section, nil without one.
func (a *Agent) replicaSettings() map[string]any {
	if a.ReplicaDir == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(a.ReplicaDir, "settings.json"))
	if err != nil {
		return nil
	}
	var doc struct {
		Data map[string]any `json:"data"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return nil
	}
	return doc.Data
}

// leaseFenceSettings reads lb_lease_fence and lb_fence_drain_min from the
// replica's settings section; ok is false without one.
func (a *Agent) leaseFenceSettings() (on bool, drainMin int64, ok bool) {
	data := a.replicaSettings()
	if data == nil {
		return false, 0, false
	}
	drainMin = LeaseFenceDrainMinDefault
	if v, ok := settingInt(data["lb_fence_drain_min"]); ok && v >= 0 && v <= LeaseFenceDrainMinMax {
		drainMin = v
	}
	v, _ := settingInt(data["lb_lease_fence"])
	return v == 1, drainMin, true
}

// settingInt reads a settings value as MAIN's row holds it: a string of
// digits, or a number.
func settingInt(v any) (int64, bool) {
	switch x := v.(type) {
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	case float64:
		return int64(x), x == float64(int64(x))
	}
	return 0, false
}

// leaseFenced reports whether the lease this node holds is past its drain on
// MAIN's clock, with the switch on (see above); why names what decided.
func (a *Agent) leaseFenced() (fenced bool, why string) {
	if a.mode.Load() < 1 {
		return false, "not a cluster node"
	}
	on, drainMin, ok := a.leaseFenceSettings()
	switch {
	case !ok:
		return false, "no replica settings"
	case !on:
		return false, "the switch is off"
	}
	l := a.Client.State.leaseState()
	if l.Exp <= 0 {
		return false, "no lease"
	}
	mainMs := a.Client.clock.nowMs()
	if mainMs <= 0 {
		return false, "MAIN's time never seen"
	}
	if mainMs/1000 < l.Exp+drainMin*60 {
		return false, "serving or draining"
	}
	return true, "the lease ran out and its drain is over"
}

// leaseFenceTick drops the fanout's viewers while the lease fence stands.
// It runs on RunFence's tick, whether MAIN answers or not.
func (a *Agent) leaseFenceTick() {
	if a.FanoutCtl == "" {
		return
	}
	if fenced, _ := a.leaseFenced(); !fenced {
		a.leaseDrops.mu.Lock()
		a.leaseDrops.done = nil // a new lease: the next fence drops afresh
		a.leaseDrops.mu.Unlock()
		return
	}
	a.dropFanoutViewers(&a.leaseDrops, "lease")
}

// dropFanoutViewers ends every viewer the fanout lists that drops has not
// dropped yet, each once; a uuid the fanout no longer lists is forgotten.
func (a *Agent) dropFanoutViewers(drops *fenceDrops, why string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://fanout/connections", nil)
	res, err := fanoutClient(a.FanoutCtl).Do(req)
	if err != nil {
		return // the fanout is not running: nothing it serves
	}
	var uuids []string
	err = json.NewDecoder(io.LimitReader(res.Body, 16<<20)).Decode(&uuids)
	res.Body.Close()
	if err != nil || res.StatusCode != http.StatusOK {
		return
	}
	drops.mu.Lock()
	defer drops.mu.Unlock()
	if drops.done == nil {
		drops.done = map[string]bool{}
	}
	seen := make(map[string]bool, len(uuids))
	n := 0
	for _, uuid := range uuids {
		if !connUUID.MatchString(uuid) {
			continue
		}
		seen[uuid] = true
		if drops.done[uuid] {
			continue
		}
		drops.done[uuid] = true
		dctx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, _ := http.NewRequestWithContext(dctx, http.MethodDelete, "http://fanout/connections/"+uuid, nil)
		if res, err := fanoutClient(a.FanoutCtl).Do(req); err == nil {
			res.Body.Close()
			n++
		}
		dcancel()
	}
	for uuid := range drops.done {
		if !seen[uuid] {
			delete(drops.done, uuid)
		}
	}
	if n > 0 {
		a.logf("cluster: %s fence: dropped %d fanout viewer(s)", why, n)
	}
}
