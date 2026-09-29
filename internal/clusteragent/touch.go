package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// The P2 lane (plan, section 8, "Ordering and backpressure"; ADR 0004, tenth
// Phase 6 increment): state of which only the newest value per key counts.
// Its first type is conn.touch, when an HLS viewer last asked for its
// playlist:
//
//	POST events {lane: "p2", events: [{type: "conn.touch", t, d: {uuid, hls_last_read}}, …]}
//
// No first_useq, nothing journalled or numbered, never a USEQ_GAP. Touches go
// on P2 only while MAIN's latest hello or heartbeat reply lists "conn.touch"
// in p2_types, CONNECTIONS is on, and the agent runs its HLS reaper (it says
// hls_reaper at hello). Then a Put whose only change is hls_last_read emits
// no P0 upsert: the registry keeps the latest value per uuid as a pending
// touch, and RunTouches sends each uuid's at most once every TouchP2Every,
// only when it differs from the last value MAIN got for it on either lane.
// Every other change still goes at once on P0.
//
// Losing P2 — a reply without conn.touch, a 400 to a P2 batch (an older
// MAIN), or the reaper off — with CONNECTIONS still on sends, at once and on
// P0, every open record whose hls_last_read differs from the last value sent
// on P0 (a value sent on P2 reached MAIN's bus, not its store), then the 10 s
// P0 touches resume. With CONNECTIONS off nothing is sent. Pending touches
// live in memory only.

// P2Touch is the P2 event type for touches.
const P2Touch = "conn.touch"

var (
	// TouchP2Every is the most often one uuid's touch goes on P2.
	TouchP2Every = 60 * time.Second
	// TouchLoop is how often the P2 loop looks for touches due.
	TouchLoop = 10 * time.Second
)

// touchClock is the time a touch is stamped with: the wall time read at start
// plus the monotonic time since, so it never steps back while the agent runs.
var touchStart = time.Now()

func touchNowMs() int64 { return touchStart.Add(time.Since(touchStart)).UnixMilli() }

type touch struct {
	uuid  string
	value int64
}

// dueTouches returns the pending touches whose TouchP2Every has passed and
// whose value MAIN does not have yet, oldest-sent first, at most max.
func (r *Registry) dueTouches(max int) []touch {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	var out []touch
	for uuid, v := range r.pending {
		if last, ok := r.lastAny[uuid]; ok && last == v {
			delete(r.pending, uuid) // MAIN already has it
			continue
		}
		if at, ok := r.p2At[uuid]; ok && now.Sub(at) < TouchP2Every {
			continue
		}
		out = append(out, touch{uuid, v})
	}
	sort.Slice(out, func(i, j int) bool {
		ai, bi := r.p2At[out[i].uuid], r.p2At[out[j].uuid]
		if !ai.Equal(bi) {
			return ai.Before(bi)
		}
		return out[i].uuid < out[j].uuid
	})
	if len(out) > max {
		out = out[:max]
	}
	return out
}

// touchesSent notes the values MAIN took on P2; a newer value stays pending.
func (r *Registry) touchesSent(sent []touch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	for _, t := range sent {
		if _, open := r.conns[t.uuid]; !open {
			delete(r.pending, t.uuid)
			continue
		}
		r.lastAny[t.uuid], r.p2At[t.uuid] = t.value, now
		if r.pending[t.uuid] == t.value {
			delete(r.pending, t.uuid)
		}
	}
}

// dropTouches forgets every pending touch (CONNECTIONS off).
func (r *Registry) dropTouches() {
	r.mu.Lock()
	r.pending = map[string]int64{}
	r.mu.Unlock()
}

// touchesToP0 sends, as one P0 batch of conn.upsert, every open record whose
// hls_last_read differs from the last value sent for it on P0, and forgets
// the pending touches: P2 was lost and MAIN's store must catch up.
func (r *Registry) touchesToP0() (int, error) {
	r.mu.Lock()
	var events []map[string]any
	var recs []map[string]any
	for uuid, c := range r.conns {
		if num(c["hls_end"]) != 0 {
			continue
		}
		if last, ok := r.lastP0[uuid]; ok && last == intOf(c["hls_last_read"]) {
			continue
		}
		rec := clone(c)
		events = append(events, map[string]any{"type": "conn.upsert", "d": map[string]any{"record": rec}})
		recs = append(recs, rec)
	}
	r.pending = map[string]int64{}
	r.mu.Unlock()
	if len(events) == 0 {
		return 0, nil
	}
	err := r.emitThenLocked(events, func() {
		for _, rec := range recs {
			uuid, _ := rec["uuid"].(string)
			if c := r.conns[uuid]; c != nil && norm(c["hls_last_read"]) == norm(rec["hls_last_read"]) {
				r.sentP0Locked(uuid, rec)
			}
		}
	})
	if err != nil {
		return 0, err
	}
	return len(events), nil
}

// p2Wanted reports whether touches may go on P2 by this reply.
func (a *Agent) p2Wanted(r *Reply) bool {
	if a.Registry == nil || r.Flows&FlowConnections == 0 {
		return false
	}
	for _, t := range r.P2Types {
		if t == P2Touch {
			return true
		}
	}
	return false
}

// setP2 switches touches on or off P2; losing it hands the registry's
// touches back to P0 (or drops them with CONNECTIONS off).
func (a *Agent) setP2(on bool) {
	if a.p2Touch.Swap(on) == on {
		return
	}
	if on {
		a.logf("cluster: HLS touches go on P2")
		return
	}
	if a.Registry == nil {
		return
	}
	if a.flows.Load()&FlowConnections == 0 {
		a.Registry.dropTouches()
		a.logf("cluster: HLS touches off P2 (CONNECTIONS off)")
		return
	}
	n, err := a.Registry.touchesToP0()
	if err != nil {
		a.logf("cluster: HLS touches back to P0: %v", err)
		return
	}
	a.logf("cluster: HLS touches back to P0 (%d record(s) caught up)", n)
}

// RunTouches is the P2 loop: every TouchLoop, one request with the touches
// due (at most one batch: a larger backlog goes a batch per interval), until
// ctx ends or MAIN stops the node.
//
// A refusal of the bulk lane (503 RATE_LIMITED with lane "bulk") stretches
// the loop's interval (laneInterval): the touches due then go after the
// longer of the busy wait and the doubled interval, and each batch MAIN
// serves halves it back toward TouchLoop.
func (a *Agent) RunTouches(ctx context.Context) {
	iv := newLaneInterval(TouchLoop)
	backoff := TouchLoop
	for sleep(ctx, backoff) {
		backoff = iv.cur
		if a.Registry == nil || !a.p2Touch.Load() {
			continue
		}
		served, err := a.sendTouchBatch(ctx)
		wait, busy := iv.next(served, err)
		backoff = wait
		if err == nil {
			continue
		}
		if fatal(err) || ctx.Err() != nil {
			return
		}
		if busy {
			// MAIN's bulk permits are all held: busy, not failing.
			a.busyRefusals.Add(1)
			continue
		}
		var d *Denial
		if errors.As(err, &d) && d.Status == 400 && d.Reason == "BAD_REQUEST" {
			// A MAIN without P2: back to P0 until a reply lists conn.touch again.
			a.logf("cluster: MAIN refused the P2 lane; HLS touches go on P0")
			a.setP2(false)
			continue
		}
		if !errors.Is(err, ErrNoEpoch) {
			a.logf("cluster: events p2: %v", err)
		}
	}
}

// sendTouchBatch sends one batch of the touches due, at most MaxBatchEvents
// events and MaxBatchBytes, and reports whether MAIN served one.
func (a *Agent) sendTouchBatch(ctx context.Context) (bool, error) {
	due := a.Registry.dueTouches(MaxBatchEvents)
	if len(due) == 0 {
		return false, nil
	}
	var events []map[string]any
	var sent []touch
	size := 0
	for _, t := range due {
		ev := map[string]any{"type": P2Touch, "t": touchNowMs(), "d": map[string]any{"uuid": t.uuid, "hls_last_read": t.value}}
		b, _ := json.Marshal(ev)
		if len(events) > 0 && size+len(b)+1 > MaxBatchBytes {
			break
		}
		size += len(b) + 1
		events, sent = append(events, ev), append(sent, t)
	}
	var out EventsResult
	if err := a.Client.Call(ctx, "events", map[string]any{"lane": "p2", "events": events}, &out, false); err != nil {
		return false, err
	}
	if out.Dropped > 0 {
		a.logf("cluster: events p2: MAIN dropped %d of %d touch(es)", out.Dropped, len(events))
	}
	a.Registry.touchesSent(sent)
	return true, nil
}
