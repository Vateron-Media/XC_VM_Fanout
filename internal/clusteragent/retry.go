package clusteragent

import (
	"context"
	"errors"
	mrand "math/rand"
	"time"
)

// MAIN's refusals that ask the agent to come back later (ADR 0004, "The
// cluster bus (Phase 2, second increment): nonces and per-op semaphores"):
//
//   - 401 REPLAY with retry_after_ms: MAIN cannot vouch for the request's
//     nonce yet (its cluster bus just started, or was lost). A request
//     stamped anew at or after the denial's main_time_ms + retry_after_ms
//     passes. The agent takes MAIN's clock from the denial (panel-signed,
//     naming this request), waits retry_after_ms (jitter only adds, up to
//     10 %, at most ReplayWaitMax) and sends the op once more with a fresh
//     nonce, stamp and MAC, BOX or SEAL. A second REPLAY in a row, or one
//     without retry_after_ms (a replay proper), takes the op's usual backoff.
//   - 503 RATE_LIMITED with retry_after_ms and op: all of the op's bus
//     permits are held (hello, token_rekey, config, conn_snapshot). MAIN is
//     busy, not failing: wait retry_after_ms ±10 % (1–60 s) and send the op
//     again, without raising its backoff. A 429 RATE_LIMITED is token_rekey's
//     once-a-minute slot, handled by recover.
//
// And from MAIN's front controller (ADR 0004, "The cluster pools (Phase 2,
// second increment)"):
//
//   - 503 STARTING with retry_after_ms (5000): MAIN's cluster pools are not
//     up yet (a boot, a restart, an update). Every op but health gets it, and
//     health is never gated. It is never fatal: every loop backs off for
//     retry_after_ms and tries again. An unsigned STARTING (MAIN cannot sign
//     at all) is a transport error like any other.

// ReplayWaitMax caps the wait before a REPLAY is retried.
var ReplayWaitMax = 10 * time.Second

// replayWait is how long to wait before retrying a REPLAY that says when a
// request stamped anew will pass; ok is false for any other refusal.
func replayWait(d *Denial) (time.Duration, bool) {
	if d == nil || d.Status != 401 || d.Reason != "REPLAY" || d.RetryAfterMs <= 0 {
		return 0, false
	}
	w := time.Duration(d.RetryAfterMs) * time.Millisecond
	w += time.Duration(mrand.Int63n(int64(w/10) + 1))
	return min(w, ReplayWaitMax), true
}

// withReplay runs send, and runs it once more when MAIN refused it with a
// REPLAY that carries retry_after_ms, after taking MAIN's clock from the
// denial (setClock) and waiting. send must build a fresh request each time.
func withReplay(ctx context.Context, setClock func(mainMs int64), send func() error) error {
	err := send()
	var d *Denial
	if !errors.As(err, &d) {
		return err
	}
	w, ok := replayWait(d)
	if !ok {
		return err
	}
	if d.MainTimeMs > 0 && setClock != nil {
		setClock(d.MainTimeMs)
	}
	if !sleep(ctx, w) {
		return err
	}
	return send()
}

// busyWait is how long to wait before sending an op MAIN refused as busy or
// starting (a verified 503 RATE_LIMITED or STARTING); ok is false for
// anything else.
func busyWait(err error) (time.Duration, bool) {
	var d *Denial
	if !errors.As(err, &d) || d.Status != 503 {
		return 0, false
	}
	var ms int64
	switch d.Reason {
	case "RATE_LIMITED":
		ms = d.RetryAfterMs
		if ms <= 0 {
			ms = 1000
		}
	case "STARTING":
		ms = d.RetryAfterMs
		if ms <= 0 {
			ms = 5000
		}
	default:
		return 0, false
	}
	return max(time.Second, min(time.Minute, jitter(time.Duration(ms)*time.Millisecond))), true
}

// Ingest permits (ADR 0004, "The cluster bus (Phase 2, fourth increment):
// ingest permits"): every ingest op (events, config, conn_snapshot,
// recording_complete) holds a permit of its lane while MAIN runs it, and
// gets a 503 RATE_LIMITED carrying `lane` when none is free. Such a refusal
// means busy, not failing: it is counted (Agent.BusyRefusals), never logged
// as an error nor counted as a failure, and the op goes again:
//
//   - lane p0 (a P0 events batch): after retry_after_ms (250–750), jitter only
//     adding, up to 10 %, clamped to P0BusyMin–P0BusyMax; the same in-flight
//     batch, before any later one; the lane's interval stays.
//   - lane bulk, events P1 and P2: the lane's current interval doubles, up to
//     BulkIntervalMax, and the lane sends again after the longer of the busy
//     wait and that interval; each batch MAIN serves halves it back toward
//     the lane's normal interval (laneInterval).
//   - lane bulk, config and conn_snapshot: again after the busy wait, as for
//     the per-op refusal; recording_complete within the socket's deadline
//     (socket.go).
//
// A 503 RATE_LIMITED without `lane` is a per-op semaphore's, handled above.

var (
	// P0BusyMin and P0BusyMax clamp P0's wait after a p0 lane refusal.
	P0BusyMin = 100 * time.Millisecond
	P0BusyMax = 5 * time.Second
	// BulkIntervalMax caps a bulk lane's stretched interval.
	BulkIntervalMax = 60 * time.Second
)

// laneRefusal returns the verified 503 RATE_LIMITED naming an ingest lane
// that err is, or nil.
func laneRefusal(err error) *Denial {
	var d *Denial
	if !errors.As(err, &d) || d.Status != 503 || d.Reason != "RATE_LIMITED" || d.Lane == "" {
		return nil
	}
	return d
}

// p0Wait is how long a P0 batch waits after its lane was refused: MAIN's
// retry_after_ms, jitter only adding (up to 10 %), with no 1 s floor.
func p0Wait(d *Denial) time.Duration {
	w := time.Duration(d.RetryAfterMs) * time.Millisecond
	if w <= 0 {
		w = 500 * time.Millisecond // the middle of MAIN's 250–750
	}
	w += time.Duration(mrand.Int63n(int64(w/10) + 1))
	return max(P0BusyMin, min(P0BusyMax, w))
}

// laneInterval is a bulk lane's current interval: it starts at the lane's
// normal one, doubles (up to BulkIntervalMax) on each lane refusal and
// halves back toward the normal one after each batch MAIN serves. Any other
// failure leaves it as it is.
type laneInterval struct {
	normal, cur time.Duration
}

func newLaneInterval(normal time.Duration) *laneInterval {
	return &laneInterval{normal: normal, cur: normal}
}

// refused stretches the interval and returns how long to wait before the
// lane sends again: the longer of the busy wait and the new interval.
func (l *laneInterval) refused(err error) time.Duration {
	l.cur = min(max(l.cur, l.normal)*2, max(BulkIntervalMax, l.normal))
	w, _ := busyWait(err)
	return max(w, l.cur)
}

// served halves the interval back toward the normal one and returns it.
func (l *laneInterval) served() time.Duration {
	l.cur = max(l.normal, l.cur/2)
	return l.cur
}

// next is how long the lane waits before it sends again, after a send that
// had a batch served (or not) and ended with err; busy reports a lane
// refusal, which the caller counts and does not log. A p0 refusal waits
// p0Wait; a bulk one stretches the interval. Any other failure keeps the
// lanes' usual backoff (twice the normal interval, 1–30 s, or a busy or
// starting MAIN's wait if longer) and leaves the interval as it is.
func (l *laneInterval) next(served bool, err error) (wait time.Duration, busy bool) {
	wait = l.cur
	if served {
		wait = l.served()
	}
	if err == nil {
		return wait, false
	}
	if d := laneRefusal(err); d != nil {
		if d.Lane == "p0" {
			return p0Wait(d), true
		}
		return l.refused(err), true
	}
	wait = min(max(time.Second, l.normal*2), 30*time.Second)
	if w, ok := busyWait(err); ok {
		wait = max(wait, w) // MAIN is starting or busy: when it says
	}
	return wait, false
}
