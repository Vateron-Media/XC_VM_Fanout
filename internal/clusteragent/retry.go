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
