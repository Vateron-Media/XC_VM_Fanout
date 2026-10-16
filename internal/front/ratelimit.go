package front

import (
	"sync"
	"time"
)

// sweepEvery bounds the bucket map: every this many checks, fully-refilled idle
// buckets are dropped. A dropped bucket is indistinguishable from a fresh one
// (both start full), so eviction never changes a decision.
const sweepEvery = 1024

// rateLimiter is a per-key token bucket matching nginx's limit_req: rate tokens
// per second up to burst. It replaces the zone the serve routes relied on (zone
// one: 20 r/s, burst 8) now that those routes no longer transit nginx.
//
// ponytail: over-rate requests are rejected, not delayed within the burst as
// nginx does without nodelay; the protection is equivalent and simpler.
type rateLimiter struct {
	rate, burst float64
	now         func() time.Time

	mu      sync.Mutex
	buckets map[string]*tokenBucket
	calls   uint64
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(ratePerSec, burst float64) *rateLimiter {
	return &rateLimiter{
		rate:    ratePerSec,
		burst:   burst,
		now:     time.Now,
		buckets: map[string]*tokenBucket{},
	}
}

// allow reports whether a request from key may proceed, consuming one token.
func (rl *rateLimiter) allow(key string) bool {
	now := rl.now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.calls++
	if rl.calls%sweepEvery == 0 {
		for k, b := range rl.buckets {
			if now.Sub(b.last).Seconds()*rl.rate >= rl.burst {
				delete(rl.buckets, k)
			}
		}
	}

	b := rl.buckets[key]
	if b == nil {
		b = &tokenBucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * rl.rate
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
