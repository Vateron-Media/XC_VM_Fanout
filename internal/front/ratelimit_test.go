package front

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiterBurstAndRefill(t *testing.T) {
	rl := newRateLimiter(20, 8)
	clock := time.Unix(1000, 0)
	rl.now = func() time.Time { return clock }

	// A cold bucket allows a full burst, then throttles.
	for i := 0; i < 8; i++ {
		if !rl.allow("1.1.1.1") {
			t.Fatalf("burst token %d denied", i)
		}
	}
	if rl.allow("1.1.1.1") {
		t.Fatal("request past the burst was not throttled")
	}
	// A different key has its own bucket.
	if !rl.allow("2.2.2.2") {
		t.Fatal("a second key was wrongly throttled")
	}
	// A second later, 20 tokens would refill (capped at burst): allowed again.
	clock = clock.Add(time.Second)
	if !rl.allow("1.1.1.1") {
		t.Fatal("no refill after a second")
	}
}

// Serve routes carry the per-viewer limit nginx applied (zone one), so a flood
// from one address gets a 503 once the burst is spent.
func TestFrontRateLimitsServeRoutes(t *testing.T) {
	gw := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	front := NewServer(gw, newNginxStub(t).url, t.TempDir(), LoadTrustedProxies())
	frozen := time.Unix(1000, 0)
	front.limiter.now = func() time.Time { return frozen } // no refill during the loop

	var allowed, limited int
	for i := 0; i < 12; i++ {
		req := httptest.NewRequest(http.MethodGet, "/hls/tok", nil)
		req.RemoteAddr = "5.5.5.5:1"
		rec := httptest.NewRecorder()
		front.ServeHTTP(rec, req)
		switch rec.Code {
		case http.StatusOK:
			allowed++
		case http.StatusServiceUnavailable:
			limited++
		}
	}
	if allowed != serveBurst || limited != 12-serveBurst {
		t.Fatalf("serve-route limiting off: %d allowed, %d limited (want %d/%d)", allowed, limited, serveBurst, 12-serveBurst)
	}
}
