package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// POST /streams/<id>/ensure drives EnsureRunning so a serve-only LB can bring a
// registered-but-idle stream up on demand (for the /stream/rtmp auth endpoint,
// which has no viewer-segment fetch to trigger the start). An unknown id is a
// fast {running:false}; a non-POST is 405.
func TestControlEnsureRoute(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	h := m.ControlHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/streams/7/ensure", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST ensure (unknown id) status %d", rec.Code)
	}
	var out struct {
		Running bool `json:"running"`
	}
	if json.Unmarshal(rec.Body.Bytes(), &out) != nil || out.Running {
		t.Fatalf("unknown id should be running:false, got %q", rec.Body.String())
	}

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/streams/7/ensure", nil))
	if rec2.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /ensure status %d, want 405", rec2.Code)
	}
}
