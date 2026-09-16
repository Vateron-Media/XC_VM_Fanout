// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// puller.Source.Headers travels down every path — the probe, the native reader
// and the ffmpeg child — but until this wire existed nothing could set it: an
// upstream that gates on a Referer, or answers on a vhost, could not be
// configured on the daemon path at all.
func TestRegisterCarriesHeadersToThePuller(t *testing.T) {
	m := NewManager(1<<20, 40000, 6, 6, time.Hour)
	h := m.ControlHandler()

	body := `{"urls":["http://example.invalid/live.ts"],"headers":["Referer: http://portal.example/","X-Token: abc"]}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/streams/7", strings.NewReader(body)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT /streams/7 = %d %s, want 204", rec.Code, rec.Body)
	}

	st := m.Get("7")
	if st == nil {
		t.Fatal("stream 7 was not registered")
	}
	st.mu.Lock()
	cfg := st.cfg
	st.mu.Unlock()
	if cfg == nil {
		t.Fatal("stream 7 has no pull config")
	}
	want := []string{"Referer: http://portal.example/", "X-Token: abc"}
	if len(cfg.Headers) != len(want) {
		t.Fatalf("Headers = %q, want %q", cfg.Headers, want)
	}
	for i := range want {
		if cfg.Headers[i] != want[i] {
			t.Errorf("Headers[%d] = %q, want %q", i, cfg.Headers[i], want[i])
		}
	}
}

// A panel that sends no headers — every panel today — must register exactly as
// it did before.
func TestRegisterWithoutHeadersLeavesThemUnset(t *testing.T) {
	m := NewManager(1<<20, 40000, 6, 6, time.Hour)
	h := m.ControlHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/streams/8",
		strings.NewReader(`{"urls":["http://example.invalid/live.ts"],"ua":"VLC"}`)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT /streams/8 = %d %s, want 204", rec.Code, rec.Body)
	}

	st := m.Get("8")
	if st == nil {
		t.Fatal("stream 8 was not registered")
	}
	st.mu.Lock()
	cfg := st.cfg
	st.mu.Unlock()
	if cfg == nil || len(cfg.Headers) != 0 {
		t.Fatalf("Headers = %q, want none", cfg.Headers)
	}
}
