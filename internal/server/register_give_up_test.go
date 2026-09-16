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

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// removedInRegistry stages what registerAttempts consecutive lost races look
// like from inside Register: every GetOrCreate hands back a Stream that has
// already left the registry, so setConfig / startIngestLocked refuse it and the
// retry runs out. Planting the flag on a stream still in the map is the only
// deterministic way to hold that state still — Unregister deletes and marks in
// one step precisely so it cannot last.
func removedInRegistry(m *Manager, id string) *Stream {
	st := m.GetOrCreate(id)
	st.mu.Lock()
	st.removed = true
	st.mu.Unlock()
	return st
}

// TestRegisterReportsARegistrationItCouldNotLand: a PUT the daemon gave up on
// must not be answered as if it had worked.
//
// Register retries a bounded number of times when a teardown takes the stream
// out from under it, and then gives up. It said so only through dlog, which
// writes nothing unless debug is on, and serveControl answered 204 No Content
// regardless. The panel re-registers on every request, so a 204 it believes is
// a registration it will not repeat with any urgency: the channel simply has no
// source on this node, and nothing in the log says why.
func TestRegisterReportsARegistrationItCouldNotLand(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	st := removedInRegistry(m, "7")

	if m.Register("7", puller.Source{URLs: []string{"http://x/a.ts"}}, 0) {
		t.Fatal("Register claimed success on a stream nothing could be started on")
	}
	st.mu.Lock()
	cfg := st.cfg
	st.mu.Unlock()
	if cfg != nil {
		t.Fatal("premise broken: the config was applied after all")
	}

	ts := httptest.NewServer(m.ControlHandler())
	defer ts.Close()
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/streams/7",
		strings.NewReader(`{"urls":["http://x/a.ts"]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /streams/7: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		t.Errorf("PUT /streams/7 answered 204 although the registration never landed: "+
			"the panel is told the channel has a source on this node when it has none (got %d)", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("PUT /streams/7 answered %d, want %d so the panel's next request retries",
			resp.StatusCode, http.StatusServiceUnavailable)
	}
}

// TestRegisterStillAnswers204WhenItLands pins the other side: the ordinary
// registration the panel makes on every viewer request must keep its 204.
func TestRegisterStillAnswers204WhenItLands(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	ts := httptest.NewServer(m.ControlHandler())
	defer ts.Close()
	defer m.Unregister("7")

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/streams/7",
		strings.NewReader(`{"urls":["http://x/a.ts"]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /streams/7: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT /streams/7 answered %d, want 204", resp.StatusCode)
	}
	if m.Get("7") == nil {
		t.Fatal("the stream was not registered")
	}
}
