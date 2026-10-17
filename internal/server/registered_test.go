// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// Registered tells the gateway whether the node holds a pull config for a
// stream — so a serve-only LB serves-and-starts an idle registered stream but
// still fails closed on an unknown one.
func TestManagerRegistered(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)

	if m.Registered("7") {
		t.Fatal("an unknown id must not be registered")
	}
	m.GetOrCreate("7") // the stream exists now, but has no pull config
	if m.Registered("7") {
		t.Fatal("a created-but-unregistered stream must not be registered")
	}
	if !m.Register("7", puller.Source{URLs: []string{"http://a/1.ts"}}, 0) {
		t.Fatal("Register should succeed")
	}
	if !m.Registered("7") {
		t.Fatal("a registered stream must report registered")
	}
}

// GET /streams lists the registered (pullable) ids for the agent's reconcile;
// a created-but-unregistered stream is not listed. DELETE still tears down.
func TestServeControlAllListsRegistered(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.Register("7", puller.Source{URLs: []string{"http://a/1.ts"}}, 0)
	m.Register("9", puller.Source{URLs: []string{"http://b/2.ts"}}, 0)
	m.GetOrCreate("3") // exists but unregistered — must not be listed

	rec := httptest.NewRecorder()
	m.serveControlAll(rec, httptest.NewRequest(http.MethodGet, "http://fanout/streams", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /streams code %d", rec.Code)
	}
	var out struct {
		Streams []string `json:"streams"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out.Streams)
	if len(out.Streams) != 2 || out.Streams[0] != "7" || out.Streams[1] != "9" {
		t.Fatalf("listed %v; want [7 9]", out.Streams)
	}
}
