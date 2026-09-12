// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestStaleChannelIsNotOnAir: has_data is what the panel reads to choose between
// the stream and the not-on-air page, and it meant "has EVER had data" — so a
// channel whose source died an hour ago still answered on air, and its viewer
// was handed a dead stream and dropped for silence 30 s later.
func TestStaleChannelIsNotOnAir(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	h := m.ControlHandler()
	st := m.GetOrCreate("9")
	st.Publish(tsfixture.PAT(0x100))
	st.lastData.Store(time.Now().Add(-time.Hour).UnixNano()) // it had a picture, once

	var probe streamStatus
	rec := do(h, http.MethodGet, "/probe/9?wait=150", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &probe); err != nil {
		t.Fatalf("bad probe body %q: %v", rec.Body, err)
	}
	if probe.HasData {
		t.Error("/probe says a channel silent for an hour has data")
	}
	var status streamStatus
	rec = do(h, http.MethodGet, "/streams/9", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("bad status body %q: %v", rec.Body, err)
	}
	if status.HasData {
		t.Error("GET /streams says a channel silent for an hour has data")
	}

	// A flowing channel answers at once.
	st.Publish(tsfixture.PAT(0x100))
	began := time.Now()
	rec = do(h, http.MethodGet, "/probe/9?wait=5000", "")
	json.Unmarshal(rec.Body.Bytes(), &probe)
	if !probe.HasData || time.Since(began) > time.Second {
		t.Errorf("a flowing channel: has_data=%v after %s, want true at once", probe.HasData, time.Since(began))
	}
}
