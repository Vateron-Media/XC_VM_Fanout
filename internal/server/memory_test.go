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

// TestMemoryReportsTheRings: GET /memory accounts for the rings — the bulk of
// the daemon's heap — stream by stream, largest first, beside the Go heap.
func TestMemoryReportsTheRings(t *testing.T) {
	m := NewManager(1<<20, 40000, 2, 6, time.Second)
	h := m.ControlHandler()

	big, small := m.GetOrCreate("big"), m.GetOrCreate("small")
	for _, st := range []*Stream{big, small} {
		st.Publish(tsfixture.PAT(0x100))
		st.Publish(tsfixture.PMT(0x100, 0x101))
	}
	for sec := int64(0); sec < 10; sec++ {
		big.Publish(tsfixture.KeyframePCR(0x101, sec*90000, sec*90000))
		for i := 0; i < 50; i++ {
			big.Publish(tsfixture.Fill(0x101))
		}
	}
	small.Publish(tsfixture.KeyframePCR(0x101, 0, 0))

	rec := do(h, http.MethodGet, "/memory", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /memory = %d", rec.Code)
	}
	var v memoryView
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("bad body %q: %v", rec.Body, err)
	}
	if v.Streams != 2 || len(v.Largest) != 2 {
		t.Fatalf("report = %+v, want both streams", v)
	}
	if v.Largest[0].ID != "big" || v.Largest[0].Bytes <= v.Largest[1].Bytes {
		t.Fatalf("largest first: %+v", v.Largest)
	}
	if v.Largest[0].Seconds < 8 || v.Largest[0].Seconds > 10 {
		t.Errorf("big ring spans %.1fs, want the ~9 s it was fed", v.Largest[0].Seconds)
	}
	var sum int64
	for _, r := range v.Largest {
		sum += int64(r.Bytes)
	}
	if v.RingBytes != sum {
		t.Errorf("ring_bytes %d, want the sum of the rings %d", v.RingBytes, sum)
	}
	if v.HeapInUseBytes == 0 || v.MappedBytes == 0 {
		t.Errorf("heap figures missing: %+v", v)
	}
	if rec := do(h, http.MethodPost, "/memory", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /memory = %d, want 405", rec.Code)
	}
}
