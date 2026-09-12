// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"encoding/json"
	"net/http"
	"runtime/metrics"
	"sort"
)

// memoryReportTop is how many of the largest rings GET /memory lists.
const memoryReportTop = 10

// ringView is one stream's ring in the memory report.
type ringView struct {
	ID      string  `json:"id"`
	Bytes   int     `json:"bytes"`
	Seconds float64 `json:"seconds"` // stream time the ring spans
	GOPs    int     `json:"gops"`
	Viewers int     `json:"viewers"`
	Gated   bool    `json:"gated"` // collapsed to the idle floor (no viewers)
}

// memoryView is GET /memory: where this daemon's memory is.
type memoryView struct {
	Streams   int   `json:"streams"`
	RingBytes int64 `json:"ring_bytes"` // every stream's ring together
	// From runtime/metrics, which — unlike runtime.ReadMemStats — does not stop
	// the world, so this is safe to poll on a busy node.
	HeapInUseBytes uint64     `json:"heap_in_use_bytes"` // heap objects, live and not yet swept
	HeapGoalBytes  uint64     `json:"heap_goal_bytes"`   // the size the GC lets the heap reach
	MappedBytes    uint64     `json:"mapped_bytes"`      // all memory the runtime holds from the OS
	ReleasedBytes  uint64     `json:"released_bytes"`    // of that, already returned (not resident)
	Largest        []ringView `json:"largest"`
}

// serveMemory reports the daemon's memory by what holds it: the per-stream join
// rings (the dominant term by design) against the Go heap as a whole. An
// operator whose node runs hot needs to tell "the rings are as big as the
// config says" from "something else is growing" without a profiler, and this is
// the one question the rest of the API could not answer.
func (m *Manager) serveMemory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	m.mu.Lock()
	streams := make([]*Stream, 0, len(m.streams))
	for _, st := range m.streams {
		streams = append(streams, st)
	}
	m.mu.Unlock()

	out := memoryView{Streams: len(streams)}
	rings := make([]ringView, 0, len(streams))
	for _, st := range streams {
		bytes, spanMS, gops := st.Hub.RingStats()
		st.mu.Lock()
		gated := !st.buffered
		st.mu.Unlock()
		out.RingBytes += int64(bytes)
		rings = append(rings, ringView{
			ID: st.id, Bytes: bytes, Seconds: float64(spanMS) / 1000, GOPs: gops,
			Viewers: st.Hub.Count(), Gated: gated,
		})
	}
	sort.Slice(rings, func(i, j int) bool { return rings[i].Bytes > rings[j].Bytes })
	if len(rings) > memoryReportTop {
		rings = rings[:memoryReportTop]
	}
	out.Largest = rings

	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/heap/goal:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}
	metrics.Read(samples)
	val := func(i int) uint64 {
		if samples[i].Value.Kind() == metrics.KindUint64 {
			return samples[i].Value.Uint64()
		}
		return 0
	}
	out.HeapInUseBytes, out.HeapGoalBytes, out.MappedBytes, out.ReleasedBytes = val(0), val(1), val(2), val(3)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
