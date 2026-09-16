// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/config"
)

// TestApplyConfigShrinkKeepsViewersInsideTheNewRing pins the half of the live
// prebuffer_max_sec change that the config contract really can promise: a viewer
// whose cursor is inside the new window keeps playing across the shrink, with no
// reconnect.
//
// A viewer deeper than the new window is a different matter — ApplyConfig prunes
// it away on the spot and that cursor reports behind on its next read (see the
// caveat on ApplyConfig). Clamping such a cursor forward to the ring's oldest
// block belongs in the ring itself (internal/tsjoin), which is the only place
// that knows a prune came from a Configure shrink rather than from the producer.
func TestApplyConfigShrinkKeepsViewersInsideTheNewRing(t *testing.T) {
	m := NewManager(1<<20, 40000, 6, 6, time.Hour) // 40 s ring
	st := m.GetOrCreate("s")
	for sec := int64(1); sec <= 60; sec++ {
		publishSecond(st, sec, 20)
	}

	// A viewer 5 s behind the edge: well inside the 10 s the operator is about to
	// leave in place.
	_, cur := st.Hub.Join(5000)

	v := config.Defaults()
	v.PrebufferMaxSec = 10 // the operator shrinks the ring to free memory
	m.ApplyConfig(v)

	burst, _, _, _, behind, ended := st.Hub.Follow(cur, joinRunBytes)
	burst.Release()
	if behind || ended {
		t.Fatalf("a viewer 5 s behind the edge was dropped by a shrink to 10 s (behind=%v ended=%v): "+
			"lowering prebuffer_max_sec must not disconnect viewers the new ring still covers", behind, ended)
	}
}
