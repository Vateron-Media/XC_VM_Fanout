// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"sync"
	"testing"
)

// TestABlipOnAProbingSourceDoesNotDemoteTheChannel: walking the source list on a
// failed start is what makes failover work, but it walks on the FIRST failure,
// without asking whether the source is actually down. One launch error or one
// slow moment on the primary therefore starts the next attempt on the backup,
// and under priority_backup the stream is then held there for the full 300s
// interval before it climbs back — two extra restarts and five minutes on a
// lesser feed for a hiccup that cost nothing before.
//
// PHP did not do that: StreamProcess::startStream probed each source in turn
// inside ONE start attempt, so a primary that was momentarily unreachable but
// answered again was used on the very next retry. A source with a probe command
// gets that much back: one more attempt, but only if it still answers.
func TestABlipOnAProbingSourceDoesNotDemoteTheChannel(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	h.failNextLaunches(errTest) // one blip, then the primary is fine

	h.sup.WithProber(func(_ context.Context, cmd string) bool { return cmd == "probe-primary" })

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary", ProbeCmd: "probe-primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup", ProbeCmd: "probe-backup"},
	}
	spec.Policy.PriorityBackupSec = 300 // what the panel always sends
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "the retry to come up", func() bool { return h.sup.State("5").Running })

	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	if len(got) != 2 || got[1] != "ffmpeg -i primary" {
		t.Fatalf("launched %q, want the retry back on the primary that still probes", got)
	}
	if st := h.sup.State("5"); st.SourceIdx != 0 {
		t.Errorf("SourceIdx = %d, want 0 — a blip must not demote the channel to a backup", st.SourceIdx)
	}
}

// TestASourceThatStopsProbingIsWalkedPastAtOnce is the other half: the retry is
// only for a source that is still there. One that does not answer costs no extra
// attempt, so failover is as quick as it was.
func TestASourceThatStopsProbingIsWalkedPastAtOnce(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	h.failNextLaunches(errTest)

	var mu sync.Mutex
	probes := 0
	h.sup.WithProber(func(_ context.Context, cmd string) bool {
		mu.Lock()
		probes++
		mu.Unlock()
		return false // the primary is genuinely gone
	})

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "primary", Cmd: "ffmpeg -i primary", ProbeCmd: "probe-primary"},
		{Label: "backup", Cmd: "ffmpeg -i backup", ProbeCmd: "probe-backup"},
	}
	spec.Policy.PriorityBackupSec = 300
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "a start on the backup", func() bool { return h.sup.State("5").Source == "backup" })

	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	if len(got) != 2 || got[1] != "ffmpeg -i backup" {
		t.Fatalf("launched %q, want the next attempt on the backup", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if probes == 0 {
		t.Error("the source was demoted without ever being probed")
	}
}
