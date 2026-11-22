// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
	"time"
)

// TestAdoptedFallbackIsRememberedForTheNextStart: a survivor whose command line
// is a source's FallbackCmd is proof that the previous daemon had already run
// that source's own Cmd and been told, with ExitUnsupported, that it cannot
// serve it. Recording only "the running process is a fallback" loses that: the
// sticky map commandFor reads stays empty, so the next restart launches the
// native remuxer again, it exits ExitUnsupported again, and the channel pays a
// wasted launch — and a spell reporting Fallback=false — on every restart.
func TestAdoptedFallbackIsRememberedForTheNextStart(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	const fallbackCmd = "ffmpeg -i http://backup/2.ts -f hls /home/xc_vm/streams/5_.m3u8"
	w := newWorld()
	const survivor = 9007
	w.add(survivor, fallbackCmd)
	h.sup.find = w.find
	h.sup.killPID = w.kill

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "http://primary/1.ts", Cmd: "xc_fanout remux -i http://primary/1.ts -o /home/xc_vm/streams/5_.m3u8"},
		{
			Label:       "http://backup/2.ts",
			Cmd:         "xc_fanout remux -i http://backup/2.ts -o /home/xc_vm/streams/5_.m3u8",
			FallbackCmd: fallbackCmd,
		},
	}
	spec.AdoptMatch = "/streams/5_"
	writePID(t, spec.PIDPath, survivor)
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "adoption", func() bool {
		st := h.sup.State("5")
		return st.Adopted && st.Running
	})
	if st := h.sup.State("5"); st.SourceIdx != 1 || !st.Fallback {
		t.Fatalf("state = %+v, want the survivor adopted on source 1's fallback command", st)
	}

	// It runs long enough to count as a start that worked (so the source list is
	// not walked), and then the encoder dies.
	time.Sleep(1200 * time.Millisecond)
	w.remove(survivor)
	h.nextProcess(t) // the replacement this daemon launches itself

	h.mu.Lock()
	got := append([]string(nil), h.launched...)
	h.mu.Unlock()
	if len(got) != 1 || got[0] != fallbackCmd {
		t.Fatalf("relaunched %q, want the fallback command the survivor was already running", got)
	}
	if st := h.sup.State("5"); !st.Fallback {
		t.Errorf("state = %+v, want Fallback still true for the relaunched encoder", st)
	}
}
