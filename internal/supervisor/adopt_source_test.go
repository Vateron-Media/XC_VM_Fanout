// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
	"time"
)

// TestAdoptedEncoderIsPlacedOnTheSourceItIsRunning: a survivor is not
// necessarily on the first source. The previous daemon (or the panel's
// watchdog) may have failed it over to a backup, and the spec the panel re-PUTs
// after an upgrade is still in priority order. Assuming source 0 told the panel
// the wrong current_source, made POST /monitor/5/source {index:0} a no-op
// ("already on it"), and switched the priority-backup climb off, because that
// only runs below the top source.
func TestAdoptedEncoderIsPlacedOnTheSourceItIsRunning(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)
	vit := &vitalsStub{}
	vit.set(Vitals{LastData: time.Now()})
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 10 * time.Millisecond

	w := newWorld()
	const survivor = 9005
	w.add(survivor, "ffmpeg -i http://backup/2.ts -f hls /home/xc_vm/streams/5_.m3u8")
	h.sup.find = w.find
	h.sup.killPID = w.kill

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "http://primary/1.ts", Cmd: "ffmpeg -i http://primary/1.ts -f hls /home/xc_vm/streams/5_.m3u8"},
		{Label: "http://backup/2.ts", Cmd: "ffmpeg -i http://backup/2.ts -f hls /home/xc_vm/streams/5_.m3u8"},
	}
	spec.AdoptMatch = "/streams/5_"
	writePID(t, spec.PIDPath, survivor)
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "adoption", func() bool { return h.sup.State("5").Adopted })

	st := h.sup.State("5")
	if st.SourceIdx != 1 || st.Source != "http://backup/2.ts" {
		t.Errorf("state reports source %d (%q), want the backup the survivor is actually running",
			st.SourceIdx, st.Source)
	}

	// And the operator's way back to the primary works, instead of being
	// answered with "already on it".
	if err := h.sup.ForceSource("5", 0); err != nil {
		t.Fatalf("ForceSource: %v", err)
	}
	waitFor(t, "a switch back to the primary", func() bool {
		return h.sup.State("5").Source == "http://primary/1.ts"
	})
}

// TestAdoptedEncoderKeepsTheFirstSourceWhenItsCommandIsUnrecognisable: the
// match is on the commands the panel handed us. A survivor whose command line
// is none of them (an older panel build composed it, say) must still be adopted
// — the point of adoption is not to run a second encoder on the source — and it
// simply reports the source the spec starts at, as it always did.
func TestAdoptedEncoderKeepsTheFirstSourceWhenItsCommandIsUnrecognisable(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	w := newWorld()
	const survivor = 9006
	w.add(survivor, "ffmpeg -i http://something/else.ts -f hls /home/xc_vm/streams/5_.m3u8")
	h.sup.find = w.find
	h.sup.killPID = w.kill

	spec := baseSpec(dir)
	spec.Sources = []Source{
		{Label: "http://primary/1.ts", Cmd: "ffmpeg -i http://primary/1.ts"},
		{Label: "http://backup/2.ts", Cmd: "ffmpeg -i http://backup/2.ts"},
	}
	spec.AdoptMatch = "/streams/5_"
	writePID(t, spec.PIDPath, survivor)
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "adoption", func() bool { return h.sup.State("5").Adopted })

	if st := h.sup.State("5"); st.SourceIdx != 0 || !st.Running {
		t.Errorf("state = %+v, want the unrecognised survivor adopted on source 0", st)
	}
}

// TestMatchRunningSourceIgnoresShellQuoting: /proc/<pid>/cmdline is the argv the
// shell produced, so the quotes and the runs of whitespace in the panel's
// command line are not in it. A match that missed over that would put the stream
// back to reporting source 0.
func TestMatchRunningSourceIgnoresShellQuoting(t *testing.T) {
	sources := []Source{
		{Label: "a", Cmd: `ffmpeg -i http://a/1.ts  -f tee "[f=hls]/tmp/5_.m3u8|[f=mpegts]/tmp/5.ts"`},
		{Label: "b", Cmd: "xc_fanout remux -i http://b/2.ts", FallbackCmd: "ffmpeg -i http://b/2.ts"},
	}

	idx, fallback, ok := matchRunningSource(
		"ffmpeg -i http://a/1.ts -f tee [f=hls]/tmp/5_.m3u8|[f=mpegts]/tmp/5.ts", sources)
	if !ok || idx != 0 || fallback {
		t.Errorf("matched %d (fallback=%v, ok=%v), want source 0's own command", idx, fallback, ok)
	}

	idx, fallback, ok = matchRunningSource("ffmpeg -i http://b/2.ts", sources)
	if !ok || idx != 1 || !fallback {
		t.Errorf("matched %d (fallback=%v, ok=%v), want source 1's fallback command", idx, fallback, ok)
	}

	if _, _, ok := matchRunningSource("ffmpeg -i http://c/3.ts", sources); ok {
		t.Error("matched a command line that is none of the spec's")
	}
	if _, _, ok := matchRunningSource("", sources); ok {
		t.Error("matched an empty command line (a process whose argv we cannot read)")
	}
	// Two sources with the same command tell us nothing about which is running.
	same := []Source{{Label: "a", Cmd: "ffmpeg -i x"}, {Label: "b", Cmd: "ffmpeg -i x"}}
	if _, _, ok := matchRunningSource("ffmpeg -i x", same); ok {
		t.Error("picked one of two sources that share a command line")
	}
}
