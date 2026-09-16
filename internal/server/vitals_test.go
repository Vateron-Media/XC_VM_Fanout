// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// publishAV publishes n video access units and n audio packets on the PIDs
// pmtWithAudio declares, so both health counters move together the way a real
// A/V stream moves them.
func publishAV(st *Stream, n int) {
	for i := 0; i < n; i++ {
		st.Publish(tsfixture.Keyframe(0x101, int64(i)*3600))
		st.Publish(tsfixture.PESStart(0x102, int64(i)*3600))
	}
}

// publishProgram publishes the PSI a stream needs before its counters move at
// all: without a PMT the join state has no video or audio PID to count on.
func publishProgram(st *Stream) {
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(pmtWithAudio(0x100, 0x101, 0x102))
}

// backdateVitals moves a stream's sample back in time, so a test can cross
// minFPSWindow without sitting out the wall clock.
func backdateVitals(t *testing.T, m *Manager, id string, d time.Duration) {
	t.Helper()
	m.vitals.mu.Lock()
	defer m.vitals.mu.Unlock()
	s := m.vitals.samples[id]
	if s == nil {
		t.Fatalf("no vitals sample for %q", id)
	}
	s.at = s.at.Add(-d)
}

// TestSampleRebaselinesWhenAStreamIsReCreatedUnderTheSameID: the health counters
// belong to the hub's join state, so they restart at zero when the Stream behind
// an id is torn down and re-created — a panel re-registration, a DELETE /streams
// followed by the next viewer. Nothing drops the sampler's entry on that path
// (only PUT/DELETE /monitor call forget), so the next poll differenced a brand
// new counter against the old stream's total and handed the supervisor a large
// NEGATIVE frame rate. supervisor/health.go treats any fps <= 0 as "the frames
// stopped" once this encoder has shown a peak, so the verdict was
// FPS_DROP_THRESHOLD and a working encoder was restarted — for a teardown that
// had nothing to do with it. The same reset froze lastAudio (the new counter is
// never "greater than" the old one), so AUDIO_LOSS could fire behind it.
func TestSampleRebaselinesWhenAStreamIsReCreatedUnderTheSameID(t *testing.T) {
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.EnableSupervision()
	st := m.GetOrCreate("42")
	publishProgram(st)
	publishAV(st, 200)

	if _, ok := m.sample("42"); !ok {
		t.Fatal("no vitals for a registered stream")
	}
	backdateVitals(t, m, "42", 4*time.Second)
	publishAV(st, 200)
	before, _ := m.sample("42")
	if before.FPS <= 0 {
		t.Fatalf("baseline fps = %v, want a real positive rate to compare against", before.FPS)
	}
	if before.LastAudio.IsZero() {
		t.Fatal("audio is flowing but LastAudio is zero")
	}

	// The stream is torn down and re-created under the same id: new Stream, new
	// hub, both counters from zero. The encoder behind it never stopped.
	m.Unregister("42")
	st = m.GetOrCreate("42")
	publishProgram(st)
	publishAV(st, 5)
	backdateVitals(t, m, "42", 4*time.Second)

	after, _ := m.sample("42")
	if after.FPS <= 0 {
		t.Fatalf("fps = %v after the stream was re-created under the same id; health.go reads any "+
			"fps <= 0 as a frame-rate collapse and restarts the encoder", after.FPS)
	}
	if !after.LastAudio.After(before.LastAudio) {
		t.Fatalf("LastAudio stuck at %v while audio flows on the re-created stream; the audio-loss "+
			"rule fires on a healthy channel", after.LastAudio)
	}

	// And the re-baseline must be a real one: the NEXT window measures the new
	// stream's own rate rather than carrying the old figure forever.
	backdateVitals(t, m, "42", 4*time.Second)
	publishAV(st, 200)
	later, _ := m.sample("42")
	if later.FPS < 45 || later.FPS > 55 {
		t.Fatalf("fps = %v over the window after the re-create, want ~50 measured on the new counters", later.FPS)
	}
}
