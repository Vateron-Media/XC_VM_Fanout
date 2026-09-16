// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
	"time"
)

// TestASilentChannelIsNotRestartedEveryAudioWindow: a stream whose PMT declares
// an audio PID that carries nothing — audio broken upstream, an encoder that
// lost the track — reports the same LastAudio for ever, so AUDIO_LOSS fires
// again audio_loss_sec after every restart. Each of those is a real outage: the
// ring is reset and every viewer rebuffers. The panel had the same fault but at
// a twentieth of the rate, because it only re-probed for audio once 300s had
// passed, so the same broken channel cost one restart per five minutes there and
// one per ~40s here.
func TestASilentChannelIsNotRestartedEveryAudioWindow(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	vit := &vitalsStub{}
	h.sup.WithVitals(vit.get)
	h.sup.healthTick = 5 * time.Millisecond

	spec := baseSpec(dir)
	spec.Health = Health{AudioLossSec: 1} // the panel hardcodes 30
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "the first start", func() bool { return h.sup.State("5").Running })

	// Audio was seen once, long before this daemon's first start, and never
	// again: that is what internal/server reports for a declared-but-silent PID.
	vit.set(Vitals{LastData: time.Now(), LastAudio: time.Now().Add(-time.Hour)})

	// One AUDIO_LOSS restart is right — the fault is real and the panel's
	// audio_restart_loss asks for it. A second one inside the retry interval is
	// not: it is the same channel, still silent, taken off air again.
	waitFor(t, "the AUDIO_LOSS restart", func() bool { return h.launchCount() >= 2 })
	time.Sleep(1500 * time.Millisecond)

	if n := h.launchCount(); n != 2 {
		t.Errorf("the channel was started %d times in ~2.5s of permanent silence, want 2 "+
			"(one start and one AUDIO_LOSS restart)", n)
	}
	n := 0
	for _, a := range actions(readLog(t, dirLog(dir))) {
		if a == EventAudioLoss {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%s logged %d times, want 1 until the retry interval is up", EventAudioLoss, n)
	}
}

// TestAudioLossPacingRearmsWhenAudioComesBack: the pacing must not blind the
// rule to a NEW fault. A channel that lost its audio, was restarted and came
// back with sound is judged from scratch, so a later loss is caught as fast as
// the first one.
func TestAudioLossPacingRearmsWhenAudioComesBack(t *testing.T) {
	h := newHarness(t)
	st := &stream{id: "5", sup: h.sup, spec: baseSpec(t.TempDir()), forced: -1}

	if !st.audioLossDue(t0, audioLossRetryDefault) {
		t.Fatal("the first audio loss must restart the stream at once")
	}
	for _, after := range []time.Duration{30 * time.Second, 2 * time.Minute, 4 * time.Minute} {
		if st.audioLossDue(t0.Add(after), audioLossRetryDefault) {
			t.Fatalf("%s later: restarted a still-silent channel inside the retry interval", after)
		}
	}
	if !st.audioLossDue(t0.Add(6*time.Minute), audioLossRetryDefault) {
		t.Fatal("a channel still silent after the retry interval must be tried again")
	}

	// Audio came back under the running encoder: the next loss is a fresh fault.
	st.clearAudioLoss()
	if !st.audioLossDue(t0.Add(7*time.Minute), audioLossRetryDefault) {
		t.Fatal("audio came back and was lost again; that must be acted on at once")
	}
}
