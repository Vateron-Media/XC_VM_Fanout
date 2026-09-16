// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"sync"
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

// TestPacedAudioLossDoesNotMuteTheOtherRules: Health.check returns the FIRST
// rule that fires, and on a channel whose audio PID is permanently silent that
// is AUDIO_LOSS on every single tick. Pacing the restart by skipping the tick
// therefore skipped every rule behind it — for as long as the silence lasted,
// not just for the retry interval — so a silent channel whose picture then froze
// was left on air showing a still image, with no event and no restart. The
// frame-rate baseline was not even measured, because that too lives behind the
// audio rule.
func TestPacedAudioLossDoesNotMuteTheOtherRules(t *testing.T) {
	dir := t.TempDir()
	h := newHarness(t)
	h.setData(true)

	var mu sync.Mutex
	clock := time.Now()
	h.sup.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	h.sup.healthTick = 5 * time.Millisecond

	vit := &vitalsStub{}
	h.sup.WithVitals(vit.get)
	silentSince := clock.Add(-time.Hour) // audio declared, nothing on it, ever

	// step moves the clock on by d with the vitals that go with it. LastData is
	// stamped first, so no tick in between sees a stale one and calls it a stall.
	step := func(d time.Duration, fps float64) {
		mu.Lock()
		at := clock.Add(d)
		mu.Unlock()
		vit.set(Vitals{LastData: at, LastAudio: silentSince, FPS: fps})
		mu.Lock()
		clock = at
		mu.Unlock()
		time.Sleep(40 * time.Millisecond) // several health ticks at this reading
	}

	spec := baseSpec(dir)
	spec.Health = Health{StallSec: 30, AudioLossSec: 30, FPSThreshold: 0.5}
	if err := h.sup.Supervise("5", spec); err != nil {
		t.Fatal(err)
	}
	h.nextProcess(t)
	waitFor(t, "the first start", func() bool { return h.sup.State("5").Running })
	vit.set(Vitals{LastData: clock, LastAudio: silentSince, FPS: 25})

	// The one AUDIO_LOSS restart the pacing allows.
	step(31*time.Second, 25)
	h.nextProcess(t)
	waitFor(t, "the AUDIO_LOSS restart", func() bool {
		st := h.sup.State("5")
		return st.Running && h.launchCount() == 2
	})

	// The replacement is silent too, so from 30s in every tick carries an
	// AUDIO_LOSS the pacing declines to act on. The picture is fine.
	step(25*time.Second, 25)
	step(20*time.Second, 25)

	// And then it freezes: no video frames at all, for longer than the stall
	// bound this stream declares. Nothing else catches that — the bytes keep
	// flowing, so the stall rule is satisfied — and it must not be hidden behind
	// a rule that is merely being paced.
	step(10*time.Second, 0)
	step(35*time.Second, 0)

	waitFor(t, "the frozen picture to be caught", func() bool { return h.launchCount() >= 3 })
	if n := countAction(readLog(t, dirLog(dir)), EventFPSDropThreshold); n != 1 {
		t.Errorf("%s logged %d times, want 1: the frozen picture must still be judged", EventFPSDropThreshold, n)
	}
	if n := countAction(readLog(t, dirLog(dir)), EventAudioLoss); n != 1 {
		t.Errorf("%s logged %d times, want 1 inside the retry interval", EventAudioLoss, n)
	}
}

// countAction counts the panel log entries carrying one action.
func countAction(entries []logLine, action string) int {
	n := 0
	for _, a := range actions(entries) {
		if a == action {
			n++
		}
	}
	return n
}
