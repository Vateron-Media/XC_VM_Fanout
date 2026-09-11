// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)

// TestStallRestartsASilentEncoder: the panel re-hashed the playlist every
// seg_time*6 and restarted when it had not moved. Same rule, measured from the
// last publish instead of through the filesystem.
func TestStallRestartsASilentEncoder(t *testing.T) {
	h := Health{StallSec: 30}
	started := t0
	base := &fpsBaseline{}

	v := Vitals{LastData: t0.Add(10 * time.Second)}
	if got := h.check(t0.Add(20*time.Second), started, v, base); got.failed() {
		t.Fatalf("restarted a stream that produced 10s ago: %+v", got)
	}
	got := h.check(t0.Add(45*time.Second), started, v, base)
	if !got.failed() || got.event != EventFfmpegError {
		t.Fatalf("verdict = %+v, want %s after 35s of silence", got, EventFfmpegError)
	}
}

// TestStallMeasuredFromStartWhenNothingEverArrived: a stream that has produced
// NOTHING must still be caught. Measuring from a zero LastData would make it
// look infinitely stale and fire the instant the first tick landed, which would
// shoot every encoder before it had a chance to open its source.
func TestStallMeasuredFromStartWhenNothingEverArrived(t *testing.T) {
	h := Health{StallSec: 30}
	base := &fpsBaseline{}
	v := Vitals{} // never produced

	if got := h.check(t0.Add(10*time.Second), t0, v, base); got.failed() {
		t.Fatalf("condemned a stream 10s into its start-up: %+v", got)
	}
	if got := h.check(t0.Add(31*time.Second), t0, v, base); !got.failed() {
		t.Fatal("a stream that never produced anything must eventually be restarted")
	}
}

// TestAudioLossNeverFiresOnAVideoOnlyStream is the check that matters most here.
// A source with no audio has none to lose; restarting it forever would be the
// obvious way to get this wrong, and a zero LastAudio is exactly what
// internal/server reports for such a stream.
func TestAudioLossNeverFiresOnAVideoOnlyStream(t *testing.T) {
	h := Health{AudioLossSec: 30}
	base := &fpsBaseline{}
	v := Vitals{LastData: t0.Add(time.Minute)} // flowing, but no audio ever

	for _, after := range []time.Duration{time.Minute, time.Hour, 24 * time.Hour} {
		if got := h.check(t0.Add(after), t0, v, base); got.failed() {
			t.Fatalf("after %s: video-only stream condemned for audio loss: %+v", after, got)
		}
	}
}

// TestAudioLossFiresOnceAudioStops: a stream that HAD audio and lost it is the
// real fault the panel's audio_restart_loss looks for.
func TestAudioLossFiresOnceAudioStops(t *testing.T) {
	h := Health{AudioLossSec: 30}
	base := &fpsBaseline{}
	v := Vitals{LastData: t0.Add(2 * time.Minute), LastAudio: t0.Add(30 * time.Second)}

	if got := h.check(t0.Add(50*time.Second), t0, v, base); got.failed() {
		t.Fatalf("fired 20s into the window: %+v", got)
	}
	got := h.check(t0.Add(70*time.Second), t0, v, base)
	if !got.failed() || got.event != EventAudioLoss {
		t.Fatalf("verdict = %+v, want %s", got, EventAudioLoss)
	}
}

// TestAudioSeenOnlyBeforeThisStartIsIgnored: evidence from the previous encoder
// must not condemn the new one, or every restart cascades into another.
func TestAudioSeenOnlyBeforeThisStartIsIgnored(t *testing.T) {
	h := Health{AudioLossSec: 30}
	base := &fpsBaseline{}
	started := t0.Add(time.Minute)
	v := Vitals{LastData: started.Add(time.Second), LastAudio: t0} // audio predates the restart

	if got := h.check(started.Add(90*time.Second), started, v, base); got.failed() {
		t.Fatalf("condemned on the previous encoder's audio: %+v", got)
	}
}

// TestFPSGraceProtectsStartup: a stream still filling its buffers has a low
// instantaneous rate. The panel's fps_delay exists for exactly this.
func TestFPSGraceProtectsStartup(t *testing.T) {
	h := Health{FPSThreshold: 0.5, FPSGraceSec: 60}
	base := &fpsBaseline{peak: 50}
	v := Vitals{FPS: 1, LastData: t0}

	if got := h.check(t0.Add(30*time.Second), t0, v, base); got.failed() {
		t.Fatalf("fired inside the grace window: %+v", got)
	}
	if got := h.check(t0.Add(90*time.Second), t0, v, base); !got.failed() {
		t.Fatal("a collapsed frame rate past the grace window must be caught")
	}
}

// TestFPSBaselineTracksThePeak: the panel froze its baseline at the first
// reading after fps_delay, so a stream that legitimately ramped up later was
// judged against its own worst moment. Tracking the peak keeps the intent
// (compare against how well this stream is known to manage) without that.
func TestFPSBaselineTracksThePeak(t *testing.T) {
	h := Health{FPSThreshold: 0.5, FPSGraceSec: 0}
	base := &fpsBaseline{}
	now := t0.Add(time.Minute)

	for _, fps := range []float64{10, 20, 25} { // ramping up
		if got := h.check(now, t0, Vitals{FPS: fps}, base); got.failed() {
			t.Fatalf("fps %v during ramp-up was treated as a drop: %+v", fps, got)
		}
	}
	if base.peak != 25 {
		t.Fatalf("baseline = %v, want the peak of 25", base.peak)
	}
	if got := h.check(now, t0, Vitals{FPS: 20}, base); got.failed() {
		t.Fatalf("20fps against a 25 baseline is above half; must not fire: %+v", got)
	}
	got := h.check(now, t0, Vitals{FPS: 5}, base)
	if !got.failed() || got.event != EventFPSDropThreshold {
		t.Fatalf("verdict = %+v, want %s for 5fps against a 25 baseline", got, EventFPSDropThreshold)
	}
}

// TestUnmeasurableFPSIsNotADrop: 0 means "could not measure" (no video PID, or
// too short a window), not "no frames". Treating it as a drop would restart
// every audio-only stream forever.
func TestUnmeasurableFPSIsNotADrop(t *testing.T) {
	h := Health{FPSThreshold: 0.5}
	base := &fpsBaseline{peak: 25}
	if got := h.check(t0.Add(time.Hour), t0, Vitals{FPS: 0}, base); got.failed() {
		t.Fatalf("an unmeasurable frame rate was treated as a drop: %+v", got)
	}
}

// TestZeroHealthJudgesNothing: a spec that carries no Health supervises the
// process and forms no opinion about its output. That is the safe default for a
// stream whose panel settings have not been mapped over yet.
func TestZeroHealthJudgesNothing(t *testing.T) {
	var h Health
	base := &fpsBaseline{}
	v := Vitals{} // nothing ever produced, no audio, no frames
	if got := h.check(t0.Add(24*time.Hour), t0, v, base); got.failed() {
		t.Fatalf("an empty policy condemned a stream: %+v", got)
	}
}

// TestAutoRestartMatchesThePanelSchedule ports MonitorCommand::isAutoRestartDue,
// including its minute granularity and PHP's date('l') weekday names.
func TestAutoRestartMatchesThePanelSchedule(t *testing.T) {
	// 2026-09-10 is a Thursday.
	a := &AutoRestart{Days: []string{"Thursday"}, At: "04:30"}

	cases := []struct {
		when time.Time
		want bool
		why  string
	}{
		{time.Date(2026, 9, 10, 4, 30, 0, 0, time.UTC), true, "the configured minute"},
		{time.Date(2026, 9, 10, 4, 30, 59, 0, time.UTC), true, "anywhere inside that minute"},
		{time.Date(2026, 9, 10, 4, 29, 0, 0, time.UTC), false, "a minute early"},
		{time.Date(2026, 9, 10, 4, 31, 0, 0, time.UTC), false, "a minute late"},
		{time.Date(2026, 9, 10, 5, 30, 0, 0, time.UTC), false, "the wrong hour"},
		{time.Date(2026, 9, 11, 4, 30, 0, 0, time.UTC), false, "Friday, not a listed day"},
	}
	for _, c := range cases {
		if got := a.due(c.when); got != c.want {
			t.Errorf("due(%s) = %v, want %v (%s)", c.when.Format(time.RFC3339), got, c.want, c.why)
		}
	}

	var nilSched *AutoRestart
	if nilSched.due(t0) {
		t.Error("an unset schedule must never be due")
	}
	for _, bad := range []*AutoRestart{
		{Days: []string{"Thursday"}, At: ""},
		{Days: nil, At: "04:30"},
		{Days: []string{"Thursday"}, At: "not-a-time"},
		{Days: []string{"Thursday"}, At: "0430"},
	} {
		if bad.due(time.Date(2026, 9, 10, 4, 30, 0, 0, time.UTC)) {
			t.Errorf("malformed schedule %+v was treated as due", bad)
		}
	}
}

// TestStallTakesPrecedence: when a stream is both silent and audio-less, the
// stall is the more useful diagnosis — the encoder produced nothing at all.
func TestStallTakesPrecedence(t *testing.T) {
	h := Health{StallSec: 10, AudioLossSec: 10}
	base := &fpsBaseline{}
	v := Vitals{LastData: t0, LastAudio: t0.Add(time.Second)}
	got := h.check(t0.Add(time.Minute), t0, v, base)
	if got.event != EventFfmpegError {
		t.Errorf("event = %q, want the stall (%s) reported ahead of audio loss", got.event, EventFfmpegError)
	}
}
