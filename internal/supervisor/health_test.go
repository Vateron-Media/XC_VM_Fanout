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
// must not condemn the new one, or every restart cascades into another. The new
// encoder gets a window of its own, measured from its start.
func TestAudioSeenOnlyBeforeThisStartIsIgnored(t *testing.T) {
	h := Health{AudioLossSec: 30}
	base := &fpsBaseline{}
	started := t0.Add(time.Minute)
	v := Vitals{LastData: started.Add(time.Second), LastAudio: t0} // audio predates the restart

	for _, after := range []time.Duration{time.Second, 20 * time.Second, 29 * time.Second} {
		if got := h.check(started.Add(after), started, v, base); got.failed() {
			t.Fatalf("after %s: condemned on the previous encoder's audio: %+v", after, got)
		}
	}
}

// TestDeclaredAudioThatNeverArrivesIsStillLost: a stream whose PMT declares an
// audio PID that carries nothing is the exact fault audio_restart_loss is for,
// and it is the state a restart FOR audio loss leaves behind when the new
// encoder is silent too: the last audio then predates every later start, so the
// rule skipped itself for the whole life of that encoder and the channel stayed
// silent with no event, no restart and no failover. The stall rule answers the
// same "nothing since the start" case by measuring from the start; so does this.
func TestDeclaredAudioThatNeverArrivesIsStillLost(t *testing.T) {
	h := Health{AudioLossSec: 30}
	base := &fpsBaseline{}
	started := t0.Add(time.Minute)
	v := Vitals{LastData: started.Add(time.Second), LastAudio: t0} // declared, never moved

	got := h.check(started.Add(31*time.Second), started, v, base)
	if !got.failed() || got.event != EventAudioLoss {
		t.Fatalf("verdict = %+v, want %s once the window has passed with no audio at all", got, EventAudioLoss)
	}
	// A stream that never had audio at all still has none to lose.
	if got := h.check(started.Add(time.Hour), started, Vitals{LastData: started}, base); got.failed() {
		t.Fatalf("video-only stream condemned for audio loss: %+v", got)
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

// TestUnmeasurableFPSIsNotADrop: until this encoder has shown a frame rate of
// its own, 0 means "could not measure" (no video PID, or too short a window),
// not "no frames". Treating it as a drop would restart every audio-only stream
// forever — and an audio-only stream never builds a baseline, which is exactly
// what tells the two apart.
func TestUnmeasurableFPSIsNotADrop(t *testing.T) {
	h := Health{FPSThreshold: 0.5}
	base := &fpsBaseline{} // nothing has ever been measured on this encoder
	for _, after := range []time.Duration{time.Minute, time.Hour, 24 * time.Hour} {
		if got := h.check(t0.Add(after), t0, Vitals{FPS: 0}, base); got.failed() {
			t.Fatalf("after %s: an unmeasurable frame rate was treated as a drop: %+v", after, got)
		}
	}
}

// TestAFrozenPictureIsADrop: a video PID that stops while the audio keeps
// flowing leaves the stall rule quiet (bytes are still arriving) and the frame
// rate at exactly 0. Skipping 0 as "unmeasurable" meant the one total failure
// the fps rule exists to catch was the one it never caught: a frozen picture ran
// on untouched. A baseline measured on THIS encoder is the proof that its video
// was measurable, which is what the panel's playlist-md5 check had instead.
//
// It takes a SUSTAINED run of zeros, not one window: see
// TestOneEmptyFPSWindowIsNotAFreeze.
func TestAFrozenPictureIsADrop(t *testing.T) {
	h := Health{FPSThreshold: 0.5, FPSGraceSec: 60}
	base := &fpsBaseline{}

	// It settled at 25fps: the baseline is its own.
	if got := h.check(t0.Add(90*time.Second), t0, Vitals{FPS: 25, LastData: t0.Add(90 * time.Second)}, base); got.failed() {
		t.Fatalf("a healthy 25fps was condemned: %+v", got)
	}
	// The frames stop. One empty window proves nothing — a bursty source has
	// those — so the run of zeros has to cover the freeze window, which is 60s
	// here, this stream having declared no stall bound of its own.
	frozenAt := t0.Add(2 * time.Minute)
	for _, after := range []time.Duration{0, 30 * time.Second, 59 * time.Second} {
		at := frozenAt.Add(after)
		if got := h.check(at, t0, Vitals{FPS: 0, LastData: at}, base); got.failed() {
			t.Fatalf("%s into the silence: condemned before the freeze window was up: %+v", after, got)
		}
	}
	at := frozenAt.Add(61 * time.Second)
	got := h.check(at, t0, Vitals{FPS: 0, LastData: at}, base)
	if !got.failed() || got.event != EventFPSDropThreshold {
		t.Fatalf("verdict = %+v, want %s for a video PID that stopped", got, EventFPSDropThreshold)
	}

	// And the grace window still protects a stream that is only starting up.
	warming := &fpsBaseline{peak: 25}
	if g := h.check(t0.Add(30*time.Second), t0, Vitals{FPS: 0}, warming); g.failed() {
		t.Fatalf("fired inside the grace window: %+v", g)
	}
}

// TestOneEmptyFPSWindowIsNotAFreeze: the frame rate this rule judges is an
// INSTANTANEOUS one — internal/server divides the video-frame counter's growth
// by the time since the previous sample, and the supervisor samples every 5s —
// so a source that arrives in bursts reads exactly 0 between them. This repo's
// own HLS puller says as much ("a live HLS source arrives a whole segment at a
// time and says nothing in between", ten seconds being common), and the panel's
// ffmpeg is not paced with -re either, so its output to the ingest socket has
// the same cadence. Condemning a single empty window restarts a healthy channel,
// and then restarts its replacement, for ever — the panel guarded against
// exactly this with `if (0 < $rFps)`.
func TestOneEmptyFPSWindowIsNotAFreeze(t *testing.T) {
	// seg_time 10 → the panel sends stall_sec = seg_time*6 = 60.
	h := Health{StallSec: 60, FPSThreshold: 0.5, FPSGraceSec: 10}
	base := &fpsBaseline{}

	// Ten minutes of a 10s-segment source sampled every 5s: a window carrying a
	// whole segment, then a window carrying nothing.
	for i := 0; i < 120; i++ {
		at := t0.Add(time.Duration(i+4) * 5 * time.Second)
		fps := 0.0
		if i%2 == 0 {
			fps = 50 // 250 frames of a 10s segment, over a 5s window
		}
		if got := h.check(at, t0, Vitals{FPS: fps, LastData: at}, base); got.failed() {
			t.Fatalf("tick %d (fps %.0f, %s in): a bursty but healthy source was condemned: %+v",
				i, fps, at.Sub(t0), got)
		}
	}

	// The rule is still armed: once the frames really stop, a run of zeros that
	// outlasts the stream's own tolerated silence is a freeze.
	at := t0.Add(20 * time.Minute)
	if got := h.check(at, t0, Vitals{FPS: 50, LastData: at}, base); got.failed() {
		t.Fatalf("a segment arrived and it was condemned anyway: %+v", got)
	}
	at = at.Add(5 * time.Second)
	if got := h.check(at, t0, Vitals{FPS: 0, LastData: at}, base); got.failed() {
		t.Fatalf("the first zero after the last segment condemned it: %+v", got)
	}
	at = at.Add(61 * time.Second)
	got := h.check(at, t0, Vitals{FPS: 0, LastData: at}, base)
	if !got.failed() || got.event != EventFPSDropThreshold {
		t.Fatalf("verdict = %+v, want %s once the zeros outlast the stall bound", got, EventFPSDropThreshold)
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
