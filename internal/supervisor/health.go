// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Health events, matching the actions MonitorCommand.php logs for the same
// conditions so the panel's existing log drain keeps classifying them.
const (
	EventAutoRestart      = "AUTO_RESTART"
	EventFPSDropThreshold = "FPS_DROP_THRESHOLD"
	EventAudioLoss        = "AUDIO_LOSS"
	EventFfmpegError      = "FFMPEG_ERROR"
)

// Health is the per-stream watchdog policy. Every field mirrors the panel
// column or setting it comes from, and every check is off when its field is
// zero — so a spec that omits Health supervises the process and nothing more.
type Health struct {
	// StallSec is how long the stream may produce NO bytes before it counts as
	// dead and the encoder is restarted (0 = off).
	//
	// This replaces the panel's md5-the-playlist poll. That check re-hashed the
	// .m3u8 every `seg_time * 6` and restarted when the hash had not moved, which
	// is a proxy for "no new segments" measured through the filesystem. The
	// daemon already stamps the moment of the last non-empty publish, so the same
	// question is a subtraction — continuous, immediate, and with no I/O.
	StallSec int `json:"stall_sec"`

	// AudioLossSec is how long the stream may carry video but no audio before it
	// counts as broken (0 = off), from the panel's `audio_restart_loss`.
	//
	// The panel ffprobed a segment every 300s and looked for an audio codec. Here
	// it is the gap since the last packet on an audio PID, which notices in
	// seconds rather than up to five minutes and costs nothing.
	AudioLossSec int `json:"audio_loss_sec"`

	// FPSThreshold restarts the stream when its frame rate falls to this
	// FRACTION of the baseline observed after warm-up (0 = off). The panel stores
	// `fps_threshold` as a percentage and gates it on `fps_restart`; PHP converts.
	FPSThreshold float64 `json:"fps_threshold"`
	// FPSGraceSec is how long after a start to leave the frame rate alone, from
	// the panel's `fps_delay`. A stream still filling its buffers has a low
	// instantaneous rate and must not be shot for it.
	FPSGraceSec int `json:"fps_grace_sec"`

	// AutoRestart is the panel's scheduled restart: a set of weekday names and a
	// HH:MM local time, as stored in `streams.auto_restart`.
	AutoRestart *AutoRestart `json:"auto_restart,omitempty"`
}

// AutoRestart is a scheduled restart window.
type AutoRestart struct {
	Days []string `json:"days"`
	At   string   `json:"at"` // "HH:MM"
}

// due reports whether the schedule fires at t. Ported from
// MonitorCommand::isAutoRestartDue, including its minute granularity: the
// weekday must match and the current hour and minute must equal the configured
// ones, so the caller has to poll at least once a minute for it to fire at all.
func (a *AutoRestart) due(t time.Time) bool {
	if a == nil || len(a.Days) == 0 || a.At == "" {
		return false
	}
	hh, mm, ok := strings.Cut(a.At, ":")
	if !ok {
		return false
	}
	hour, err1 := strconv.Atoi(strings.TrimSpace(hh))
	min, err2 := strconv.Atoi(strings.TrimSpace(mm))
	if err1 != nil || err2 != nil {
		return false
	}
	if t.Hour() != hour || t.Minute() != min {
		return false
	}
	today := t.Weekday().String() // "Monday" — the same names PHP's date('l') gives
	for _, d := range a.Days {
		if strings.EqualFold(strings.TrimSpace(d), today) {
			return true
		}
	}
	return false
}

// Vitals is what the daemon knows about a stream's output right now. The
// supervisor asks for it on a tick; internal/server answers from the hub, which
// is already parsing these packets to build the join snapshot.
type Vitals struct {
	// LastData is when the stream last published a non-empty chunk. Zero means it
	// has never produced anything.
	LastData time.Time
	// LastAudio is when a packet last arrived on an audio elementary stream.
	// Zero means no audio PID has ever been seen, which is NOT the same as audio
	// having been lost — a stream that never had audio must not be restarted for
	// losing it.
	LastAudio time.Time
	// FPS is the frame rate measured over the last window, or 0 when it cannot be
	// measured (no video PID, or not enough frames yet).
	FPS float64
}

// VitalsFunc reports a stream's current vitals. A nil one disables every check
// that needs them, which is what keeps the supervisor usable on its own.
type VitalsFunc func(id string) (Vitals, bool)

// healthVerdict is why a check decided to restart, or the zero value for "keep
// running". The event is what gets logged; the reason is what an operator reads
// in /monitor/<id>.
type healthVerdict struct {
	event  string
	reason string
	// switchSource asks for the restart to happen on a DIFFERENT source, and
	// switchTo says which. A bool rather than a sentinel index because zero is a
	// perfectly valid source, and the zero VALUE of this struct has to keep
	// meaning "nothing is wrong".
	switchSource bool
	switchTo     int
}

func (v healthVerdict) failed() bool { return v.event != "" }

// fpsBaseline tracks the frame rate a healthy stream settles at, so a DROP can
// be told from a stream that is simply low-frame-rate by nature.
//
// The panel took its baseline from the first reading after `fps_delay` and never
// moved it, so a stream that legitimately ramped up later was measured against
// its own worst moment. This takes the peak seen since the grace window ended,
// which is the same intent (compare against how well this stream is known to be
// able to do) without that failure mode.
type fpsBaseline struct {
	peak float64
	// zeroSince is when the current unbroken run of zero readings began, or the
	// zero time when the last reading carried frames. A freeze is a RUN of them:
	// see dropped.
	zeroSince time.Time
}

func (b *fpsBaseline) observe(fps float64) {
	if fps > b.peak {
		b.peak = fps
	}
}

// dropped reports whether fps has fallen to below `threshold` of the peak.
// A zero peak (nothing measured yet) never trips.
//
// frozenFor is how long the rate must stay at zero before the picture counts as
// frozen; now is the moment this reading was taken.
func (b *fpsBaseline) dropped(now time.Time, fps, threshold float64, frozenFor time.Duration) bool {
	if b.peak <= 0 || threshold <= 0 {
		return false
	}
	if fps <= 0 {
		// A rate of 0 means "could not measure" only until this encoder has
		// shown one: no video PID, or a window too short to divide by. A peak
		// rules both out — this stream's video WAS measurable here — so 0 now
		// means the frames stopped. That is a frozen picture, and with audio
		// still flowing it is invisible to the stall rule.
		//
		// But ONE zero window does not say that. The rate is measured over the
		// last few seconds, and a bursty source — a live HLS origin delivers a
		// whole segment and then says nothing until the next one, ten seconds
		// later — empties every other window as a matter of course. Condemning
		// that restarts a healthy channel on a cadence its own upstream sets,
		// which is why the panel skipped 0 outright. Only a run of zeros long
		// enough to outlast the silence this stream tolerates is a freeze.
		if b.zeroSince.IsZero() {
			b.zeroSince = now
		}
		return now.Sub(b.zeroSince) >= frozenFor
	}
	b.zeroSince = time.Time{}
	return fps < b.peak*threshold
}

// defaultFreezeWindow is how long a zero frame rate must last to be a freeze
// when the stream has no stall bound to borrow one from. Sixty seconds is the
// longest segment duration internal/nativesrc will honour from a live playlist
// (clampTargetDuration's maximum), so it is the longest gap a healthy bursty
// source can leave between deliveries.
const defaultFreezeWindow = 60 * time.Second

// freezeWindow is how long the frame rate must read zero before the picture is
// called frozen.
//
// It is the stall bound, which the panel derives as seg_time*6 and always sends:
// that IS this stream's statement of how long it may say nothing, and it is the
// cadence at which the panel's own playlist-md5 check caught a stopped encoder.
// Anything shorter judges a freeze on less evidence than the panel used.
func (h Health) freezeWindow() time.Duration {
	if h.StallSec > 0 {
		return time.Duration(h.StallSec) * time.Second
	}
	return defaultFreezeWindow
}

// check runs every enabled health rule against a stream's vitals and returns the
// first failure. now is passed in so the whole thing is a pure function of its
// inputs and can be tested without waiting for real time to pass.
//
// startedAt bounds the grace windows: a check must never fire on evidence
// gathered before the current encoder started, or every restart cascades into
// another one.
func (h Health) check(now, startedAt time.Time, v Vitals, base *fpsBaseline) healthVerdict {
	sinceStart := now.Sub(startedAt)

	// Stalled output. Measured from the last data OR from the start, so a stream
	// that has never produced anything is caught by the same rule rather than
	// looking infinitely stale.
	if h.StallSec > 0 {
		limit := time.Duration(h.StallSec) * time.Second
		last := v.LastData
		if last.IsZero() || last.Before(startedAt) {
			last = startedAt
		}
		if now.Sub(last) >= limit {
			return healthVerdict{
				event:  EventFfmpegError,
				reason: fmt.Sprintf("no output for %s", now.Sub(last).Round(time.Second)),
			}
		}
	}

	// Audio loss. Only meaningful once audio has been seen at all: a video-only
	// channel has no audio to lose, and restarting it forever would be the
	// obvious way to get that wrong. A zero LastAudio is exactly that case and
	// is left alone.
	//
	// Audio last seen BEFORE this encoder started is not evidence against it
	// either — that would make every restart cascade into another — but it is
	// not a reason to stop looking. It is measured from the start instead, the
	// same answer the stall rule gives to "nothing since this encoder came up":
	// a stream that declares an audio PID and carries nothing on it for the whole
	// window is broken, whether the silence began before this start or during it.
	// Skipping instead left the rule switched off for the entire life of any
	// encoder that never carried audio — including the replacement an AUDIO_LOSS
	// restart had just started — so the channel stayed silent with no further
	// event, restart or failover.
	if h.AudioLossSec > 0 && !v.LastAudio.IsZero() {
		limit := time.Duration(h.AudioLossSec) * time.Second
		last := v.LastAudio
		if last.Before(startedAt) {
			last = startedAt
		}
		if now.Sub(last) >= limit {
			return healthVerdict{
				event:  EventAudioLoss,
				reason: fmt.Sprintf("no audio for %s", now.Sub(last).Round(time.Second)),
			}
		}
	}

	// Frame rate, once the stream has had its grace period to settle.
	if h.FPSThreshold > 0 && sinceStart >= time.Duration(h.FPSGraceSec)*time.Second {
		if base.dropped(now, v.FPS, h.FPSThreshold, h.freezeWindow()) {
			reason := fmt.Sprintf("fps %.1f fell below %.0f%% of the %.1f baseline",
				v.FPS, h.FPSThreshold*100, base.peak)
			if v.FPS <= 0 {
				reason = fmt.Sprintf("no video frames for %s, against a %.1f baseline",
					now.Sub(base.zeroSince).Round(time.Second), base.peak)
			}
			return healthVerdict{event: EventFPSDropThreshold, reason: reason}
		}
		base.observe(v.FPS)
	}

	return healthVerdict{}
}
