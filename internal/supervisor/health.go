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
}

func (b *fpsBaseline) observe(fps float64) {
	if fps > b.peak {
		b.peak = fps
	}
}

// dropped reports whether fps has fallen to below `threshold` of the peak.
// A zero peak (nothing measured yet) never trips.
func (b *fpsBaseline) dropped(fps, threshold float64) bool {
	if b.peak <= 0 || threshold <= 0 || fps <= 0 {
		return false
	}
	return fps < b.peak*threshold
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
	// obvious way to get that wrong.
	if h.AudioLossSec > 0 && !v.LastAudio.IsZero() && v.LastAudio.After(startedAt) {
		limit := time.Duration(h.AudioLossSec) * time.Second
		if now.Sub(v.LastAudio) >= limit {
			return healthVerdict{
				event:  EventAudioLoss,
				reason: fmt.Sprintf("no audio for %s", now.Sub(v.LastAudio).Round(time.Second)),
			}
		}
	}

	// Frame rate, once the stream has had its grace period to settle.
	if h.FPSThreshold > 0 && sinceStart >= time.Duration(h.FPSGraceSec)*time.Second {
		if v.FPS > 0 {
			if base.dropped(v.FPS, h.FPSThreshold) {
				return healthVerdict{
					event: EventFPSDropThreshold,
					reason: fmt.Sprintf("fps %.1f fell below %.0f%% of the %.1f baseline",
						v.FPS, h.FPSThreshold*100, base.peak),
				}
			}
			base.observe(v.FPS)
		}
	}

	return healthVerdict{}
}
