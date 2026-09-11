// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/supervisor"
)

// vitalsSampler turns the hub's monotonic health counters into the rates and
// timestamps the supervisor judges a stream by.
//
// The counters themselves are cumulative on purpose — internal/tsjoin stays a
// parser and keeps no policy — so somebody has to remember the previous reading
// to say "audio is still flowing" or "the frame rate is 25". That is this: one
// sample per stream, replaced on each poll.
//
// It is also what makes these checks cheap. The panel answered the same three
// questions by hashing a playlist on disk, ffprobing a segment every five
// minutes, and reading a progress file ffmpeg wrote — per stream, forever. Here
// they are two integer subtractions against counters that were already being
// incremented while the packets went past.
type vitalsSampler struct {
	mu      sync.Mutex
	samples map[string]*vitalsSample
}

type vitalsSample struct {
	at          time.Time
	audioPkts   int64
	videoFrames int64

	// lastAudio is when audio was last observed to be MOVING, not when a packet
	// happened to arrive. It only advances when the counter has actually grown
	// since the previous sample, which is what makes "no audio for 30s" mean it.
	lastAudio time.Time
	// fps is the rate measured over the most recent interval, held so a caller
	// polling faster than a meaningful window still gets a usable answer.
	fps float64
}

func newVitalsSampler() *vitalsSampler {
	return &vitalsSampler{samples: make(map[string]*vitalsSample)}
}

// forget drops a stream's sample, so a restarted encoder is measured fresh
// rather than against counters from its previous life.
func (s *vitalsSampler) forget(id string) {
	s.mu.Lock()
	delete(s.samples, id)
	s.mu.Unlock()
}

// minFPSWindow is how much time must pass between samples before a frame-rate
// figure is worth believing. Below this, a couple of frames either way swings
// the result wildly and a healthy stream looks like it collapsed.
const minFPSWindow = 2 * time.Second

// sample reads a stream's counters and folds them into rates. ok is false when
// the stream is not registered with this manager at all, which the supervisor
// treats as "nothing to judge yet" rather than as a fault.
func (m *Manager) sample(id string) (supervisor.Vitals, bool) {
	st := m.Get(id)
	if st == nil {
		return supervisor.Vitals{}, false
	}

	audio, video, hasAudio := st.Hub.Counters()
	now := time.Now()

	s := m.vitals
	s.mu.Lock()
	prev := s.samples[id]
	cur := &vitalsSample{at: now, audioPkts: audio, videoFrames: video}
	if prev != nil {
		cur.lastAudio, cur.fps = prev.lastAudio, prev.fps

		if audio > prev.audioPkts {
			cur.lastAudio = now
		}
		if elapsed := now.Sub(prev.at); elapsed >= minFPSWindow {
			cur.fps = float64(video-prev.videoFrames) / elapsed.Seconds()
		} else {
			// Too soon to re-measure: keep the previous reading AND its clock, so
			// a fast poller does not permanently reset the window and never
			// produce a figure at all.
			cur.at = prev.at
			cur.audioPkts, cur.videoFrames = prev.audioPkts, prev.videoFrames
		}
	} else if hasAudio && audio > 0 {
		// First sample of a stream that already carries audio: treat it as
		// present now rather than as missing since the epoch.
		cur.lastAudio = now
	}
	s.samples[id] = cur
	lastAudio, fps := cur.lastAudio, cur.fps
	s.mu.Unlock()

	// A source with no audio stream at all reports a zero LastAudio, which the
	// audio-loss rule reads as "nothing to lose" and skips. A video-only channel
	// must never be restarted for being video-only.
	if !hasAudio {
		lastAudio = time.Time{}
	}

	return supervisor.Vitals{
		LastData:  time.Unix(0, st.lastData.Load()),
		LastAudio: lastAudio,
		FPS:       fps,
	}, true
}
