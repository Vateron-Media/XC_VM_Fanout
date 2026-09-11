// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package remux

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsseg"
)

// progress appends ffmpeg `-progress` blocks to a file: key=value lines ending
// in progress=continue (or progress=end). The panel's streams cron tails exactly
// this format for a stream's speed, frame rate and bitrate, then truncates the
// file — so it is opened per report, in append mode, and a truncation between
// reports is harmless.
type progress struct {
	path    string
	started time.Time
	total   int64

	winStart time.Time
	winBytes int64
	kbps     float64
}

func newProgress(path string) *progress {
	now := time.Now()
	return &progress{path: path, started: now, winStart: now}
}

func (p *progress) add(n int) {
	p.total += int64(n)
	p.winBytes += int64(n)
}

func (p *progress) report(st tsseg.Stats, final bool) {
	now := time.Now()
	if el := now.Sub(p.winStart).Seconds(); el >= 1 {
		p.kbps = float64(p.winBytes*8) / el / 1000
		p.winStart, p.winBytes = now, 0
	}
	wall := now.Sub(p.started).Seconds()
	media := st.MediaSec
	if media <= 0 {
		media = wall // no video clock (yet): a live copy runs at wall speed
	}
	speed := 0.0
	if wall > 0 {
		speed = media / wall
	}
	us := int64(media * 1e6)
	state := "continue"
	if final {
		state = "end"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "frame=%d\n", st.Frames)
	fmt.Fprintf(&b, "fps=%.2f\n", st.FPS)
	b.WriteString("stream_0_0_q=-1.0\n")
	fmt.Fprintf(&b, "bitrate=%.1fkbits/s\n", p.kbps)
	fmt.Fprintf(&b, "total_size=%d\n", p.total)
	fmt.Fprintf(&b, "out_time_us=%d\n", us)
	fmt.Fprintf(&b, "out_time_ms=%d\n", us) // ffmpeg's own misnomer: also microseconds
	fmt.Fprintf(&b, "out_time=%s\n", clock(us))
	b.WriteString("dup_frames=0\ndrop_frames=0\n")
	fmt.Fprintf(&b, "speed=%.3gx\n", speed)
	fmt.Fprintf(&b, "progress=%s\n", state)

	f, err := os.OpenFile(p.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_, _ = f.WriteString(b.String())
	_ = f.Close()
}

// clock renders microseconds as ffmpeg does: HH:MM:SS.micro.
func clock(us int64) string {
	if us < 0 {
		us = 0
	}
	s := us / 1e6
	return fmt.Sprintf("%02d:%02d:%02d.%06d", s/3600, s/60%60, s%60, us%1e6)
}
