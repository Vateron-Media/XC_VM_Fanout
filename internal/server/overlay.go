package server

import (
	"bytes"
	"context"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/hub"
)

// overlayTSDuration is how long an admin "send message" banner stays burned onto
// a live-TS viewer's stream before it rejoins the raw fan-out. Mirrors the legacy
// one-segment overlay; kept short since it costs a transient per-viewer re-encode.
const overlayTSDuration = 5 * time.Second

// pendingSignal is an admin "send message" overlay queued for one viewer uuid.
// It reproduces the legacy admin "send message" feature: a text banner burned into the
// video (ffmpeg drawtext) shown once to a single viewer, then cleared.
type pendingSignal struct {
	text     string
	fontSize int
	color    string
	x, y     int
	expires  time.Time // zero = no expiry
}

// signalStore holds the pending per-uuid overlays. PHP pushes them over the
// control socket (POST /signal/<uuid>); serveHLS/serveLive consume them one-shot.
type signalStore struct {
	mu sync.Mutex
	m  map[string]pendingSignal
}

func newSignalStore() *signalStore { return &signalStore{m: make(map[string]pendingSignal)} }

func (s *signalStore) set(uuid string, sig pendingSignal) {
	s.mu.Lock()
	if s.m == nil {
		s.m = make(map[string]pendingSignal)
	}
	s.m[uuid] = sig
	s.mu.Unlock()
}

// peek reports whether a live (non-expired) signal is queued for uuid, without
// consuming it. Cheap guard so the hot path skips the map delete when there is
// nothing to apply.
func (s *signalStore) peek(uuid string) bool {
	if uuid == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sig, ok := s.m[uuid]
	if !ok {
		return false
	}
	if !sig.expires.IsZero() && time.Now().After(sig.expires) {
		delete(s.m, uuid)
		return false
	}
	return true
}

// take returns and removes a non-expired signal for uuid (one-shot, mirroring
// the legacy per-segment overlay that unlinked the signal file after applying).
func (s *signalStore) take(uuid string) (pendingSignal, bool) {
	if uuid == "" {
		return pendingSignal{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sig, ok := s.m[uuid]
	if !ok {
		return pendingSignal{}, false
	}
	delete(s.m, uuid)
	if !sig.expires.IsZero() && time.Now().After(sig.expires) {
		return pendingSignal{}, false
	}
	return sig, true
}

// parseXY resolves the overlay position from the legacy "<x>x<y>" offset string,
// falling back to a random position (matching the legacy overlay's rand ranges) when
// it is empty or malformed.
func parseXY(offset string) (int, int) {
	if xs, ys, ok := strings.Cut(offset, "x"); ok {
		x, ex := strconv.Atoi(strings.TrimSpace(xs))
		y, ey := strconv.Atoi(strings.TrimSpace(ys))
		if ex == nil && ey == nil {
			return x, y
		}
	}
	return 150 + rand.Intn(231), 110 + rand.Intn(141) // 150..380, 110..250
}

// escapeDrawtext escapes a user-supplied message for ffmpeg's drawtext `text=`
// option (which treats \ : ' % specially, and must stay single-line). The value
// is passed to ffmpeg as one argv element, so there is no shell involved — this
// is purely filtergraph-syntax safety.
func escapeDrawtext(s string) string {
	return strings.NewReplacer(
		`\`, `\\`,
		`:`, `\:`,
		`'`, `\'`,
		`%`, `\%`,
		"\n", " ",
		"\r", " ",
	).Replace(s)
}

// sanitizeColor keeps only characters valid in an ffmpeg colour (a hex like
// "#RRGGBB" or a name like "white", optionally "name@0.8"); everything else is
// dropped, falling back to white.
func sanitizeColor(c string) string {
	var b strings.Builder
	for _, r := range c {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '#' || r == '@' || r == '.' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "white"
	}
	return b.String()
}

// overlaySegment re-encodes a self-contained MPEG-TS segment with a drawtext
// banner (the admin "send message" feature). It mirrors the legacy PHP byte-path overlay's
// ffmpeg invocation but pipes the segment in and out in memory. On ANY error it
// returns the original bytes unchanged — a signal must never break playback.
func (m *Manager) overlaySegment(seg []byte, sig pendingSignal, codec string) []byte {
	if len(seg) == 0 || m.ffmpegBin == "" || m.fontPath == "" {
		return seg
	}
	if codec == "" {
		codec = "h264"
	}
	filter := "drawtext=fontfile=" + m.fontPath +
		":text='" + escapeDrawtext(sig.text) + "'" +
		":fontsize=" + strconv.Itoa(sig.fontSize) +
		":x=" + strconv.Itoa(sig.x) +
		":y=" + strconv.Itoa(sig.y) +
		":fontcolor=" + sanitizeColor(sig.color)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, m.ffmpegBin,
		"-nostdin", "-hide_banner", "-loglevel", "quiet", "-y",
		"-i", "pipe:0",
		"-filter_complex", filter,
		"-map", "0", "-vcodec", codec, "-preset", "ultrafast",
		"-acodec", "copy", "-scodec", "copy",
		"-mpegts_flags", "+initial_discontinuity",
		"-f", "mpegts", "pipe:1",
	)
	cmd.Stdin = bytes.NewReader(seg)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil || out.Len() == 0 {
		return seg // graceful: serve the plain segment on any encode failure
	}
	return out.Bytes()
}

// overlayTSWindow burns the signal's banner onto a live-TS viewer's stream for
// overlayTSDuration by piping it through ffmpeg drawtext. A fresh clean-join
// snapshot re-seeds ffmpeg's decoder at a keyframe; hub chunks then feed its
// stdin while its stdout goes to the viewer via write(). After the window ffmpeg
// stops and the caller resumes the raw tail (the player resyncs on the next
// keyframe the hub emits). Returns false ONLY if the viewer connection broke
// (caller should stop serving); overlay disabled / ffmpeg failure returns true
// so the caller simply continues raw — a signal never breaks playback.
//
// The feed goroutine is the sole consumer of sub.C() for the window and is
// joined before returning, so it can never steal chunks from the raw loop after.
func (m *Manager) overlayTSWindow(st *Stream, sub *hub.Sub, write func([]byte) error, sig pendingSignal, codec string) bool {
	if m.ffmpegBin == "" || m.fontPath == "" {
		return true
	}
	if codec == "" {
		codec = "h264"
	}
	filter := "drawtext=fontfile=" + m.fontPath +
		":text='" + escapeDrawtext(sig.text) + "'" +
		":fontsize=" + strconv.Itoa(sig.fontSize) +
		":x=" + strconv.Itoa(sig.x) +
		":y=" + strconv.Itoa(sig.y) +
		":fontcolor=" + sanitizeColor(sig.color)

	ctx, cancel := context.WithTimeout(context.Background(), overlayTSDuration+10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, m.ffmpegBin,
		"-nostdin", "-hide_banner", "-loglevel", "quiet", "-y",
		"-fflags", "+genpts", "-i", "pipe:0",
		"-filter_complex", filter,
		"-map", "0", "-vcodec", codec, "-preset", "ultrafast",
		"-acodec", "copy", "-scodec", "copy",
		"-mpegts_flags", "+initial_discontinuity",
		"-f", "mpegts", "pipe:1",
	)
	stdin, err1 := cmd.StdinPipe()
	stdout, err2 := cmd.StdoutPipe()
	if err1 != nil || err2 != nil || cmd.Start() != nil {
		return true // couldn't start ffmpeg → continue raw
	}

	stopFeed := make(chan struct{})
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		defer stdin.Close()
		_, _ = stdin.Write(st.Hub.Snapshot(0)) // clean keyframe entry for the decoder
		deadline := time.After(overlayTSDuration)
		for {
			select {
			case b, ok := <-sub.C():
				if !ok {
					return
				}
				if _, err := stdin.Write(b); err != nil {
					return
				}
			case <-deadline:
				return
			case <-sub.Done():
				return
			case <-stopFeed:
				return
			}
		}
	}()

	ok := true
	buf := make([]byte, 32*1024)
	for {
		n, rerr := stdout.Read(buf)
		if n > 0 {
			if werr := write(buf[:n]); werr != nil {
				ok = false
				break
			}
		}
		if rerr != nil {
			break
		}
	}
	close(stopFeed)
	<-feedDone
	_ = cmd.Wait()
	return ok
}
