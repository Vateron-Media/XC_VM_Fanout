// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"bytes"
	"context"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/hub"
)

// overlayTSDuration is how long an admin "send message" banner stays burned onto
// a live-TS viewer's stream before it rejoins the raw fan-out. Mirrors the legacy
// one-segment overlay; kept short since it costs a transient per-viewer re-encode.
const overlayTSDuration = defaults.OverlayTSDuration

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
//
// n mirrors len(m) as an atomic. serveLive consults this store on EVERY chunk it
// delivers, so with the lock alone the whole daemon's live-TS fan-out — every
// viewer of every stream, tens of thousands of chunks a second — funnelled
// through one mutex to ask a question whose answer is almost always "no": a
// signal is a manual admin action, so the map is empty essentially always. The
// counter turns that question into a single atomic load and the mutex is taken
// only when a signal really is queued.
type signalStore struct {
	n  atomic.Int64 // == len(m); read on the live-TS hot path without the lock
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
	s.n.Store(int64(len(s.m)))
	s.mu.Unlock()
}

// peek reports whether a live (non-expired) signal is queued for uuid, without
// consuming it. Cheap guard so the hot path skips the map delete when there is
// nothing to apply — and, via n, skips the lock entirely when nothing is queued
// for anyone.
func (s *signalStore) peek(uuid string) bool {
	if uuid == "" || s.n.Load() == 0 {
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
		s.n.Store(int64(len(s.m)))
		return false
	}
	return true
}

// take returns and removes a non-expired signal for uuid (one-shot, mirroring
// the legacy per-segment overlay that unlinked the signal file after applying).
func (s *signalStore) take(uuid string) (pendingSignal, bool) {
	if uuid == "" || s.n.Load() == 0 {
		return pendingSignal{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sig, ok := s.m[uuid]
	if !ok {
		return pendingSignal{}, false
	}
	delete(s.m, uuid)
	s.n.Store(int64(len(s.m)))
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
		codec = defaults.OverlayDefaultCodec
	}
	filter := "drawtext=fontfile=" + m.fontPath +
		":text='" + escapeDrawtext(sig.text) + "'" +
		":fontsize=" + strconv.Itoa(sig.fontSize) +
		":x=" + strconv.Itoa(sig.x) +
		":y=" + strconv.Itoa(sig.y) +
		":fontcolor=" + sanitizeColor(sig.color)

	ctx, cancel := context.WithTimeout(context.Background(), defaults.OverlaySegmentTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, m.ffmpegBin,
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-i", "pipe:0",
		// -vf, not -filter_complex: an unlabeled filtergraph input used to bind
		// itself to the first video stream, but ffmpeg 7 refuses to resolve it
		// alongside -map 0 ("Cannot find a matching stream for unlabeled input pad")
		// and the whole re-encode fails — which, being best-effort, showed up as the
		// signal silently doing nothing. -vf applies to the mapped video stream on
		// every ffmpeg version, and -map 0 keeps audio/subs flowing as before.
		"-vf", filter,
		"-map", "0", "-vcodec", codec, "-preset", "ultrafast",
		"-acodec", "copy", "-scodec", "copy",
		"-mpegts_flags", "+initial_discontinuity",
		"-f", "mpegts", "pipe:1",
	)
	cmd.Stdin = bytes.NewReader(seg)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil || out.Len() == 0 {
		// graceful: serve the plain segment on any encode failure, but log why so
		// a misconfigured font/codec doesn't fail silently on every signal.
		dlog.Logf("signal", "overlay segment re-encode failed (%v), serving plain: %s", err, strings.TrimSpace(errBuf.String()))
		return seg
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
		codec = defaults.OverlayDefaultCodec
	}
	filter := "drawtext=fontfile=" + m.fontPath +
		":text='" + escapeDrawtext(sig.text) + "'" +
		":fontsize=" + strconv.Itoa(sig.fontSize) +
		":x=" + strconv.Itoa(sig.x) +
		":y=" + strconv.Itoa(sig.y) +
		":fontcolor=" + sanitizeColor(sig.color)

	ctx, cancel := context.WithTimeout(context.Background(), overlayTSDuration+defaults.OverlayTSWindowGrace)
	defer cancel()
	cmd := exec.CommandContext(ctx, m.ffmpegBin,
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-fflags", "+genpts", "-i", "pipe:0",
		"-vf", filter, // see overlaySegment: -filter_complex cannot bind under ffmpeg 7
		"-map", "0", "-vcodec", codec, "-preset", "ultrafast",
		"-acodec", "copy", "-scodec", "copy",
		"-mpegts_flags", "+initial_discontinuity",
		"-f", "mpegts", "pipe:1",
	)
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	stdin, err1 := cmd.StdinPipe()
	stdout, err2 := cmd.StdoutPipe()
	if err1 != nil || err2 != nil {
		dlog.Logf("signal", "overlay TS window: pipe setup failed (%v / %v), continuing raw", err1, err2)
		return true
	}
	if err := cmd.Start(); err != nil {
		dlog.Logf("signal", "overlay TS window: ffmpeg start failed (%v), continuing raw", err)
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
	if err := cmd.Wait(); err != nil && ctx.Err() == nil {
		// ctx.Err() != nil means our own deadline/kill ended the window (expected);
		// anything else is a real overlay ffmpeg failure worth surfacing.
		dlog.Logf("signal", "overlay TS window: ffmpeg exited (%v): %s", err, strings.TrimSpace(errBuf.String()))
	}
	return ok
}
