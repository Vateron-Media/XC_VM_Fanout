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
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// overlayTSDuration is how long an admin "send message" banner stays burned onto
// a live-TS viewer's stream before it rejoins the raw fan-out. Mirrors the legacy
// one-segment overlay; kept short since it costs a transient per-viewer re-encode.
//
// A var, not a const, purely so the tests can shrink the window instead of
// sitting out five seconds of wall clock per case; nothing in the daemon ever
// assigns to it.
var overlayTSDuration = defaults.OverlayTSDuration

// pendingSignal is an admin "send message" overlay queued for one viewer uuid.
// It reproduces the legacy admin "send message" feature: a text banner burned into the
// video (ffmpeg drawtext) shown once to a single viewer, then cleared.
type pendingSignal struct {
	// uuid is the viewer this signal is addressed to — the map key, carried in
	// the value as well so a consumer that was handed the signal alone still
	// knows whose it is. overlayTSWindow needs it to watch that viewer's kill
	// channel for the length of the banner window.
	uuid     string
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
	// Sweep BEFORE inserting, so this signal is queued whatever its own TTL says
	// and only somebody else's dead one is dropped.
	s.sweepLocked(time.Now())
	sig.uuid = uuid
	s.m[uuid] = sig
	s.n.Store(int64(len(s.m)))
	s.mu.Unlock()
}

// sweepLocked drops every expired entry and re-syncs the atomic count. Callers
// hold mu.
//
// A signal is addressed to one viewer, and that viewer may never come back to
// collect it: the admin messages a uuid that has already disconnected, an HLS
// viewer that stopped polling, or a uuid another node serves. Deleting an entry
// only when ITS OWN uuid looks again therefore left it in the map for the life
// of the process, and while n stayed above zero every live-TS viewer on the node
// took the mutex below on every chunk it was delivered — the whole point of the
// atomic count, lost to one ordinary admin action. Sweeping is cheap: signals
// are a manual action, so the map holds a handful of entries at most, and this
// only ever runs when a signal is queued or when a lookup already missed.
func (s *signalStore) sweepLocked(now time.Time) {
	for u, sig := range s.m {
		if !sig.expires.IsZero() && now.After(sig.expires) {
			delete(s.m, u)
		}
	}
	s.n.Store(int64(len(s.m)))
}

// peek reports whether a live (non-expired) signal is queued for uuid, without
// consuming it. Cheap guard so the hot path skips the map delete when there is
// nothing to apply — and, via n, skips the lock entirely when nothing is queued
// for anyone.
func (s *signalStore) peek(uuid string) bool {
	if uuid == "" || s.n.Load() == 0 {
		return false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	sig, ok := s.m[uuid]
	if ok && (sig.expires.IsZero() || !now.After(sig.expires)) {
		return true
	}
	// Nothing for this viewer. We are already holding the lock the fast path is
	// trying to avoid, so clear out anything nobody is coming back for — that,
	// and not this uuid's own miss, is what keeps the lock hot for everyone.
	s.sweepLocked(now)
	return false
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
// option. The value is passed to ffmpeg as one argv element, so no shell is
// involved — this is purely filtergraph syntax. Newlines collapse to spaces:
// drawtext takes one line.
//
// Combined with the `expansion=none` drawtextFilter sets, the message is drawn
// exactly as typed. Without it drawtext runs its own %{…} expansion over the
// text, so "50% off" logged "Stray %" and drew NOTHING (the banner silently
// vanished), a backslash was eaten, and "%{e:…}" was evaluated as an expression —
// %{e:while(1,1)} would have spun a per-viewer ffmpeg until its kill timeout.
func escapeDrawtext(s string) string {
	return escapeFilterArg(strings.NewReplacer("\n", " ", "\r", " ").Replace(s))
}

// escapeFilterArg escapes one value for a -vf filter option, for BOTH of the
// passes ffmpeg unescapes it in: the option parser (av_opt_set, which splits
// options on ':') and, before that, the filtergraph parser (which ends a filter
// on ',' or ';' and reads '[' ']' as link labels). Each pass consumes one layer
// of backslashes, so a character that matters to the inner one has to survive the
// outer one too — hence escaping twice rather than once.
//
// The value used to be wrapped in single quotes, with an apostrophe written as
// the shell's close-escape-reopen sequence instead. That survives only the first
// pass: the option parser then saw a bare quote, and swallowed the REST of the
// filter into the text. A viewer fingerprinted by username saw
// "obrien:fontsize=20:x=10:y=10:..." drawn at the default size, position and
// colour — an unreadable non-fingerprint — while "it's Bob's" simply lost its
// apostrophes. Quoting cannot express this, so nothing is quoted any more.
func escapeFilterArg(s string) string {
	optionLevel := strings.NewReplacer(`\`, `\\`, `'`, `\'`, `:`, `\:`).Replace(s)
	return strings.NewReplacer(
		`\`, `\\`,
		`'`, `\'`,
		`,`, `\,`,
		`;`, `\;`,
		`[`, `\[`,
		`]`, `\]`,
	).Replace(optionLevel)
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

// drawtextFilter builds the -vf value that burns sig's banner onto the video.
// One builder for both the HLS-segment and the live-TS overlay: they must draw
// the same thing, and a divergence here is invisible until a viewer sees it.
//
// expansion=none makes the message literal text rather than a drawtext template.
// The panel sends a viewer's uuid, a viewer's username or an admin's free text —
// never a template — so expansion only ever mangled a legitimate message ('%' put
// the whole banner out) or ran something it should not have.
//
// The font path is operator config, not viewer input, but it lands in the same
// filtergraph and needs the same escaping: a directory holding a ',' or a ':' made
// ffmpeg exit before it drew anything, so every signal on the node silently did
// nothing.
func drawtextFilter(fontPath string, sig pendingSignal) string {
	return "drawtext=fontfile=" + escapeFilterArg(fontPath) +
		":text=" + escapeDrawtext(sig.text) +
		":fontsize=" + strconv.Itoa(sig.fontSize) +
		":x=" + strconv.Itoa(sig.x) +
		":y=" + strconv.Itoa(sig.y) +
		":fontcolor=" + sanitizeColor(sig.color) +
		":expansion=none"
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
	filter := drawtextFilter(m.fontPath, sig)

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
		// Stay on the source's clock. Without these ffmpeg rebases the output to
		// start at zero plus its mux delay, so a segment whose first PTS was 19.4 s
		// came back at 1.4 s. The playlist is cut from the ring and shared by every
		// viewer, so it cannot carry an #EXT-X-DISCONTINUITY for one viewer's
		// overlaid sequence — which RFC 8216 requires for a timestamp change — and a
		// player that places segments by PTS misplaces or stalls on the very segment
		// carrying the message. -copyts alone still shifts by the mux delay, hence
		// the two zeroes.
		"-copyts", "-muxdelay", "0", "-muxpreload", "0",
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

// killChan returns the channel the panel's kick closes for one live-TS viewer
// uuid on this stream (see Stream.dropConn), or nil when that uuid is not
// connected here — which a nil-safe select reads as "never ready", exactly what
// a caller with no viewer to watch wants. It lives here because the banner
// window is its only caller: nothing else needs to watch a kick it did not
// itself hand out.
func (s *Stream) killChan(uuid string) <-chan struct{} {
	if uuid == "" {
		return nil
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	cs := s.conns[uuid]
	if cs == nil {
		return nil
	}
	return cs.kill
}

// laterCursor returns whichever of two cursors on the same ring is further on.
// Block ids are monotonic and a cursor only ever moves forward inside a block, so
// comparing them lexicographically is the ring's own ordering.
func laterCursor(a, b tsjoin.Cursor) tsjoin.Cursor {
	if b.GOP > a.GOP || (b.GOP == a.GOP && b.Off > a.Off) {
		return b
	}
	return a
}

// overlayTSWindow burns the signal's banner onto a live-TS viewer's stream for
// overlayTSDuration by piping it through ffmpeg drawtext. ffmpeg's stdin gets the
// latest PAT/PMT and then ONE contiguous run of ring bytes, followed by cursor
// from the start of the viewer's own block; its stdout goes to the viewer via
// write(). After the window ffmpeg stops and the caller resumes the raw tail from
// the returned cursor (the player resyncs on the next keyframe the ring emits).
//
// It returns the cursor to resume the raw fan-out from — advanced to wherever the
// feed reached, so there is no gap or duplication across the window — and false
// ONLY if the viewer connection broke (caller should stop serving). Overlay
// disabled / ffmpeg failure returns (cur, true) so the caller simply continues
// raw from where it was: a signal never breaks playback. That holds for an
// ffmpeg that started and then died as much as for one that never started: if
// nothing reached the viewer the ORIGINAL cursor comes back, however much of the
// ring ffmpeg read before failing, because none of it was shown.
//
// The feed goroutine is the sole reader of the ring for the window and is joined
// before returning, so the returned cursor is stable and it can never race the
// raw loop after.
func (m *Manager) overlayTSWindow(st *Stream, cur tsjoin.Cursor, write func([]byte) error, sig pendingSignal, codec string) (tsjoin.Cursor, bool) {
	if m.ffmpegBin == "" || m.fontPath == "" {
		return cur, true
	}
	if codec == "" {
		codec = defaults.OverlayDefaultCodec
	}
	filter := drawtextFilter(m.fontPath, sig)

	ctx, cancel := context.WithTimeout(context.Background(), overlayTSDuration+defaults.OverlayTSWindowGrace)
	defer cancel()
	cmd := exec.CommandContext(ctx, m.ffmpegBin,
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-fflags", "+genpts", "-i", "pipe:0",
		"-vf", filter, // see overlaySegment: -filter_complex cannot bind under ffmpeg 7
		"-map", "0", "-vcodec", codec, "-preset", "ultrafast",
		"-acodec", "copy", "-scodec", "copy",
		// Stay on the stream's own clock (see overlaySegment). It matters twice
		// over here: the raw fan-out resumes on that clock the instant the window
		// ends, so a rebase made the viewer's timeline jump down to ~1.4 s and
		// straight back up again, with only the first packet flagged.
		"-copyts", "-muxdelay", "0", "-muxpreload", "0",
		"-mpegts_flags", "+initial_discontinuity",
		"-f", "mpegts", "pipe:1",
	)
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	stdin, err1 := cmd.StdinPipe()
	stdout, err2 := cmd.StdoutPipe()
	if err1 != nil || err2 != nil {
		dlog.Logf("signal", "overlay TS window: pipe setup failed (%v / %v), continuing raw", err1, err2)
		return cur, true
	}
	if err := cmd.Start(); err != nil {
		dlog.Logf("signal", "overlay TS window: ffmpeg start failed (%v), continuing raw", err)
		return cur, true // couldn't start ffmpeg → continue raw
	}

	// Watch the panel's kick for the length of the window. DELETE
	// /connections/<uuid> only closes this channel — dropConn does not touch the
	// socket — so every write to a kicked viewer keeps succeeding, and serveLive
	// looks at the channel once per loop iteration, of which this whole window is
	// one. An admin kill, a connection-limit eviction or a line banned mid-banner
	// therefore went on being served for the rest of overlayTSDuration (five
	// seconds in production) after the panel had already ended the session.
	//
	// Cancelling the window's context is the same lever the failed-write path
	// pulls: ffmpeg dies, its pipes close, the stdout loop and the feed unwind at
	// once. The session is handed back ALIVE on purpose — serveLive's own killC
	// select then returns on the very next iteration with "dropped by panel",
	// which is what the operator did; alive=false would log it as a client that
	// closed. The goroutine is joined before returning, so it can never outlive
	// the window or call cancel on a later one.
	if kill := st.killChan(sig.uuid); kill != nil {
		watchStop, watchDone := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-kill:
				cancel()
			case <-watchStop:
			}
		}()
		defer func() { close(watchStop); <-watchDone }()
	}

	stopFeed := make(chan struct{})
	feedDone := make(chan struct{})
	endCur := cur
	go func() {
		// Rewind to the START of the viewer's own block and read forward from
		// there. The ring is cut at random-access points, so a block start is a
		// decoder entry point, and everything after it is one unbroken run of the
		// stream — which is the whole contract of this window.
		//
		// It used to seed ffmpeg with a live Snapshot(0) and then follow cur. But
		// cur is ALWAYS behind the live edge here: serveLive looks for a queued
		// signal only at the top of its loop, which it reaches right after a wake
		// (a Publish has appended bytes since the cursor was set) or while the
		// viewer is still taking its join history. So the snapshot and the follow
		// were two different places in the stream spliced together. A viewer parked
		// at the edge had the newest block encoded twice; one further back got the
		// live GOP and then mid-GOP bytes from seconds earlier, referencing
		// pictures the decoder never had. Either way the banner window opened on
		// macroblock garbage — and on a long GOP that is the whole window.
		c := tsjoin.Cursor{GOP: cur.GOP}
		rewound := c != cur
		// endCur is read by the caller only after feedDone closes, so this write
		// is safely published; stdin.Close (registered later, so it runs first on
		// return) lets ffmpeg drain and finish its output. The resume point never
		// goes backwards: the replayed head of the block was already sent raw, so
		// the caller must carry on from cur even if the feed never got that far.
		defer func() { endCur = laterCursor(cur, c); close(feedDone) }()
		defer stdin.Close()
		head, _ := st.Hub.Join(0) // latest PAT/PMT, so ffmpeg can find the program
		if _, err := stdin.Write(head); err != nil {
			return
		}
		deadline := time.After(overlayTSDuration)
		for {
			select {
			case <-deadline:
				return
			case <-stopFeed:
				return
			default:
			}
			b, next, atEnd, wake, behind, ended := st.Hub.Follow(c, joinRunBytes)
			if behind && rewound {
				// The block's head was pruned between the viewer's last read and
				// now, but cur itself can still be live (a ring shorter than one
				// GOP — see ReadFrom). Feed from there rather than let an admin
				// message cost the viewer its session.
				b.Release()
				c, rewound = cur, false
				continue
			}
			rewound = false
			if behind || ended {
				b.Release()
				return
			}
			var werr error
			for _, p := range b.Parts {
				if _, werr = stdin.Write(p); werr != nil {
					break
				}
			}
			b.Release()
			if werr != nil {
				return
			}
			c = next
			if atEnd {
				select {
				case <-wake:
				case <-deadline:
					return
				case <-stopFeed:
					return
				}
			}
		}
	}()

	ok := true
	var delivered int64
	buf := make([]byte, 32*1024)
	for {
		n, rerr := stdout.Read(buf)
		if n > 0 {
			if werr := write(buf[:n]); werr != nil {
				ok = false
				// Kill ffmpeg before joining the feed. Nobody drains its stdout
				// from here on, so it fills that pipe, blocks, and stops reading
				// its stdin — leaving the feed goroutine stuck inside
				// stdin.Write, which is not a select and never sees stopFeed. The
				// join below then waited for the context's own kill, holding this
				// handler, an ffmpeg process and a pinned ring run (which
				// suspends GOP-buffer recycling for the whole stream) for the
				// rest of the window, long after the viewer had gone. Killing
				// ffmpeg closes the pipe, so the blocked write returns EPIPE at
				// once and the feed unwinds.
				cancel()
				break
			}
			delivered += int64(n)
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
	if delivered == 0 {
		// ffmpeg never produced a byte: a codec this build lacks, a colour
		// drawtext rejects, a filter that would not parse. It had still READ
		// (probed) whatever the feed pushed at it, so endCur points past bytes
		// the viewer was never shown — resuming there cut seconds out of its
		// stream for a banner it never saw. Nothing was delivered, so the window
		// never happened: carry on raw from exactly where the viewer was.
		dlog.Logf("signal", "overlay TS window: ffmpeg produced no output, resuming raw from the viewer's own cursor")
		return cur, ok
	}
	return endCur, ok
}
