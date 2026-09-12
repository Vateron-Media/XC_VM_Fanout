// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package hub fans one producer's MPEG-TS byte stream out to many viewers.
//
// One Hub per live stream. The producer calls Publish, which folds each chunk
// into the single in-memory ring (internal/tsjoin) and wakes any viewer parked at
// the live edge. A viewer follows the ring by cursor for its whole session — the
// join history and then the live tail — via Join + Follow (ADR 0004): it reads the
// ring forward in pinned runs and, at the edge, parks on the wake signal until the
// next Publish. A viewer slower than the stream has its cursor pruned off the
// ring's tail (Follow reports it behind) and is let go, so it can never stall the
// producer or the other viewers. There is no per-viewer copy and no per-viewer
// queue: the ring is the one byte store for join, live tail and HLS alike.
package hub

import (
	"sync"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// Burst is a viewer's join burst: the tables, then keyframe-aligned history.
//
// It is NOT a copy of that history. Parts are the ring's own GOP buffers, pinned
// so that prune cannot recycle them into new GOPs while the viewer is being
// written — the caller writes them straight to its socket and then Releases the
// pin. The burst used to be copied into one contiguous buffer per join: up to
// the whole client prebuffer (30 s by the panel's default, ~30 MB of an 8 Mbit/s
// channel), allocated on every connect and held for as long as the viewer took
// to drain it. A zap storm across a node's channels made that the daemon's
// largest memory term — 90 joins across 30 channels grew the heap by 2 GB in
// five seconds — and the copy bought nothing the pin did not already provide.
//
// Head is a private copy: PAT and PMT are mutated in place as new ones arrive.
type Burst struct {
	Head  []byte
	Parts [][]byte

	h    *Hub
	once sync.Once
}

// Len is the burst's size in bytes.
func (b *Burst) Len() int {
	n := len(b.Head)
	for _, p := range b.Parts {
		n += len(p)
	}
	return n
}

// Bytes returns the burst as one contiguous copy — for callers that need a
// single buffer (tests); a viewer writes Head and Parts instead.
func (b *Burst) Bytes() []byte {
	out := make([]byte, 0, b.Len())
	out = append(out, b.Head...)
	for _, p := range b.Parts {
		out = append(out, p...)
	}
	return out
}

// Release unpins the burst's GOP buffers, letting the ring recycle them again.
// Call it once the burst has been written or abandoned; the Parts must not be
// read afterwards. Safe to call more than once.
func (b *Burst) Release() {
	b.once.Do(func() {
		if b.h == nil {
			return
		}
		b.h.mu.Lock()
		b.h.join.Unpin()
		b.h.mu.Unlock()
	})
}

// Hub is the per-stream fan-out point.
type Hub struct {
	mu   sync.Mutex
	join *tsjoin.State

	// wake is the follower notify signal (ADR 0004): a channel closed and
	// replaced under h.mu on every data Publish, so a viewer parked at the live
	// edge (Follow, atEnd) wakes and reads the newly-appended ring bytes. Capturing
	// it under the same lock that Publish takes to close it is what makes it
	// lost-wakeup-free — see Follow. closed is set once by CloseAll (teardown), so
	// a parked follower wakes to an ended stream instead of blocking forever.
	wake   chan struct{}
	closed bool
}

// New returns a Hub. maxGOP caps a single GOP (bytes); maxPrebufMS is how many
// milliseconds of keyframe-aligned history the join state keeps for client
// prebuffer (0 = current GOP only).
func New(maxGOP int, maxPrebufMS int64) *Hub {
	return &Hub{join: tsjoin.New(maxGOP, maxPrebufMS), wake: make(chan struct{})}
}

// signalWake wakes every follower parked at the live edge and re-arms the signal
// for the next Publish. Caller holds h.mu and has checked !h.closed (after CloseAll
// the channel is closed for good and must not be closed again).
func (h *Hub) signalWake() {
	close(h.wake)
	h.wake = make(chan struct{})
}

// Publish folds a packet-aligned chunk into the ring and wakes any viewer parked
// at the live edge (ADR 0004). There is no per-chunk copy and no broadcast: the
// chunk is folded in place — join.Update copies what it retains into the ring's
// own (recycled) GOP buffers and keeps no reference to the input, and Update runs
// to completion before Publish returns, so the caller may reuse its buffer at
// once. Viewers read those bytes back out of the ring themselves (Follow), so the
// only work here is the single ring copy every published byte already needed. This
// is what removed the per-chunk broadcast allocation and the redundant second copy
// that the old push-to-N-channels fan-out made on every watched stream.
//
// The wake is skipped for an empty chunk (nothing was appended to wake for) and
// after teardown (the wake channel is already closed for good — see CloseAll).
func (h *Hub) Publish(chunk []byte) {
	h.mu.Lock()
	h.join.Update(chunk)
	if len(chunk) > 0 && !h.closed {
		h.signalWake()
	}
	h.mu.Unlock()
}

// Follow returns the next run of ring bytes from cursor c — about max of them,
// pinned in a Burst the caller writes and then Releases — and the cursor after
// it. It is the pull-based live path of ADR 0004: a viewer follows the ring by
// cursor for its whole session (join and tail), reading forward in runs and,
// when it reaches the live edge, parking on wake until the next Publish.
//
//   - behind: c has left the ring (a reader slower than the stream) — drop it.
//   - ended: the stream was torn down (CloseAll) — return.
//   - atEnd: the run reached the live edge; wake is a channel closed by the next
//     data Publish (or by teardown). Select on it — together with the caller's
//     ctx / kick / idle-timeout — then call Follow again. wake is nil when there
//     is more to read right now (keep reading before parking).
//
// The wake channel is captured under the same lock Publish takes to close it, so
// a Publish landing between this return and the caller's select finds the channel
// already closed: the select fires at once and the caller re-reads. No wakeup is
// lost. Nothing is pinned when the returned Burst has no parts.
func (h *Hub) Follow(c tsjoin.Cursor, max int) (burst *Burst, next tsjoin.Cursor, atEnd bool, wake <-chan struct{}, behind, ended bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return &Burst{}, c, false, nil, false, true
	}
	parts, next, atEnd, behind := h.join.ReadFrom(c, max)
	if behind {
		return &Burst{}, c, false, nil, true, false
	}
	burst = &Burst{Parts: parts}
	if len(parts) > 0 {
		burst.h = h // pinned: Release unpins
	}
	if atEnd {
		wake = h.wake // park on this; the next Publish closes it
	}
	return burst, next, atEnd, wake, false, false
}

// Join begins a viewer's session: the header to write first (PAT/PMT, copied —
// they are mutated in place) and a cursor at the start of its prebufMS of history.
// The viewer then reads the ring forward from that cursor with Follow — the
// history and then the live tail — with nothing copied per viewer and nothing
// queued: the history is already in the ring, once, for everyone.
func (h *Hub) Join(prebufMS int64) ([]byte, tsjoin.Cursor) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.join.JoinStart(nil, prebufMS)
}

// Snapshot returns a fresh clean-entry snapshot (PAT/PMT + keyframe, optionally
// prebufMS of history) — used to re-seed a decoder mid-stream (e.g. the transient
// ffmpeg that burns a "send message" overlay for one viewer). Only the capture is
// under the lock; it copies out with the lock released, the pin making that safe.
func (h *Hub) Snapshot(prebufMS int64) []byte {
	h.mu.Lock()
	head, parts := h.join.SnapshotPin(nil, prebufMS)
	h.mu.Unlock()
	return h.copyPinned(head, parts)
}

// copyPinned appends a pinned capture's GOP bytes to head and releases the pin.
// Runs with the lock RELEASED: the pin is what makes that safe.
func (h *Hub) copyPinned(head []byte, parts [][]byte) []byte {
	defer func() {
		h.mu.Lock()
		h.join.Unpin()
		h.mu.Unlock()
	}()
	for _, p := range parts {
		head = append(head, p...)
	}
	return head
}

// Configure live-reconfigures the TS prebuffer depth (ms) and the HLS segment
// view (target ms, window). The ring is the single cache; HLS is cut from it, so
// one call retunes both. A reduced depth frees retained history at once — this is
// how a panel-driven config change shrinks memory without recreating the stream.
func (h *Hub) Configure(prebufMS, hlsTargetMS int64, hlsWindow int) {
	h.mu.Lock()
	h.join.Configure(prebufMS, hlsTargetMS, hlsWindow)
	h.mu.Unlock()
}

// Flush drops everything the ring holds — every retained GOP, the HLS segment
// view and the cached playlist. Called when a stream's producer is stopped for
// good (the reaper's idle-stop): from that moment the retained bytes are not a
// buffer, they are a frozen picture of whenever the source was last alive, and
// they would sit resident for as long as the channel stays registered. At the
// defaults that is ~20 s of video per idle channel, which across a few hundred
// registered channels was the daemon's largest idle memory term.
func (h *Hub) Flush() {
	h.mu.Lock()
	h.join.Reset()
	h.mu.Unlock()
}

// SetGated collapses the ring to the idle fraction (true) or restores the full
// buffer (false) — the viewer gate. HLS keeps being cut from the ring either way.
func (h *Hub) SetGated(gated bool) {
	h.mu.Lock()
	h.join.SetGated(gated)
	h.mu.Unlock()
}

// SetIdleRatio sets the fraction of the buffer kept while gated (0 < r ≤ 1).
func (h *Hub) SetIdleRatio(r float64) {
	h.mu.Lock()
	h.join.SetIdleRatio(r)
	h.mu.Unlock()
}

// HLSPlaylist renders the HLS media playlist from the ring's segment view, or ""
// when no segments are ready yet. Serialised against the producer.
func (h *Hub) HLSPlaylist() string {
	h.mu.Lock()
	pl := h.join.HLSPlaylist()
	h.mu.Unlock()
	return pl
}

// HLSSegment assembles HLS segment seq from the ring, or nil if it is unknown or
// has aged out.
//
// Like Subscribe, only the CAPTURE is under the lock: the concatenation that
// follows is a multi-megabyte copy, and doing it under the hub lock stalled the
// stream's producer for its duration once per segment. The pin keeps prune from
// recycling those GOP buffers mid-copy.
func (h *Hub) HLSSegment(seq int) []byte {
	h.mu.Lock()
	head, parts, ok := h.join.HLSSegmentPin(nil, seq)
	h.mu.Unlock()
	if !ok {
		return nil
	}
	n := len(head)
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	out = append(out, head...)
	return h.copyPinned(out, parts)
}

// Counters reports the join state's health counters (audio packets, video access
// units, and whether the source declares audio at all), for the supervisor's
// stalled/audio-loss/frame-rate checks. Serialised against the producer.
func (h *Hub) Counters() (audioPkts, videoFrames int64, hasAudio bool) {
	h.mu.Lock()
	a, v, ok := h.join.Counters()
	h.mu.Unlock()
	return a, v, ok
}

// NoKeyframeCuts reports how many ring blocks were closed because the source
// produced no random-access point within the GOP cap. Non-zero means this source
// carries no random_access_indicator — which is also why it yields no HLS
// segments. Surfaced in the debug per-stream snapshot.
func (h *Hub) NoKeyframeCuts() int64 {
	h.mu.Lock()
	n := h.join.NoKeyframeCuts()
	h.mu.Unlock()
	return n
}

// CloseAll marks the hub torn down and wakes every viewer parked at the live edge
// so its next Follow returns ended and its serveLive unwinds. Called when a stream
// is removed from the registry (DELETE /streams|/ingest): without it the viewers
// following it at that moment block forever on a hub that will never publish again
// — their handler goroutines, their connStat entries and the whole Stream (hub,
// ring and all) stay alive until each client happens to disconnect, while the
// stream is already out of the registry and so invisible to /connections. Waking
// them lets each serveLive return and run its deferred detach/removeConn.
// Idempotent: the wake channel is closed for good here and never re-armed, and
// closed short-circuits Follow, Publish and signalWake from now on.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		close(h.wake)
	}
	h.mu.Unlock()
}

// RingStats reports the join ring's bytes, span (ms) and GOP count.
func (h *Hub) RingStats() (bytes int, spanMS int64, gops int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.join.RingStats()
}
