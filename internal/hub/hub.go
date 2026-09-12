// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

// Package hub fans one producer's MPEG-TS byte stream out to many subscribers.
//
// One Hub per live stream. The producer calls Publish; each subscriber gets a
// clean join snapshot (PAT/PMT + current GOP) followed by the live tail. A
// subscriber that cannot keep up (its buffer fills) is dropped so it can never
// stall the producer or the other subscribers.
package hub

import (
	"sync"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// subQueue bounds how much a single subscriber may fall behind before it is
// dropped (number of pending chunks).
const subQueue = defaults.SubscriberQueue

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

// Sub is a single subscriber's delivery channel.
type Sub struct {
	ch   chan []byte
	done chan struct{}
	once sync.Once
}

// C is the stream of chunks to write to the client.
func (s *Sub) C() <-chan []byte { return s.ch }

// Done is closed when the subscriber is dropped or unsubscribed.
func (s *Sub) Done() <-chan struct{} { return s.done }

func (s *Sub) close() { s.once.Do(func() { close(s.done) }) }

// Hub is the per-stream fan-out point.
type Hub struct {
	mu   sync.Mutex
	subs map[*Sub]struct{}
	join *tsjoin.State
}

// New returns a Hub. maxGOP caps a single GOP (bytes); maxPrebufMS is how many
// milliseconds of keyframe-aligned history the join state keeps for client
// prebuffer (0 = current GOP only).
func New(maxGOP int, maxPrebufMS int64) *Hub {
	return &Hub{subs: make(map[*Sub]struct{}), join: tsjoin.New(maxGOP, maxPrebufMS)}
}

// Publish folds a packet-aligned chunk into the join state and broadcasts it to
// every subscriber. Slow subscribers are dropped rather than blocked.
//
// The per-chunk copy is made ONLY when there are subscribers: a subscriber holds
// the buffer in its channel across later Publish calls, so it must get a stable
// copy the caller cannot overwrite. With no subscribers (a fed-but-unwatched
// stream — the common idle case) the chunk is folded in place: join.Update copies
// what it retains into the ring's own (recycled) GOP buffers and keeps no
// reference to the input, and Update runs to completion before Publish returns, so
// the caller may reuse its buffer either way. Skipping the copy here removes the
// dominant per-chunk allocation on idle streams (it was the main remaining GC
// churn once GOP buffers were recycled).
func (h *Hub) Publish(chunk []byte) {
	h.mu.Lock()
	if len(h.subs) == 0 {
		h.join.Update(chunk)
		h.mu.Unlock()
		return
	}

	b := make([]byte, len(chunk))
	copy(b, chunk)
	h.join.Update(b)
	for s := range h.subs {
		select {
		case s.ch <- b:
		default:
			// Subscriber is too slow — drop it.
			delete(h.subs, s)
			s.close()
		}
	}
	h.mu.Unlock()
}

// Subscribe registers a new subscriber and returns it together with the join
// burst the caller must write before draining Sub.C(), and then Release. prebufMS
// is how many milliseconds of prebuffer the subscriber wants (0 = current GOP
// only). Registering the subscriber and capturing the burst happen under the same
// lock, so the live tail continues exactly where the burst ends — no gap, no
// duplication.
//
// Only the capture is under the lock, and nothing is copied at all: the burst's
// GOP buffers are pinned, which is what lets the caller write them — for as long
// as a slow viewer takes — with the lock released and the producer running.
// Everything published from the moment of registration reaches this subscriber
// through its channel instead.
func (h *Hub) Subscribe(prebufMS int64) (*Sub, *Burst) {
	h.mu.Lock()
	head, parts := h.join.SnapshotPin(nil, prebufMS)
	s := &Sub{ch: make(chan []byte, subQueue), done: make(chan struct{})}
	h.subs[s] = struct{}{}
	h.mu.Unlock()

	return s, &Burst{Head: head, Parts: parts, h: h}
}

// Snapshot returns a fresh clean-entry snapshot (PAT/PMT + keyframe, optionally
// prebufMS of history) without subscribing — used to re-seed a decoder mid-stream
// (e.g. the transient ffmpeg that burns a "send message" overlay for one viewer).
// Like Subscribe, it copies with the lock released.
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

// Unsubscribe removes a subscriber (idempotent).
func (h *Hub) Unsubscribe(s *Sub) {
	h.mu.Lock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		s.close()
	}
	h.mu.Unlock()
}

// CloseAll drops every subscriber, as Unsubscribe does for one. Called when a
// stream is torn down (DELETE /streams|/ingest): without it the viewers attached
// at that moment keep blocking on a hub that will never publish again — their
// handler goroutines, their connStat entries and the whole Stream (hub, ring and
// all) stay alive until each client happens to disconnect, while the stream is
// already out of the registry and so invisible to /connections. Closing them lets
// serveLive return and run its deferred detach/removeConn.
func (h *Hub) CloseAll() int {
	h.mu.Lock()
	n := len(h.subs)
	for s := range h.subs {
		delete(h.subs, s)
		s.close()
	}
	h.mu.Unlock()
	return n
}

// Count returns the current number of subscribers.
func (h *Hub) Count() int {
	h.mu.Lock()
	n := len(h.subs)
	h.mu.Unlock()
	return n
}
