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

// snapPool recycles the join-burst buffer a viewer receives on connect. That
// burst is a contiguous copy of up to the whole prebuffer ring (tens of MB at a
// deep prebuffer); allocating one per connect is the biggest transient on the hot
// path and, under many concurrent joins, drives the process heap high-water mark
// that the runtime then holds resident. Pooling caps the live buffers at the
// concurrent-join count and lets them be reused; sync.Pool still releases idle
// ones to GC, so nothing is pinned. Buffers are stored by pointer to avoid an
// allocation on every Put.
var snapPool = sync.Pool{New: func() any { b := make([]byte, 0); return &b }}

func snapGet() []byte { return (*snapPool.Get().(*[]byte))[:0] }

// ReleaseSnapshot returns a join-burst buffer (from Subscribe) to the pool once
// the caller has finished writing it. Safe to call once with any snapshot slice;
// a nil/empty-capacity slice is ignored.
func ReleaseSnapshot(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:0]
	snapPool.Put(&b)
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
// snapshot the caller must send before draining Sub.C(). prebufMS is how many
// milliseconds of prebuffer the subscriber wants (0 = current GOP only).
// Registering the subscriber and capturing the snapshot happen under the same
// lock, so the live tail continues exactly where the snapshot ends — no gap, no
// duplication. The returned snapshot is drawn from a pool; the caller SHOULD
// ReleaseSnapshot it once written so the buffer can be reused.
//
// Only the CAPTURE is under the lock. The copy that follows is the expensive
// part — up to the whole ring, ~5 ms for a 40 s / 14 MB prebuffer — and running
// it under the lock stalled the stream's producer for that long on every join,
// serialising a join storm (a channel going live, an EPG event) into a stall
// proportional to the number of joiners. The capture pins the GOP buffers so
// prune cannot recycle them mid-copy; everything published from the moment of
// registration reaches this subscriber through its channel instead, so releasing
// the lock early costs no atomicity.
func (h *Hub) Subscribe(prebufMS int64) (*Sub, []byte) {
	buf := snapGet()
	h.mu.Lock()
	head, parts := h.join.SnapshotPin(buf, prebufMS)
	s := &Sub{ch: make(chan []byte, subQueue), done: make(chan struct{})}
	h.subs[s] = struct{}{}
	h.mu.Unlock()

	return s, h.copyPinned(head, parts)
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
// has aged out. Serialised against the producer.
func (h *Hub) HLSSegment(seq int) []byte {
	h.mu.Lock()
	b := h.join.HLSSegment(seq)
	h.mu.Unlock()
	return b
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
