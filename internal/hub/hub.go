// Package hub fans one producer's MPEG-TS byte stream out to many subscribers.
//
// One Hub per live stream. The producer calls Publish; each subscriber gets a
// clean join snapshot (PAT/PMT + current GOP) followed by the live tail. A
// subscriber that cannot keep up (its buffer fills) is dropped so it can never
// stall the producer or the other subscribers.
package hub

import (
	"sync"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsjoin"
)

// subQueue bounds how much a single subscriber may fall behind before it is
// dropped (number of pending chunks).
const subQueue = 256

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
// every subscriber. Slow subscribers are dropped rather than blocked. The chunk
// is copied, so the caller may reuse its buffer.
func (h *Hub) Publish(chunk []byte) {
	b := make([]byte, len(chunk))
	copy(b, chunk)

	h.mu.Lock()
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
// duplication.
func (h *Hub) Subscribe(prebufMS int64) (*Sub, []byte) {
	h.mu.Lock()
	snap := h.join.Snapshot(prebufMS)
	s := &Sub{ch: make(chan []byte, subQueue), done: make(chan struct{})}
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s, snap
}

// Snapshot returns a fresh clean-entry snapshot (PAT/PMT + keyframe, optionally
// prebufMS of history) without subscribing — used to re-seed a decoder mid-stream
// (e.g. the transient ffmpeg that burns a "send message" overlay for one viewer).
func (h *Hub) Snapshot(prebufMS int64) []byte {
	h.mu.Lock()
	snap := h.join.Snapshot(prebufMS)
	h.mu.Unlock()
	return snap
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

// Count returns the current number of subscribers.
func (h *Hub) Count() int {
	h.mu.Lock()
	n := len(h.subs)
	h.mu.Unlock()
	return n
}
