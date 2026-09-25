package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// GET /events (control socket) is this daemon's feed of monitor transitions,
// for the node's xc_agent (cluster API plan, Phase 5): a supervised stream's
// state as GET /monitor/<id> reports it, each time it changes, in order.
//
//	GET /events?boot=<boot>&since=<seq>&wait=<sec>
//	→ {"boot", "seq", "reset", "events": [{"seq", "type": "monitor", "stream", "state"}]}
//
// Events after `since` come back at once; with none, the request is held up
// to `wait` seconds (at most 25) for the next. A consumer that is new, behind
// the ring, or talking to another daemon life (`boot` differs) gets `reset`
// and every supervised stream's current state instead, then carries on from
// `seq`. Viewer open/close joins the feed with the connection registry
// (Phase 6).

// EventsRing is how many transitions the daemon keeps for consumers catching up.
var EventsRing = 4096

// EventsWatch is how often supervised streams are compared with what was last
// published.
var EventsWatch = 250 * time.Millisecond

// EventsMaxWait caps how long a request is held.
const EventsMaxWait = 25 * time.Second

type monitorEvent struct {
	Seq    uint64           `json:"seq"`
	Type   string           `json:"type"`
	Stream string           `json:"stream"`
	State  monitorStateView `json:"state"`
}

type eventLog struct {
	mu     sync.Mutex
	boot   string
	seq    uint64
	ring   []monitorEvent
	wake   chan struct{} // closed and replaced on every append
	last   map[string]string
	latest map[string]monitorStateView
}

func newEventLog() *eventLog {
	b := make([]byte, 8)
	rand.Read(b)
	return &eventLog{boot: hex.EncodeToString(b), wake: make(chan struct{}), last: map[string]string{}, latest: map[string]monitorStateView{}}
}

// fingerprint is what counts as a transition: everything but the counters
// that move on their own (uptime, the sampled bitrate).
func fingerprint(v monitorStateView) string {
	v.UptimeMS = 0
	if v.Meta != nil {
		m := *v.Meta
		m.BitrateKbps = 0
		v.Meta = &m
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// observe records the states seen now and publishes the ones that changed.
func (l *eventLog) observe(states map[string]monitorStateView) {
	l.mu.Lock()
	defer l.mu.Unlock()
	changed := false
	ids := make([]string, 0, len(states))
	for id := range states {
		ids = append(ids, id)
	}
	sort.Strings(ids) // one order for transitions seen in the same tick
	for _, id := range ids {
		v := states[id]
		fp := fingerprint(v)
		l.latest[id] = v
		if l.last[id] == fp {
			continue
		}
		l.last[id] = fp
		l.seq++
		l.ring = append(l.ring, monitorEvent{Seq: l.seq, Type: "monitor", Stream: id, State: v})
		changed = true
	}
	for id := range l.last {
		if _, ok := states[id]; !ok {
			delete(l.last, id) // no longer supervised: PHP's reconcile releases it
			delete(l.latest, id)
		}
	}
	if over := len(l.ring) - EventsRing; over > 0 {
		l.ring = append([]monitorEvent(nil), l.ring[over:]...)
	}
	if changed {
		close(l.wake)
		l.wake = make(chan struct{})
	}
}

type eventsReply struct {
	Boot   string         `json:"boot"`
	Seq    uint64         `json:"seq"`
	Reset  bool           `json:"reset"`
	Events []monitorEvent `json:"events"`
}

// since answers a consumer: what follows seq, a reset snapshot, or (nil, wake)
// when there is nothing yet.
func (l *eventLog) since(boot string, seq uint64) (*eventsReply, <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	oldest := l.seq + 1
	if len(l.ring) > 0 {
		oldest = l.ring[0].Seq
	}
	if boot != l.boot || seq > l.seq || seq+1 < oldest {
		out := &eventsReply{Boot: l.boot, Seq: l.seq, Reset: true, Events: []monitorEvent{}}
		for id, v := range l.latest {
			out.Events = append(out.Events, monitorEvent{Seq: l.seq, Type: "monitor", Stream: id, State: v})
		}
		return out, nil
	}
	if seq == l.seq {
		return nil, l.wake
	}
	out := &eventsReply{Boot: l.boot, Seq: l.seq, Events: []monitorEvent{}}
	for _, e := range l.ring {
		if e.Seq > seq {
			out.Events = append(out.Events, e)
		}
	}
	return out, nil
}

// StartEvents watches the supervised streams until ctx ends.
func (m *Manager) StartEvents(ctx context.Context) {
	go func() {
		t := time.NewTicker(EventsWatch)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			m.events.observe(m.supervisedStates())
		}
	}()
}

func (m *Manager) supervisedStates() map[string]monitorStateView {
	out := map[string]monitorStateView{}
	if m.sup == nil {
		return out
	}
	for _, id := range m.sup.IDs() {
		if st := m.sup.State(id); st.Supervised {
			out[id] = m.monitorState(id, st)
		}
	}
	return out
}

func (m *Manager) serveEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	seq, _ := strconv.ParseUint(q.Get("since"), 10, 64)
	wait, _ := strconv.Atoi(q.Get("wait"))
	deadline := time.NewTimer(min(EventsMaxWait, time.Duration(max(0, wait))*time.Second))
	defer deadline.Stop()
	for {
		out, wake := m.events.since(q.Get("boot"), seq)
		if out == nil {
			select {
			case <-wake:
				continue
			case <-deadline.C:
				m.events.mu.Lock()
				out = &eventsReply{Boot: m.events.boot, Seq: m.events.seq, Events: []monitorEvent{}}
				m.events.mu.Unlock()
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
		return
	}
}
