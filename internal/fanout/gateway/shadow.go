package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The shadow comparison (ADR 0005, the plan's 12.1 rollout): in shadow,
// nginx mirrors each request to the gateway, which judges it, and PHP, which
// answers it, reports what it answered under the same nginx request id
// (Core/Gateway/GatewayShadow). The book matches the two, whichever arrives
// first, and counts each request the gateway would have answered (serve,
// deny, redirect) as agreeing with PHP or not; one it would have handed to
// PHP anyway is only counted (deferred). A node is ready to serve once it has
// compared for days with no disagreement (MAIN's Cluster Nodes page reads
// this through the node's audit report).

const (
	// shadowPairWindow is how long one side waits for the other.
	shadowPairWindow = 30 * time.Second
	// shadowSamples is how many disagreements are kept, the newest.
	shadowSamples = 10
	// shadowPendingMax bounds the requests waiting for their other side.
	shadowPendingMax = 50000
	// shadowSaveEvery throttles writing the state file.
	shadowSaveEvery = 10 * time.Second
)

// Disagreement is one request the gateway and PHP answered differently (no token in it).
type Disagreement struct {
	At      int64  `json:"at"`
	Kind    string `json:"kind"`
	Gateway string `json:"gateway"` // "<action> <reason>"
	PHP     string `json:"php"`
	Stream  int    `json:"stream"`
}

// ShadowState is the comparison so far, kept across restarts.
type ShadowState struct {
	Since        int64          `json:"since"` // the first comparison (unix seconds), 0 before it
	Agree        uint64         `json:"agree"`
	Disagree     uint64         `json:"disagree"`
	Deferred     uint64         `json:"deferred"`
	Unmatched    uint64         `json:"unmatched"` // one side never came
	LastDisagree int64          `json:"last_disagree"`
	Samples      []Disagreement `json:"samples"`
}

type shadowSide struct {
	at      time.Time
	kind    string
	verdict *Verdict
	php     string
}

// ShadowBook pairs the gateway's verdicts with PHP's answers.
type ShadowBook struct {
	mu        sync.Mutex
	pending   map[string]*shadowSide
	state     ShadowState
	path      string
	savedAt   time.Time
	expiredAt time.Time
	dirty     bool
	now       func() time.Time
}

// NewShadowBook keeps its state in path ("" keeps it in memory only),
// reading what an earlier run left there.
func NewShadowBook(path string) *ShadowBook {
	b := &ShadowBook{pending: map[string]*shadowSide{}, path: path, now: time.Now}
	if path != "" {
		if raw, err := os.ReadFile(path); err == nil {
			_ = json.Unmarshal(raw, &b.state)
		}
	}
	return b
}

// Gateway records the gateway's verdict on request id.
func (b *ShadowBook) Gateway(id, kind string, v Verdict) {
	if id == "" {
		return
	}
	b.side(id, func(s *shadowSide) { s.kind, s.verdict = kind, &v })
}

// PHP records what PHP answered request id: serve, deny, redirect, blocked, or status-<code>.
func (b *ShadowBook) PHP(id, kind, outcome string) {
	if id == "" || outcome == "" {
		return
	}
	b.side(id, func(s *shadowSide) {
		if s.kind == "" {
			s.kind = kind
		}
		s.php = outcome
	})
}

func (b *ShadowBook) side(id string, set func(*shadowSide)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.expireLocked(now)
	s := b.pending[id]
	if s == nil {
		if len(b.pending) >= shadowPendingMax {
			b.state.Unmatched++
			return
		}
		s = &shadowSide{at: now}
		b.pending[id] = s
	}
	set(s)
	if s.verdict == nil || s.php == "" {
		return
	}
	delete(b.pending, id)
	b.compareLocked(s, now)
	b.saveLocked(now)
}

// compareLocked is one request's comparison: the gateway's answer and PHP's.
func (b *ShadowBook) compareLocked(s *shadowSide, now time.Time) {
	if s.verdict.Action == PHP {
		b.state.Deferred++
		return
	}
	if b.state.Since == 0 {
		b.state.Since = now.Unix()
	}
	if string(s.verdict.Action) == s.php {
		b.state.Agree++
		return
	}
	b.state.Disagree++
	b.state.LastDisagree = now.Unix()
	b.state.Samples = append(b.state.Samples, Disagreement{At: now.Unix(), Kind: s.kind, Gateway: string(s.verdict.Action) + " " + s.verdict.Reason, PHP: s.php, Stream: s.verdict.Stream})
	if len(b.state.Samples) > shadowSamples {
		b.state.Samples = b.state.Samples[len(b.state.Samples)-shadowSamples:]
	}
	b.dirty = true
}

// expireLocked drops the requests whose other side did not come in time
// (looked at once a second: the map can hold a busy node's half-minute).
func (b *ShadowBook) expireLocked(now time.Time) {
	if now.Sub(b.expiredAt) < time.Second {
		return
	}
	b.expiredAt = now
	for id, s := range b.pending {
		if now.Sub(s.at) > shadowPairWindow {
			delete(b.pending, id)
			b.state.Unmatched++
		}
	}
}

// saveLocked writes the state, at most every shadowSaveEvery (at once after a disagreement).
func (b *ShadowBook) saveLocked(now time.Time) {
	if b.path == "" || (!b.dirty && now.Sub(b.savedAt) < shadowSaveEvery) {
		return
	}
	raw, err := json.Marshal(b.state)
	if err != nil {
		return
	}
	tmp := b.path + ".tmp"
	if os.MkdirAll(filepath.Dir(b.path), 0o755) == nil && os.WriteFile(tmp, raw, 0o644) == nil {
		_ = os.Rename(tmp, b.path)
	}
	b.savedAt, b.dirty = now, false
}

// State is a copy of the comparison so far.
func (b *ShadowBook) State() ShadowState {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.state
	st.Samples = append([]Disagreement{}, b.state.Samples...)
	return st
}
