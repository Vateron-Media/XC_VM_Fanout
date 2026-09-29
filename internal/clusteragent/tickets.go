package clusteragent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// The data plane's tickets (ADR 0004, Phase 8): what the loopback relay
// proxy (relayproxy.go) signs each upstream connect with. MAIN mints them
// for a node whose DATAPLANE flow is on and sends them two ways:
//
//   - in the R2 stream record's `tickets` slot, {"files": {ref: wire} | null,
//     "relay": wire | null} | null — a record stored replaces its stream's
//     tickets, a removal drops them;
//   - on the streams op's delta path, once per epoch (TicketEpoch): the
//     delta asks {"tickets": {"epoch": held, "from": id}} and MAIN answers
//     {"tickets": {"epoch", "streams": {"<id>": TICKETS}, "next", "withheld"}}.
//
// The record's ETag is taken with the slot empty and no ticket bumps a
// version, so a refresh changes no file PHP compares. The tickets the proxy
// uses live apart, in replica/tickets.json (0600), refreshed on the delta
// path. A record's own copy stays in its `data` as MAIN signed it, so
// streams/<id>.json (0600) holds the tickets that came with the record,
// never refreshed there and read by nothing: its ETag, which PHP and the
// resync compare, is taken without them.
// Each ticket is verified before it is kept: the panel's signature, its
// lifetime, and that it names this node (child or fetcher) at the token's
// generation. A ticket is never logged.

// FlowDataplane is the DATAPLANE flow bit (MAIN's NodeRegistry::FLOW_DATAPLANE).
const FlowDataplane = 128

// TicketEpoch is how often MAIN mints a node's tickets anew (the panel's
// DataPlane::EPOCH).
const TicketEpoch = 10800

// streamTickets are one stream's tickets.
type streamTickets struct {
	Relay string            `json:"relay,omitempty"`
	Files map[string]string `json:"files,omitempty"`
}

// ticketPending is a refresh MAIN answered in pages: the lowest epoch of the
// pages taken, and the stream id to go on from.
type ticketPending struct {
	Epoch int64 `json:"epoch"`
	From  int64 `json:"from"`
}

// ticketFile is replica/tickets.json.
type ticketFile struct {
	// Epoch whose tickets the node holds in full (0: none).
	Epoch   int64                    `json:"epoch"`
	Pending *ticketPending           `json:"pending,omitempty"`
	Streams map[string]streamTickets `json:"streams"`
}

// ticketStore holds the tickets in memory, with an index of the file refs.
type ticketStore struct {
	mu    sync.RWMutex
	path  string
	f     ticketFile
	files map[string]int64 // ref -> stream id
	// Following a file another process writes (TicketsFollowFile): what it
	// was when last read, and when it was last looked at.
	follow    bool
	modAt     time.Time
	size      int64
	checkedAt time.Time
}

// TicketsFollowEvery is how often a followed tickets.json is looked at.
var TicketsFollowEvery = time.Second

type ticketsAsk struct {
	Epoch int64 `json:"epoch"`
	From  int64 `json:"from"`
}

type ticketsReply struct {
	Epoch    int64                      `json:"epoch"`
	Streams  map[string]json.RawMessage `json:"streams"`
	Next     *int64                     `json:"next"`
	Withheld bool                       `json:"withheld"`
}

// tickets is the node's ticket store, loaded from replica/tickets.json on
// first use; nil without a replica.
func (a *Agent) tickets() *ticketStore {
	if a.ReplicaDir == "" {
		return nil
	}
	a.ticketsOnce.Do(func() {
		s := &ticketStore{path: filepath.Join(a.ReplicaDir, "tickets.json"), follow: a.TicketsFollowFile}
		s.load()
		a.ticketStore = s
	})
	if a.ticketStore.follow {
		a.ticketStore.refollow()
	}
	return a.ticketStore
}

// load reads tickets.json (mu held for writing, or not yet shared).
func (s *ticketStore) load() {
	s.f = ticketFile{}
	if fi, err := os.Stat(s.path); err == nil {
		s.modAt, s.size = fi.ModTime(), fi.Size()
		if b, err := os.ReadFile(s.path); err == nil {
			json.Unmarshal(b, &s.f)
		}
	} else {
		s.modAt, s.size = time.Time{}, -1
	}
	if s.f.Streams == nil {
		s.f.Streams = map[string]streamTickets{}
	}
	s.index()
}

// refollow reads a followed tickets.json again when it changed, looking at
// most every TicketsFollowEvery.
func (s *ticketStore) refollow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.checkedAt) < TicketsFollowEvery {
		return
	}
	s.checkedAt = time.Now()
	fi, err := os.Stat(s.path)
	if err == nil && fi.ModTime().Equal(s.modAt) && fi.Size() == s.size || err != nil && s.size == -1 {
		return
	}
	s.load()
}

// index rebuilds the ref index (mu held for writing, or not yet shared).
// Streams that read the same file each hold a ticket for its ref, refreshed
// at different times; the index names the one whose ticket expires last (on
// a tie, the lowest stream id), whatever order the map is walked in, so a
// read never picks a stale ticket while a fresher one is held.
func (s *ticketStore) index() {
	s.files = map[string]int64{}
	best := map[string]int64{} // ref -> exp of the indexed ticket
	for id, t := range s.f.Streams {
		n, _ := strconv.ParseInt(id, 10, 64)
		for ref, wire := range t.Files {
			exp := ticketExp(wire)
			held, ok := s.files[ref]
			if !ok || exp > best[ref] || exp == best[ref] && n < held {
				s.files[ref], best[ref] = n, exp
			}
		}
	}
}

// ticketExp is a stored ticket's `exp`, read without checking its signature
// (each was verified before it was kept, and is again before it is used);
// 0 when it does not read.
func ticketExp(wire string) int64 {
	doc, _, ok := cc.SplitSigned(wire, cc.TicketMaxWire)
	if !ok {
		return 0
	}
	var t struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(doc, &t) != nil {
		return 0
	}
	return t.Exp
}

// save writes tickets.json (mu held).
func (s *ticketStore) save() error {
	b, err := json.Marshal(s.f)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	return writeFileMode(s.path, b, 0o600)
}

// verified is the ticket if it verifies now for this node, and names it as
// the child (rly) or the fetcher (fil) at the token's generation.
func (a *Agent) verifiedTicket(tag, wire string) (*cc.Ticket, bool) {
	t, ok := cc.VerifyTicket(a.Client.State.PanelSignPub, tag, wire, a.Client.MainNowMs()/1000)
	if !ok {
		return nil, false
	}
	who, gen := "child_sid", "child_gen"
	if tag == "fil" {
		who, gen = "fetcher_sid", "fetcher_gen"
	}
	sid, ok1 := t.Int(who)
	g, ok2 := t.Int(gen)
	if !ok1 || !ok2 || sid != a.Client.State.ServerID {
		return nil, false
	}
	if tok, has := a.Client.Current(); has && tok.Gen != 0 && int64(tok.Gen) != g {
		return nil, false
	}
	if self := a.selfGen.Load(); self != 0 && self != g {
		return nil, false // MAIN's: the generation main.json names
	}
	return t, true
}

// parseTickets checks one stream's TICKETS as MAIN sent them and keeps what
// verifies; ok is false when the document is not TICKETS at all.
func (a *Agent) parseTickets(id int64, raw json.RawMessage) (streamTickets, bool) {
	var out streamTickets
	if len(raw) == 0 || string(raw) == "null" {
		return out, true
	}
	var doc struct {
		Relay *string           `json:"relay"`
		Files map[string]string `json:"files"`
	}
	if json.Unmarshal(raw, &doc) != nil {
		return out, false
	}
	if doc.Relay != nil {
		if t, ok := a.verifiedTicket("rly", *doc.Relay); ok {
			if s, _ := t.Int("stream_id"); s == id {
				out.Relay = *doc.Relay
			}
		}
	}
	for ref, wire := range doc.Files {
		if t, ok := a.verifiedTicket("fil", wire); ok {
			if r, _ := t.String("ref"); r == ref {
				if out.Files == nil {
					out.Files = map[string]string{}
				}
				out.Files[ref] = wire
			}
		}
	}
	return out, true
}

// setStreams replaces the tickets of the streams given (none: dropped).
func (s *ticketStore) setStreams(m map[int64]streamTickets) error {
	if s == nil || len(m) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range m {
		key := strconv.FormatInt(id, 10)
		if t.Relay == "" && len(t.Files) == 0 {
			delete(s.f.Streams, key)
		} else {
			s.f.Streams[key] = t
		}
	}
	s.index()
	return s.save()
}

// relay is the relay ticket held for a stream.
func (s *ticketStore) relay(id int64) string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.f.Streams[strconv.FormatInt(id, 10)].Relay
}

// file is the file ticket held for a ref.
func (s *ticketStore) file(ref string) string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.files[ref]
	if !ok {
		return ""
	}
	return s.f.Streams[strconv.FormatInt(id, 10)].Files[ref]
}

// held lists the streams with tickets, ascending (tests, and the report).
func (s *ticketStore) held() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ids []int64
	for id := range s.f.Streams {
		n, _ := strconv.ParseInt(id, 10, 64)
		ids = append(ids, n)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ticketsAsk is what a delta asks of the ticket refresh: nil while the
// DATAPLANE flow is off (the held epoch then goes back to 0, so the flow
// turned on again refreshes every stream) or while this epoch's are held.
func (a *Agent) ticketsAsk() *ticketsAsk {
	s := a.tickets()
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.flows.Load()&FlowDataplane == 0 {
		if s.f.Epoch != 0 || s.f.Pending != nil {
			s.f.Epoch, s.f.Pending = 0, nil
			s.save()
		}
		return nil
	}
	if s.f.Pending != nil {
		return &ticketsAsk{Epoch: s.f.Epoch, From: s.f.Pending.From}
	}
	if s.f.Epoch >= a.Client.MainNowMs()/1000/TicketEpoch {
		return nil
	}
	return &ticketsAsk{Epoch: s.f.Epoch}
}

// ticketsTake stores a refresh page and reports whether MAIN has more.
func (a *Agent) ticketsTake(r *ticketsReply) (bool, error) {
	s := a.tickets()
	if s == nil || r == nil {
		return false, nil
	}
	m := map[int64]streamTickets{}
	for key, raw := range r.Streams {
		id, err := strconv.ParseInt(key, 10, 64)
		if err != nil || id < 1 {
			continue
		}
		if t, ok := a.parseTickets(id, raw); ok {
			m[id] = t
		}
	}
	if err := s.setStreams(m); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	low := r.Epoch
	if s.f.Pending != nil && s.f.Pending.Epoch < low {
		low = s.f.Pending.Epoch
	}
	switch {
	case r.Next != nil && !r.Withheld:
		s.f.Pending = &ticketPending{Epoch: low, From: *r.Next}
	case r.Withheld:
		// Without a licence: kept as it was, asked again at the next poll.
		s.f.Pending = nil
	default:
		s.f.Pending = nil
		s.f.Epoch = low
	}
	return s.f.Pending != nil, s.save()
}
