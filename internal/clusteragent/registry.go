package clusteragent

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The connection registry (plan, Phase 6): on a node whose CONNECTIONS flow
// is on, the node's PHP records its viewers here, through the local socket,
// instead of in MAIN's Redis or lines_live — the stream endpoints' hot path
// makes no WAN call. Each record is the connection as the endpoints build it
// (ConnectionTracker's Redis record shape). Every change is mirrored to MAIN
// as a P0 event, so MAIN's store, which the reaper, the limits and the admin
// read, stays current:
//
//	conn.upsert {record}   a new or changed connection (a change of
//	                       hls_last_read alone at most every TouchEvery)
//	conn.remove {uuid}     the node removed it
//
// A close decided on MAIN (a kick, a limit, MAIN's reaper) reaches the node
// as a `conn.close {uuid, remove}` command, applied here without an event:
// MAIN already did it.
//
//	PUT    /v1/conn/{uuid}         upsert, body: the record
//	GET    /v1/conn/{uuid}         the record, or 404
//	POST   /v1/conn/find           {match: {column: value}} → the first match, or 404
//	POST   /v1/conn/oldest         {user_id} → the line's oldest open connection, or 404
//	POST   /v1/conn/{uuid}/touch   {hls_last_read} → the record, or 404
//	POST   /v1/conn/{uuid}/close   {remove}: a close already made in MAIN's store; no event
//	POST   /v1/conn/seed           {records, reset}: load records with no event (cluster:seed-connections)
//	DELETE /v1/conn/{uuid}
//
// Every heartbeat carries the registry's Digest. When MAIN's store for the
// node drifts from it, MAIN asks for the whole registry (conn_snapshot).
//
// The HLS reaper (Reap): an HLS viewer has no worker to watch, only its
// playlist requests. One that has made none for HLSReapAfter is ended here
// (hls_end 1, a P0 conn.upsert), and MAIN closes it as it closes any ended
// viewer. The time is the node's own: when a request last changed the
// record's hls_last_read, so neither a clock step nor MAIN being out of reach
// ends anyone. MAIN leaves its own 30 s rule for such nodes (the agent says
// "hls_reaper" at hello) until the node falls silent for longer than
// cluster_orphan_conn_ttl_sec.

// TouchEvery is the most often a change of hls_last_read alone reaches MAIN.
// A panel that predates the agent's reaper closes an HLS viewer after 30 s
// without one, so it stays under that.
var TouchEvery = 10 * time.Second

// HLSReapAfter is how long an HLS viewer may go without a playlist request.
var HLSReapAfter = 30 * time.Second

// FlowConnections is the CONNECTIONS flow bit: the registry holds the node's viewers.
const FlowConnections = 64

var connUUID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type Registry struct {
	mu     sync.Mutex
	conns  map[string]map[string]any
	sentAt map[string]time.Time // last conn.upsert per uuid
	readAt map[string]time.Time // last change of hls_last_read per uuid (node clock)
	snap   string
	dirty  bool
	emit   func(events []map[string]any) error
	now    func() time.Time
	logf   func(string, ...any)
}

// NewRegistry loads the node's registry from its snapshot (if any). emit
// spools events for MAIN.
func NewRegistry(snap string, emit func([]map[string]any) error, logf func(string, ...any)) *Registry {
	r := &Registry{conns: map[string]map[string]any{}, sentAt: map[string]time.Time{}, readAt: map[string]time.Time{}, snap: snap, emit: emit, now: time.Now, logf: logf}
	if b, err := os.ReadFile(snap); err == nil {
		var saved map[string]map[string]any
		if json.Unmarshal(b, &saved) == nil {
			for k, v := range saved {
				if connUUID.MatchString(k) {
					r.conns[k] = v
					// A restart is not a read, but it gives every viewer a
					// full HLSReapAfter to show up again.
					r.readAt[k] = r.now()
				}
			}
		}
	}
	return r
}

// Put records a connection and mirrors it to MAIN. The event is spooled
// first: a failure leaves the registry as it was, and the node's PHP falls
// back to MAIN's store for this write.
func (r *Registry) Put(uuid string, rec map[string]any) error {
	rec["uuid"] = uuid
	r.mu.Lock()
	old := r.conns[uuid]
	send := old == nil || !sameBut(old, rec, "hls_last_read") || r.now().Sub(r.sentAt[uuid]) >= TouchEvery
	r.mu.Unlock()
	if send {
		if err := r.emit([]map[string]any{{"type": "conn.upsert", "d": map[string]any{"record": rec}}}); err != nil {
			return err
		}
	}
	r.mu.Lock()
	if old == nil || norm(old["hls_last_read"]) != norm(rec["hls_last_read"]) {
		r.readAt[uuid] = r.now()
	}
	r.conns[uuid] = rec
	r.dirty = true
	if send {
		r.sentAt[uuid] = r.now()
	}
	r.mu.Unlock()
	return nil
}

// Reap ends the open HLS viewers with no playlist request for after, and
// returns how many it ended. Each goes to MAIN as a P0 conn.upsert with
// hls_end 1 before the registry changes; one whose event cannot be spooled
// stays open and is tried again on the next pass.
func (r *Registry) Reap(after time.Duration) int {
	r.mu.Lock()
	now := r.now()
	var stale []map[string]any
	for uuid, c := range r.conns {
		if fmt.Sprint(c["container"]) != "hls" || num(c["hls_end"]) != 0 {
			continue
		}
		at, ok := r.readAt[uuid]
		if !ok {
			r.readAt[uuid] = now
			continue
		}
		if now.Sub(at) >= after {
			ended := clone(c)
			ended["hls_end"] = 1
			stale = append(stale, ended)
		}
	}
	r.mu.Unlock()
	n := 0
	for _, rec := range stale {
		uuid, _ := rec["uuid"].(string)
		if err := r.emit([]map[string]any{{"type": "conn.upsert", "d": map[string]any{"record": rec}}}); err != nil {
			r.logf("cluster: ending HLS viewer %s: %v", uuid, err)
			continue
		}
		r.mu.Lock()
		// Unless a request re-opened it meanwhile.
		if c := r.conns[uuid]; c != nil && num(c["hls_end"]) == 0 && norm(c["hls_last_read"]) == norm(rec["hls_last_read"]) {
			r.conns[uuid] = rec
			r.sentAt[uuid] = r.now()
			r.dirty = true
			n++
		}
		r.mu.Unlock()
	}
	return n
}

// Get returns a copy of a record.
func (r *Registry) Get(uuid string) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return clone(r.conns[uuid])
}

// Touch refreshes hls_last_read and returns the record.
func (r *Registry) Touch(uuid string, lastRead any) (map[string]any, error) {
	rec := r.Get(uuid)
	if rec == nil {
		return nil, nil
	}
	rec["hls_last_read"] = lastRead
	return rec, r.Put(uuid, rec)
}

// Find returns the first record matching every column (in uuid order, so it
// is stable).
func (r *Registry) Find(match map[string]any) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.conns))
	for k := range r.conns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ok := true
		for col, want := range match {
			if fmt.Sprint(r.conns[k][col]) != fmt.Sprint(want) {
				ok = false
				break
			}
		}
		if ok {
			return clone(r.conns[k])
		}
	}
	return nil
}

// Oldest returns a line's oldest open connection (by date_start).
func (r *Registry) Oldest(userID any) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best map[string]any
	for _, c := range r.conns {
		if fmt.Sprint(c["user_id"]) != fmt.Sprint(userID) || fmt.Sprint(c["hls_end"]) == "1" {
			continue
		}
		if best == nil || num(c["date_start"]) < num(best["date_start"]) {
			best = c
		}
	}
	return clone(best)
}

// Delete removes a connection and tells MAIN.
func (r *Registry) Delete(uuid string) error {
	if err := r.emit([]map[string]any{{"type": "conn.remove", "d": map[string]any{"uuid": uuid}}}); err != nil {
		return err
	}
	r.mu.Lock()
	if _, had := r.conns[uuid]; had {
		delete(r.conns, uuid)
		r.dirty = true
	}
	delete(r.sentAt, uuid)
	delete(r.readAt, uuid)
	r.mu.Unlock()
	return nil
}

// Close applies a close MAIN decided: removed, or ended (an HLS viewer's
// hls_end = 1, as MAIN's own store now says). No event goes back.
func (r *Registry) Close(uuid string, remove bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if remove {
		delete(r.conns, uuid)
		delete(r.sentAt, uuid)
		delete(r.readAt, uuid)
	} else if c := r.conns[uuid]; c != nil {
		c["hls_end"] = 1
	}
	r.dirty = true
}

// ConnDigest is a node's open connections in one short value; MAIN computes
// the same from its store (Domain\Cluster\ConnectionDigest).
type ConnDigest struct {
	Count int    `json:"count"`
	Users int    `json:"users"`
	Xor64 string `json:"xor64"`
}

// Digest summarises the open connections (hls_end not set): their number,
// their distinct owners, and the XOR of the first 8 bytes of
// SHA-256(uuid "\n" owner).
func (r *Registry) Digest() ConnDigest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return digestOf(r.conns)
}

func digestOf(conns map[string]map[string]any) ConnDigest {
	users := map[string]bool{}
	var x uint64
	n := 0
	for uuid, c := range conns {
		if num(c["hls_end"]) != 0 {
			continue
		}
		o := owner(c)
		n++
		users[o] = true
		h := sha256.Sum256([]byte(uuid + "\n" + o))
		x ^= binary.BigEndian.Uint64(h[:8])
	}
	return ConnDigest{Count: n, Users: len(users), Xor64: fmt.Sprintf("%016x", x)}
}

// owner is a connection's owner as PHP's ConnectionDigest::owner() writes it.
func owner(c map[string]any) string {
	if id := intOf(c["user_id"]); id != 0 {
		return "u:" + strconv.FormatInt(id, 10)
	}
	ident := ""
	if v, ok := c["hmac_identifier"]; ok && v != nil {
		ident = fmt.Sprint(v)
	}
	return "h:" + strconv.FormatInt(intOf(c["hmac_id"]), 10) + ":" + ident
}

// intOf reads a JSON number or numeric string as PHP's intval() would.
func intOf(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	case string:
		i, _ := strconv.ParseInt(strings.TrimSpace(n), 10, 64)
		return i
	case bool:
		if n {
			return 1
		}
	}
	return 0
}

// Records returns a copy of every record, in uuid order.
func (r *Registry) Records() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.conns))
	for k := range r.conns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, clone(r.conns[k]))
	}
	return out
}

// Seed loads records as they are, with no event: MAIN's store already holds
// them (cluster:seed-connections, before the CONNECTIONS switch). reset
// empties the registry first. Records without a valid uuid are skipped.
func (r *Registry) Seed(records []map[string]any, reset bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reset {
		r.conns = map[string]map[string]any{}
		r.sentAt = map[string]time.Time{}
		r.readAt = map[string]time.Time{}
	}
	n := 0
	for _, c := range records {
		uuid, _ := c["uuid"].(string)
		if c == nil || !connUUID.MatchString(uuid) {
			continue
		}
		r.conns[uuid] = c
		r.sentAt[uuid] = r.now()
		r.readAt[uuid] = r.now()
		n++
	}
	r.dirty = true
	return n
}

// Save writes the snapshot when something changed (written aside, renamed in).
func (r *Registry) Save() error {
	r.mu.Lock()
	if !r.dirty {
		r.mu.Unlock()
		return nil
	}
	b, err := json.Marshal(r.conns)
	r.dirty = false
	r.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := r.snap + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, r.snap)
}

func sameBut(a, b map[string]any, except string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if k == except {
			continue
		}
		w, ok := b[k]
		if !ok || !reflect.DeepEqual(norm(v), norm(w)) {
			return false
		}
	}
	return true
}

// norm compares JSON numbers and PHP's ints alike.
func norm(v any) any {
	switch n := v.(type) {
	case float64, int, int64, json.Number:
		return fmt.Sprint(n)
	}
	return v
}

func num(v any) float64 {
	var f float64
	fmt.Sscan(fmt.Sprint(v), &f)
	return f
}

func clone(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// spoolP0 writes events as one P0 spool file (as PHP's EventSpool does).
func spoolP0(dir string, events []map[string]any) error {
	var body []byte
	now := time.Now().UnixMilli()
	for _, e := range events {
		e["t"] = now
		line, err := json.Marshal(e)
		if err != nil {
			return err
		}
		body = append(append(body, line...), '\n')
	}
	p0 := filepath.Join(dir, "p0")
	if err := os.MkdirAll(p0, 0o750); err != nil {
		return err
	}
	name := fmt.Sprintf("%019d-agent-%04x.ndjson", monotonicNs(), rand.Intn(0x10000))
	tmp := filepath.Join(p0, "."+name+".tmp")
	if err := os.WriteFile(tmp, body, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(p0, name))
}

// connHandler serves /v1/conn on the local socket.
func (r *Registry) connHandler(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/v1/conn/")
	body := map[string]any{}
	if req.Method == http.MethodPut || req.Method == http.MethodPost {
		b, err := io.ReadAll(io.LimitReader(req.Body, MaxSocketBody+1))
		if err != nil || len(b) > MaxSocketBody || json.Unmarshal(b, &body) != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
	}
	reply := func(rec map[string]any, err error) {
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if rec == nil {
			http.NotFound(w, req)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rec)
	}
	switch {
	case req.Method == http.MethodPost && path == "find":
		match, _ := body["match"].(map[string]any)
		if len(match) == 0 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		reply(r.Find(match), nil)
	case req.Method == http.MethodPost && path == "oldest":
		reply(r.Oldest(body["user_id"]), nil)
	case req.Method == http.MethodPost && path == "seed":
		raw, _ := body["records"].([]any)
		records := make([]map[string]any, 0, len(raw))
		for _, v := range raw {
			if c, ok := v.(map[string]any); ok {
				records = append(records, c)
			}
		}
		reset, _ := body["reset"].(bool)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"seeded": r.Seed(records, reset)})
	case req.Method == http.MethodPost && strings.HasSuffix(path, "/close") && connUUID.MatchString(strings.TrimSuffix(path, "/close")):
		// A close the node's PHP made in MAIN's store itself (its reaper, its
		// own kick): the registry follows, with no event.
		remove, _ := body["remove"].(bool)
		r.Close(strings.TrimSuffix(path, "/close"), remove)
		w.WriteHeader(http.StatusNoContent)
	case req.Method == http.MethodPost && strings.HasSuffix(path, "/touch") && connUUID.MatchString(strings.TrimSuffix(path, "/touch")):
		reply(r.Touch(strings.TrimSuffix(path, "/touch"), body["hls_last_read"]))
	case !connUUID.MatchString(path):
		http.Error(w, "bad request", http.StatusBadRequest)
	case req.Method == http.MethodPut:
		err := r.Put(path, body)
		reply(body, err)
	case req.Method == http.MethodGet:
		reply(r.Get(path), nil)
	case req.Method == http.MethodDelete:
		if err := r.Delete(path); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "not allowed", http.StatusMethodNotAllowed)
	}
}
