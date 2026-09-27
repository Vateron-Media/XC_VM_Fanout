package clusteragent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The R2 streams section (ADR 0004, "The R2 streams section on MAIN (Phase
// 7, ninth increment)" and "… on the node, and the catalogue sections
// (twelfth increment)"): one `stream` record per stream the node holds,
// asked for through the `streams` op on the bulk lane, never sent whole.
//
//	replica/streams.json          {"since": cursor}; 0 until a full pass completed
//	replica/streams/<id>.rep      the sealed record as received
//	replica/streams/<id>.json     {"etag", "ver", "data" exactly as signed}
//
// The directory is 0700 and every file 0600 (stream sources may carry an
// upstream's credentials), each written to a dot-named temporary file and
// renamed, the .rep before its .json; a removal deletes the .json, then the
// .rep. state.json keeps the cursor (streams_since), a full pass in
// progress (streams_pass) and the last resync's time (streams_resync_at).
//
//   - Delta: {since}, after every config sync and at once while `more`.
//   - Full pass, on `full` or a cursor of 0: the range walk from 0, no
//     deltas meanwhile; the cursor becomes the head of the pass's first
//     reply once the walk ends with nothing withheld.
//   - Resync: the same walk every StreamsResyncEvery (±10 %), the cursor
//     unchanged.
//   - The walk sends, per call, the ETags of the held streams with an id of
//     at least `from`, at most MaxStreamHashes, taken from the files as
//     stored at that moment.
//
// One record of a reply that does not verify rejects the whole reply. The
// section follows the STREAMS flow (8): while it is off the cursor is 0 and
// every file is kept. No record, its data or a diff of it is ever logged:
// only stream ids.

const (
	// FeatureStreams, said at hello, has MAIN serve the streams op.
	FeatureStreams = "streams"
	// MaxStreamID is the top of the id range a walk covers.
	MaxStreamID = 2147483647
)

var (
	// MaxStreamHashes is the most ETags one resync call names.
	MaxStreamHashes = 2000
	// StreamsResyncEvery is how often the section hashes are walked.
	StreamsResyncEvery = 5 * time.Minute
	// StreamsBusyRetries is how often one streams request is sent again
	// while MAIN answers that it is busy, before the sync waits for the
	// next poll.
	StreamsBusyRetries = 20
)

// StreamPass is a full pass in progress: the head of its first reply (0
// until one came), where the walk goes on from, and whether a reply
// withheld records.
type StreamPass struct {
	Head     int64 `json:"head"`
	From     int64 `json:"from"`
	Withheld bool  `json:"withheld,omitempty"`
}

type streamEntry struct {
	ID     int64  `json:"id"`
	Ver    int64  `json:"ver"`
	Etag   string `json:"etag"`
	Sealed string `json:"sealed"`
}

type streamsReply struct {
	Ver      int64         `json:"ver"`
	Head     int64         `json:"head"`
	More     bool          `json:"more"`
	Next     *int64        `json:"next"`
	Streams  []streamEntry `json:"streams"`
	Removed  []int64       `json:"removed"`
	Full     bool          `json:"full"`
	Withheld int           `json:"withheld"`
}

type streamResync struct {
	From   int64             `json:"from"`
	To     int64             `json:"to"`
	Hashes map[string]string `json:"hashes"`
}

type streamsRequest struct {
	Since  int64         `json:"since"`
	Resync *streamResync `json:"resync,omitempty"`
}

// checkedStream is a record of a reply that opened and verified.
type checkedStream struct {
	id     int64
	sealed []byte
	json   []byte
}

func streamsDir(dir string) string { return filepath.Join(dir, "streams") }

// streamsSince reads replica/streams.json's cursor; -1 when there is none.
func streamsSince(dir string) int64 {
	b, err := os.ReadFile(filepath.Join(dir, "streams.json"))
	if err != nil {
		return -1
	}
	var doc struct {
		Since *int64 `json:"since"`
	}
	if json.Unmarshal(b, &doc) != nil || doc.Since == nil || *doc.Since < 0 {
		return -1
	}
	return *doc.Since
}

// streamsCursor is the cursor held: state.json's, unless streams.json says
// 0 or is missing (the flow went off, or no pass completed).
func streamsCursor(dir string, st ReplicaState) int64 {
	if s := streamsSince(dir); s <= 0 || st.StreamsSince <= 0 {
		return 0
	}
	return st.StreamsSince
}

// writeStreamsSince writes streams.json. A cursor above 0 is written only
// while the STREAMS flow is on (publish zeroes it when the flow goes off),
// and only once streams/ exists.
func (a *Agent) writeStreamsSince(dir string, since int64) error {
	a.streamsFileMu.Lock()
	defer a.streamsFileMu.Unlock()
	if a.flows.Load()&FlowStreams == 0 {
		since = 0
	}
	if since > 0 {
		if err := os.MkdirAll(streamsDir(dir), 0o700); err != nil {
			return err
		}
	}
	return writeFileMode(filepath.Join(dir, "streams.json"), []byte(`{"since":`+strconv.FormatInt(since, 10)+`}`), 0o600)
}

// streamsFlow notes the STREAMS bit of the flows about to be written to
// flows.json: when it is off (first seen, or turned off), streams.json is
// rewritten with a cursor of 0 before flows.json, so PHP never takes an
// aged section. The replica loop then zeroes state.json's cursor too.
func (a *Agent) streamsFlow(flows int) {
	if a.ReplicaDir == "" {
		return
	}
	on := flows&FlowStreams != 0
	seen := int32(1)
	if on {
		seen = 2
	}
	if old := a.streamsSeen.Swap(seen); old == seen || on {
		return
	}
	if s := streamsSince(a.ReplicaDir); s > 0 {
		if err := a.writeStreamsSince(a.ReplicaDir, 0); err != nil {
			a.logf("cluster: replica: streams: %v", err)
		}
	}
}

// SyncStreams brings the streams section up to date: a delta, a full pass
// when one is due or in progress, and the resync when its time has come.
// With the STREAMS flow off it only zeroes the cursor.
func (a *Agent) SyncStreams(ctx context.Context) error {
	dir := a.ReplicaDir
	if dir == "" {
		return nil
	}
	a.replicaMu.Lock()
	defer a.replicaMu.Unlock()
	st := LoadReplicaState(dir)
	if a.flows.Load()&FlowStreams == 0 {
		if st.StreamsSince != 0 || st.StreamsPass != nil {
			st.StreamsSince, st.StreamsPass = 0, nil
			return a.saveReplicaState(dir, st)
		}
		return nil
	}
	s := &streamSync{a: a, dir: dir, st: st, cursor: streamsCursor(dir, st)}
	defer func() {
		if s.changed {
			a.runApply(ctx)
		}
	}()
	if s.st.StreamsPass == nil && s.cursor > 0 {
		full, err := s.deltas(ctx)
		if err != nil {
			return err
		}
		if full {
			s.st.StreamsPass = &StreamPass{}
			if err := s.save(); err != nil {
				return err
			}
		}
	}
	if s.st.StreamsPass != nil || s.cursor == 0 {
		return s.fullPass(ctx)
	}
	if a.streamsResyncNow || time.Now().Unix() >= s.st.StreamsResyncAt+int64(a.resyncEvery()/time.Second) {
		return s.resync(ctx)
	}
	return nil
}

// resyncEvery is this agent's resync interval, StreamsResyncEvery ±10 %.
func (a *Agent) resyncEvery() time.Duration {
	if a.streamsEvery == 0 {
		a.streamsEvery = jitter(StreamsResyncEvery)
	}
	return a.streamsEvery
}

// streamSync is one SyncStreams run, replicaMu held.
type streamSync struct {
	a       *Agent
	dir     string
	st      ReplicaState
	cursor  int64
	changed bool
}

func (s *streamSync) save() error {
	s.st.StreamsSince = s.cursor
	return s.a.saveReplicaState(s.dir, s.st)
}

// deltas asks for what changed past the cursor while MAIN has more; it
// reports whether MAIN answered `full`.
func (s *streamSync) deltas(ctx context.Context) (bool, error) {
	for round := 0; round < 1000; round++ {
		r, err := s.a.streamsCall(ctx, streamsRequest{Since: s.cursor})
		if err != nil {
			return false, err
		}
		if r.Full {
			return true, nil
		}
		if err := s.apply(r); err != nil {
			return false, err
		}
		if r.Ver != s.cursor {
			s.cursor = r.Ver
			if err := s.save(); err != nil {
				return false, err
			}
			if err := s.a.writeStreamsSince(s.dir, s.cursor); err != nil {
				return false, err
			}
		}
		if !r.More {
			return false, nil
		}
	}
	return false, nil
}

// fullPass walks every stream from where the pass stands; the cursor
// becomes the head of its first reply once it ends with nothing withheld.
func (s *streamSync) fullPass(ctx context.Context) error {
	if s.st.StreamsPass == nil {
		s.st.StreamsPass = &StreamPass{}
	}
	pass := s.st.StreamsPass
	err := s.walk(ctx, pass.From, func(r *streamsReply, next int64) error {
		if pass.Head == 0 {
			pass.Head = max(r.Head, 1)
		}
		pass.From = next
		pass.Withheld = pass.Withheld || r.Withheld > 0
		return s.save()
	})
	if err != nil {
		return err
	}
	if !pass.Withheld {
		s.cursor = pass.Head
	}
	s.st.StreamsPass = nil
	s.st.StreamsResyncAt = time.Now().Unix()
	s.a.streamsResyncNow = false
	if err := s.save(); err != nil {
		return err
	}
	if !pass.Withheld {
		return s.a.writeStreamsSince(s.dir, s.cursor)
	}
	return nil
}

// resync walks the section hashes with the cursor unchanged.
func (s *streamSync) resync(ctx context.Context) error {
	if err := s.walk(ctx, 0, func(*streamsReply, int64) error { return nil }); err != nil {
		return err
	}
	s.st.StreamsResyncAt = time.Now().Unix()
	s.a.streamsResyncNow = false
	return s.save()
}

// walk is the range walk from `from`: each call names the ETags of the held
// streams from `from` on (at most MaxStreamHashes), `to` is the last of them
// when more remain, else MaxStreamID; the next call goes on from `next`,
// else `to + 1`. done is called after each reply is applied, with where the
// walk goes on from.
func (s *streamSync) walk(ctx context.Context, from int64, done func(r *streamsReply, next int64) error) error {
	for round := 0; round < 100000; round++ {
		hashes, to := s.a.streamHashes(s.dir, from)
		r, err := s.a.streamsCall(ctx, streamsRequest{Since: s.cursor, Resync: &streamResync{From: from, To: to, Hashes: hashes}})
		if err != nil {
			return err
		}
		if err := s.apply(r); err != nil {
			return err
		}
		next := to + 1
		if r.Next != nil {
			if *r.Next <= from || *r.Next > to+1 {
				return fmt.Errorf("clusteragent: streams: MAIN's next %d is outside %d..%d", *r.Next, from, to)
			}
			next = *r.Next
		}
		if err := done(r, next); err != nil {
			return err
		}
		if to == MaxStreamID && r.Next == nil {
			return nil
		}
		from = next
	}
	return errors.New("clusteragent: streams: the walk did not end")
}

// streamHashes is one walk call's hashes: the ETags of the held streams
// with an id of at least from, at most MaxStreamHashes, as stored now, and
// the call's `to`. A stream whose record did not verify is left out, so
// MAIN resends it.
func (a *Agent) streamHashes(dir string, from int64) (map[string]string, int64) {
	ids := heldStreams(dir)
	hashes := map[string]string{}
	to := int64(MaxStreamID)
	n := 0
	for i, id := range ids {
		if id < from {
			continue
		}
		if n == MaxStreamHashes {
			to = ids[i-1]
			break
		}
		n++
		if a.streamsBad[id] {
			continue
		}
		if etag := storedStreamEtag(dir, id); etag != "" {
			hashes[strconv.FormatInt(id, 10)] = etag
		}
	}
	return hashes, to
}

// heldStreams lists the ids of streams/<id>.json, ascending.
func heldStreams(dir string) []int64 {
	entries, _ := os.ReadDir(streamsDir(dir))
	var ids []int64
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok || strings.HasPrefix(name, ".") {
			continue
		}
		if id, err := strconv.ParseInt(name, 10, 64); err == nil && id >= 1 && strconv.FormatInt(id, 10) == name {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// storedStreamEtag is the etag field of streams/<id>.json as stored (never
// recomputed from its data), or "".
func storedStreamEtag(dir string, id int64) string {
	b, err := os.ReadFile(filepath.Join(streamsDir(dir), strconv.FormatInt(id, 10)+".json"))
	if err != nil {
		return ""
	}
	var doc struct {
		Etag string `json:"etag"`
	}
	if json.Unmarshal(b, &doc) != nil || !etagRe.MatchString(doc.Etag) {
		return ""
	}
	return doc.Etag
}

// apply stores a reply's records and removals, or nothing of it when one of
// its records does not verify.
func (s *streamSync) apply(r *streamsReply) error {
	var ok []checkedStream
	for _, e := range r.Streams {
		c, err := s.a.checkStream(e)
		if err != nil {
			return err
		}
		ok = append(ok, c)
	}
	for _, id := range r.Removed {
		if id < 1 {
			return fmt.Errorf("clusteragent: streams: a removal of stream %d", id)
		}
	}
	if len(ok) == 0 && len(r.Removed) == 0 {
		return nil
	}
	sd := streamsDir(s.dir)
	if err := os.MkdirAll(sd, 0o700); err != nil {
		return err
	}
	for _, c := range ok {
		name := filepath.Join(sd, strconv.FormatInt(c.id, 10))
		if err := writeFileMode(name+".rep", c.sealed, 0o600); err != nil {
			return fmt.Errorf("clusteragent: streams: writing stream %d", c.id)
		}
		if err := writeFileMode(name+".json", c.json, 0o600); err != nil {
			return fmt.Errorf("clusteragent: streams: writing stream %d", c.id)
		}
		delete(s.a.streamsBad, c.id)
		s.changed = true
	}
	for _, id := range r.Removed {
		name := filepath.Join(sd, strconv.FormatInt(id, 10))
		jsonErr := os.Remove(name + ".json")
		repErr := os.Remove(name + ".rep")
		if jsonErr == nil || repErr == nil {
			s.changed = true
		}
		delete(s.a.streamsBad, id)
	}
	return nil
}

type streamDoc struct {
	Section  string          `json:"section"`
	Node     string          `json:"node"`
	Gen      *int64          `json:"gen"`
	StreamID int64           `json:"stream_id"`
	Ver      *int64          `json:"ver"`
	Etag     string          `json:"etag"`
	Data     json.RawMessage `json:"data"`
}

// checkStream opens and verifies one ENTRY. Errors name the stream only.
func (a *Agent) checkStream(e streamEntry) (checkedStream, error) {
	bad := fmt.Errorf("clusteragent: streams: the record of stream %d does not verify", e.ID)
	sealed, err := base64.StdEncoding.DecodeString(e.Sealed)
	if err != nil || e.ID < 1 || !etagRe.MatchString(e.Etag) {
		return checkedStream{}, bad
	}
	doc, err := a.openStream(sealed, e.ID)
	if err != nil || doc.Etag != e.Etag {
		return checkedStream{}, bad
	}
	if tok, ok := a.Client.Current(); ok && doc.Gen != nil && *doc.Gen != tok.Gen {
		return checkedStream{}, bad
	}
	out := append([]byte(`{"etag":"`+doc.Etag+`","ver":`+strconv.FormatInt(*doc.Ver, 10)+`,"data":`), doc.Data...)
	return checkedStream{id: e.ID, sealed: sealed, json: append(out, '}')}, nil
}

// openStream opens a stream record and checks that it is this node's
// record of stream id.
func (a *Agent) openStream(sealed []byte, id int64) (*streamDoc, error) {
	payload, err := a.Client.OpenRecord(sealed, "rep")
	if err != nil {
		return nil, err
	}
	var doc streamDoc
	if err := json.Unmarshal(payload, &doc); err != nil || doc.Section != "stream" || doc.Node != a.Client.State.NodeUUID ||
		doc.StreamID != id || doc.Ver == nil || *doc.Ver < 0 || !etagRe.MatchString(doc.Etag) || len(doc.Data) == 0 || doc.Data[0] != '{' {
		return nil, errors.New("not this node's record of the stream")
	}
	return &doc, nil
}

// recheckStreams verifies every stored stream record with the node's keys;
// the streams whose record fails are left out of the next resync's hashes
// (streamsBad), and that resync runs at the next sync. replicaMu is held.
func (a *Agent) recheckStreams(dir string) {
	var bad []int64
	for _, id := range heldStreams(dir) {
		name := filepath.Join(streamsDir(dir), strconv.FormatInt(id, 10))
		sealed, err := os.ReadFile(name + ".rep")
		if err == nil {
			var doc *streamDoc
			doc, err = a.openStream(sealed, id)
			if err == nil && storedStreamEtag(dir, id) != doc.Etag {
				err = errors.New("etag")
			}
		}
		if err != nil {
			bad = append(bad, id)
		}
	}
	a.streamsBad = map[int64]bool{}
	for _, id := range bad {
		a.streamsBad[id] = true
	}
	if len(bad) > 0 {
		a.streamsResyncNow = true
		a.logf("cluster: replica: %d stored stream record(s) no longer verify (streams %s); asking MAIN again", len(bad), joinIDs(bad, 20))
	}
}

func joinIDs(ids []int64, most int) string {
	var parts []string
	for i, id := range ids {
		if i == most {
			parts = append(parts, "…")
			break
		}
		parts = append(parts, strconv.FormatInt(id, 10))
	}
	return strings.Join(parts, ", ")
}

// streamsCall sends one streams request. MAIN busy (503 RATE_LIMITED, with
// or without a lane) is waited out and the same request sent again; a
// 409 FLOW_OFF naming the missing feature has the agent say hello again.
func (a *Agent) streamsCall(ctx context.Context, req streamsRequest) (*streamsReply, error) {
	for busy := 0; ; busy++ {
		var r streamsReply
		err := a.Client.Call(ctx, "streams", req, &r, false)
		if err == nil {
			return &r, nil
		}
		var d *Denial
		if errors.As(err, &d) && d.Status == 503 && d.Reason == "RATE_LIMITED" && busy < StreamsBusyRetries {
			if laneRefusal(err) != nil {
				a.busyRefusals.Add(1)
			}
			w, _ := busyWait(err)
			if !sleep(ctx, w) {
				return nil, err
			}
			continue
		}
		if errors.As(err, &d) && d.Status == 409 && d.Reason == "FLOW_OFF" {
			var doc struct {
				Feature string `json:"feature"`
			}
			if json.Unmarshal(d.Doc, &doc) == nil && doc.Feature == FeatureStreams {
				a.helloLater(ctx, "MAIN has not recorded the streams feature")
			}
		}
		return nil, err
	}
}
