package clusteragent

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cc "github.com/Vateron-Media/XC_VM_Fanout/internal/clustercrypto"
)

// streamsMain is MAIN's streams op in miniature (StreamReplica): the streams
// this node holds with their data, a version per stream the node holds or
// held, the counter's head and the node's floor, and the paging bounds.
type streamsMain struct {
	*fakeMain
	boxPub []byte

	mu       sync.Mutex
	data     map[int64]string // held streams: id -> data (JSON object)
	vers     map[int64]int64  // id -> version of its row (held or not)
	head     int64
	floor    int64
	maxRows  int            // a delta's rows per call
	maxRecs  int            // records per reply
	maxExam  int            // held streams a resync call examines
	withhold map[int64]bool // records MAIN cannot sign
	forge    map[int64]bool // records signed under another key
	refuse   []func(w http.ResponseWriter, nonce []byte)
	asked    []map[string]any // every streams request, opened
	// alter rewrites a stream's signed payload (another node, another
	// section), still sealed to this node and signed by the panel.
	alter map[int64]func(payload string) string
	// served runs after each streams reply is built, m.mu held.
	served  func()
	configs int // config calls
}

func newStreamsMain(t *testing.T) (*streamsMain, *Agent) {
	f, st := newFake(t)
	sk, pub, _ := cc.NewX25519()
	st.NodeBoxSk = sk
	m := &streamsMain{fakeMain: f, boxPub: pub, data: map[int64]string{}, vers: map[int64]int64{}, head: 1,
		maxRows: 1000, maxRecs: 200, maxExam: 1000, withhold: map[int64]bool{}, forge: map[int64]bool{}, alter: map[int64]func(string) string{}}
	f.answer = m.answer
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	st.MainURLs = []string{srv.URL + "/cluster/v1/"}
	st.path = filepath.Join(t.TempDir(), "agent.json")
	a := &Agent{Client: NewClient(st, "xc_agent/test"), Logf: t.Logf, ReplicaDir: t.TempDir()}
	a.flows.Store(FlowStreams)
	oldD := ApplyDebounce
	ApplyDebounce = 0
	t.Cleanup(func() { ApplyDebounce = oldD })
	return m, a
}

// set changes stream id (held, with this data), bumping its version.
func (m *streamsMain) set(id int64, data string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.head++
	m.data[id], m.vers[id] = data, m.head
}

// drop takes stream id off the node, bumping its version.
func (m *streamsMain) drop(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.head++
	delete(m.data, id)
	m.vers[id] = m.head
}

func etagFor(data string) string { h := sha256.Sum256([]byte(data)); return hex.EncodeToString(h[:]) }

func (m *streamsMain) entry(t testing.TB, id int64) map[string]any {
	data, ver := m.data[id], m.vers[id]
	etag := etagFor(data)
	payload := `{"v":1,"section":"stream","node":"` + m.uuid + `","gen":1,"stream_id":` + strconv.FormatInt(id, 10) + `,"ver":` + strconv.FormatInt(ver, 10) + `,"etag":"` + etag + `","iat":1,"data":` + data + `}`
	if f := m.alter[id]; f != nil {
		payload = f(payload)
	}
	key := m.panel
	if m.forge[id] {
		_, key, _ = ed25519.GenerateKey(nil)
	}
	body := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	body = append(append(body, payload...), ed25519.Sign(key, cc.PanelSigInput("rep", []byte(payload)))...)
	sealed, err := cc.Seal(m.boxPub, "replica", m.uuid, body)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"id": id, "ver": ver, "etag": etag, "sealed": base64.StdEncoding.EncodeToString(sealed)}
}

func (m *streamsMain) answer(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
	op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	body, _ := io.ReadAll(r.Body)
	plain, _ := cc.Unbox(m.keys.EncUp, reqCtx, body)
	var req map[string]any
	json.Unmarshal(plain, &req)
	if op == "config" {
		m.mu.Lock()
		m.configs++
		m.mu.Unlock()
		m.box(w, reqCtx, map[string]any{"blocklist": map[string]any{"seq": 0}})
		return
	}
	if op != "streams" {
		m.box(w, reqCtx, map[string]any{"state": "active", "mode": 1})
		return
	}
	m.mu.Lock()
	m.asked = append(m.asked, req)
	if len(m.refuse) > 0 {
		next := m.refuse[0]
		m.refuse = m.refuse[1:]
		m.mu.Unlock()
		next(w, nonce)
		return
	}
	out := m.serve(req)
	if m.served != nil {
		m.served()
	}
	m.mu.Unlock()
	m.box(w, reqCtx, out)
}

func (m *streamsMain) serve(req map[string]any) map[string]any {
	since := int64(req["since"].(float64))
	streams, removed := []any{}, []any{}
	withheld := 0
	rs, resync := req["resync"].(map[string]any)
	if !resync {
		if since <= 0 || since < m.floor || since > m.head {
			return map[string]any{"ver": since, "head": m.head, "more": false, "full": true, "streams": streams, "removed": removed}
		}
		var ids []int64
		for id, v := range m.vers {
			if v > since {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return m.vers[ids[i]] < m.vers[ids[j]] })
		more := len(ids) > m.maxRows
		if more {
			ids = ids[:m.maxRows]
		}
		ver := since
		for _, id := range ids {
			if _, held := m.data[id]; held {
				if m.withhold[id] {
					withheld++
					more = false
					break
				}
				if len(streams) == m.maxRecs {
					more = true
					break
				}
				streams = append(streams, m.entry(nil, id))
			} else {
				removed = append(removed, id)
			}
			ver = m.vers[id]
		}
		out := map[string]any{"ver": ver, "head": m.head, "more": more, "streams": streams, "removed": removed}
		if withheld > 0 {
			out["withheld"] = withheld
		}
		return out
	}
	from, to := int64(rs["from"].(float64)), int64(rs["to"].(float64))
	hashes, _ := rs["hashes"].(map[string]any)
	var held []int64
	for id := range m.data {
		if id >= from && id <= to {
			held = append(held, id)
		}
	}
	sort.Slice(held, func(i, j int) bool { return held[i] < held[j] })
	var next any
	bound := to
	if len(held) > m.maxExam {
		held = held[:m.maxExam]
		next = held[len(held)-1] + 1
		bound = held[len(held)-1]
	}
	ids := append([]int64{}, held...)
	for k := range hashes {
		id, _ := strconv.ParseInt(k, 10, 64)
		if _, ok := m.data[id]; !ok && id <= bound {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, id := range ids {
		key := strconv.FormatInt(id, 10)
		if _, ok := m.data[id]; !ok {
			removed = append(removed, id)
			continue
		}
		if hashes[key] == etagFor(m.data[id]) {
			continue
		}
		if m.withhold[id] {
			withheld++
			continue
		}
		if len(streams) == m.maxRecs {
			next = ids[i]
			break
		}
		streams = append(streams, m.entry(nil, id))
	}
	out := map[string]any{"ver": since, "head": m.head, "next": next, "streams": streams, "removed": removed}
	if withheld > 0 {
		out["withheld"] = withheld
	}
	return out
}

func (m *streamsMain) requests() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]map[string]any{}, m.asked...)
}

func streamFile(a *Agent, id int64, ext string) string {
	return filepath.Join(a.ReplicaDir, "streams", strconv.FormatInt(id, 10)+ext)
}

func storedIDs(a *Agent) string {
	var out []string
	for _, id := range heldStreams(a.ReplicaDir) {
		out = append(out, strconv.FormatInt(id, 10))
	}
	return strings.Join(out, ",")
}

const src = `{"stream":{"id":%d,"stream_source":"[\"http://user:S3cretUpstreamPass@origin/live/%d.ts\"]"},"tickets":null}`

func streamData(id int64, rev string) string {
	return strings.Replace(strings.Replace(src, "%d", strconv.FormatInt(id, 10), 2), "live/", "live/"+rev, 1)
}

func TestStreamsFullPassThenDeltas(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	for _, id := range []int64{1, 2, 3} {
		m.set(id, streamData(id, "a"))
	}
	// A new node: cursor 0, so a full pass, walking from 0 with no hashes.
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	req := m.requests()
	if len(req) != 1 || req[0]["since"] != float64(0) || req[0]["resync"].(map[string]any)["from"] != float64(0) ||
		req[0]["resync"].(map[string]any)["to"] != float64(MaxStreamID) || len(req[0]["resync"].(map[string]any)["hashes"].(map[string]any)) != 0 {
		t.Fatalf("first requests %v", req)
	}
	st := LoadReplicaState(a.ReplicaDir)
	if storedIDs(a) != "1,2,3" || st.StreamsSince != m.head || streamsSince(a.ReplicaDir) != m.head || st.StreamsPass != nil {
		t.Fatalf("after the pass: %s, state %+v, streams.json %d", storedIDs(a), st, streamsSince(a.ReplicaDir))
	}
	// A change and a removal reach it by a delta.
	m.set(2, streamData(2, "b"))
	m.drop(3)
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	req = m.requests()
	if last := req[len(req)-1]; last["since"] != float64(4) || last["resync"] != nil {
		t.Fatalf("delta request %v", last)
	}
	b, _ := os.ReadFile(streamFile(a, 2, ".json"))
	if storedIDs(a) != "1,2" || !strings.Contains(string(b), "live/b2.ts") || LoadReplicaState(a.ReplicaDir).StreamsSince != m.head {
		t.Fatalf("after the delta: %s, %s", storedIDs(a), b)
	}
	if _, err := os.Stat(streamFile(a, 3, ".rep")); !os.IsNotExist(err) {
		t.Fatal("a removed stream's record kept")
	}
	// Nothing changed: one delta, nothing stored.
	n := len(m.requests())
	if err := a.SyncStreams(ctx); err != nil || len(m.requests()) != n+1 {
		t.Fatalf("an idle sync asked %d times: %v", len(m.requests())-n, err)
	}
}

func TestStreamsDeltaAsksAgainWhileMore(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	m.set(1, streamData(1, "a"))
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	m.maxRows = 1
	for _, id := range []int64{2, 3, 4} {
		m.set(id, streamData(id, "a"))
	}
	n := len(m.requests())
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	if storedIDs(a) != "1,2,3,4" || len(m.requests())-n != 3 || LoadReplicaState(a.ReplicaDir).StreamsSince != m.head {
		t.Fatalf("%s after %d requests", storedIDs(a), len(m.requests())-n)
	}
}

func TestStreamsFullAndWithheld(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, m *streamsMain, a *Agent)
	}{
		{"a withheld record keeps a new node at 0, and the next poll walks again", func(t *testing.T, m *streamsMain, a *Agent) {
			m.set(1, streamData(1, "a"))
			m.set(2, streamData(2, "a"))
			m.withhold[2] = true
			if err := a.SyncStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			if st := LoadReplicaState(a.ReplicaDir); st.StreamsSince != 0 || streamsSince(a.ReplicaDir) > 0 || storedIDs(a) != "1" || st.StreamsPass != nil {
				t.Fatalf("withheld pass: cursor %d, %s", st.StreamsSince, storedIDs(a))
			}
			delete(m.withhold, 2)
			if err := a.SyncStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			// The walk names the ETag held, so MAIN sends only stream 2.
			last := m.requests()[len(m.requests())-1]["resync"].(map[string]any)["hashes"].(map[string]any)
			if len(last) != 1 || last["1"] != etagFor(streamData(1, "a")) || storedIDs(a) != "1,2" || LoadReplicaState(a.ReplicaDir).StreamsSince != m.head {
				t.Fatalf("second pass: hashes %v, %s", last, storedIDs(a))
			}
		}},
		{"a delta stops before a withheld record", func(t *testing.T, m *streamsMain, a *Agent) {
			m.set(1, streamData(1, "a"))
			a.SyncStreams(context.Background())
			m.set(2, streamData(2, "a"))
			v2 := m.head
			m.set(3, streamData(3, "a"))
			m.withhold[3] = true
			if err := a.SyncStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			if LoadReplicaState(a.ReplicaDir).StreamsSince != v2 || storedIDs(a) != "1,2" {
				t.Fatalf("cursor %d, %s", LoadReplicaState(a.ReplicaDir).StreamsSince, storedIDs(a))
			}
		}},
		{"full on a cursor below the floor walks with the hashes held", func(t *testing.T, m *streamsMain, a *Agent) {
			m.set(1, streamData(1, "a"))
			m.set(2, streamData(2, "a"))
			a.SyncStreams(context.Background())
			m.floor = m.head + 5
			m.head += 10
			m.set(2, streamData(2, "b"))
			n := len(m.requests())
			if err := a.SyncStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			req := m.requests()[n:]
			if len(req) != 2 || req[0]["resync"] != nil || req[1]["resync"] == nil || len(req[1]["resync"].(map[string]any)["hashes"].(map[string]any)) != 2 {
				t.Fatalf("requests %v", req)
			}
			b, _ := os.ReadFile(streamFile(a, 2, ".json"))
			if LoadReplicaState(a.ReplicaDir).StreamsSince != m.head || !strings.Contains(string(b), "live/b2") {
				t.Fatalf("after full: cursor %d", LoadReplicaState(a.ReplicaDir).StreamsSince)
			}
		}},
		{"full above MAIN's head (a restore)", func(t *testing.T, m *streamsMain, a *Agent) {
			m.set(1, streamData(1, "a"))
			a.SyncStreams(context.Background())
			m.head = 1
			m.set(4, streamData(4, "a"))
			if err := a.SyncStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			// head 2 == the old cursor: served as a delta; then MAIN goes back further.
			m.head = 0
			m.set(5, streamData(5, "a"))
			if err := a.SyncStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			if storedIDs(a) != "1,4,5" || LoadReplicaState(a.ReplicaDir).StreamsSince != m.head {
				t.Fatalf("%s cursor %d head %d", storedIDs(a), LoadReplicaState(a.ReplicaDir).StreamsSince, m.head)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, a := newStreamsMain(t)
			c.run(t, m, a)
		})
	}
}

func TestStreamsRangeWalk(t *testing.T) {
	old := MaxStreamHashes
	MaxStreamHashes = 2
	defer func() { MaxStreamHashes = old }()
	_, a := newStreamsMain(t)
	os.MkdirAll(filepath.Join(a.ReplicaDir, "streams"), 0o700)
	for _, id := range []int64{3, 7, 10, 12, 40} {
		os.WriteFile(streamFile(a, id, ".json"), []byte(`{"etag":"`+etagFor(strconv.FormatInt(id, 10))+`","ver":1,"data":{}}`), 0o600)
	}
	// Not a stream: never named.
	os.WriteFile(filepath.Join(a.ReplicaDir, "streams", ".9.json.tmp"), []byte(`x`), 0o600)
	os.WriteFile(filepath.Join(a.ReplicaDir, "streams", "12.rep"), []byte(`x`), 0o600)
	cases := []struct {
		from  int64
		bad   []int64
		names string
		to    int64
	}{
		{0, nil, "3,7", 7},
		{8, nil, "10,12", 12},
		{13, nil, "40", MaxStreamID},
		{41, nil, "", MaxStreamID},
		{0, []int64{7}, "3", 7}, // a record that did not verify: in the range, not named
	}
	for _, c := range cases {
		a.streamsBad = map[int64]bool{}
		for _, id := range c.bad {
			a.streamsBad[id] = true
		}
		hashes, to := a.streamHashes(a.ReplicaDir, c.from)
		var names []string
		for k, v := range hashes {
			id, _ := strconv.ParseInt(k, 10, 64)
			if v != etagFor(k) || id < c.from || id > to {
				t.Fatalf("from %d: %s=%s", c.from, k, v)
			}
			names = append(names, k)
		}
		sort.Slice(names, func(i, j int) bool { a, _ := strconv.Atoi(names[i]); b, _ := strconv.Atoi(names[j]); return a < b })
		if strings.Join(names, ",") != c.names || to != c.to {
			t.Fatalf("from %d: named %v, to %d; want %s, %d", c.from, names, to, c.names, c.to)
		}
	}
}

func TestStreamsResyncWalksInPagesWithTheCursorUnchanged(t *testing.T) {
	old := MaxStreamHashes
	MaxStreamHashes = 2
	defer func() { MaxStreamHashes = old }()
	m, a := newStreamsMain(t)
	ctx := context.Background()
	for id := int64(1); id <= 5; id++ {
		m.set(id, streamData(id, "a"))
	}
	m.maxRecs = 2 // a pass over five streams takes pages, `next` included
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	cursor := LoadReplicaState(a.ReplicaDir).StreamsSince
	if storedIDs(a) != "1,2,3,4,5" || cursor != m.head {
		t.Fatalf("pass: %s cursor %d", storedIDs(a), cursor)
	}
	// A change no delta carries (a node's write), and one stream gone
	// without a row: only the resync finds them.
	m.mu.Lock()
	m.data[4] = streamData(4, "c")
	delete(m.data, 2)
	m.mu.Unlock()
	if err := a.SyncStreams(ctx); err != nil { // the resync is not due yet
		t.Fatal(err)
	}
	if storedIDs(a) != "1,2,3,4,5" {
		t.Fatal("a resync before its time")
	}
	st := LoadReplicaState(a.ReplicaDir)
	st.StreamsResyncAt -= int64(2 * StreamsResyncEvery / time.Second)
	a.saveStreamsState(a.ReplicaDir, st)
	n := len(m.requests())
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(streamFile(a, 4, ".json"))
	if storedIDs(a) != "1,3,4,5" || !strings.Contains(string(b), "live/c4") || LoadReplicaState(a.ReplicaDir).StreamsSince != cursor {
		t.Fatalf("resync: %s, cursor %d", storedIDs(a), LoadReplicaState(a.ReplicaDir).StreamsSince)
	}
	// One delta, then the walk: every call carries the cursor, pages of at
	// most two hashes, the last to the top of the range.
	req := m.requests()[n:]
	if req[0]["resync"] != nil || len(req) < 3 {
		t.Fatalf("requests %v", req)
	}
	for _, r := range req[1:] {
		rs := r["resync"].(map[string]any)
		if r["since"] != float64(cursor) || len(rs["hashes"].(map[string]any)) > 2 {
			t.Fatalf("walk request %v", r)
		}
	}
	if last := req[len(req)-1]["resync"].(map[string]any); last["to"] != float64(MaxStreamID) {
		t.Fatalf("the walk ended at %v", last)
	}
}

func TestStreamsOneBadRecordRejectsTheReply(t *testing.T) {
	m, a := newStreamsMain(t)
	logs := &logSink{}
	a.Logf = logs.logf(t)
	m.set(1, streamData(1, "a"))
	m.set(2, streamData(2, "a"))
	m.forge[2] = true
	err := a.SyncStreams(context.Background())
	if err == nil || !strings.Contains(err.Error(), "stream 2") {
		t.Fatalf("got %v", err)
	}
	if storedIDs(a) != "" || LoadReplicaState(a.ReplicaDir).StreamsSince != 0 {
		t.Fatalf("stored %s from a reply with a forged record", storedIDs(a))
	}
	if strings.Contains(err.Error(), "S3cret") {
		t.Fatal("the error carries the record")
	}
	// Another node's record, or another stream's under this id, is refused too.
	delete(m.forge, 2)
	for _, bad := range []func(e map[string]any){
		func(e map[string]any) { e["id"] = int64(9) },
		func(e map[string]any) { e["etag"] = etagFor("x") },
		func(e map[string]any) { e["sealed"] = "%%" },
	} {
		m.mu.Lock()
		e := m.entry(t, 1)
		m.mu.Unlock()
		bad(e)
		_, err := (&streamSync{a: a, dir: a.ReplicaDir}).a.checkStream(streamEntry{ID: toInt(e["id"]), Ver: 1, Etag: e["etag"].(string), Sealed: e["sealed"].(string)})
		if err == nil {
			t.Fatalf("accepted %v", e["id"])
		}
	}
}

func toInt(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return 0
}

func TestStreamFilesLayoutAndModes(t *testing.T) {
	m, a := newStreamsMain(t)
	m.set(7, `{"stream":{"id":7,"note":"<a & b>"},"tickets":null}`)
	if err := a.SyncStreams(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(filepath.Join(a.ReplicaDir, "streams")); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("streams/: %v", err)
	}
	for _, f := range []string{streamFile(a, 7, ".rep"), streamFile(a, 7, ".json"), filepath.Join(a.ReplicaDir, "streams.json"), filepath.Join(a.ReplicaDir, "state.json")} {
		if fi, err := os.Stat(f); err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v", f, err)
		}
	}
	entries, _ := os.ReadDir(filepath.Join(a.ReplicaDir, "streams"))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			t.Fatalf("temporary file left: %s", e.Name())
		}
	}
	// .json holds the data exactly as signed, and the record as received.
	b, _ := os.ReadFile(streamFile(a, 7, ".json"))
	if want := `{"etag":"` + etagFor(`{"stream":{"id":7,"note":"<a & b>"},"tickets":null}`) + `","ver":2,"data":{"stream":{"id":7,"note":"<a & b>"},"tickets":null}}`; string(b) != want {
		t.Fatalf("7.json\n got %s\nwant %s", b, want)
	}
	rep, _ := os.ReadFile(streamFile(a, 7, ".rep"))
	if _, err := a.openStream(rep, 7); err != nil {
		t.Fatalf("7.rep: %v", err)
	}
	if s, _ := os.ReadFile(filepath.Join(a.ReplicaDir, "streams.json")); string(s) != `{"since":2}` {
		t.Fatalf("streams.json %s", s)
	}
	// The .rep is written before its .json: when it cannot be, neither is.
	m.set(8, streamData(8, "a"))
	os.MkdirAll(filepath.Join(streamFile(a, 8, ".rep"), "x"), 0o700)
	if err := a.SyncStreams(context.Background()); err == nil {
		t.Fatal("stored a stream whose record could not be written")
	}
	if _, err := os.Stat(streamFile(a, 8, ".json")); err == nil {
		t.Fatal("8.json written without its record")
	}
	// A removal deletes .json, then .rep: none left.
	os.RemoveAll(streamFile(a, 8, ".rep"))
	m.drop(8)
	m.drop(7)
	if err := a.SyncStreams(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, _ = os.ReadDir(filepath.Join(a.ReplicaDir, "streams"))
	if len(entries) != 0 {
		t.Fatalf("left after the removals: %v", entries)
	}
	if _, err := os.Stat(filepath.Join(a.ReplicaDir, "streams")); err != nil {
		t.Fatal("streams/ removed with its last stream")
	}
}

func TestStreamsCursorAndPassSurviveARestart(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	for id := int64(1); id <= 3; id++ {
		m.set(id, streamData(id, "a"))
	}
	m.maxExam = 1
	// The pass stops after its first page (MAIN fails the second call).
	calls := 0
	orig := m.fakeMain.answer
	m.fakeMain.answer = func(w http.ResponseWriter, r *http.Request, reqCtx, nonce []byte) {
		if strings.HasSuffix(r.URL.Path, "/streams") {
			calls++
			if calls == 2 {
				m.refuse1(w, nonce)
				return
			}
		}
		orig(w, r, reqCtx, nonce)
	}
	if err := a.SyncStreams(ctx); err == nil {
		t.Fatal("the pass did not stop")
	}
	st := LoadReplicaState(a.ReplicaDir)
	if st.StreamsPass == nil || st.StreamsPass.From != 2 || st.StreamsPass.Head != m.head || st.StreamsSince != 0 {
		t.Fatalf("pass kept %+v, cursor %d", st.StreamsPass, st.StreamsSince)
	}
	// A new agent over the same files goes on from there, with no delta.
	m.fakeMain.answer = orig
	b := &Agent{Client: a.Client, Logf: t.Logf, ReplicaDir: a.ReplicaDir}
	b.flows.Store(FlowStreams)
	n := len(m.requests())
	if err := b.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	req := m.requests()[n]
	if req["resync"].(map[string]any)["from"] != float64(2) {
		t.Fatalf("resumed with %v", req)
	}
	if storedIDs(b) != "1,2,3" || LoadReplicaState(b.ReplicaDir).StreamsSince != m.head {
		t.Fatalf("%s cursor %d", storedIDs(b), LoadReplicaState(b.ReplicaDir).StreamsSince)
	}
	// And the cursor too: the next agent asks a delta from it.
	c := &Agent{Client: a.Client, Logf: t.Logf, ReplicaDir: a.ReplicaDir}
	c.flows.Store(FlowStreams)
	n = len(m.requests())
	c.SyncStreams(ctx)
	if req := m.requests()[n]; req["resync"] != nil || req["since"] != float64(m.head) {
		t.Fatalf("after a restart asked %v", req)
	}
}

// refuse1 answers a signed 503 DB.
func (m *streamsMain) refuse1(w http.ResponseWriter, nonce []byte) {
	m.fakeMain.refuse(w, 503, nonce, "DB", nil)
}

func TestStreamsRefusals(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	m.set(1, streamData(1, "a"))
	a.SyncStreams(ctx)
	m.set(2, streamData(2, "a"))
	// Busy (the per-op semaphore, then the bulk lane): the same request after the wait.
	m.refuse = append(m.refuse,
		func(w http.ResponseWriter, n []byte) {
			m.fakeMain.refuse(w, 503, n, "RATE_LIMITED", map[string]any{"retry_after_ms": 1000, "op": "streams"})
		},
		func(w http.ResponseWriter, n []byte) {
			m.fakeMain.refuse(w, 503, n, "RATE_LIMITED", map[string]any{"retry_after_ms": 1000, "op": "streams", "lane": "bulk"})
		})
	n := len(m.requests())
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	req := m.requests()[n:]
	if len(req) != 3 || req[0]["since"] != req[2]["since"] || storedIDs(a) != "1,2" || a.BusyRefusals() != 1 {
		t.Fatalf("requests %v, busy %d", req, a.BusyRefusals())
	}
	// 503 DB, 409 NOT_ACTIVE, 404 UNKNOWN_OP: every file and the cursor kept.
	m.set(3, streamData(3, "a"))
	cursor := LoadReplicaState(a.ReplicaDir).StreamsSince
	for _, c := range []struct {
		status int
		reason string
	}{{503, "DB"}, {409, "NOT_ACTIVE"}, {404, "UNKNOWN_OP"}} {
		m.refuse = append(m.refuse, func(w http.ResponseWriter, n []byte) { m.fakeMain.refuse(w, c.status, n, c.reason, nil) })
		if err := a.SyncStreams(ctx); err == nil {
			t.Fatalf("%s taken", c.reason)
		}
		if storedIDs(a) != "1,2" || LoadReplicaState(a.ReplicaDir).StreamsSince != cursor || streamsSince(a.ReplicaDir) != cursor {
			t.Fatalf("%s changed what is held", c.reason)
		}
	}
	// FLOW_OFF naming the feature: hello again.
	m.refuse = append(m.refuse, func(w http.ResponseWriter, n []byte) {
		m.fakeMain.refuse(w, 409, n, "FLOW_OFF", map[string]any{"flow": "streams", "feature": "streams"})
	})
	a.stopCh = make(chan error, 1)
	a.helloing.Store(true) // the hello itself is not under test
	var d *Denial
	if err := a.SyncStreams(ctx); !errors.As(err, &d) || d.Reason != "FLOW_OFF" {
		t.Fatalf("got %v", err)
	}
}

func TestStreamsFollowTheFlow(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	flows := filepath.Join(t.TempDir(), "flows.json")
	a.FlowsFile = flows
	m.set(1, streamData(1, "a"))
	m.set(2, streamData(2, "a"))
	a.publish(&Reply{State: "active", Mode: 1, Flows: FlowStreams})
	a.SyncStreams(ctx)
	if streamsSince(a.ReplicaDir) != m.head {
		t.Fatal("no pass")
	}
	// STREAMS off: streams.json says 0 as flows.json does; the files are
	// kept; MAIN is not asked; state.json's cursor goes to 0.
	a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
	if streamsSince(a.ReplicaDir) != 0 {
		t.Fatal("streams.json kept its cursor with STREAMS off")
	}
	n := len(m.requests())
	if err := a.SyncStreams(ctx); err != nil || len(m.requests()) != n {
		t.Fatalf("asked MAIN with STREAMS off: %v", err)
	}
	if st := LoadReplicaState(a.ReplicaDir); st.StreamsSince != 0 || storedIDs(a) != "1,2" {
		t.Fatalf("state %d, %s", st.StreamsSince, storedIDs(a))
	}
	// On again: a full pass naming what is held, so only changes come.
	m.set(2, streamData(2, "b"))
	a.publish(&Reply{State: "active", Mode: 1, Flows: FlowStreams})
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	req := m.requests()[n]
	if len(req["resync"].(map[string]any)["hashes"].(map[string]any)) != 2 || streamsSince(a.ReplicaDir) != m.head {
		t.Fatalf("pass after the flow came back: %v", req)
	}
	// The STREAMS bit changing either way runs cluster:apply.
	kicks := a.kickChans()
	for len(kicks.apply) > 0 {
		<-kicks.apply
	}
	a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
	if len(kicks.apply) != 1 {
		t.Fatal("no apply when STREAMS went off")
	}
}

func TestStreamRecordsAreCheckedAtStart(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	m.set(1, streamData(1, "a"))
	m.set(2, streamData(2, "a"))
	a.SyncStreams(ctx)
	// Stream 2's record no longer verifies (planted, or under old keys).
	os.WriteFile(streamFile(a, 2, ".rep"), []byte("not a record"), 0o600)
	// The agent's first config sync checks what is stored under the node's
	// keys, and has the streams sync check its records.
	if err := a.SyncReplica(ctx); err != nil {
		t.Fatal(err)
	}
	n := len(m.requests())
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	// The resync runs at once, naming stream 1 only: MAIN resends stream 2.
	req := m.requests()[n:]
	hashes := req[len(req)-1]["resync"].(map[string]any)["hashes"].(map[string]any)
	rep, _ := os.ReadFile(streamFile(a, 2, ".rep"))
	if len(hashes) != 1 || hashes["1"] == nil || len(a.streamsBad) != 0 {
		t.Fatalf("hashes %v", hashes)
	}
	if _, err := a.openStream(rep, 2); err != nil {
		t.Fatal("stream 2 not stored again")
	}
	// The same keys: the next config sync checks nothing, so the streams
	// sync is a delta.
	os.WriteFile(streamFile(a, 2, ".rep"), []byte("not a record"), 0o600)
	a.SyncReplica(ctx)
	n = len(m.requests())
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	if req := m.requests()[n:]; len(req) != 1 || req[0]["resync"] != nil {
		t.Fatalf("checked again under the same keys: %v", req)
	}
	// A new box key (a re-enrolment): every record sealed to the old one
	// fails, and MAIN resends them all.
	sk, pub, _ := cc.NewX25519()
	a.Client.State.mu.Lock()
	a.Client.State.NodeBoxSk = sk
	a.Client.State.mu.Unlock()
	m.mu.Lock()
	m.boxPub = pub
	m.mu.Unlock()
	a.SyncReplica(ctx)
	n = len(m.requests())
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	req = m.requests()[n:]
	if hashes := req[len(req)-1]["resync"].(map[string]any)["hashes"].(map[string]any); len(hashes) != 0 {
		t.Fatalf("named records sealed to the old key: %v", hashes)
	}
	for _, id := range []int64{1, 2} {
		rep, _ := os.ReadFile(streamFile(a, id, ".rep"))
		if _, err := a.openStream(rep, id); err != nil {
			t.Fatalf("stream %d not stored again under the new key", id)
		}
	}
}

func TestStreamsNeverLogged(t *testing.T) {
	m, a := newStreamsMain(t)
	logs := &logSink{}
	a.Logf = logs.logf(t)
	m.set(1, streamData(1, "a"))
	m.set(2, streamData(2, "a"))
	m.forge[2] = true
	var errs []string
	if err := a.SyncStreams(context.Background()); err != nil {
		errs = append(errs, err.Error())
	}
	delete(m.forge, 2)
	a.SyncStreams(context.Background())
	os.WriteFile(streamFile(a, 1, ".rep"), []byte("x"), 0o600)
	a.recheckReplica()
	a.SyncStreams(context.Background())
	logs.mu.Lock()
	all := strings.Join(logs.lines, "\n") + strings.Join(errs, "\n")
	logs.mu.Unlock()
	for _, leak := range []string{"S3cretUpstreamPass", "stream_source", etagFor(streamData(1, "a"))} {
		if strings.Contains(all, leak) {
			t.Fatalf("logged %q:\n%s", leak, all)
		}
	}
}

func (m *streamsMain) configCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.configs
}

// countApplies has the agent's cluster:apply record what streams.json said
// each time it ran.
func countApplies(a *Agent) func() []int64 {
	var mu sync.Mutex
	var seen []int64
	a.Apply = func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, streamsSince(a.ReplicaDir))
		return nil
	}
	return func() []int64 {
		mu.Lock()
		defer mu.Unlock()
		return append([]int64(nil), seen...)
	}
}

// The streams sync runs on its own loop: while a walk waits out MAIN's
// busy refusals, config.changed still fetches at once and a flow change
// still applies at once.
func TestStreamsWalkHoldsUpNoConfigSync(t *testing.T) {
	m, a := newStreamsMain(t)
	applies := countApplies(a)
	m.set(1, streamData(1, "a"))
	busy := func(w http.ResponseWriter, nonce []byte) {
		m.fakeMain.refuse(w, 503, nonce, "RATE_LIMITED", map[string]any{"op": "streams", "lane": "bulk", "retry_after_ms": 1000})
	}
	for i := 0; i < StreamsBusyRetries; i++ {
		m.refuse = append(m.refuse, busy)
	}
	a.flowsApply(FlowStreams) // the flows last published
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.RunReplica(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitUntil := func(what string, bound time.Duration, cond func() bool) {
		t.Helper()
		for end := time.Now().Add(bound); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			if cond() {
				return
			}
		}
		t.Fatalf("%s: not within %s", what, bound)
	}
	waitUntil("the walk waiting out a busy refusal", 5*time.Second, func() bool { return len(m.requests()) >= 1 })
	waitUntil("the first apply", 5*time.Second, func() bool { return len(applies()) >= 1 })
	configs := m.configCalls()
	a.ConfigChanged()
	waitUntil("config.changed's sync", 500*time.Millisecond, func() bool { return m.configCalls() > configs })
	n := len(applies())
	a.flowsApply(FlowStreams | FlowConfig)
	waitUntil("the flow change's apply", 500*time.Millisecond, func() bool { return len(applies()) > n })
	if len(m.requests()) >= StreamsBusyRetries {
		t.Fatal("the walk was not still busy: the test proves nothing")
	}
}

// The pass's end makes streams.json say a cursor above 0, and PHP gets its
// cluster:apply then, even when the pass stored nothing.
func TestStreamsApplyAfterAPassThatStoresNothing(t *testing.T) {
	t.Run("a new node that holds no stream", func(t *testing.T) {
		_, a := newStreamsMain(t)
		applies := countApplies(a)
		if err := a.SyncStreams(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := applies(); len(got) != 1 || got[0] <= 0 {
			t.Fatalf("applies saw streams.json %v", got)
		}
	})
	t.Run("STREAMS on, off and on with nothing changed", func(t *testing.T) {
		m, a := newStreamsMain(t)
		ctx := context.Background()
		m.set(1, streamData(1, "a"))
		a.publish(&Reply{State: "active", Mode: 1, Flows: FlowStreams})
		a.SyncStreams(ctx)
		a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
		a.SyncStreams(ctx)
		applies := countApplies(a)
		a.publish(&Reply{State: "active", Mode: 1, Flows: FlowStreams})
		if err := a.SyncStreams(ctx); err != nil {
			t.Fatal(err)
		}
		if got := applies(); len(got) != 1 || got[0] != m.head {
			t.Fatalf("applies saw streams.json %v, want [%d]", got, m.head)
		}
		// An idle delta applies nothing.
		a.SyncStreams(ctx)
		if got := applies(); len(got) != 1 {
			t.Fatalf("applies %v after an idle delta", got)
		}
	})
}

// cluster:apply runs after a stored record and after a removal.
func TestStreamsApplyAfterRecordsAndRemovals(t *testing.T) {
	m, a := newStreamsMain(t)
	applies := countApplies(a)
	ctx := context.Background()
	m.set(1, streamData(1, "a"))
	m.set(2, streamData(2, "a"))
	a.SyncStreams(ctx)
	m.set(1, streamData(1, "b"))
	a.SyncStreams(ctx)
	if len(applies()) != 2 {
		t.Fatalf("applies %v after a stored record", applies())
	}
	m.drop(2)
	a.SyncStreams(ctx)
	if len(applies()) != 3 || storedIDs(a) != "1" {
		t.Fatalf("applies %v after a removal (%s)", applies(), storedIDs(a))
	}
}

// With STREAMS off streams.json says 0, whatever a pass wrote meanwhile,
// before flows.json says the flow is off; a write that failed is healed.
func TestStreamsJSONIsZeroWhileTheFlowIsOff(t *testing.T) {
	t.Run("a pass ending as the flow goes off", func(t *testing.T) {
		_, a := newStreamsMain(t)
		a.publish(&Reply{State: "active", Mode: 1, Flows: FlowStreams})
		a.writeStreamsSince(a.ReplicaDir, 0) // a new node's pass under way
		a.streamsFileMu.Lock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
		}()
		time.Sleep(20 * time.Millisecond)
		// The pass's end, just before the zeroing takes the lock.
		writeFileMode(filepath.Join(a.ReplicaDir, "streams.json"), []byte(`{"since":7}`), 0o600)
		a.streamsFileMu.Unlock()
		<-done
		if s := streamsSince(a.ReplicaDir); s != 0 {
			t.Fatalf("streams.json says %d with STREAMS off", s)
		}
	})
	t.Run("zeroed before flows.json is written", func(t *testing.T) {
		_, a := newStreamsMain(t)
		// flows.json cannot be written: streams.json is 0 all the same.
		a.FlowsFile = filepath.Join(t.TempDir(), "flows.json")
		a.publish(&Reply{State: "active", Mode: 1, Flows: FlowStreams})
		a.writeStreamsSince(a.ReplicaDir, 7)
		os.Remove(a.FlowsFile)
		os.MkdirAll(filepath.Join(a.FlowsFile, "busy"), 0o700)
		a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
		if s := streamsSince(a.ReplicaDir); s != 0 {
			t.Fatalf("streams.json says %d when flows.json was to be written", s)
		}
	})
	t.Run("healed by the next sync", func(t *testing.T) {
		m, a := newStreamsMain(t)
		a.publish(&Reply{State: "active", Mode: 1, Flows: 0})
		writeFileMode(filepath.Join(a.ReplicaDir, "streams.json"), []byte(`{"since":7}`), 0o600)
		n := len(m.requests())
		if err := a.SyncStreams(context.Background()); err != nil || len(m.requests()) != n {
			t.Fatalf("%v", err)
		}
		if s := streamsSince(a.ReplicaDir); s != 0 {
			t.Fatalf("streams.json still says %d", s)
		}
	})
}

// A record that failed its check is left out of the next walk only: a
// stream MAIN no longer holds for the node goes at the walk after.
func TestStreamsFailedRecordIsNamedAgain(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	m.set(1, streamData(1, "a"))
	m.set(2, streamData(2, "a"))
	a.SyncStreams(ctx)
	m.mu.Lock()
	delete(m.data, 2) // no longer the node's, without a row saying so
	m.mu.Unlock()
	os.WriteFile(streamFile(a, 2, ".rep"), []byte("not a record"), 0o600)
	a.SyncReplica(ctx)
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	if storedIDs(a) != "1,2" {
		t.Fatalf("after the first walk: %s", storedIDs(a))
	}
	st := LoadReplicaState(a.ReplicaDir)
	st.StreamsResyncAt = 0
	a.saveStreamsState(a.ReplicaDir, st)
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	if storedIDs(a) != "1" {
		t.Fatalf("after the second walk: %s", storedIDs(a))
	}
}

// A record naming another node or another section is refused with its
// whole reply, even sealed to this node and signed by the panel.
func TestStreamsRecordOfAnotherNodeOrSection(t *testing.T) {
	for name, alter := range map[string]func(string) string{
		"another node":    func(p string) string { return strings.Replace(p, `"node":"`, `"node":"1`, 1) },
		"another section": func(p string) string { return strings.Replace(p, `"section":"stream"`, `"section":"servers"`, 1) },
		"another stream":  func(p string) string { return strings.Replace(p, `"stream_id":2`, `"stream_id":3`, 1) },
	} {
		t.Run(name, func(t *testing.T) {
			m, a := newStreamsMain(t)
			m.set(1, streamData(1, "a"))
			m.set(2, streamData(2, "a"))
			m.alter[2] = alter
			if err := a.SyncStreams(context.Background()); err == nil || !strings.Contains(err.Error(), "stream 2") {
				t.Fatalf("got %v", err)
			}
			if storedIDs(a) != "" {
				t.Fatalf("stored %s", storedIDs(a))
			}
		})
	}
}

// A reply with a removal and a record that does not verify applies
// nothing: the removed stream's files stay.
func TestStreamsBadReplyRemovesNothing(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	m.set(1, streamData(1, "a"))
	m.set(2, streamData(2, "a"))
	a.SyncStreams(ctx)
	m.drop(1)
	m.set(2, streamData(2, "b"))
	m.forge[2] = true
	if err := a.SyncStreams(ctx); err == nil {
		t.Fatal("a forged record was taken")
	}
	for _, ext := range []string{".json", ".rep"} {
		if _, err := os.Stat(streamFile(a, 1, ext)); err != nil {
			t.Fatalf("stream 1's %s went with a reply that did not verify", ext)
		}
	}
}

// MAIN's head moving during a pass of several pages: the cursor is the
// first reply's head, so a stream changed after it was walked comes in the
// next delta.
func TestStreamsPassCursorIsTheFirstHead(t *testing.T) {
	m, a := newStreamsMain(t)
	ctx := context.Background()
	for id := int64(1); id <= 3; id++ {
		m.set(id, streamData(id, "a"))
	}
	m.maxRecs = 1
	first := m.head
	bumped := false
	m.served = func() {
		if !bumped {
			bumped = true
			m.head++
			m.data[1], m.vers[1] = streamData(1, "b"), m.head
		}
	}
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	if got := LoadReplicaState(a.ReplicaDir).StreamsSince; got != first {
		t.Fatalf("cursor %d, want the first reply's head %d", got, first)
	}
	if err := a.SyncStreams(ctx); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(streamFile(a, 1, ".json"))
	if !strings.Contains(string(b), "live/b1.ts") {
		t.Fatalf("the change made mid-pass never came: %s", b)
	}
}
