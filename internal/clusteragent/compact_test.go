package clusteragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// connLine is a conn.upsert spool line for a record.
func connLine(rec map[string]any) string {
	b, _ := json.Marshal(map[string]any{"type": "conn.upsert", "t": 1, "d": map[string]any{"record": rec}})
	return string(b)
}

func connEnd(typ, uuid string) string {
	return fmt.Sprintf(`{"type":%q,"t":1,"d":{"uuid":%q}}`, typ, uuid)
}

func hlsRec(uuid string, read int, end int) map[string]any {
	return map[string]any{"uuid": uuid, "user_id": 1, "stream_id": 9, "container": "hls", "pid": 0, "hls_end": end, "hls_last_read": read, "date_start": 100}
}

func writeSpool(t *testing.T, dir string, seq int, lines ...string) string {
	t.Helper()
	os.MkdirAll(dir, 0o750)
	name := fmt.Sprintf("%019d-1-0000.ndjson", seq)
	if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.Join(lines, "\n")+"\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	return name
}

// mainOutcome is what MAIN's store and history hold after applying P0
// connection events as ConnectionIngest does (an upsert overwrites the whole
// record and releases the viewer's reservation, a remove drops it, a close
// writes its activity row from the stored row and drops it), and what the
// store held for the viewers each conn.limit concerns when it was queued.
type mainOutcome struct {
	Store    map[string]map[string]any
	Activity []map[string]any
	Released map[string]bool
	AtLimits []map[string]map[string]any // the store but hls_last_read, at each conn.limit
}

func applyMain(t *testing.T, lines []string) mainOutcome {
	t.Helper()
	out := mainOutcome{Store: map[string]map[string]any{}, Released: map[string]bool{}}
	for _, l := range lines {
		var ev struct {
			Type string         `json:"type"`
			D    map[string]any `json:"d"`
		}
		if err := json.Unmarshal([]byte(l), &ev); err != nil {
			t.Fatalf("%s: %v", l, err)
		}
		switch ev.Type {
		case "conn.upsert":
			rec := ev.D["record"].(map[string]any)
			uuid := rec["uuid"].(string)
			out.Store[uuid] = rec
			out.Released[uuid] = true
		case "conn.remove":
			delete(out.Store, ev.D["uuid"].(string))
		case "conn.close":
			uuid := ev.D["uuid"].(string)
			if row := out.Store[uuid]; row != nil {
				act := clone(row)
				delete(act, "hls_last_read")
				out.Activity = append(out.Activity, act)
				delete(out.Store, uuid)
			}
		case "conn.limit":
			view := map[string]map[string]any{}
			for k, v := range out.Store {
				c := clone(v)
				delete(c, "hls_last_read")
				view[k] = c
			}
			out.AtLimits = append(out.AtLimits, view)
		}
	}
	return out
}

func TestCompactionFoldsAViewersTouchesOnly(t *testing.T) {
	_, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	b := func(read int) map[string]any { r := hlsRec("b", read, 0); r["container"] = "ts"; return r }
	pid := b(3)
	pid["pid"] = 9
	in := []string{
		connLine(hlsRec("a", 1, 0)), // 0: a's first run starts: carries a@3
		connLine(b(1)),              // 1: b's run starts: carries b@2
		connLine(hlsRec("a", 2, 0)), // folded
		`{"type":"conn.limit","t":1,"d":{"uuid":"a","ip":"1.2.3.4","user_agent":"x","user_id":1}}`,
		connLine(b(2)),              // folded
		connLine(hlsRec("a", 3, 0)), // folded
		connLine(hlsRec("a", 3, 1)), // the reaper's end: a new run, carries a@4 ended
		connLine(pid),               // another key changed: stays
		connLine(hlsRec("a", 4, 1)), // folded
		`{"type":"stream.state","t":1,"d":{"stream_id":9,"server_id":7,"fields":{"pid":1}}}`,
		connEnd("conn.close", "a"),  // never folded across
		connLine(hlsRec("a", 5, 0)), // re-opened (an HLS uuid comes back)
		connEnd("conn.remove", "a"),
	}
	writeSpool(t, ls.dir, 1, in[:5]...)
	writeSpool(t, ls.dir, 2, in[5:]...)
	ls.lane.Compact = 1
	res, err := ls.compactTail(nil)
	if err != nil || res.folded != 4 {
		t.Fatalf("folded %d, %v", res.folded, err)
	}
	want := []string{connLine(hlsRec("a", 3, 0)), connLine(b(2)), in[3], connLine(hlsRec("a", 4, 1)), in[7], in[9], in[10], in[11], in[12]}
	got := laneLines(t, ls)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("compacted:\n%s", strings.Join(got, "\n"))
	}
	if o, c := applyMain(t, in), applyMain(t, got); !reflect.DeepEqual(o, c) {
		t.Fatalf("MAIN's outcome changed:\n%+v\n%+v", o, c)
	}
}

// TestCompactionLeavesMAINsOutcomeAsItWas folds random connection histories
// and checks MAIN ends where the whole history would take it, with every
// conn.limit finding the store as it would have.
func TestCompactionLeavesMAINsOutcomeAsItWas(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for round := 0; round < 40; round++ {
		_, a := newEventsMain(t)
		ls := laneFor(a, "p0")
		uuids := []string{"a", "b", "c", "d"}
		recs := map[string]map[string]any{}
		var in []string
		for i := 0; i < 200; i++ {
			u := uuids[rng.Intn(len(uuids))]
			rec := recs[u]
			switch k := rng.Intn(20); {
			case rec == nil || k < 10:
				if rec == nil {
					rec = hlsRec(u, i, 0)
				} else {
					rec = clone(rec)
					rec["hls_last_read"] = i
				}
				recs[u] = rec
				in = append(in, connLine(rec))
			case k < 12:
				rec = clone(rec)
				rec["hls_end"] = 1 - rec["hls_end"].(int)
				recs[u] = rec
				in = append(in, connLine(rec))
			case k < 13:
				rec = clone(rec)
				rec["pid"] = rng.Intn(3)
				recs[u] = rec
				in = append(in, connLine(rec))
			case k < 15:
				in = append(in, fmt.Sprintf(`{"type":"conn.limit","t":1,"d":{"uuid":%q,"user_id":1}}`, u))
			case k < 16:
				in = append(in, connEnd("conn.close", u))
				delete(recs, u)
			case k < 17:
				in = append(in, connEnd("conn.remove", u))
				delete(recs, u)
			default:
				in = append(in, fmt.Sprintf(`{"type":"stream.state","t":1,"d":{"stream_id":%d,"fields":{"n":%d}}}`, rng.Intn(3), i))
			}
		}
		for seq, start := 1, 0; start < len(in); seq++ {
			end := min(len(in), start+1+rng.Intn(30))
			writeSpool(t, ls.dir, seq, in[start:end]...)
			start = end
		}
		ls.lane.Compact = 1
		res, err := ls.compactTail(nil)
		if err != nil {
			t.Fatal(err)
		}
		got := laneLines(t, ls)
		if res.folded == 0 || len(got) != len(in)-res.folded {
			t.Fatalf("round %d: folded %d, %d of %d left", round, res.folded, len(got), len(in))
		}
		if o, c := applyMain(t, in), applyMain(t, got); !reflect.DeepEqual(o, c) {
			t.Fatalf("round %d: MAIN's outcome changed:\n%+v\n%+v", round, o, c)
		}
	}
}

// TestP0CompactsBehindAStuckBatch: MAIN is out of reach with a batch in
// flight; the backlog behind it is still compacted, the in-flight files stay
// byte for byte, and once MAIN is back the batch goes first under its number.
func TestP0CompactsBehindAStuckBatch(t *testing.T) {
	old := HousekeepEvery
	HousekeepEvery = 0
	defer func() { HousekeepEvery = old }()
	m, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	ls.lane.Compact = 200
	st := func(stream int, n string) string {
		return fmt.Sprintf(`{"type":"stream.state","t":1,"d":{"stream_id":%d,"server_id":7,"n":%q}}`, stream, n)
	}
	first := writeSpool(t, ls.dir, 1, st(1, "a"))
	m.refuse = 1000
	if _, _, err := a.shipOnce(context.Background(), ls, 1); err == nil {
		t.Fatal("MAIN answered")
	}
	flBefore, _ := os.ReadFile(ls.state)
	bytesBefore, _ := os.ReadFile(filepath.Join(ls.dir, first))
	for i := 2; i < 40; i++ {
		writeSpool(t, ls.dir, i, st(1, fmt.Sprint("b", i)), st(2, fmt.Sprint("c", i)))
	}
	if _, _, err := a.shipOnce(context.Background(), ls, 1); err == nil {
		t.Fatal("MAIN answered")
	}
	flAfter, _ := os.ReadFile(ls.state)
	bytesAfter, err := os.ReadFile(filepath.Join(ls.dir, first))
	if err != nil || !bytes.Equal(bytesBefore, bytesAfter) || !bytes.Equal(flBefore, flAfter) {
		t.Fatalf("the in-flight batch changed: %v\n%s\n%s", err, flBefore, flAfter)
	}
	if files, _ := ls.spooled(); len(files) != 2 {
		t.Fatalf("not compacted behind the batch: %d files", len(files))
	}
	m.refuse = 0
	next := drain(t, a, ls, 1)
	if got := strings.Join(m.applied["p0"], ","); got != "a,b39,c39" || next != 4 {
		t.Fatalf("applied %s, next %d", got, next)
	}
}

// TestP0CompactsBeforeMAINsCursorIsKnown: an agent started during the outage
// never gets a hello; its lane still bounds the backlog.
func TestP0CompactsBeforeMAINsCursorIsKnown(t *testing.T) {
	_, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	for i := 1; i < 30; i++ {
		writeSpool(t, ls.dir, i, connLine(hlsRec("a", i, 0)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	a.RunEvents(ctx, Lane{Name: "p0", Interval: 20 * time.Millisecond, Compact: 100})
	if got := laneLines(t, ls); len(got) != 1 || got[0] != connLine(hlsRec("a", 29, 0)) {
		t.Fatalf("lane holds %v", got)
	}
}

// TestP0SkipsHousekeepingOnAnUnreadableInflightRecord: before MAIN's cursor
// is known, an in-flight record that exists but cannot be read (here a
// directory: EISDIR, not ENOENT) skips the pass. Taking the lane as having
// no batch in flight would compact that batch's files with the tail.
func TestP0SkipsHousekeepingOnAnUnreadableInflightRecord(t *testing.T) {
	_, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	want := map[string][]byte{}
	for i := 1; i < 30; i++ {
		name := writeSpool(t, ls.dir, i, connLine(hlsRec("a", i, 0)))
		want[name], _ = os.ReadFile(filepath.Join(ls.dir, name))
	}
	if err := os.MkdirAll(ls.state, 0o750); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	a.RunEvents(ctx, Lane{Name: "p0", Interval: 20 * time.Millisecond, Compact: 100})
	files, _ := ls.spooled()
	if len(files) != len(want) {
		t.Fatalf("the lane holds %d files, not %d", len(files), len(want))
	}
	for name, b := range want {
		if got, err := os.ReadFile(filepath.Join(ls.dir, name)); err != nil || !bytes.Equal(got, b) {
			t.Fatalf("%s changed: %v", name, err)
		}
	}
	if _, err := os.Stat(ls.manifestPath()); err == nil {
		t.Fatal("a compaction was committed")
	}
}

// TestP0CompactionBacksOffAfterAFailure: a compaction given up before its
// commit (no room for its copy, or staging failing) leaves the tail as it
// was and is not tried again until its back-off, doubling, has passed; one
// that goes through resets it.
func TestP0CompactionBacksOffAfterAFailure(t *testing.T) {
	_, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	ls.lane.Compact = 1
	for i := 1; i <= 5; i++ {
		writeSpool(t, ls.dir, i, connLine(hlsRec("a", i, 0)))
	}
	before := laneLines(t, ls)
	unchanged := func(step string) {
		t.Helper()
		if got := laneLines(t, ls); !reflect.DeepEqual(got, before) {
			t.Fatalf("%s: the tail changed: %v", step, got)
		}
		if _, err := os.Stat(ls.stagingDir()); err == nil {
			t.Fatalf("%s: staging left behind", step)
		}
		if _, err := os.Stat(ls.manifestPath()); err == nil {
			t.Fatalf("%s: a manifest left behind", step)
		}
	}

	// No room for the copy: not begun.
	statfs := 0
	realFree := freeSpace
	swap(t, &freeSpace, func(string) (int64, error) { statfs++; return compactReserve, nil })
	if _, err := ls.compactTail(nil); err == nil || !strings.Contains(err.Error(), "no room") {
		t.Fatalf("compacted with no room: %v", err)
	}
	unchanged("no room")
	if res, err := ls.compactTail(nil); err != nil || res.size != 0 || statfs != 1 {
		t.Fatalf("tried again at once: %+v, %v (%d statfs)", res, err, statfs)
	}
	if wait := time.Until(ls.retryAt); wait <= CompactRetry/2 || wait > CompactRetry {
		t.Fatalf("backs off %s", wait)
	}

	// Staging fails (its sync): given up, staging removed, the back-off doubles.
	freeSpace = realFree
	realSync := syncDir
	syncs := 0
	swap(t, &syncDir, func(dir string) error {
		if dir == ls.stagingDir() {
			syncs++
			return errors.New("ENOSPC")
		}
		return realSync(dir)
	})
	ls.retryAt = time.Time{}
	if _, err := ls.compactTail(nil); err == nil || syncs != 1 {
		t.Fatalf("staging did not fail: %v", err)
	}
	unchanged("staging failed")
	if wait := time.Until(ls.retryAt); wait <= CompactRetry || wait > 2*CompactRetry {
		t.Fatalf("backs off %s after two failures", wait)
	}
	if _, err := ls.compactTail(nil); err != nil || syncs != 1 {
		t.Fatalf("tried again at once: %v", err)
	}

	// Past the back-off, with room: compacted, and the back-off reset.
	syncDir = realSync
	ls.retryAt = time.Now().Add(-time.Second)
	if res, err := ls.compactTail(nil); err != nil || res.folded != 4 {
		t.Fatalf("folded %d, %v", res.folded, err)
	}
	if ls.failures != 0 || !ls.retryAt.IsZero() {
		t.Fatalf("back-off kept: %d failures, until %s", ls.failures, ls.retryAt)
	}
	if got := laneLines(t, ls); len(got) != 1 || got[0] != connLine(hlsRec("a", 5, 0)) {
		t.Fatalf("lane holds %v", got)
	}
}

func TestP1CapSparesTheInflightBatch(t *testing.T) {
	_, a := newEventsMain(t)
	ls := laneFor(a, "p1")
	ls.lane.Cap = 100
	for i := 1; i <= 5; i++ {
		spool(t, a.SpoolDir, "p1", i, fmt.Sprintf("l%d", i)) // 44 bytes each
	}
	fl := &inflight{First: 1, Count: 1, Files: []string{fmt.Sprintf("%019d-1-0000.ndjson", 1)}}
	ls.housekeep(fl, t.Logf)
	var got []string
	for _, l := range laneLines(t, ls) {
		var ev struct {
			Type string
			D    struct {
				N     string
				Count int
			}
		}
		json.Unmarshal([]byte(l), &ev)
		got = append(got, ev.D.N+fmt.Sprint(ev.D.Count))
	}
	if strings.Join(got, ",") != "2,l10,l40,l50" {
		t.Fatalf("lane holds %v", got)
	}
}

// TestP0NeverDropsAndBacksOff: P0 is never dropped (ADR 0004), however large
// a backlog of events that do not fold; once compacted, such a backlog is not
// rewritten again until it has doubled.
func TestP0NeverDropsAndBacksOff(t *testing.T) {
	_, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	var lines []string
	for i := 0; i < 400; i++ {
		lines = append(lines, connLine(hlsRec(fmt.Sprintf("v%03d", i), i, 0)), connEnd("conn.close", fmt.Sprintf("v%03d", i)))
		lines = append(lines, fmt.Sprintf(`{"type":"conn.limit","t":1,"d":{"uuid":"v%03d","user_id":1}}`, i))
		lines = append(lines, fmt.Sprintf(`{"type":"security.block_ip","t":1,"d":{"ip":"10.0.%d.%d","reason":"flood"}}`, i/256, i%256))
	}
	lines = append(lines, `{"type":"stream.state","t":1,"d":{"stream_id":1,"fields":{"n":1}}}`, `{"type":"stream.state","t":1,"d":{"stream_id":1,"fields":{"n":2}}}`)
	for i := 0; i < len(lines); i += 50 {
		writeSpool(t, ls.dir, i+1, lines[i:min(len(lines), i+50)]...)
	}
	ls.lane.Compact = 1 << 10
	res, err := ls.compactTail(nil)
	if err != nil || res.folded != 1 {
		t.Fatalf("folded %d, %v", res.folded, err)
	}
	got := laneLines(t, ls)
	if len(got) != len(lines)-1 || !reflect.DeepEqual(got[:len(got)-1], lines[:len(lines)-2]) {
		t.Fatalf("the backlog lost events: %d of %d kept", len(got), len(lines))
	}
	before, _ := ls.spooled()
	res, err = ls.compactTail(nil)
	after, _ := ls.spooled()
	if err != nil || res.size != 0 || len(after) != len(before) || after[0].Name() != before[0].Name() {
		t.Fatalf("rewritten again at once: %+v, %v", res, err)
	}
	writeSpool(t, ls.dir, 1<<30, lines[:len(lines)-2]...)
	writeSpool(t, ls.dir, 1<<31, lines[:4]...)
	if res, err = ls.compactTail(nil); err != nil || res.size == 0 {
		t.Fatalf("not compacted once doubled: %+v, %v", res, err)
	}
	if got := laneLines(t, ls); len(got) != 2*(len(lines)-2)+4+1 {
		t.Fatalf("%d events kept", len(got))
	}
}

// swap sets *fn to fake for the rest of the test.
func swap[F any](t *testing.T, fn *F, fake F) {
	t.Helper()
	old := *fn
	*fn = fake
	t.Cleanup(func() { *fn = old })
}

// TestP0CompactionCommitThatFailsLate: the manifest is in although its
// directory's sync failed. The chunks are then kept for the roll forward, the
// lane holds, and the next pass finishes it: no event lost or sent twice.
func TestP0CompactionCommitThatFailsLate(t *testing.T) {
	old := HousekeepEvery
	HousekeepEvery = 0
	defer func() { HousekeepEvery = old }()
	m, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	ls.lane.Compact = 1
	for i := 1; i <= 5; i++ {
		writeSpool(t, ls.dir, i, fmt.Sprintf(`{"type":"stream.state","t":1,"d":{"stream_id":1,"n":"s%d"}}`, i), fmt.Sprintf(`{"type":"conn.limit","t":1,"d":{"uuid":"u","n":"l%d"}}`, i))
	}
	failed := false
	realSync := syncDir
	swap(t, &syncDir, func(dir string) error {
		if dir == filepath.Dir(ls.manifestPath()) && !failed {
			failed = true
			return errors.New("EIO")
		}
		return realSync(dir)
	})
	if _, err := ls.compactTail(nil); err == nil || !failed {
		t.Fatal("the commit did not fail")
	}
	if _, err := os.Stat(ls.manifestPath()); err != nil {
		t.Fatal("the manifest is not in")
	}
	if _, err := os.Stat(ls.stagingDir()); err != nil {
		t.Fatal("the chunks were removed with the manifest in")
	}
	next := drain(t, a, ls, 1)
	if got := strings.Join(m.applied["p0"], ","); got != "l1,l2,l3,l4,s5,l5" || next != 7 {
		t.Fatalf("MAIN got %s (next %d)", got, next)
	}
}

// TestP0HoldsWhileARollForwardIsUnfinished: a roll forward that fails part
// way (the replaced files gone, a chunk not moved in) holds the lane in that
// pass and after, so a newer file never goes before the compaction.
func TestP0HoldsWhileARollForwardIsUnfinished(t *testing.T) {
	old := HousekeepEvery
	HousekeepEvery = 0
	defer func() { HousekeepEvery = old }()
	m, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	ls.lane.Compact = 1
	for i := 1; i <= 5; i++ {
		writeSpool(t, ls.dir, i, fmt.Sprintf(`{"type":"stream.state","t":1,"d":{"stream_id":1,"n":"s%d"}}`, i))
	}
	fails := 2
	swap(t, &moveChunk, func(src, dst string) error {
		if fails > 0 {
			fails--
			// A newer file lands while the compaction is stuck.
			writeSpool(t, ls.dir, 100+fails, `{"type":"stream.state","t":1,"d":{"stream_id":2,"n":"new"}}`)
			return errors.New("EIO")
		}
		return os.Rename(src, dst)
	})
	for pass := 0; pass < 2; pass++ {
		if _, _, err := a.shipOnce(context.Background(), ls, 1); err == nil {
			t.Fatalf("pass %d: the lane did not hold", pass)
		}
		if len(m.applied["p0"]) != 0 {
			t.Fatalf("pass %d: MAIN got %v before the compaction", pass, m.applied["p0"])
		}
	}
	// Finished, then the newer files compacted too: stream 2 once.
	drain(t, a, ls, 1)
	if got := strings.Join(m.applied["p0"], ","); got != "s5,new" {
		t.Fatalf("MAIN got %s", got)
	}
}

// TestP0RollForwardWithAChunkMissing: a committed compaction whose chunk is
// gone is given up while every replaced file is still there (the tail as it
// was), and holds the lane once one is gone: nothing is removed without the
// chunks that replace it.
func TestP0RollForwardWithAChunkMissing(t *testing.T) {
	setup := func(t *testing.T) (*eventsMain, *Agent, *laneSpool, *compactManifest) {
		m, a := newEventsMain(t)
		ls := laneFor(a, "p0")
		var tail []string
		for i := 1; i <= 3; i++ {
			tail = append(tail, writeSpool(t, ls.dir, i, fmt.Sprintf(`{"type":"stream.state","t":1,"d":{"stream_id":1,"n":"s%d"}}`, i)))
		}
		man, _, err := ls.stage(tail)
		if err != nil {
			t.Fatal(err)
		}
		if err := ls.commit(man); err != nil {
			t.Fatal(err)
		}
		os.Remove(filepath.Join(ls.stagingDir(), man.Staged[0]))
		return m, a, ls, man
	}
	t.Run("nothing replaced yet", func(t *testing.T) {
		m, a, ls, _ := setup(t)
		if err := ls.recoverCompaction(); err == nil {
			t.Fatal("no error")
		}
		if _, err := os.Stat(ls.manifestPath()); !os.IsNotExist(err) {
			t.Fatal("manifest kept")
		}
		drain(t, a, ls, 1)
		if got := strings.Join(m.applied["p0"], ","); got != "s1,s2,s3" {
			t.Fatalf("MAIN got %s", got)
		}
	})
	t.Run("a replaced file gone", func(t *testing.T) {
		m, a, ls, man := setup(t)
		os.Remove(filepath.Join(ls.dir, man.Replaced[0]))
		for pass := 0; pass < 2; pass++ {
			if _, _, err := a.shipOnce(context.Background(), ls, 1); err == nil {
				t.Fatal("the lane did not hold")
			}
		}
		if len(m.applied["p0"]) != 0 {
			t.Fatalf("MAIN got %v", m.applied["p0"])
		}
		if got := laneLines(t, ls); len(got) != 2 {
			t.Fatalf("the lane holds %v", got)
		}
		if _, err := os.Stat(ls.manifestPath()); err != nil {
			t.Fatal("manifest removed")
		}
	})
}

// TestP0CompactionMemoryIsBoundedByKeys: a large backlog of a few viewers'
// touches is compacted without holding it.
func TestP0CompactionMemoryIsBoundedByKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("large spool")
	}
	_, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	pad := strings.Repeat("x", 200)
	var total int64
	var b strings.Builder
	seq, read := 1, 0
	for total < 8<<20 {
		b.Reset()
		for i := 0; i < 4000; i++ {
			read++
			rec := hlsRec(fmt.Sprintf("v%04d", i%2000), read, 0)
			rec["user_agent"] = pad
			b.WriteString(connLine(rec))
			b.WriteByte('\n')
		}
		os.MkdirAll(ls.dir, 0o750)
		os.WriteFile(filepath.Join(ls.dir, fmt.Sprintf("%019d-1-0000.ndjson", seq)), []byte(b.String()), 0o640)
		total += int64(b.Len())
		seq++
	}
	defer debug.SetGCPercent(debug.SetGCPercent(20))
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var s runtime.MemStats
			runtime.ReadMemStats(&s)
			if s.HeapAlloc > peak.Load() {
				peak.Store(s.HeapAlloc)
			}
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	ls.lane.Compact = 1 << 20
	res, err := ls.compactTail(nil)
	close(stop)
	<-done
	if err != nil || res.size > 2<<20 {
		t.Fatalf("size %d, %v", res.size, err)
	}
	if got := laneLines(t, ls); len(got) != 2000 {
		t.Fatalf("%d events left", len(got))
	}
	grew := int64(peak.Load()) - int64(base)
	t.Logf("a %d MB backlog: heap grew by at most %d KB", total>>20, grew>>10)
	if grew > 6<<20 {
		t.Fatalf("a %d MB backlog took %d MB of heap", total>>20, grew>>20)
	}
	files, _ := ls.spooled()
	for _, f := range files {
		evs, n, _ := readSpoolFile(filepath.Join(ls.dir, f.Name()))
		if len(evs) > MaxBatchEvents || n > MaxBatchBytes {
			t.Fatalf("%s: %d events, %d bytes: more than a batch", f.Name(), len(evs), n)
		}
	}
}

func TestP0CompactionChunksFitABatch(t *testing.T) {
	oldE, oldB := MaxBatchEvents, MaxBatchBytes
	MaxBatchEvents, MaxBatchBytes = 3, 1<<10
	defer func() { MaxBatchEvents, MaxBatchBytes = oldE, oldB }()
	m, a := newEventsMain(t)
	ls := laneFor(a, "p0")
	var lines []string
	for i := 0; i < 10; i++ {
		lines = append(lines, fmt.Sprintf(`{"type":"stream.worker","t":1,"d":{"stream_id":%d,"worker":1,"n":"w%d"}}`, i, i))
	}
	writeSpool(t, ls.dir, 1, lines...)
	ls.lane.Compact = 1
	if _, err := ls.compactTail(nil); err != nil {
		t.Fatal(err)
	}
	if files, _ := ls.spooled(); len(files) != 4 {
		t.Fatalf("%d chunks", len(files))
	}
	drain(t, a, ls, 1)
	if got := strings.Join(m.applied["p0"], ","); got != "w0,w1,w2,w3,w4,w5,w6,w7,w8,w9" {
		t.Fatalf("applied %s", got)
	}
}

// TestP0CompactionSurvivesACrash: before its commit a compaction leaves the
// backlog as it was; after it, at any step of rolling forward, the next pass
// finishes it, and the lane never sends a file and its compaction both.
func TestP0CompactionSurvivesACrash(t *testing.T) {
	setup := func(t *testing.T) (*eventsMain, *Agent, *laneSpool, []string) {
		m, a := newEventsMain(t)
		ls := laneFor(a, "p0")
		var tail []string
		for i := 1; i <= 5; i++ {
			tail = append(tail, writeSpool(t, ls.dir, i, connLine(hlsRec("a", i, 0)), fmt.Sprintf(`{"type":"stream.state","t":1,"d":{"stream_id":1,"n":"s%d"}}`, i)))
		}
		ls.lane.Compact = 1
		return m, a, ls, tail
	}
	compacted := []string{connLine(hlsRec("a", 5, 0)), `{"d":{"fields":{},"n":"s5","stream_id":1},"t":1,"type":"stream.state"}`}

	t.Run("before the commit", func(t *testing.T) {
		_, a, ls, tail := setup(t)
		before := laneLines(t, ls)
		if _, _, err := ls.stage(tail); err != nil {
			t.Fatal(err)
		}
		fresh := laneFor(a, "p0")
		if err := fresh.recoverCompaction(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(fresh.stagingDir()); !os.IsNotExist(err) {
			t.Fatal("staging left behind")
		}
		if got := laneLines(t, fresh); !reflect.DeepEqual(got, before) {
			t.Fatalf("lane holds %v", got)
		}
	})
	for _, step := range []int{0, 2, 5, 6} {
		t.Run(fmt.Sprintf("after the commit, %d step(s) in", step), func(t *testing.T) {
			m, a, ls, tail := setup(t)
			man, _, err := ls.stage(tail)
			if err != nil {
				t.Fatal(err)
			}
			if err := ls.commit(man); err != nil {
				t.Fatal(err)
			}
			// The crash: some replaced files removed, maybe the chunk moved in.
			for i := 0; i < step && i < len(man.Replaced); i++ {
				os.Remove(filepath.Join(ls.dir, man.Replaced[i]))
			}
			if step > len(man.Replaced) {
				os.Rename(filepath.Join(ls.stagingDir(), man.Staged[0]), filepath.Join(ls.dir, man.Staged[0]))
			}
			fresh := laneFor(a, "p0")
			drain(t, a, fresh, 1)
			if _, err := os.Stat(fresh.manifestPath()); !os.IsNotExist(err) {
				t.Fatal("manifest left behind")
			}
			if strings.Join(m.applied["p0"], ",") != ",s5" || m.cursor["p0"] != int64(len(compacted)) {
				t.Fatalf("MAIN got %d events (%v)", m.cursor["p0"], m.applied["p0"])
			}
		})
	}
	t.Run("the rolled-forward lane", func(t *testing.T) {
		_, _, ls, _ := setup(t)
		if _, err := ls.compactTail(nil); err != nil {
			t.Fatal(err)
		}
		if got := laneLines(t, ls); !reflect.DeepEqual(got, compacted) {
			t.Fatalf("lane holds\n%s", strings.Join(got, "\n"))
		}
	})
}

func TestReadSpoolFileSkipsALineTooLongAndReadsOn(t *testing.T) {
	old := MaxBatchBytes
	MaxBatchBytes = 100
	defer func() { MaxBatchBytes = old }()
	dir := t.TempDir()
	long := fmt.Sprintf(`{"type":"stream.state","d":{"x":%q}}`, strings.Repeat("y", 200<<10))
	os.WriteFile(filepath.Join(dir, "f"), []byte(`{"type":"a","d":{}}`+"\n"+long+"\n  "+`{"type":"b","d":{}}`), 0o640)
	evs, _, err := readSpoolFile(filepath.Join(dir, "f"))
	if err != nil || len(evs) != 2 || string(evs[1]) != `{"type":"b","d":{}}` {
		t.Fatalf("%q, %v", evs, err)
	}
}
