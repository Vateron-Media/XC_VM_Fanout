package clusteragent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Events (plan, sections 7 and 8, Phase 5): the node's PHP writes events for
// MAIN into a spool, one file per write, renamed in when complete
// (Core\Cluster\EventSpool):
//
//	spool/p0/<hrtime>-<pid>-<rand>.ndjson   stream state: gap-checked, never dropped
//	spool/p1/<hrtime>-<pid>-<rand>.ndjson   logs: oldest dropped past the cap
//
// One loop per lane sends the oldest files as a batch to MAIN's `events` op,
// numbered from the lane's cursor (useq), and deletes them once MAIN has
// applied them. Before sending, the batch (its first number and its files)
// is written to <lane>.inflight, so after a crash or a lost reply the same
// files go again under the same numbers and MAIN, which applies a batch and
// its cursor together, does not apply them twice.

// Lane is one event lane.
type Lane struct {
	Name     string
	Interval time.Duration
	// Cap is the most bytes the lane's spool may hold before its oldest files
	// are dropped (reported to MAIN as a `skip`); 0 never drops.
	Cap int64
	// Compact is the size past which the lane's spool is collapsed to the
	// latest state per key instead (P0, which is never dropped); 0 never.
	Compact int64
}

// Lanes are P0 (stream state, within ~250 ms) and P1 (logs, batched).
var Lanes = []Lane{
	{Name: "p0", Interval: 200 * time.Millisecond, Compact: 128 << 20},
	{Name: "p1", Interval: 5 * time.Second, Cap: 64 << 20},
}

// Batch limits: MAIN takes up to 5000 events and 8 MB per request.
var (
	MaxBatchEvents = 2000
	MaxBatchBytes  = 1 << 20
)

// EventsResult is MAIN's reply to `events`.
type EventsResult struct {
	Useq    int64 `json:"useq"`
	Applied int   `json:"applied"`
	Dropped int   `json:"dropped"`
}

type inflight struct {
	First int64    `json:"first"`
	Count int      `json:"count"`
	Files []string `json:"files"`
}

type laneSpool struct {
	lane  Lane
	dir   string // spool/<lane>
	state string // spool/<lane>.inflight
}

// cursor is MAIN's cursor for a lane from the latest hello; -1 until one arrives.
func (a *Agent) cursor(lane string) int64 {
	if lane == "p0" {
		return a.cursorP0.Load() - 1
	}
	return a.cursorP1.Load() - 1
}

// RunEvents sends one lane's spool to MAIN until ctx ends or MAIN stops the node.
func (a *Agent) RunEvents(ctx context.Context, lane Lane) {
	ls := &laneSpool{lane: lane, dir: filepath.Join(a.SpoolDir, lane.Name), state: filepath.Join(a.SpoolDir, lane.Name+".inflight")}
	var next int64 // the number the next event gets; 0 until MAIN's cursor is known
	backoff := lane.Interval
	for sleep(ctx, backoff) {
		backoff = lane.Interval
		if next == 0 {
			c := a.cursor(lane.Name)
			if c < 0 {
				continue
			}
			next = c + 1
		}
		n, err := a.shipOnce(ctx, ls, next)
		if n > 0 {
			next = n
		}
		if err != nil {
			if fatal(err) || ctx.Err() != nil {
				return
			}
			if !errors.Is(err, ErrNoEpoch) {
				a.logf("cluster: events %s: %v", lane.Name, err)
			}
			backoff = min(max(time.Second, lane.Interval*2), 30*time.Second)
		}
	}
}

// shipOnce sends at most one batch (resending an in-flight one first) and
// returns the next number to use.
func (a *Agent) shipOnce(ctx context.Context, ls *laneSpool, next int64) (int64, error) {
	fl, err := ls.loadInflight()
	if err != nil {
		return next, err
	}
	switch {
	case fl == nil:
		if ls.lane.Cap > 0 {
			ls.enforceCap()
		}
		if ls.lane.Compact > 0 {
			if n, err := ls.compact(); err != nil {
				a.logf("cluster: events %s: compacting: %v", ls.lane.Name, err)
			} else if n > 0 {
				a.logf("cluster: events %s: backlog past %d MB collapsed (%d events folded)", ls.lane.Name, ls.lane.Compact>>20, n)
			}
		}
		files, events, err := ls.collect()
		if err != nil || len(events) == 0 {
			return next, err
		}
		fl = &inflight{First: next, Count: len(events), Files: files}
		if err := ls.saveInflight(fl); err != nil {
			return next, err
		}
	case fl.First+int64(fl.Count)-1 < next:
		// Resumed after a restart and MAIN's cursor already covers it: applied.
		ls.finish(fl)
		return next, nil
	case fl.First != next:
		// Not applied, and MAIN expects another number (its cursor moved back).
		fl.First = next
		if err := ls.saveInflight(fl); err != nil {
			return next, err
		}
	}
	events, err := ls.read(fl.Files)
	if err != nil {
		return next, err
	}
	if len(events) != fl.Count {
		// Only this loop deletes spool files; a count that changed means the
		// spool was tampered with. Drop the batch rather than guess.
		a.logf("cluster: events %s: in-flight batch changed on disk; dropping it", ls.lane.Name)
		ls.finish(fl)
		return next, nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		var out EventsResult
		err = a.Client.Call(ctx, "events", map[string]any{"lane": ls.lane.Name, "first_useq": fl.First, "events": events}, &out, false)
		var d *Denial
		if errors.As(err, &d) && d.Reason == "USEQ_GAP" {
			expected := expectedUseq(d)
			if expected <= 0 {
				return next, err
			}
			if expected > fl.First+int64(fl.Count)-1 {
				ls.finish(fl) // already applied
				return expected, nil
			}
			fl.First = expected
			if err := ls.saveInflight(fl); err != nil {
				return next, err
			}
			next = expected
			continue
		}
		if err != nil {
			return next, err
		}
		if out.Dropped > 0 {
			a.logf("cluster: events %s: MAIN refused %d of %d (flow off?)", ls.lane.Name, out.Dropped, fl.Count)
		}
		ls.finish(fl)
		return max(fl.First+int64(fl.Count), out.Useq+1), nil
	}
	return next, errors.New("clusteragent: events kept hitting a gap")
}

func expectedUseq(d *Denial) int64 {
	var doc struct {
		Expected int64 `json:"expected_useq"`
	}
	if json.Unmarshal(d.Doc, &doc) != nil {
		return 0
	}
	return doc.Expected
}

// spooled lists the lane's complete files, oldest first.
func (ls *laneSpool) spooled() ([]os.DirEntry, error) {
	entries, err := os.ReadDir(ls.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := entries[:0]
	for _, e := range entries {
		if e.Type().IsRegular() && !strings.HasPrefix(e.Name(), ".") && strings.HasSuffix(e.Name(), ".ndjson") {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out, nil
}

// collect takes the oldest files up to the batch limits (at least one).
func (ls *laneSpool) collect() ([]string, []json.RawMessage, error) {
	entries, err := ls.spooled()
	if err != nil {
		return nil, nil, err
	}
	var files []string
	var events []json.RawMessage
	size := 0
	for _, e := range entries {
		evs, n, err := readSpoolFile(filepath.Join(ls.dir, e.Name()))
		if err != nil {
			continue
		}
		if len(evs) == 0 {
			os.Remove(filepath.Join(ls.dir, e.Name())) // nothing usable in it
			continue
		}
		if len(files) > 0 && (len(events)+len(evs) > MaxBatchEvents || size+n > MaxBatchBytes) {
			break
		}
		files = append(files, e.Name())
		events = append(events, evs...)
		size += n
	}
	return files, events, nil
}

func (ls *laneSpool) read(files []string) ([]json.RawMessage, error) {
	var events []json.RawMessage
	for _, f := range files {
		evs, _, err := readSpoolFile(filepath.Join(ls.dir, f))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		events = append(events, evs...)
	}
	return events, nil
}

// readSpoolFile returns the file's events (one JSON object per line; a line
// that is not one is skipped) and its size.
func readSpoolFile(path string) ([]json.RawMessage, int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	var out []json.RawMessage
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 64<<10), MaxBatchBytes)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		var probe struct {
			Type string          `json:"type"`
			D    json.RawMessage `json:"d"`
		}
		if len(line) == 0 || json.Unmarshal(line, &probe) != nil || probe.Type == "" {
			continue
		}
		out = append(out, append(json.RawMessage{}, line...))
	}
	return out, len(b), nil
}

// enforceCap drops the oldest files while the lane holds more than its cap.
func (ls *laneSpool) enforceCap() {
	entries, err := ls.spooled()
	if err != nil {
		return
	}
	var total int64
	sizes := make([]int64, len(entries))
	for i, e := range entries {
		if info, err := e.Info(); err == nil {
			sizes[i] = info.Size()
			total += sizes[i]
		}
	}
	dropped := 0
	for i := 0; total > ls.lane.Cap && i < len(entries); i++ {
		path := filepath.Join(ls.dir, entries[i].Name())
		evs, _, _ := readSpoolFile(path)
		if os.Remove(path) == nil {
			dropped += len(evs)
			total -= sizes[i]
		}
	}
	if dropped > 0 {
		// Reported to MAIN as a `skip`, from a file that sorts before any PHP
		// spool file, so it goes in the next batch and survives a restart.
		line, _ := json.Marshal(map[string]any{"type": "skip", "t": time.Now().UnixMilli(), "d": map[string]int{"count": dropped}})
		name := fmt.Sprintf("%019d-skip-%d.ndjson", 0, time.Now().UnixNano())
		if err := os.WriteFile(filepath.Join(ls.dir, "."+name+".tmp"), append(line, '\n'), 0o640); err == nil {
			os.Rename(filepath.Join(ls.dir, "."+name+".tmp"), filepath.Join(ls.dir, name))
		}
	}
}

func (ls *laneSpool) loadInflight() (*inflight, error) {
	b, err := os.ReadFile(ls.state)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var fl inflight
	if json.Unmarshal(b, &fl) != nil || fl.First < 1 || len(fl.Files) == 0 {
		os.Remove(ls.state) // unreadable: its files go again as a new batch
		return nil, nil
	}
	return &fl, nil
}

// saveInflight persists the batch before it is sent (written aside, synced, renamed in).
func (ls *laneSpool) saveInflight(fl *inflight) error {
	b, _ := json.Marshal(fl)
	tmp := ls.state + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, ls.state)
}

// finish deletes an applied batch's files, then its in-flight record.
func (ls *laneSpool) finish(fl *inflight) {
	for _, f := range fl.Files {
		os.Remove(filepath.Join(ls.dir, filepath.Base(f)))
	}
	os.Remove(ls.state)
}

// compact collapses a backlog past the lane's Compact size to the latest
// state per key (the plan's p0_reset): P0 events describe state, so only the
// last word on each stream row, worker, recording or movie matters, merged
// where an event carries part of it. Events of other types are kept as they
// are. The result replaces the oldest file, so it still goes first; the rest
// are removed after. A crash between the two only repeats state, which MAIN
// applies idempotently. Returns how many events were folded away.
func (ls *laneSpool) compact() (int, error) {
	entries, err := ls.spooled()
	if err != nil || len(entries) < 2 {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	if total <= ls.lane.Compact {
		return 0, nil
	}
	type slot struct {
		last int
		ev   map[string]any
	}
	slots := map[string]*slot{}
	var order []string
	n := 0
	for _, e := range entries {
		evs, _, err := readSpoolFile(filepath.Join(ls.dir, e.Name()))
		if err != nil {
			return 0, err
		}
		for _, raw := range evs {
			var ev map[string]any
			if json.Unmarshal(raw, &ev) != nil {
				continue
			}
			key, merge := compactKey(ev, n)
			n++
			if s, ok := slots[key]; ok {
				if merge != "" {
					ev = mergeInto(s.ev, ev, merge)
				}
				s.ev, s.last = ev, n
			} else {
				slots[key] = &slot{last: n, ev: ev}
				order = append(order, key)
			}
		}
	}
	sort.SliceStable(order, func(i, j int) bool { return slots[order[i]].last < slots[order[j]].last })
	var body []byte
	for _, k := range order {
		line, _ := json.Marshal(slots[k].ev)
		body = append(append(body, line...), '\n')
	}
	first := filepath.Join(ls.dir, entries[0].Name())
	tmp := filepath.Join(ls.dir, "."+entries[0].Name()+".compact.tmp")
	if err := os.WriteFile(tmp, body, 0o640); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, first); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	for _, e := range entries[1:] {
		os.Remove(filepath.Join(ls.dir, e.Name()))
	}
	return n - len(order), nil
}

// compactKey is what an event describes, and which part of it merges ("" to
// replace): the latest event for a key carries the state.
func compactKey(ev map[string]any, i int) (string, string) {
	d, _ := ev["d"].(map[string]any)
	id := func(k string) string { return fmt.Sprint(d[k]) }
	switch ev["type"] {
	case "stream.state":
		if _, ok := d["ssid"]; ok {
			return "state:r" + id("ssid"), "fields"
		}
		return "state:s" + id("stream_id") + ":" + id("server_id"), "fields"
	case "stream.monitor":
		return "monitor:" + id("stream_id"), ""
	case "stream.worker":
		return "worker:" + id("stream_id") + ":" + id("worker"), ""
	case "recording.state":
		return "recording:" + id("id"), ""
	case "vod.analysis":
		return "vod:" + id("stream_id"), "props"
	}
	return fmt.Sprintf("keep:%d", i), ""
}

// mergeInto lays the newer event's part over the older one's.
func mergeInto(older, newer map[string]any, part string) map[string]any {
	od, _ := older["d"].(map[string]any)
	nd, _ := newer["d"].(map[string]any)
	op, _ := od[part].(map[string]any)
	np, _ := nd[part].(map[string]any)
	merged := map[string]any{}
	for k, v := range op {
		merged[k] = v
	}
	for k, v := range np {
		merged[k] = v
	}
	nd[part] = merged
	return newer
}
