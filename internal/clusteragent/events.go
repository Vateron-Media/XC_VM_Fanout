package clusteragent

import (
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
//	spool/p0/<hrtime>-<pid>-<rand>.ndjson   state: gap-checked, never dropped (compacted, compact.go)
//	spool/p1/<hrtime>-<pid>-<rand>.ndjson   logs: oldest dropped past the cap
//
// One loop per lane sends the oldest files as a batch to MAIN's `events` op,
// numbered from the lane's cursor (useq), and deletes them once MAIN has
// applied them. Before sending, the batch (its first number and its files)
// is written to <lane>.inflight, so after a crash or a lost reply the same
// files go again under the same numbers and MAIN, which applies a batch and
// its cursor together, does not apply them twice.
//
// A lane's bounds (P1's cap, P0's compaction, compact.go) apply
// to the backlog behind the in-flight batch, and hold while that batch is
// stuck (MAIN out of reach) and before MAIN's cursor is known: the in-flight
// files themselves are never touched, so a resend is the same batch byte for
// byte.

// Lane is one event lane.
type Lane struct {
	Name     string
	Interval time.Duration
	// Cap is the most bytes the lane's spool may hold before its oldest files
	// are dropped (reported to MAIN as a `skip`); 0 never drops.
	Cap int64
	// Compact is the size past which the lane's spool is collapsed to the
	// latest state per key instead (P0, which is never dropped, compact.go);
	// 0 never.
	Compact int64
}

// Lanes are P0 (stream state, within ~250 ms) and P1 (logs, batched).
var Lanes = []Lane{
	{Name: "p0", Interval: 200 * time.Millisecond, Compact: 128 << 20},
	{Name: "p1", Interval: 5 * time.Second, Cap: 64 << 20},
}

// HousekeepEvery is the most often a lane's backlog is measured against its
// bounds.
var HousekeepEvery = time.Second

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
	// checked is when the backlog was last measured (housekeep); compacted
	// is its size after the last compaction, 0 once it fell below Compact.
	// retryAt is when a compaction that failed is tried again, after
	// failures in a row (compactTail).
	checked   time.Time
	compacted int64
	retryAt   time.Time
	failures  int
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
	iv := newLaneInterval(lane.Interval)
	backoff := lane.Interval
	for sleep(ctx, backoff) {
		backoff = iv.cur
		if next == 0 {
			c := a.cursor(lane.Name)
			if c < 0 {
				// No hello yet (MAIN out of reach since the agent started):
				// nothing is sent, but the backlog is still bounded. An
				// in-flight record that cannot be read skips the pass: its
				// batch's files would be taken for the tail and compacted,
				// and MAIN may already hold them under their numbers.
				if fl, err := ls.loadInflight(); err == nil {
					ls.housekeep(fl, a.logf)
				} else {
					a.logf("cluster: events %s: %v", lane.Name, err)
				}
				continue
			}
			next = c + 1
		}
		n, served, err := a.shipOnce(ctx, ls, next)
		if n > 0 {
			next = n
		}
		wait, busy := iv.next(served, err)
		backoff = wait
		if err != nil {
			if fatal(err) || ctx.Err() != nil {
				return
			}
			if busy {
				// MAIN's permits for the lane are all held: busy, not
				// failing. The in-flight batch goes again, before any other.
				a.busyRefusals.Add(1)
				continue
			}
			if !errors.Is(err, ErrNoEpoch) {
				a.logf("cluster: events %s: %v", lane.Name, err)
			}
		}
	}
}

// shipOnce sends at most one batch (resending an in-flight one first) and
// returns the next number to use, and whether MAIN served a batch.
func (a *Agent) shipOnce(ctx context.Context, ls *laneSpool, next int64) (int64, bool, error) {
	fl, err := ls.loadInflight()
	if err != nil {
		return next, false, err
	}
	if !ls.housekeep(fl, a.logf) {
		// A compaction a crash interrupted could not be finished: sending
		// now could send a file and its compaction both.
		return next, false, errors.New("clusteragent: the lane's compaction is unfinished")
	}
	switch {
	case fl == nil:
		files, events, err := ls.collect()
		if err != nil || len(events) == 0 {
			return next, false, err
		}
		fl = &inflight{First: next, Count: len(events), Files: files}
		if err := ls.saveInflight(fl); err != nil {
			return next, false, err
		}
	case fl.First+int64(fl.Count)-1 < next:
		// Resumed after a restart and MAIN's cursor already covers it: applied.
		ls.finish(fl)
		return next, false, nil
	case fl.First != next:
		// Not applied, and MAIN expects another number (its cursor moved back).
		fl.First = next
		if err := ls.saveInflight(fl); err != nil {
			return next, false, err
		}
	}
	events, err := ls.read(fl.Files)
	if err != nil {
		return next, false, err
	}
	if len(events) != fl.Count {
		// Only this loop deletes spool files; a count that changed means the
		// spool was tampered with. Drop the batch rather than guess.
		a.logf("cluster: events %s: in-flight batch changed on disk; dropping it", ls.lane.Name)
		ls.finish(fl)
		return next, false, nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		var out EventsResult
		batch := map[string]any{"lane": ls.lane.Name, "first_useq": fl.First, "events": events}
		if ls.lane.Name == "p0" {
			err = a.Client.CallP0(ctx, batch, &out)
		} else {
			err = a.Client.Call(ctx, "events", batch, &out, false)
		}
		var d *Denial
		if errors.As(err, &d) && d.Reason == "USEQ_GAP" {
			expected := expectedUseq(d)
			if expected <= 0 {
				return next, false, err
			}
			if expected > fl.First+int64(fl.Count)-1 {
				ls.finish(fl) // already applied
				return expected, false, nil
			}
			fl.First = expected
			if err := ls.saveInflight(fl); err != nil {
				return next, false, err
			}
			next = expected
			continue
		}
		if err != nil {
			return next, false, err
		}
		if out.Dropped > 0 {
			a.logf("cluster: events %s: MAIN refused %d of %d (flow off?)", ls.lane.Name, out.Dropped, fl.Count)
		}
		ls.finish(fl)
		return max(fl.First+int64(fl.Count), out.Useq+1), true, nil
	}
	return next, false, errors.New("clusteragent: events kept hitting a gap")
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
// that is not one, or is longer than a batch, is skipped) and its size.
func readSpoolFile(path string) ([]json.RawMessage, int, error) {
	var out []json.RawMessage
	n, err := eachLine(path, MaxBatchBytes, func(_ int64, line []byte) error {
		var probe struct {
			Type string          `json:"type"`
			D    json.RawMessage `json:"d"`
		}
		if json.Unmarshal(line, &probe) == nil && probe.Type != "" {
			out = append(out, append(json.RawMessage{}, line...))
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return out, int(n), nil
}

// housekeep bounds the backlog behind the in-flight batch fl (nil: none):
// it first finishes a compaction a crash interrupted (false when it cannot,
// and nothing may be collected), then, at most every HousekeepEvery, drops
// P1's oldest files past its cap or compacts P0 (compact.go). The in-flight
// batch's files are left as they are.
func (ls *laneSpool) housekeep(fl *inflight, logf func(string, ...any)) bool {
	if err := ls.recoverCompaction(); err != nil {
		logf("cluster: events %s: finishing a compaction: %v", ls.lane.Name, err)
		if _, statErr := os.Stat(ls.manifestPath()); statErr == nil {
			return false
		}
	}
	if (ls.lane.Cap == 0 && ls.lane.Compact == 0) || time.Since(ls.checked) < HousekeepEvery {
		return true
	}
	ls.checked = time.Now()
	exclude := map[string]bool{}
	if fl != nil {
		for _, f := range fl.Files {
			exclude[filepath.Base(f)] = true
		}
	}
	if ls.lane.Cap > 0 {
		ls.enforceCap(exclude)
	}
	if ls.lane.Compact > 0 {
		res, err := ls.compactTail(exclude)
		switch {
		case err != nil:
			logf("cluster: events %s: compacting: %v", ls.lane.Name, err)
			// Committed but not rolled forward: the lane holds until the
			// next pass finishes it, or it would send a file and its
			// compaction both.
			if _, statErr := os.Stat(ls.manifestPath()); statErr == nil {
				return false
			}
		case res.folded > 0:
			logf("cluster: events %s: backlog past %d MB collapsed (%d events folded, %d MB left)", ls.lane.Name, ls.lane.Compact>>20, res.folded, res.size>>20)
		}
	}
	return true
}

// enforceCap drops the oldest files (but those in exclude, the in-flight
// batch's) while the lane holds more than its cap.
func (ls *laneSpool) enforceCap(exclude map[string]bool) {
	entries, err := ls.spooled()
	if err != nil {
		return
	}
	var total int64
	sizes := make([]int64, len(entries))
	for i, e := range entries {
		if exclude[e.Name()] {
			continue
		}
		if info, err := e.Info(); err == nil {
			sizes[i] = info.Size()
			total += sizes[i]
		}
	}
	dropped := 0
	for i := 0; total > ls.lane.Cap && i < len(entries); i++ {
		if exclude[entries[i].Name()] {
			continue
		}
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
		writeSkip(ls.dir, dropped, fmt.Sprint(time.Now().UnixNano()))
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
	return writeFile(ls.state, b, fileWrite{perm: 0o640})
}

// finish deletes an applied batch's files, then its in-flight record.
func (ls *laneSpool) finish(fl *inflight) {
	for _, f := range fl.Files {
		os.Remove(filepath.Join(ls.dir, filepath.Base(f)))
	}
	os.Remove(ls.state)
}
