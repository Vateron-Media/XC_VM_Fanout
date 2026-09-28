package clusteragent

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode"
)

// P0 compaction (plan, section 8; ADR 0004, "P0 compaction"). P0 is never
// dropped, so a long MAIN outage would grow it without end; past the lane's
// Compact size the agent collapses the backlog behind the in-flight batch
// instead, keeping the latest state per key. The in-flight batch is never
// touched: MAIN may already have applied it under its numbers, and it goes
// again byte for byte. The rest (the tail) is rewritten:
//
//   - stream.state per row, with its fields merged; stream.monitor per
//     stream; stream.worker per stream and worker; recording.state per
//     recording; vod.analysis per movie, with its props merged; node.state,
//     with its fields merged. Each goes where its key's last event was.
//   - conn.upsert: a run of one viewer's upserts whose records differ only in
//     hls_last_read (its throttled P0 touches, touchesToP0's catch-up) is one
//     upsert, where the run began, carrying the run's last record. Anything
//     else about the viewer (another key, hls_end either way, pid, adm_uuid)
//     and its conn.remove or conn.close end the run: they stay, in order.
//     MAIN (ConnectionIngest) applies an upsert as a create-or-overwrite of
//     the whole record, so the store ends with the same row. The viewer is in
//     the store from the same point and is open or ended at every point just
//     as before, so the limits MAIN drains meanwhile (conn.limit, queued and
//     read later against the store) count it the same, and its reservation is
//     released at the same point. Only its hls_last_read is newer earlier,
//     which ends no one sooner. Nothing is folded across a remove or a close:
//     conn.close writes the viewer's activity row from the row the upserts
//     made, and an upsert before a remove is what released its reservation.
//   - Every other event (conn.remove, conn.close, conn.limit,
//     security.block_ip, a type this agent does not know) stays as it is, in
//     order.
//
// Nothing is dropped, whatever the size: a compaction only removes events
// whose state a later event of the same key carries. So a backlog of events
// that do not fold (conn.limit, security.block_ip, viewers' opens and ends)
// is only as bounded as the outage; compaction then backs off (below) rather
// than rewrite it on every pass. A compaction that fails before its commit
// (a full disk, above all) backs off too, and one is not begun without room
// for its copy (compactTail).
//
// Memory is bounded by what folds, not by the backlog: the tail is read
// twice, a line at a time, and only a slot per key (with the merged part), a
// run per viewer and the position of each multi-event run are held. The
// result is written as chunk files of at most MaxBatchEvents events and
// MaxBatchBytes each, so every one fits a batch. They replace the tail and
// sort where its oldest file was, so the result still goes first.
//
// Crash safety: the chunks are written (synced) into a staging directory
// beside the lane, then a manifest naming the files they replace and the
// chunks is written aside, synced and renamed in; that is the commit. Rolling
// forward removes the replaced files, then moves the chunks in, then removes
// the manifest. A crash before the commit leaves the tail as it was (the
// staging directory is removed); after it, the next pass rolls forward before
// the lane collects anything, so the lane never sends both a file and its
// compaction. A roll forward removes a replaced file only once every chunk is
// in staging or already moved in; a committed compaction it cannot finish
// (a chunk lost, a failed rename) keeps its manifest and holds the lane: the
// lane sends nothing until it is finished, and no event is lost.

// compaction is what a compaction did.
type compaction struct {
	folded int   // events folded into another
	size   int64 // the compacted tail's size
}

// compactManifest is the commit record of a compaction.
type compactManifest struct {
	Gen      string   `json:"gen"`
	Replaced []string `json:"replaced"`
	Staged   []string `json:"staged"`
}

func (ls *laneSpool) manifestPath() string { return ls.dir + ".compact" }

func (ls *laneSpool) stagingDir() string {
	return filepath.Join(filepath.Dir(ls.dir), "."+filepath.Base(ls.dir)+".compact")
}

// compactTail compacts the tail (the lane's files not in exclude, the
// in-flight batch) when it is past the lane's threshold.
func (ls *laneSpool) compactTail(exclude map[string]bool) (compaction, error) {
	entries, err := ls.spooled()
	if err != nil {
		return compaction{}, err
	}
	var tail []string
	var total int64
	for _, e := range entries {
		if exclude[e.Name()] {
			continue
		}
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
		tail = append(tail, e.Name())
	}
	if total < ls.lane.Compact {
		ls.compacted = 0 // caught up: the next backlog starts from the lane's size
	}
	// Past the lane's size, and past twice what the last compaction left, so
	// a backlog of that many events that do not fold is not rewritten on
	// every pass.
	threshold := max(ls.lane.Compact, 2*ls.compacted)
	if len(tail) == 0 || total <= threshold || time.Now().Before(ls.retryAt) {
		return compaction{}, nil
	}
	// The copy is staged on the lane's filesystem, and PHP's spool and the
	// agent's own P0 writes share it: a compaction that would leave less
	// than compactReserve free is not begun, rather than fill the disk and
	// have those writes fail (and their events lost at the source).
	if free, err := freeSpace(ls.dir); err == nil && free < total+compactReserve {
		return compaction{}, ls.compactFailed(fmt.Errorf("%d MB free, %d MB backlog: no room to compact it", free>>20, total>>20))
	}
	m, res, err := ls.stage(tail)
	if err != nil {
		return compaction{}, ls.compactFailed(err)
	}
	if err := ls.commit(m); err != nil {
		// The manifest may be in although the commit failed (the rename went
		// through, the directory's sync did not): the chunks are then the
		// only copy of the tail once a roll forward begins, so they stay for
		// it (housekeep's next pass finishes it, whatever the back-off).
		// Staging is discarded only while nothing was committed.
		if _, statErr := os.Stat(ls.manifestPath()); errors.Is(statErr, os.ErrNotExist) {
			os.RemoveAll(ls.stagingDir())
			return compaction{}, ls.compactFailed(err)
		}
		return compaction{}, err
	}
	ls.failures, ls.retryAt = 0, time.Time{}
	if err := ls.rollForward(m); err != nil {
		return compaction{}, err
	}
	ls.compacted = res.size
	return res, nil
}

// compactFailed backs a compaction that was given up (nothing committed) off:
// the next is tried CompactRetry later, doubling with each failure in a row
// up to CompactRetryMax. The likeliest failure during an outage is a full
// disk, and a pass that reads the whole tail and writes it again every
// second would keep the disk full and busy.
func (ls *laneSpool) compactFailed(err error) error {
	wait := CompactRetryMax
	if ls.failures < 16 {
		wait = min(CompactRetry<<ls.failures, CompactRetryMax)
	}
	ls.failures++
	ls.retryAt = time.Now().Add(wait)
	return fmt.Errorf("%w (next try in %s)", err, wait)
}

// CompactRetry and CompactRetryMax space the compactions tried after one
// failed (compactFailed).
var (
	CompactRetry    = 10 * time.Second
	CompactRetryMax = 10 * time.Minute
)

// compactReserve is the free space a compaction leaves on the lane's
// filesystem, past the copy it stages.
const compactReserve = 64 << 20

// freeSpace is the space free to the agent on dir's filesystem. A variable,
// so the tests can fake a full disk.
var freeSpace = func(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// spoolEvent is what compaction needs to know of one event.
type spoolEvent struct {
	kind int
	key  string // the slot's key, or the viewer's uuid
	part string // a slot's part that merges ("" replaces)
	fp   [32]byte
	d    map[string]json.RawMessage
}

const (
	evKeep    = iota // stays as it is, where it is
	evSlot           // the latest per key, where the key's last event was
	evUpsert         // conn.upsert: folds into its run
	evConnEnd        // conn.remove or conn.close: ends the viewer's run
)

// classify reads one spool line; false for a line readSpoolFile would skip.
func classify(line []byte) (spoolEvent, bool) {
	var ev struct {
		Type string          `json:"type"`
		D    json.RawMessage `json:"d"`
	}
	if json.Unmarshal(line, &ev) != nil || ev.Type == "" {
		return spoolEvent{}, false
	}
	var d map[string]json.RawMessage
	if json.Unmarshal(ev.D, &d) != nil || d == nil {
		return spoolEvent{kind: evKeep}, true
	}
	id := func(k string) string {
		var v any
		json.Unmarshal(d[k], &v)
		return fmt.Sprint(v)
	}
	slot := func(key, part string) (spoolEvent, bool) {
		return spoolEvent{kind: evSlot, key: key, part: part, d: d}, true
	}
	switch ev.Type {
	case "stream.state":
		if _, ok := d["ssid"]; ok {
			return slot("state:r"+id("ssid"), "fields")
		}
		return slot("state:s"+id("stream_id")+":"+id("server_id"), "fields")
	case "stream.monitor":
		return slot("monitor:"+id("stream_id"), "")
	case "stream.worker":
		return slot("worker:"+id("stream_id")+":"+id("worker"), "")
	case "recording.state":
		return slot("recording:"+id("id"), "")
	case "vod.analysis":
		return slot("vod:"+id("stream_id"), "props")
	case "node.state":
		return slot("node", "fields")
	case "conn.upsert":
		var rec map[string]json.RawMessage
		var uuid string
		if json.Unmarshal(d["record"], &rec) != nil || json.Unmarshal(rec["uuid"], &uuid) != nil || !connUUID.MatchString(uuid) {
			return spoolEvent{kind: evKeep}, true
		}
		// The event but for the record's hls_last_read: two upserts with
		// the same differ in that alone. Values are compared as spooled (a
		// value written another way only keeps two upserts apart).
		delete(rec, "hls_last_read")
		delete(d, "record")
		h := sha256.New()
		for _, m := range []map[string]json.RawMessage{d, rec} {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(h, "%d:%s%d:", len(k), k, len(m[k]))
				h.Write(m[k])
			}
			h.Write([]byte{'|'})
		}
		var fp [32]byte
		h.Sum(fp[:0])
		return spoolEvent{kind: evUpsert, key: uuid, fp: fp}, true
	case "conn.remove", "conn.close":
		var uuid string
		if json.Unmarshal(d["uuid"], &uuid) != nil {
			return spoolEvent{kind: evKeep}, true
		}
		return spoolEvent{kind: evConnEnd, key: uuid}, true
	}
	return spoolEvent{kind: evKeep}, true
}

// lineAt is where a line is: the tail's file, its offset and length.
type lineAt struct {
	file int
	off  int64
	n    int
}

type slotState struct {
	last  int64 // ordinal of the key's last event
	count int
	part  map[string]json.RawMessage // the merged part so far
}

type connRun struct {
	first, last int64
	fp          [32]byte
	at          lineAt // the run's last event
}

// stage writes the tail's compaction into the staging directory and returns
// its manifest; nothing in the lane changes.
func (ls *laneSpool) stage(tail []string) (*compactManifest, compaction, error) {
	staging := ls.stagingDir()
	os.RemoveAll(staging)
	if err := os.MkdirAll(staging, 0o750); err != nil {
		return nil, compaction{}, err
	}
	fail := func(err error) (*compactManifest, compaction, error) {
		os.RemoveAll(staging)
		return nil, compaction{}, err
	}

	// Pass 1: the last event of every key, and the viewers' runs.
	slots := map[string]*slotState{}
	open := map[string]*connRun{}   // the viewer's current run
	runs := map[string][]*connRun{} // its runs of more than one upsert, in order
	var n int64
	endRun := func(uuid string) {
		if r := open[uuid]; r != nil && r.last > r.first {
			runs[uuid] = append(runs[uuid], r)
		}
		delete(open, uuid)
	}
	for fi, name := range tail {
		_, err := eachLine(filepath.Join(ls.dir, name), MaxBatchBytes, func(off int64, line []byte) error {
			ev, ok := classify(line)
			if !ok {
				return nil
			}
			i := n
			n++
			switch ev.kind {
			case evSlot:
				s := slots[ev.key]
				if s == nil {
					s = &slotState{}
					slots[ev.key] = s
				}
				if ev.part != "" {
					var np map[string]json.RawMessage
					json.Unmarshal(ev.d[ev.part], &np)
					if s.part == nil {
						s.part = map[string]json.RawMessage{}
					}
					for k, v := range np {
						s.part[k] = append(json.RawMessage(nil), v...)
					}
				}
				s.last = i
				s.count++
			case evUpsert:
				at := lineAt{fi, off, len(line)}
				if r := open[ev.key]; r != nil && r.fp == ev.fp {
					r.last, r.at = i, at
					break
				}
				endRun(ev.key)
				open[ev.key] = &connRun{first: i, last: i, fp: ev.fp, at: at}
			case evConnEnd:
				endRun(ev.key)
			}
			return nil
		})
		if err != nil {
			return fail(err)
		}
	}
	for uuid := range open {
		endRun(uuid)
	}

	// Pass 2: write what stays, in order.
	gen := fmt.Sprintf("%019d%04x", monotonicNs(), rand.Intn(0x10000))
	cw := &chunkWriter{dir: staging, prefix: spoolPrefix(tail[0]), gen: gen}
	var res compaction
	n = 0
	for _, name := range tail {
		_, err := eachLine(filepath.Join(ls.dir, name), MaxBatchBytes, func(_ int64, line []byte) error {
			ev, ok := classify(line)
			if !ok {
				return nil
			}
			i := n
			n++
			switch ev.kind {
			case evSlot:
				s := slots[ev.key]
				if s.last != i {
					return nil
				}
				if s.count > 1 && ev.part != "" {
					merged, err := withPart(line, ev.part, s.part)
					if err != nil {
						return err
					}
					line = merged
				}
				return cw.write(line)
			case evUpsert:
				q := runs[ev.key]
				if len(q) == 0 || i < q[0].first {
					return cw.write(line) // a run of one
				}
				r := q[0]
				if i == r.last {
					runs[ev.key] = q[1:]
				}
				if i != r.first {
					return nil // folded into the run's first place
				}
				last, err := readLineAt(filepath.Join(ls.dir, tail[r.at.file]), r.at)
				if err != nil {
					return err
				}
				return cw.write(last)
			}
			return cw.write(line)
		})
		if err != nil {
			cw.close()
			return fail(err)
		}
	}
	if err := cw.close(); err != nil {
		return fail(err)
	}
	if err := syncDir(staging); err != nil {
		return fail(err)
	}
	res.folded = int(n) - cw.events
	res.size = cw.total
	return &compactManifest{Gen: gen, Replaced: tail, Staged: cw.names}, res, nil
}

// withPart is the event line with its d[part] replaced by merged.
func withPart(line []byte, part string, merged map[string]json.RawMessage) ([]byte, error) {
	var top, d map[string]json.RawMessage
	if err := json.Unmarshal(line, &top); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(top["d"], &d); err != nil {
		return nil, err
	}
	var err error
	if d[part], err = json.Marshal(merged); err != nil {
		return nil, err
	}
	if top["d"], err = json.Marshal(d); err != nil {
		return nil, err
	}
	return json.Marshal(top)
}

// spoolPrefix is a spool file name's first field (the writer's clock), under
// which the chunks of its compaction sort.
func spoolPrefix(name string) string {
	name = strings.TrimSuffix(name, ".ndjson")
	if i := strings.IndexByte(name, '-'); i > 0 {
		return name[:i]
	}
	return name
}

func readLineAt(path string, at lineAt) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b := make([]byte, at.n)
	if _, err := f.ReadAt(b, at.off); err != nil {
		return nil, err
	}
	return b, nil
}

// chunkWriter writes the compacted lines as files that each fit a batch:
// <prefix>-compact-<gen>-<n>.ndjson, which sort in order, before the files
// spooled after the compaction's first one.
type chunkWriter struct {
	dir, prefix, gen string
	f                *os.File
	w                *bufio.Writer
	n, bytes         int
	names            []string
	events           int
	total            int64
}

func (cw *chunkWriter) write(line []byte) error {
	if cw.f != nil && (cw.n+1 > MaxBatchEvents || cw.bytes+len(line)+1 > MaxBatchBytes) {
		if err := cw.close(); err != nil {
			return err
		}
	}
	if cw.f == nil {
		name := fmt.Sprintf("%s-compact-%s-%06d.ndjson", cw.prefix, cw.gen, len(cw.names))
		f, err := os.OpenFile(filepath.Join(cw.dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
		if err != nil {
			return err
		}
		cw.f, cw.w, cw.n, cw.bytes = f, bufio.NewWriterSize(f, 64<<10), 0, 0
		cw.names = append(cw.names, name)
	}
	cw.w.Write(line)
	if err := cw.w.WriteByte('\n'); err != nil {
		return err
	}
	cw.n++
	cw.events++
	cw.bytes += len(line) + 1
	cw.total += int64(len(line) + 1)
	return nil
}

// close flushes and syncs the current chunk.
func (cw *chunkWriter) close() error {
	if cw.f == nil {
		return nil
	}
	f := cw.f
	cw.f = nil
	if err := cw.w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// commit writes the manifest (aside, synced, renamed in): from here the
// compaction is rolled forward, even after a crash.
func (ls *laneSpool) commit(m *compactManifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	path := ls.manifestPath()
	if err := writeSynced(path+".tmp", b); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// rollForward replaces the tail by its compaction: the replaced files go,
// then the chunks come in, then the manifest goes. Each step can be repeated.
// Nothing is removed unless every chunk is in staging or already in the lane:
// a chunk found in neither would take its events with the replaced files, so
// the manifest stays, and with it the lane's hold.
func (ls *laneSpool) rollForward(m *compactManifest) error {
	staging := ls.stagingDir()
	for _, name := range m.Staged {
		name = filepath.Base(name)
		if _, err := os.Stat(filepath.Join(staging, name)); err == nil {
			continue
		}
		if _, err := os.Stat(filepath.Join(ls.dir, name)); err != nil {
			return ls.abandon(m, fmt.Errorf("compaction chunk %s is neither staged nor in the lane: %w", name, err))
		}
	}
	for _, name := range m.Replaced {
		if err := os.Remove(filepath.Join(ls.dir, filepath.Base(name))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	for _, name := range m.Staged {
		src := filepath.Join(staging, filepath.Base(name))
		if _, err := os.Stat(src); errors.Is(err, os.ErrNotExist) {
			continue // moved in before a crash (checked above)
		}
		if err := moveChunk(src, filepath.Join(ls.dir, filepath.Base(name))); err != nil {
			return err
		}
	}
	if err := syncDir(ls.dir); err != nil {
		return err
	}
	os.RemoveAll(staging)
	if err := os.Remove(ls.manifestPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(filepath.Dir(ls.manifestPath()))
}

// abandon gives up a committed compaction whose chunks are not all there
// (only something outside the agent removes a staged chunk). While every
// replaced file is still in the lane, which is so until the roll forward's
// first removal, nothing has changed: the manifest and staging go and the
// tail stays as it was. Otherwise the manifest stays and the lane is held,
// since the replaced files already gone survive only in the chunks.
func (ls *laneSpool) abandon(m *compactManifest, cause error) error {
	for _, name := range m.Replaced {
		if _, err := os.Stat(filepath.Join(ls.dir, filepath.Base(name))); err != nil {
			return cause
		}
	}
	if err := os.Remove(ls.manifestPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Join(cause, err)
	}
	if err := syncDir(filepath.Dir(ls.manifestPath())); err != nil {
		return errors.Join(cause, err)
	}
	os.RemoveAll(ls.stagingDir())
	return fmt.Errorf("%w; compaction given up, the tail is as it was", cause)
}

// recoverCompaction finishes a committed compaction a crash interrupted, or
// discards one that was not committed. It runs before the lane collects.
func (ls *laneSpool) recoverCompaction() error {
	b, err := os.ReadFile(ls.manifestPath())
	if errors.Is(err, os.ErrNotExist) {
		if _, err := os.Stat(ls.stagingDir()); err == nil {
			return os.RemoveAll(ls.stagingDir()) // not committed: the tail is as it was
		}
		return nil
	}
	if err != nil {
		return err
	}
	var m compactManifest
	if err := json.Unmarshal(b, &m); err != nil {
		// Renamed in only once complete and synced, so this is not a crash's
		// doing. Nothing can be rolled forward: the files are left as they are.
		os.RemoveAll(ls.stagingDir())
		os.Remove(ls.manifestPath())
		return fmt.Errorf("unreadable compaction manifest removed: %w", err)
	}
	return ls.rollForward(&m)
}

// writeSkip spools a P1 `skip` of count events in a file that sorts before
// any other, so it goes in the lane's next batch and survives a restart. tag
// names the file (one per drop).
func writeSkip(dir string, count int, tag string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	line, _ := json.Marshal(map[string]any{"type": "skip", "t": time.Now().UnixMilli(), "d": map[string]int{"count": count}})
	name := fmt.Sprintf("%019d-skip-%s.ndjson", 0, tag)
	tmp := filepath.Join(dir, "."+name+".tmp")
	if err := os.WriteFile(tmp, append(line, '\n'), 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

func writeSynced(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
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
	return f.Close()
}

// syncDir makes the renames in a directory durable. A variable, as is
// moveChunk, so the tests can have them fail.
var syncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return err
	}
	return nil
}

// moveChunk moves a staged chunk into the lane.
var moveChunk = os.Rename

// eachLine calls fn with each non-blank line of a file, trimmed, and where it
// starts; a line longer than max is skipped, and the file read on. line is
// valid only during the call. It returns the bytes read.
func eachLine(path string, max int, fn func(off int64, line []byte) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64<<10)
	var start, pos int64
	var buf []byte
	over := false
	for {
		chunk, err := r.ReadSlice('\n')
		pos += int64(len(chunk))
		if errors.Is(err, bufio.ErrBufferFull) {
			if !over && len(buf)+len(chunk) <= max+1 {
				buf = append(buf, chunk...)
			} else {
				over, buf = true, buf[:0]
			}
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return pos, err
		}
		line := chunk
		if len(buf) > 0 && !over {
			if len(buf)+len(chunk) <= max+1 {
				buf = append(buf, chunk...)
				line = buf
			} else {
				over = true
			}
		}
		if !over {
			lead := len(line) - len(bytes.TrimLeftFunc(line, unicode.IsSpace))
			if t := bytes.TrimSpace(line); len(t) > 0 && len(t) <= max {
				if ferr := fn(start+int64(lead), t); ferr != nil {
					return pos, ferr
				}
			}
		}
		buf, over, start = buf[:0], false, pos
		if err != nil {
			return pos, nil
		}
	}
}
