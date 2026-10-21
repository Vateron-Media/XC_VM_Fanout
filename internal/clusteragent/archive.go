package clusteragent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// archive.go ports the PHP TV-archive/catch-up writer (Cli/Commands/
// ArchiveCommand.php, `console.php archive`): it records a running stream's
// on-disk HLS segments (STREAMS_PATH/<id>_<N>.ts — present in both fanout modes)
// into rolling 1-minute files ARCHIVE_PATH/<id>/<Y-m-d:H-i>.ts (+ a .offset
// companion), which /admin/timeshift serves. The start/stop trigger (one worker
// per tv_archive stream) is a later slice; this file is the worker itself.

// findKeyframe is StreamUtils::findKeyframe: the byte offset of the first video
// keyframe in a TS file, written to the minute's .offset companion. The PHP
// oracle ALWAYS returns 0 — its keyframe test compares a 4-byte packet slice to
// a 3-byte literal ("?\r\x01"), which is never equal in PHP, so the offset is
// never advanced and the scan's break (guarded by 0 < offset) never fires.
// Faithful port: 0. If a future panel fixes that comparison, re-port the real
// 188-byte scan here with a parity vector.
func findKeyframe(string) int64 { return 0 }

// archiveMinuteName is the minute file's name: PHP's gmdate('Y-m-d:H-i') (UTC)
// plus ".ts" — the exact layout /admin/timeshift reads.
func archiveMinuteName(sec int64) string {
	return time.Unix(sec, 0).UTC().Format("2006-01-02:15-04") + ".ts"
}

// pruneArchive ports ArchiveCommand::deleteSegments: keep the newest `limit`
// segment files in dir (oldest-first by mtime), deleting each over-limit segment
// and its .offset companion. .offset files are not counted toward the limit
// (else retention keeps half the configured minutes). The caller passes
// tv_archive_duration × 24 × 60 as the limit.
func pruneArchive(dir string, limit int) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type seg struct {
		name string
		mod  time.Time
	}
	var segs []seg
	for _, e := range ents {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".offset") {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		segs = append(segs, seg{e.Name(), fi.ModTime()})
	}
	if len(segs) <= limit {
		return nil
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].mod.Before(segs[j].mod) })
	for _, s := range segs[:len(segs)-limit] {
		_ = os.Remove(filepath.Join(dir, s.name))
		_ = os.Remove(filepath.Join(dir, s.name+".offset"))
	}
	return nil
}

// minuteWriter appends bytes into the current UTC-minute file in dir, rotating
// on the wall-clock minute (ArchiveCommand's inner loop): when the minute
// changes it closes the old file — writing <file>.offset = findKeyframe(file) —
// and opens the new one. Files are opened lazily on the first byte of a minute,
// so a minute with no bytes leaves no stub (PHP's "discard empty"). The caller
// writes in chunks, so rotation is chunk-granular, as in PHP.
type minuteWriter struct {
	dir      string
	now      func() time.Time
	cur      *os.File
	curName  string
	curBytes int64
}

func newMinuteWriter(dir string, now func() time.Time) *minuteWriter {
	return &minuteWriter{dir: dir, now: now}
}

func (w *minuteWriter) write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	name := archiveMinuteName(w.now().Unix())
	if w.cur == nil || name != w.curName {
		if err := w.closeCur(); err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(w.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		w.cur, w.curName, w.curBytes = f, name, 0
	}
	n, err := w.cur.Write(b)
	w.curBytes += int64(n)
	return err
}

// closeCur flushes and closes the open minute file: an empty one (no bytes
// written) is removed, else its .offset companion is written. No-op when none
// is open.
func (w *minuteWriter) closeCur() error {
	if w.cur == nil {
		return nil
	}
	path := filepath.Join(w.dir, w.curName)
	_ = w.cur.Sync()
	err := w.cur.Close()
	w.cur = nil
	if w.curBytes == 0 {
		_ = os.Remove(path)
		return err
	}
	if werr := os.WriteFile(path+".offset", []byte(strconv.FormatInt(findKeyframe(path), 10)), 0o644); werr != nil && err == nil {
		err = werr
	}
	return err
}

func (w *minuteWriter) close() error { return w.closeCur() }

// archiveJob records one running stream's on-disk HLS into minute files — the
// agent's port of ArchiveCommand's body. now/running are injected (so the loop
// is testable without a live stream); poll/stall default to 50ms / 300s (PHP's
// no-progress bound). The agent owns the goroutine's lifecycle (a later slice),
// replacing PHP's PID marker + process-title kill.
type archiveJob struct {
	streamID     int64
	streamsDir   string // STREAMS_PATH
	archiveDir   string // ARCHIVE_PATH/<id>
	durationDays int    // tv_archive_duration
	now          func() time.Time
	running      func() bool
	logf         func(string, ...any)
	poll         time.Duration
	stall        time.Duration
}

var playlistSegRe = regexp.MustCompile(`_(\d+)\.ts`)

// lastPlaylistSeg is PHP's `end(preg_match_all('/_(\d+)\.ts/'))`: the last
// segment number listed in the playlist, or -1 when none/unreadable.
func lastPlaylistSeg(playlist string) int {
	b, err := os.ReadFile(playlist)
	if err != nil {
		return -1
	}
	m := playlistSegRe.FindAllStringSubmatch(string(b), -1)
	if len(m) == 0 {
		return -1
	}
	n, _ := strconv.Atoi(m[len(m)-1][1])
	return n
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

// run follows STREAMS_PATH/<id>_<N>.ts (complete once <id>_<N+1>.ts exists),
// appending each through a minuteWriter, until the stream stops, the playlist
// disappears, ctx ends, or no progress for `stall`. It starts one past the last
// segment the playlist lists (avoiding a partial), and follows a forward gap
// (purge/skip) or a backward reset (restart) by re-reading the playlist.
func (j *archiveJob) run(ctx context.Context) error {
	if j.now == nil {
		j.now = time.Now
	}
	if j.running == nil {
		j.running = func() bool { return true }
	}
	if j.logf == nil {
		j.logf = func(string, ...any) {}
	}
	poll := j.poll
	if poll == 0 {
		poll = 50 * time.Millisecond
	}
	stall := j.stall
	if stall == 0 {
		stall = 300 * time.Second
	}
	if err := os.MkdirAll(j.archiveDir, 0o755); err != nil {
		return err
	}
	playlist := filepath.Join(j.streamsDir, fmt.Sprintf("%d_.m3u8", j.streamID))
	seg := lastPlaylistSeg(playlist) + 1 // -1+1=0 when none listed
	limit := j.durationDays * 24 * 60
	w := newMinuteWriter(j.archiveDir, j.now)
	defer w.close()
	lastPrune, lastProgress := j.now(), j.now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !j.running() || !fileExists(playlist) {
			return nil
		}
		if j.now().Sub(lastPrune) >= time.Hour {
			_ = pruneArchive(j.archiveDir, limit)
			lastPrune = j.now()
		}
		segFile := filepath.Join(j.streamsDir, fmt.Sprintf("%d_%d.ts", j.streamID, seg))
		nextFile := filepath.Join(j.streamsDir, fmt.Sprintf("%d_%d.ts", j.streamID, seg+1))
		if !fileExists(segFile) || !fileExists(nextFile) {
			if fresh := lastPlaylistSeg(playlist); fresh >= 0 && (fresh > seg+1 || fresh < seg-1) {
				seg = fresh // forward gap or backward restart: follow the source counter
				lastProgress = j.now()
				continue
			}
			if j.now().Sub(lastProgress) >= stall {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(poll):
			}
			continue
		}
		if err := j.appendSegment(segFile, w); err != nil {
			return err
		}
		seg++
		lastProgress = j.now()
	}
}

// appendSegment streams a completed source segment into the minute writer in
// 64 KiB chunks (rotation is chunk-granular, as in PHP). A segment pruned
// between the existence check and open is skipped (nil), and the caller advances.
func (j *archiveJob) appendSegment(path string, w *minuteWriter) error {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	buf := make([]byte, 65536)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			if werr := w.write(buf[:n]); werr != nil {
				return werr
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				return nil
			}
			return rerr
		}
	}
}
