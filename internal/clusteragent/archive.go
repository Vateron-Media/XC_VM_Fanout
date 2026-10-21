package clusteragent

import (
	"os"
	"path/filepath"
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
