package clusteragent

import (
	"os"
	"path/filepath"
	"sort"
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
