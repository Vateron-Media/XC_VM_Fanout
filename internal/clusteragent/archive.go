package clusteragent

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
