package clusteragent

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// StreamUtils::findKeyframe always returns 0 (its keyframe test compares a
// 4-byte packet slice to a 3-byte literal, never equal in PHP), so the archive
// .offset is always "0". Faithful port: findKeyframe ≡ 0.
func TestFindKeyframe(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "seg.ts")
	pkt := make([]byte, 188)
	pkt[0] = 0x47 // TS sync; a plausible packet, which PHP still scores as offset 0
	if err := os.WriteFile(p, bytes.Repeat(pkt, 4), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := findKeyframe(p); got != 0 {
		t.Fatalf("findKeyframe=%d, want 0 (faithful to the PHP oracle)", got)
	}
	if got := findKeyframe(filepath.Join(dir, "missing.ts")); got != 0 {
		t.Fatalf("missing file: findKeyframe=%d, want 0", got)
	}
}

func TestArchiveMinuteName(t *testing.T) {
	sec := time.Date(2026, 10, 21, 12, 34, 56, 0, time.UTC).Unix()
	if got := archiveMinuteName(sec); got != "2026-10-21:12-34.ts" {
		t.Fatalf("archiveMinuteName=%q, want 2026-10-21:12-34.ts", got)
	}
}

// writeSeg makes a segment file + its .offset companion with a set mtime.
func writeSeg(t *testing.T, dir, name string, b byte, mt time.Time) string {
	t.Helper()
	seg := filepath.Join(dir, name)
	if err := os.WriteFile(seg, []byte{b}, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seg+".offset", []byte("0"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(seg, mt, mt)
	_ = os.Chtimes(seg+".offset", mt, mt)
	return name
}

func TestPruneArchiveKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	var names []string
	for i := 0; i < 5; i++ {
		mt := base.Add(time.Duration(i) * time.Minute)
		names = append(names, writeSeg(t, dir, archiveMinuteName(mt.Unix()), byte(i), mt))
	}
	if err := pruneArchive(dir, 3); err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		seg := filepath.Join(dir, n)
		_, segErr := os.Stat(seg)
		_, offErr := os.Stat(seg + ".offset")
		if i < 2 { // the two oldest pruned, with their .offset
			if segErr == nil || offErr == nil {
				t.Fatalf("segment %d (%s) should be pruned with its .offset", i, n)
			}
		} else { // the three newest kept, with their .offset
			if segErr != nil || offErr != nil {
				t.Fatalf("segment %d (%s) should be kept with its .offset", i, n)
			}
		}
	}
}

func TestPruneArchiveIgnoresOffsetInCount(t *testing.T) {
	dir := t.TempDir()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 3; i++ {
		mt := base.Add(time.Duration(i) * time.Minute)
		writeSeg(t, dir, archiveMinuteName(mt.Unix()), byte(i), mt)
	}
	if err := pruneArchive(dir, 3); err != nil { // 3 segments + 3 .offset = 6 files, limit 3
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(dir)
	segs := 0
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".offset") {
			segs++
		}
	}
	if segs != 3 {
		t.Fatalf("segments after prune=%d, want 3 (.offset must not count toward the limit)", segs)
	}
}

func TestMinuteWriterRotates(t *testing.T) {
	dir := t.TempDir()
	clk := time.Date(2026, 10, 21, 12, 0, 30, 0, time.UTC)
	w := newMinuteWriter(dir, func() time.Time { return clk })
	if err := w.write([]byte("AAAA")); err != nil { // minute 12-00
		t.Fatal(err)
	}
	clk = clk.Add(time.Minute) // cross into 12-01
	if err := w.write([]byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	n0 := filepath.Join(dir, "2026-10-21:12-00.ts")
	n1 := filepath.Join(dir, "2026-10-21:12-01.ts")
	if b, _ := os.ReadFile(n0); string(b) != "AAAA" {
		t.Fatalf("minute 12-00 = %q, want AAAA", b)
	}
	if b, _ := os.ReadFile(n1); string(b) != "BBBB" {
		t.Fatalf("minute 12-01 = %q, want BBBB", b)
	}
	for _, n := range []string{n0, n1} {
		if b, err := os.ReadFile(n + ".offset"); err != nil || string(b) != "0" {
			t.Fatalf("%s.offset = %q err=%v, want 0", filepath.Base(n), b, err)
		}
	}
}

func TestMinuteWriterDiscardsEmpty(t *testing.T) {
	dir := t.TempDir()
	clk := time.Date(2026, 10, 21, 12, 0, 30, 0, time.UTC)
	w := newMinuteWriter(dir, func() time.Time { return clk })
	w.write([]byte("AAAA"))        // 12-00
	clk = clk.Add(2 * time.Minute) // skip 12-01 entirely
	w.write([]byte("CCCC"))        // 12-02
	w.close()
	if _, err := os.Stat(filepath.Join(dir, "2026-10-21:12-01.ts")); !os.IsNotExist(err) {
		t.Fatalf("an empty minute (12-01) must produce no file, got err=%v", err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "2026-10-21:12-00.ts")); string(b) != "AAAA" {
		t.Fatalf("12-00 = %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "2026-10-21:12-02.ts")); string(b) != "CCCC" {
		t.Fatalf("12-02 = %q", b)
	}
}

func archiveWaitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func newArchiveJob(dir, arch string, clk time.Time, running func() bool) *archiveJob {
	return &archiveJob{
		streamID: 42, streamsDir: dir, archiveDir: arch, durationDays: 1,
		now: func() time.Time { return clk }, running: running, logf: func(string, ...any) {},
		poll: 10 * time.Millisecond, stall: 300 * time.Millisecond,
	}
}

func TestArchiveRunAppendsCompletedSegments(t *testing.T) {
	dir, arch := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(dir, "42_.m3u8"), []byte("#EXTM3U\n"), 0o644) // no refs → start at 0
	for i, c := range []string{"AAAA", "BBBB", "CCCC", "DDDD"} {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("42_%d.ts", i)), []byte(c), 0o644)
	}
	clk := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	j := newArchiveJob(dir, arch, clk, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { j.run(ctx); close(done) }()
	minute := filepath.Join(arch, "2026-10-21:12-00.ts")
	// seg 0,1,2 are complete (have a successor); seg 3 (DDDD) has no _4 → excluded.
	archiveWaitFor(t, func() bool { b, _ := os.ReadFile(minute); return string(b) == "AAAABBBBCCCC" }, 3*time.Second)
	cancel()
	<-done // run() must stop writing before TempDir cleanup, else "directory not empty"
	if b, _ := os.ReadFile(minute); string(b) != "AAAABBBBCCCC" {
		t.Fatalf("minute = %q, want AAAABBBBCCCC", b)
	}
}

func TestArchiveRunWaitsForNext(t *testing.T) {
	dir, arch := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(dir, "42_.m3u8"), []byte("#EXTM3U\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "42_0.ts"), []byte("AAAA"), 0o644) // no _1 → seg 0 incomplete
	clk := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	j := newArchiveJob(dir, arch, clk, func() bool { return true })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	j.run(ctx) // exits on the 300ms stall, nothing appended
	if _, err := os.Stat(filepath.Join(arch, "2026-10-21:12-00.ts")); !os.IsNotExist(err) {
		t.Fatalf("no complete segment → no minute file, got err=%v", err)
	}
}

func TestArchiveRunForwardGap(t *testing.T) {
	dir, arch := t.TempDir(), t.TempDir()
	pl := filepath.Join(dir, "42_.m3u8")
	os.WriteFile(pl, []byte("#EXTM3U\n"), 0o644) // start at 0, nothing present yet
	clk := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	j := newArchiveJob(dir, arch, clk, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { j.run(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond) // let it park waiting at seg 0
	// forward jump: segments purged ahead, playlist now lists _5; _5 and _6 present.
	os.WriteFile(filepath.Join(dir, "42_5.ts"), []byte("FFFF"), 0o644)
	os.WriteFile(filepath.Join(dir, "42_6.ts"), []byte("IIII"), 0o644)
	os.WriteFile(pl, []byte("#EXTM3U\n#EXTINF:6,\n42_5.ts\n"), 0o644)
	minute := filepath.Join(arch, "2026-10-21:12-00.ts")
	archiveWaitFor(t, func() bool { b, _ := os.ReadFile(minute); return string(b) == "FFFF" }, 2*time.Second)
	cancel()
	<-done // run() must stop writing before TempDir cleanup
}

// Parity: a minute the worker assembles equals the source segments concatenated
// in order, and its .offset is "0" (faithful to StreamUtils::findKeyframe).
func TestArchiveRunParity(t *testing.T) {
	dir, arch := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(dir, "42_.m3u8"), []byte("#EXTM3U\n"), 0o644)
	segs := []string{"seg-zero-", "seg-one--", "seg-two--"}
	for i, c := range segs {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("42_%d.ts", i)), []byte(c), 0o644)
	}
	os.WriteFile(filepath.Join(dir, "42_3.ts"), []byte("tail"), 0o644) // successor → seg 2 completes
	clk := time.Date(2026, 10, 21, 8, 15, 0, 0, time.UTC)
	j := newArchiveJob(dir, arch, clk, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { j.run(ctx); close(done) }()
	minute := filepath.Join(arch, "2026-10-21:08-15.ts")
	want := strings.Join(segs, "")
	archiveWaitFor(t, func() bool { b, _ := os.ReadFile(minute); return string(b) == want }, 3*time.Second)
	cancel()
	<-done // run()'s deferred close writes .offset; wait before asserting + cleanup
	if b, err := os.ReadFile(minute + ".offset"); err != nil || string(b) != "0" {
		t.Fatalf("minute .offset = %q err=%v, want 0", b, err)
	}
}

func TestArchiveRunStopsWhenNotRunning(t *testing.T) {
	dir, arch := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(dir, "42_.m3u8"), []byte("#EXTM3U\n"), 0o644)
	clk := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	j := newArchiveJob(dir, arch, clk, func() bool { return false }) // not running
	done := make(chan error, 1)
	go func() { done <- j.run(context.Background()) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not stop when running()=false")
	}
}
