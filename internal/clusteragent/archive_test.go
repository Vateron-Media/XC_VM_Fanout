package clusteragent

import (
	"bytes"
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
