package clusteragent

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
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
