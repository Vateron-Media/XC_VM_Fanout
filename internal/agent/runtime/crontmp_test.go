package clusteragent

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func ageFile(t *testing.T, path string, secs int) {
	t.Helper()
	old := time.Now().Add(-time.Duration(secs) * time.Second)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
}

func touchAt(t *testing.T, path string, secs int) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	ageFile(t, path, secs)
}

func TestSweepTmp(t *testing.T) {
	a := newDrainAgent(t)
	a.PlaylistDir = t.TempDir()
	for _, d := range []string{a.cronsDir(), a.floodTmpDir()} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	floodSettings(t, a.ReplicaDir, map[string]any{"cache_playlists": 3600})

	// top-level TMP_PATH files
	touchAt(t, filepath.Join(a.TmpRoot, "stale.txt"), 601)            // swept (>= 600)
	touchAt(t, filepath.Join(a.TmpRoot, "fresh.txt"), 599)            // kept (< 600)
	touchAt(t, filepath.Join(a.TmpRoot, "ministra_abc"), 99999)       // kept (ministra_, any age)
	touchAt(t, filepath.Join(a.TmpRoot, "fanout_binary_check"), 1000) // kept (KEEP 172800)
	// a live pinned lock (old mtime) is kept; a stale dead-pid lock is swept
	live := cronLockPath(a.cronsDir(), "X")
	rel, ok := acquireCronLock(live)
	if !ok {
		t.Fatal("acquire live lock")
	}
	defer rel()
	ageFile(t, live, 700)
	dead := cronLockPath(a.cronsDir(), "DEAD")
	if err := os.WriteFile(dead, []byte("2147480000 123"), 0o640); err != nil {
		t.Fatal(err)
	}
	ageFile(t, dead, 700)
	// FLOOD: a fresh block marker survives, a stale one is swept
	touchAt(t, filepath.Join(a.floodTmpDir(), "block_1.2.3.4"), 10)  // fresh -> kept (Gate 3 state)
	touchAt(t, filepath.Join(a.floodTmpDir(), "block_5.5.5.5"), 700) // stale -> swept
	// playlist prune by cache_playlists (3600)
	touchAt(t, filepath.Join(a.PlaylistDir, "old.m3u"), 7200) // pruned
	touchAt(t, filepath.Join(a.PlaylistDir, "new.m3u"), 10)   // kept

	if err := a.sweepTmp(); err != nil {
		t.Fatal(err)
	}

	gone := func(p string) bool { _, err := os.Stat(p); return os.IsNotExist(err) }
	checks := []struct {
		path string
		want bool // want gone?
	}{
		{filepath.Join(a.TmpRoot, "stale.txt"), true},
		{filepath.Join(a.TmpRoot, "fresh.txt"), false},
		{filepath.Join(a.TmpRoot, "ministra_abc"), false},
		{filepath.Join(a.TmpRoot, "fanout_binary_check"), false},
		{live, false},
		{dead, true},
		{filepath.Join(a.floodTmpDir(), "block_1.2.3.4"), false},
		{filepath.Join(a.floodTmpDir(), "block_5.5.5.5"), true},
		{filepath.Join(a.PlaylistDir, "old.m3u"), true},
		{filepath.Join(a.PlaylistDir, "new.m3u"), false},
	}
	for _, c := range checks {
		if gone(c.path) != c.want {
			t.Errorf("%s: gone=%v want %v", filepath.Base(c.path), gone(c.path), c.want)
		}
	}
}
