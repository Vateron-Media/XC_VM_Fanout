package clusteragent

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// crontmp ports cron:tmp (TmpCronJob): sweep the node's tmp dirs of files older
// than their max age, keeping a protected set and live cron locks, then prune
// the playlist cache. Pure filesystem; no DB.

// tmpKeep are the per-basename max-age overrides (seconds); everything else is 600.
var tmpKeep = map[string]int64{
	"fanout_binary_check":     172800,
	"xcvm_core_check":         172800,
	"ytdlp_check":             172800,
	"ffmpeg_check":            172800,
	"replica_servers_checked": 172800,
	".update.tar.gz":          86400,
}

func tmpMaxAge(name string) int64 {
	if v, ok := tmpKeep[name]; ok {
		return v
	}
	return 600
}

// keepsCronLock ports TmpCronJob::keepsLock: a file named lock_* held by a live
// PINNED holder (pid + matching starttime) is never swept. A legacy pid-only
// lock is not pinned, so it falls to the age rule.
func keepsCronLock(path string) bool {
	return strings.HasPrefix(filepath.Base(path), "lock_") && cronLockPinnedHeld(path)
}

// cronLockPinnedHeld is cronLockHeld with ProcessManager's $rPinned=true: a
// pid-only (legacy) lock has no holder.
func cronLockPinnedHeld(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	if len(strings.Fields(string(b))) < 2 {
		return false
	}
	return cronLockHeld(path)
}

// sweepTmp ports TmpCronJob's sweep over the seven tmp dirs + the playlist prune.
func (a *Agent) sweepTmp() error {
	dirs := []string{a.TmpRoot, a.cronsDir(), a.divergenceTmpDir(), a.floodTmpDir(), a.ministraTmpDir(), a.signalsTmpDir(), a.logsTmpDir()}
	now := time.Now()
	for _, d := range dirs {
		ents, err := os.ReadDir(d)
		if err != nil {
			continue // a missing dir has nothing to sweep (PHP mkdirs it; immaterial here)
		}
		for _, e := range ents {
			if e.IsDir() {
				continue // one level, files only
			}
			name := e.Name()
			full := filepath.Join(d, name)
			fi, err := e.Info()
			if err != nil {
				continue
			}
			if int64(now.Sub(fi.ModTime()).Seconds()) >= tmpMaxAge(name) &&
				!strings.Contains(strings.ToLower(name), "ministra_") &&
				!keepsCronLock(full) {
				_ = os.Remove(full)
			}
		}
	}
	a.prunePlaylists(now)
	return nil
}

// prunePlaylists ports TmpCronJob's PLAYLIST_PATH prune: delete files whose age
// >= cache_playlists seconds. Skipped when PlaylistDir or the setting is unset
// (PHP treats an unset setting as 0 and would clear every playlist each run —
// guarded here so a missing setting does not nuke live caches).
func (a *Agent) prunePlaylists(now time.Time) {
	if a.PlaylistDir == "" {
		return
	}
	cp, ok := settingInt(a.replicaSettings()["cache_playlists"])
	if !ok {
		return
	}
	ents, err := os.ReadDir(a.PlaylistDir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		if int64(now.Sub(fi.ModTime()).Seconds()) >= cp {
			_ = os.Remove(filepath.Join(a.PlaylistDir, e.Name()))
		}
	}
}
