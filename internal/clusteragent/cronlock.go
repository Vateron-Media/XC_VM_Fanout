package clusteragent

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// cronlock ports src/Core/Process/ProcessManager.php's cron lock so the Go
// drains and a still-scheduled PHP cron are mutually exclusive on one node: the
// lock file is CRONS_TMP_PATH/lock_<md5(FQCN)>, its content is "<pid>" or
// "<pid> <starttime_ticks>" (field 22 of /proc/<pid>/stat, USER_HZ 100), and a
// Go-held lock is intelligible to PHP's cronLockHolder and vice-versa.

func md5hex(s string) string {
	// nosemgrep: go.lang.security.audit.crypto.use_of_weak_crypto.use-of-md5
	sum := md5.Sum([]byte(s)) // naming parity with ProcessManager::cronLockPath, not security
	return hex.EncodeToString(sum[:])
}

// cronLockPath mirrors ProcessManager::cronLockPath($class).
func cronLockPath(cronsDir, phpClass string) string {
	return filepath.Join(cronsDir, "lock_"+md5hex(phpClass))
}

// procStat reads /proc/<pid>/stat and returns the process state byte and
// starttime (field 22, ticks since boot). ok=false when stat can't be read
// (a non-Linux host or a vanished pid).
func procStat(pid int) (state byte, startTicks int64, ok bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, false
	}
	s := string(b)
	// comm (field 2) is parenthesised and may contain spaces/')'; fields after
	// the final ')' start at state (field 3), so field N is index N-3.
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return 0, 0, false
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) < 20 || fields[0] == "" {
		return 0, 0, false
	}
	st, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return fields[0][0], st, true
}

func procAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// cronLockHeld ports ProcessManager::cronLockHolder (unpinned): is the lock held
// by a live process? Dead pid / zombie / mismatched starttime → not held; a
// pid-only legacy lock is held only while its mtime is < 30 min old.
func cronLockHeld(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	tokens := strings.Fields(string(b))
	if len(tokens) == 0 {
		return false
	}
	pid, err := strconv.Atoi(tokens[0])
	if err != nil || pid <= 0 || !procAlive(pid) {
		return false
	}
	if len(tokens) == 1 {
		fi, err := os.Stat(path)
		return err == nil && time.Since(fi.ModTime()) < 1800*time.Second
	}
	state, start, ok := procStat(pid)
	if !ok {
		return true // unreadable stat → trust by pid, as PHP does
	}
	if state == 'Z' {
		return false
	}
	want, err := strconv.ParseInt(tokens[1], 10, 64)
	return err == nil && start == want
}

// acquireCronLock claims the lock, writing "<pid> <starttime>" (no newline), or
// returns ok=false when a live holder holds it (the caller skips this pass).
// release unlinks the lock, as CronTrait's shutdown does.
func acquireCronLock(path string) (release func(), ok bool) {
	if cronLockHeld(path) {
		return nil, false
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o775); err != nil {
		return nil, false
	}
	pid := os.Getpid()
	content := strconv.Itoa(pid)
	if _, start, ok := procStat(pid); ok {
		content += " " + strconv.FormatInt(start, 10)
	}
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		return nil, false
	}
	return func() { _ = os.Remove(path) }, true
}
