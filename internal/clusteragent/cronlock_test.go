package clusteragent

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCronLockPath(t *testing.T) {
	p := cronLockPath("/t/crons/", `XcVm\Cli\CronJobs\StreamsLogsCronJob`)
	if filepath.Base(p) != "lock_"+md5hex(`XcVm\Cli\CronJobs\StreamsLogsCronJob`) {
		t.Fatalf("path = %s", p)
	}
}

func TestCronLockAcquireExcludes(t *testing.T) {
	p := cronLockPath(t.TempDir(), "X")
	rel, ok := acquireCronLock(p)
	if !ok {
		t.Fatal("first acquire must succeed")
	}
	if _, ok2 := acquireCronLock(p); ok2 {
		t.Fatal("second acquire while held must fail")
	}
	rel()
	if _, ok3 := acquireCronLock(p); !ok3 {
		t.Fatal("acquire after release must succeed")
	}
}

func TestCronLockStaleDeadPidReclaimed(t *testing.T) {
	p := cronLockPath(t.TempDir(), "X")
	if err := os.WriteFile(p, []byte("2147480000 123"), 0o640); err != nil {
		t.Fatal(err)
	}
	if cronLockHeld(p) {
		t.Fatal("a dead pid must not be held")
	}
	if _, ok := acquireCronLock(p); !ok {
		t.Fatal("must reclaim a dead-pid lock")
	}
}

func TestCronLockContentFormat(t *testing.T) {
	p := cronLockPath(t.TempDir(), "X")
	rel, ok := acquireCronLock(p)
	if !ok {
		t.Fatal("acquire")
	}
	defer rel()
	b, _ := os.ReadFile(p)
	parts := strings.Fields(string(b))
	if len(parts) != 2 || parts[0] != strconv.Itoa(os.Getpid()) || strings.Contains(string(b), "\n") {
		t.Fatalf("lock content = %q (want \"<pid> <start>\", no newline)", b)
	}
}
