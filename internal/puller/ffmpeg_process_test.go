// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// alive reports whether pid is a live process. A reaped-but-not-yet-collected
// zombie still answers signal 0, so the state field of /proc/<pid>/stat is the
// answer that means what the test is asking: is something still RUNNING.
func alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	// "<pid> (<comm>) <state> ..." — comm can contain spaces and parentheses, so
	// the state is the first field after the LAST ')'.
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 || i+2 >= len(b) {
		return false
	}
	return b[i+2] != 'Z'
}

// waitForPidFile polls until path holds a pid, or the deadline passes.
func waitForPidFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stand-in ffmpeg never recorded its worker pid at %s", path)
	return 0
}

// killLater makes sure a runaway from a failing assertion does not outlive the
// test run.
func killLater(t *testing.T, pid int) {
	t.Helper()
	t.Cleanup(func() {
		if alive(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
}

// wrapperFfmpeg writes a stand-in for the wrapper scripts operators really put
// in streamConfig.ffmpeg (a ulimit shim, cpulimit, a logging wrapper): a shell
// that does NOT exec, so the daemon's direct child is the shell and the process
// that actually writes the stream is a GRANDCHILD holding the inherited stdout
// pipe. The trailing `:` keeps any shell from exec-optimising the last command
// away, so the two-level tree is guaranteed.
func wrapperFfmpeg(t *testing.T) (bin, pidPath string) {
	t.Helper()
	dir := t.TempDir()
	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	payloadPath := filepath.Join(dir, "p.ts")
	if err := os.WriteFile(payloadPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	pidPath = filepath.Join(dir, "worker.pid")
	bin = filepath.Join(dir, "ffmpeg-wrap")
	script := "#!/bin/sh\n" +
		"sh -c 'echo $$ > " + pidPath + "; while :; do cat " + payloadPath + "; sleep 0.2; done'\n" +
		":\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, pidPath
}

// TestFfmpegWrapperLeavesNoOrphan: stopping a stream must take down everything
// the ffmpeg command started, and must let runFfmpeg return.
//
// exec.CommandContext's cancel kills only the DIRECT child. With a wrapper
// script that is the shell, and the real ffmpeg underneath it survives, keeps
// the inherited stdout pipe open and keeps writing — so ingest.Copy never sees
// EOF, runFfmpeg never returns, and one puller goroutine plus one orphan ffmpeg
// (still holding the provider's connection slot) are leaked per stopped stream,
// for the life of the daemon.
func TestFfmpegWrapperLeavesNoOrphan(t *testing.T) {
	bin, pidPath := wrapperFfmpeg(t)
	const raw = "http://origin.invalid/live.ts"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var got int
	done := make(chan error, 1)
	go func() {
		done <- convert(ctx,
			Source{URLs: []string{raw}, FfmpegBin: bin, Backend: BackendFfmpeg, Label: "t"},
			raw, nil, 12032, func(b []byte) { mu.Lock(); got += len(b); mu.Unlock() })
	}()

	worker := waitForPidFile(t, pidPath)
	killLater(t, worker)

	// Wait until the grandchild is genuinely streaming, so this is the "orphan
	// keeps writing" case and not a race with start-up.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := got
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	n := got
	mu.Unlock()
	if n == 0 {
		t.Fatal("the wrapper never delivered any bytes; the test is not exercising a live child")
	}

	// Stream stop / daemon shutdown.
	cancel()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("runFfmpeg never returned after the stream was stopped: the puller goroutine is leaked, and so is the ffmpeg holding the source")
	}

	// And the grandchild must be gone, not reparented to init and still pulling.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && alive(worker) {
		time.Sleep(20 * time.Millisecond)
	}
	if alive(worker) {
		t.Fatalf("pid %d survived the stream stop: an orphan ffmpeg still holds the provider connection", worker)
	}
}

// TestFfmpegWaitBoundedByADescendantHoldingStderr: even when something the
// command started escaped the process group and still holds a pipe, cmd.Wait
// must not block forever.
//
// cmd.Stderr is a tailBuffer, not an *os.File, so os/exec creates its own pipe
// and a copy goroutine, and Wait blocks in awaitGoroutines until every write end
// of that pipe is closed. A descendant with its own session (a daemonising
// wrapper) holds that write end, and Wait has no bound of its own at all.
func TestFfmpegWaitBoundedByADescendantHoldingStderr(t *testing.T) {
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid not available to build an out-of-group pipe holder")
	}
	dir := t.TempDir()
	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	payloadPath := filepath.Join(dir, "p.ts")
	if err := os.WriteFile(payloadPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	pidPath := filepath.Join(dir, "escapee.pid")
	bin := filepath.Join(dir, "fakeffmpeg")
	// setsid puts the holder in its OWN session and process group, deliberately
	// out of reach of a process-group kill. Its stdout goes to /dev/null so only
	// STDERR is still held: the main body then emits the payload and exits,
	// stdout closes, ingest.Copy ends cleanly, and Wait is what is left blocking.
	script := "#!/bin/sh\n" +
		"setsid sh -c 'echo $$ > " + pidPath + "; sleep 120' >/dev/null &\n" +
		"cat " + payloadPath + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	const raw = "http://origin.invalid/live.ts"
	done := make(chan error, 1)
	go func() {
		done <- convert(context.Background(),
			Source{URLs: []string{raw}, FfmpegBin: bin, Backend: BackendFfmpeg, Label: "t"},
			raw, nil, 12032, func([]byte) {})
	}()

	killLater(t, waitForPidFile(t, pidPath))
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("cmd.Wait blocked on a pipe held by a descendant outside the process group: the puller goroutine is wedged for as long as that descendant lives")
	}
}
