// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/nativesrc"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// watchGroupKills records every process-group kill the puller issues, together
// with the answer the kernel gave, and still performs it.
func watchGroupKills(t *testing.T) func() []error {
	t.Helper()
	var mu sync.Mutex
	var got []error
	real := killProcessGroup
	killProcessGroup = func(pgid int) error {
		err := real(pgid)
		mu.Lock()
		got = append(got, err)
		mu.Unlock()
		return err
	}
	t.Cleanup(func() { killProcessGroup = real })
	return func() []error {
		mu.Lock()
		defer mu.Unlock()
		return append([]error(nil), got...)
	}
}

// TestFfmpegGroupKillLandsOnAChildWeStillOwn: the group kill must be issued
// while the child is still ours, and exactly once.
//
// syscall.Kill(-pid, SIGKILL) has none of os.Process.Kill's done guard — that
// one checks the flag Process.Wait sets and answers ErrProcessDone — so the pid
// it signals is only safe for as long as nothing has reaped it. os/exec runs
// cmd.Cancel on its own goroutine and Cmd.Wait reaps the pid BEFORE it waits
// for that goroutine, so leaving the kill to os/exec meant it usually landed
// after the reap, on a pid the kernel was already free to hand to somebody
// else. ESRCH out of the kill is the proof that it did: the group was gone
// before we asked for it.
func TestFfmpegGroupKillLandsOnAChildWeStillOwn(t *testing.T) {
	kills := watchGroupKills(t)

	dir := t.TempDir()
	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	payloadPath := filepath.Join(dir, "p.ts")
	if err := os.WriteFile(payloadPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	// A stand-in ffmpeg that emits the payload and exits 0 — the ordinary end
	// of a source, and the path that ends with our own cancel.
	bin := filepath.Join(dir, "fakeffmpeg")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\ncat "+payloadPath+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	err := runFfmpeg(context.Background(), Source{FfmpegBin: bin, Label: "t"},
		"http://origin.invalid/live.ts", nativesrc.DefaultSourceIdleTimeout, 12032, func([]byte) {})
	if err != nil && err != io.EOF {
		t.Fatalf("runFfmpeg: %v", err)
	}

	got := kills()
	if len(got) != 1 {
		t.Fatalf("the process group was signalled %d times, want exactly once: %v", len(got), got)
	}
	if errors.Is(got[0], syscall.ESRCH) {
		t.Fatalf("the group kill was issued after the child had been reaped (ESRCH): kill(-pid) signals whatever owns that pid now, and nothing in it checks that the pid is still ours")
	}
}
