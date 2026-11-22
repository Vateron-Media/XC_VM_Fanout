//go:build unix

// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestShellProberKillsTheWholeProbeGroup: the panel's probe command is a
// compound shell line ("timeout N ffprobe … | grep -q Video"), so /bin/sh does
// not exec into it and the real work runs in a child. Cancelling the probe
// signalled the shell alone and left that child behind, holding its connection
// to an origin that accepts TCP and never answers — once per stream per higher
// source, every priority-backup interval. Setpgid was already asked for; the
// kill has to use it.
func TestShellProberKillsTheWholeProbeGroup(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "probe-child.pid")
	// The shell backgrounds a child, records its pid, then waits — the shape any
	// pipeline or ';' in a probe command produces.
	cmd := "sh -c 'sleep 60 & echo $! > " + marker + "; wait'"

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if shellProber(ctx, cmd) {
		t.Fatal("a probe that never finished was reported reachable")
	}

	childPID := readPID(marker)
	if childPID == 0 {
		t.Skip("the probe's child never recorded its pid; nothing to assert about")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, alive := findProcess(childPID); !alive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("probe child pid %d outlived the probe's timeout and is still holding the source open", childPID)
}

// TestShellProberStillAnswers: the group kill must not change what a probe
// REPORTS — reachable is a zero exit, and nothing else.
func TestShellProberStillAnswers(t *testing.T) {
	if !shellProber(context.Background(), "exit 0") {
		t.Error("a probe that succeeded was reported unreachable")
	}
	if shellProber(context.Background(), "exit 1") {
		t.Error("a probe that failed was reported reachable")
	}
	if shellProber(context.Background(), "") {
		t.Error("an empty probe command was reported reachable")
	}
}
