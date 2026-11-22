//go:build !unix

// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package supervisor

import (
	"context"
	"errors"
)

// shellLauncher is Unix-only: supervising an encoder needs process groups and
// signals (see launch_unix.go), and the daemon ships as a static Linux binary.
// This stub exists so the package still builds — and its tests, which supply
// their own in-process Launcher, still run — on a developer's non-Unix machine.
func shellLauncher(context.Context, string, string) (Process, error) {
	return nil, errors.New("supervisor: launching encoders is only supported on unix")
}

// shellProber is Unix-only for the same reason as shellLauncher. Reporting
// every source unreachable is the safe answer here: it means the priority
// switch never fires, not that a working stream is moved onto a dead feed.
func shellProber(context.Context, string) bool { return false }

// findProcess and killProcess are Unix-only, like the launcher. Reporting every
// pid dead is the safe answer: adoption simply never happens, so nothing is
// mistakenly inherited on a platform where the daemon does not run anyway.
func findProcess(int) (string, bool) { return "", false }

func killProcess(int) {}
