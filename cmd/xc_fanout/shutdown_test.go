// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package main

import (
	"path/filepath"
	"testing"
	"time"
)

// TestShutdownStopsTheControlApiBeforeDetaching: on SIGTERM the daemon detaches
// supervision — the encoders are left running for the next daemon to adopt —
// and then drains its HTTP surfaces. The control socket must already be closed
// by the time that detach happens.
//
// It was not: detach ran first, then the CLIENT surface was drained, and a
// live-TS viewer is never idle, so that took the whole grace period. Throughout
// it the control socket kept accepting. A DELETE /monitor/<id> from the panel
// in that window found an already-emptied process table, so Release returned
// false and the handler still answered 204. The encoder — its own process
// group, not tied to the daemon's context — kept running with its pid file, and
// the panel, believing the channel stopped, never handed it to the next daemon.
// Nothing adopted or reaped it while it held the provider connection and went
// on writing HLS for a "stopped" channel.
func TestShutdownStopsTheControlApiBeforeDetaching(t *testing.T) {
	dir := t.TempDir()
	ctlPath := filepath.Join(dir, "control.sock")
	clientPath := filepath.Join(dir, "http.sock")

	ctlSrv, cleanupCtl, err := listenUnix(ctlPath, nameHandler("ctl"))
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupCtl()
	clientSrv, cleanupClient, err := listenUnix(clientPath, nameHandler("client"))
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupClient()

	if got, err := sockGet(ctlPath); err != nil || got != "ctl" {
		t.Fatalf("control socket not reachable before shutdown: body=%q err=%v", got, err)
	}

	answered := false
	shutdownDaemon(ctlSrv, clientSrv, func() {
		// The panel's DELETE /monitor/<id> lands exactly here.
		if _, err := sockGet(ctlPath); err == nil {
			answered = true
		}
	}, 2*time.Second)

	if answered {
		t.Error("the control API was still serving when supervision was detached: a DELETE /monitor/<id> in that window answers 204 and leaves the encoder running untracked")
	}
	if _, err := sockGet(clientPath); err == nil {
		t.Error("the client surface is still serving after shutdown")
	}
}
