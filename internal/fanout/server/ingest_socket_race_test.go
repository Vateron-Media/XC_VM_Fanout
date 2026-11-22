// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"net"
	"os"
	"testing"
	"time"
)

// TestStopIngestLeavesAFreshListenersSocketAlone: a stream being torn down must
// unlink only the ingest socket it still owns.
//
// Unregister now takes the stream out of the registry FIRST and tears it down
// after, which is what stops a racing registration from finding the corpse. It
// also opens a gap: a second PUT /ingest for the same id lands in it, gets a
// fresh Stream from GetOrCreate, and startIngestLocked removes and re-binds
// <ingestDir>/<id>.sock — all before the doomed stream reaches
// stopIngestLocked. That then ran os.Remove on the same path and unlinked the
// LIVE stream's socket file.
//
// The live stream stays in the registry with ingestLn != nil, so every later
// RegisterIngest takes the "already listening" short-circuit and hands the
// panel a path that no longer exists: the channel cannot be fed again until
// someone DELETEs it. Pre-fix the same interleaving broke the connect too, but
// the stale stream was removed from the registry immediately afterwards, so the
// next registration recovered; delete-first is what makes it stick.
func TestStopIngestLeavesAFreshListenersSocketAlone(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.SetIngestDir(dir)

	sock, err := m.RegisterIngest("9", 0)
	if err != nil {
		t.Fatalf("RegisterIngest: %v", err)
	}
	doomed := m.Get("9")

	// DELETE /streams/9 takes the stream out of the registry, and is preempted
	// before it tears it down.
	m.mu.Lock()
	delete(m.streams, "9")
	m.mu.Unlock()
	doomed.mu.Lock()
	doomed.removed = true
	doomed.mu.Unlock()

	// A second PUT /ingest/9 lands in the gap and binds the same path on a fresh
	// stream — which is exactly what the retry is meant to do.
	sock2, err := m.RegisterIngest("9", 0)
	if err != nil {
		t.Fatalf("RegisterIngest after the teardown started: %v", err)
	}
	if sock2 != sock {
		t.Fatalf("ingest socket path changed: %s vs %s", sock2, sock)
	}
	fresh := m.Get("9")
	if fresh == nil || fresh == doomed {
		t.Fatal("the second registration did not land on a fresh registered stream")
	}
	defer m.Unregister("9")

	// ...and only now does the DELETE finish tearing the old stream down.
	doomed.mu.Lock()
	doomed.stopIngestLocked()
	doomed.mu.Unlock()

	if _, serr := os.Stat(sock2); serr != nil {
		t.Fatalf("the torn-down stream unlinked the live stream's ingest socket: %v", serr)
	}

	// The operator-visible symptom: the panel re-registers, is told the stream is
	// listening, and the producer cannot connect.
	again, err := m.RegisterIngest("9", 0)
	if err != nil {
		t.Fatalf("re-registering ingest: %v", err)
	}
	c, derr := net.Dial("unix", again)
	if derr != nil {
		t.Fatalf("the producer cannot reach the registered stream at %s: %v — "+
			"nothing can feed this channel again until it is DELETEd", again, derr)
	}
	defer c.Close()
	if !waitFor(func() bool {
		fresh.ingestMu.Lock()
		defer fresh.ingestMu.Unlock()
		return len(fresh.ingestConns) > 0
	}) {
		t.Fatal("the producer was never accepted by the registered stream")
	}
}
