// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
)

// TestRegisterRacingUnregisterStartsNoOrphanPuller replays the one interleaving
// that leaks an upstream connection for the lifetime of the daemon.
//
// The panel re-registers a stream on every request, so PUT /streams/7 and
// DELETE /streams/7 do overlap. Register looks the stream up and configures it in
// two steps; a teardown that runs in between used to leave Register holding a
// live *Stream that the registry no longer contained. setConfig then stored the
// config and, with TS viewers not yet unwound from CloseAll, started a puller on
// it. Nothing could ever stop that puller again: the reaper, DELETE and
// /connections all walk m.streams, and it was not in m.streams — one provider
// connection slot held open, feeding a closed hub, until the daemon restarted.
func TestRegisterRacingUnregisterStartsNoOrphanPuller(t *testing.T) {
	origin, count := countingOrigin(t)

	m := NewManager(1<<20, 0, 2, 6, time.Second)
	a := puller.Source{URLs: []string{origin.URL + "/a.ts"}, Backend: puller.BackendNative}
	m.Register("7", a, 0)
	st := m.Get("7")
	st.attach() // a TS viewer: refs>0, so a setConfig would start a puller
	if !waitFor(func() bool { return count("/a.ts") == 1 }) {
		t.Fatalf("timed out waiting for the pull of /a.ts (%d connections)", count("/a.ts"))
	}

	// PUT /streams/7 gets this far and is preempted...
	stOld := m.GetOrCreate("7")
	// ...the DELETE runs to completion...
	m.Unregister("7")
	// ...and the PUT resumes on the pointer it is still holding.
	b := a
	b.URLs = []string{origin.URL + "/b.ts"}
	stOld.setConfig(b, 0)
	st.detach() // the viewer woken by CloseAll finally unwinds

	time.Sleep(500 * time.Millisecond) // a native pull dials at once; give it room
	if n := count("/b.ts"); n != 0 {
		t.Errorf("orphan puller: %d pull(s) of the edited source on a stream nothing can reach", n)
	}
	stOld.mu.Lock()
	running := stOld.running
	stOld.mu.Unlock()
	if running {
		t.Error("a stream removed from the registry is still marked running")
	}
	if m.Get("7") != nil {
		t.Error("the torn-down stream is back in the registry")
	}

	// The lost PUT is not silently dropped: a fresh registration still works.
	m.Register("7", b, 0)
	fresh := m.Get("7")
	if fresh == nil {
		t.Fatal("re-registering after the race left nothing in the registry")
	}
	if fresh == stOld {
		t.Fatal("re-registration reused the torn-down stream")
	}
	fresh.attach()
	defer func() {
		fresh.detach()
		m.Unregister("7")
		origin.CloseClientConnections()
	}()
	if !waitFor(func() bool { return count("/b.ts") == 1 }) {
		t.Fatalf("the re-registered stream never pulled its source (%d connections to /b.ts)", count("/b.ts"))
	}
}

// TestRegisterIngestRacingUnregisterOpensNoOrphanListener: the same two-step
// window in RegisterIngest re-opened <id>.sock on a stream the registry had just
// dropped. A producer (the stream's ffmpeg tee) then connected to it and fed an
// orphan hub, with the accept goroutine leaked for the life of the daemon.
func TestRegisterIngestRacingUnregisterOpensNoOrphanListener(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(1<<20, 0, 2, 6, time.Second)
	m.SetIngestDir(dir)

	sock, err := m.RegisterIngest("9", 0)
	if err != nil {
		t.Fatalf("RegisterIngest: %v", err)
	}

	// PUT /ingest/9 gets this far and is preempted...
	stOld := m.GetOrCreate("9")
	// ...the DELETE tears the stream down and removes the socket...
	m.Unregister("9")
	// ...and the PUT resumes on the pointer it is still holding.
	stOld.mu.Lock()
	err = stOld.startIngestLocked(sock, 0)
	ln := stOld.ingestLn
	stOld.mu.Unlock()
	if err == nil || ln != nil {
		t.Error("an ingest listener was opened on a stream the registry no longer holds")
	}
	if c, derr := net.Dial("unix", sock); derr == nil {
		c.Close()
		t.Error("a producer can still connect to the torn-down stream's socket")
	}
	if _, serr := m.RegisterIngest("9", 0); serr != nil {
		t.Fatalf("re-registering ingest after the race failed: %v", serr)
	}
	defer m.Unregister("9")
	if got := filepath.Join(dir, "9.sock"); got != sock {
		t.Fatalf("ingest socket path changed: %s vs %s", got, sock)
	}
	c, derr := net.Dial("unix", sock)
	if derr != nil {
		t.Fatalf("producer dial after re-registration: %v", derr)
	}
	defer c.Close()
	fresh := m.Get("9")
	if fresh == nil || fresh == stOld {
		t.Fatal("ingest re-registration did not land on a fresh registered stream")
	}
	if !waitFor(func() bool {
		fresh.ingestMu.Lock()
		defer fresh.ingestMu.Unlock()
		return len(fresh.ingestConns) > 0
	}) {
		t.Fatal("the producer was never accepted by the re-registered stream")
	}
}
