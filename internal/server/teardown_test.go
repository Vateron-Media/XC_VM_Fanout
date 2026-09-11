// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestUnregisterReleasesAttachedViewers: DELETE /streams/<id> takes the stream
// out of the registry, so its viewers vanish from /connections and PHP closes
// their lines_live rows — while their handler goroutines sit forever on a hub
// that will never publish again, pinning the Stream, its hub and its whole ring.
// The teardown must drop them so each serveLive returns and runs its cleanup.
func TestUnregisterReleasesAttachedViewers(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	st := mgr.GetOrCreate("5")
	feedStream(st)

	ts := httptest.NewServer(mgr.ClientHandler())
	defer ts.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.Get(ts.URL + "/live/5?c=watching&prebuffer=0")
		if err == nil {
			// Drain until the daemon hangs up.
			buf := make([]byte, 4096)
			for {
				if _, err := resp.Body.Read(buf); err != nil {
					break
				}
			}
			resp.Body.Close()
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && st.Hub.Count() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if st.Hub.Count() == 0 {
		t.Fatal("viewer never subscribed")
	}

	mgr.Unregister("5")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("viewer left hanging on a torn-down stream (orphaned hub, ring never freed)")
	}
	if n := len(st.connUUIDs()); n != 0 {
		t.Errorf("torn-down stream still tracks %d viewer(s): %v", n, st.connUUIDs())
	}
	if st.Hub.Count() != 0 {
		t.Errorf("torn-down stream still has %d subscriber(s)", st.Hub.Count())
	}
}

// TestUnregisterHangsUpOnProducer: closing the ingest listener is not enough —
// an already-connected producer (the stream's ffmpeg tee) went on feeding a
// Stream that Unregister had removed from the registry, an orphan holding a full
// ring that nothing could reach or free.
func TestUnregisterHangsUpOnProducer(t *testing.T) {
	mgr := NewManager(1<<20, 0, 2, 6, time.Second)
	mgr.SetIngestDir(t.TempDir())

	sock, err := mgr.RegisterIngest("7", 0)
	if err != nil {
		t.Fatalf("RegisterIngest: %v", err)
	}
	st := mgr.Get("7")

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("producer dial: %v", err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st.ingestMu.Lock()
		n := len(st.ingestConns)
		st.ingestMu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mgr.Unregister("7")

	// The producer's socket must now be closed from our end.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("producer connection still open after teardown — it keeps feeding an orphaned stream")
	}
	st.ingestMu.Lock()
	left := len(st.ingestConns)
	st.ingestMu.Unlock()
	if left != 0 {
		t.Errorf("%d producer connection(s) still tracked after teardown", left)
	}
}
