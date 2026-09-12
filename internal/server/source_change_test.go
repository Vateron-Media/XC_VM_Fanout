// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestSourceEditReachesAWatchedChannel: a running puller holds the Source it
// started with, so an edit in the panel reached a watched channel only after its
// audience had been gone for the grace period. A changed source restarts the
// pull; the identical re-registration the panel sends on every request does not.
func TestSourceEditReachesAWatchedChannel(t *testing.T) {
	var mu sync.Mutex
	conns := map[string]int{}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conns[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "video/mp2t")
		fl, _ := w.(http.Flusher)
		for r.Context().Err() == nil {
			if _, err := w.Write(tsfixture.PAT(0x100)); err != nil {
				return
			}
			fl.Flush()
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer origin.Close()
	count := func(path string) int { mu.Lock(); defer mu.Unlock(); return conns[path] }
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	m := NewManager(1<<20, 0, 2, 6, time.Second)
	a := puller.Source{URLs: []string{origin.URL + "/a.ts"}, Backend: puller.BackendNative}
	m.Register("7", a, 0)
	st := m.Get("7")
	st.attach() // a viewer: the puller runs
	defer func() {
		st.detach()
		m.Unregister("7") // stop the puller, or the origin waits on its open pull
		origin.CloseClientConnections()
	}()
	waitFor("the pull of /a.ts", func() bool { return count("/a.ts") == 1 })

	m.Register("7", a, 0) // the panel's next request: same config
	time.Sleep(300 * time.Millisecond)
	if n := count("/a.ts"); n != 1 {
		t.Fatalf("re-registering an identical source reconnected (%d connections to /a.ts)", n)
	}

	b := a
	b.URLs = []string{origin.URL + "/b.ts"}
	m.Register("7", b, 0) // the operator edits the source
	waitFor("the edited source to be pulled while the channel is watched", func() bool { return count("/b.ts") >= 1 })
}

// TestProducerAcceptedAsTheListenerStopsIsRefused: Accept can hand back a
// connection just before the ingest listener is closed. It used to be added to
// the connection set after stopIngestLocked had emptied it — a producer feeding
// a stream nothing could reach or stop. A connection from a stopped listener's
// generation is refused.
func TestProducerAcceptedAsTheListenerStopsIsRefused(t *testing.T) {
	st := NewManager(1<<20, 0, 2, 6, time.Second).GetOrCreate("3")
	st.ingestMu.Lock()
	st.ingestGen++
	gen := st.ingestGen // the listener that accepts...
	st.ingestMu.Unlock()

	st.closeIngestConns() // ...is stopped before the accept goroutine gets to add it

	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if st.addIngestConn(a, gen) {
		t.Fatal("a producer accepted by a stopped listener was admitted")
	}
	st.ingestMu.Lock()
	n := len(st.ingestConns)
	st.ingestMu.Unlock()
	if n != 0 {
		t.Fatalf("%d producer(s) tracked after the listener stopped", n)
	}
}
