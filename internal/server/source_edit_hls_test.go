// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// countingOrigin is a TS origin that never ends a response and counts how many
// times each path was fetched — one count per pull the daemon actually opened.
func countingOrigin(t *testing.T) (*httptest.Server, func(string) int) {
	t.Helper()
	var mu sync.Mutex
	conns := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	t.Cleanup(srv.Close)
	return srv, func(path string) int { mu.Lock(); defer mu.Unlock(); return conns[path] }
}

// TestSourceEditReachesAnHLSOnlyChannel: HLS holds no ref — a playlist or segment
// request only stamps lastAccess — so a channel watched over HLS alone runs its
// puller with refs==0. An edit that landed then used to be dropped on the floor:
// setConfig restarted only under refs>0, every later touch() found running=true
// and returned at once, and the polls kept lastAccess fresh so the reaper never
// idle-stopped it either. The operator swapped a dead URL for a working one, the
// panel reported the new source, and the channel went on pulling the dead one
// until the daemon was restarted. The same window swallows an edit made while the
// TS audience is away but still inside the grace period.
func TestSourceEditReachesAnHLSOnlyChannel(t *testing.T) {
	origin, count := countingOrigin(t)

	m := NewManager(1<<20, 0, 2, 6, time.Second)
	a := puller.Source{URLs: []string{origin.URL + "/a.ts"}, Backend: puller.BackendNative}
	m.Register("7", a, 0)
	st := m.Get("7")
	st.touch() // an HLS request: it starts the puller but takes no ref
	defer func() {
		m.Unregister("7") // stop the puller, or the origin waits on its open pull
		origin.CloseClientConnections()
	}()
	if !waitFor(func() bool { return count("/a.ts") == 1 }) {
		t.Fatalf("timed out waiting for the pull of /a.ts (%d connections)", count("/a.ts"))
	}
	st.mu.Lock()
	refs := st.refs
	st.mu.Unlock()
	if refs != 0 {
		t.Fatalf("an HLS-only audience took %d ref(s); this test no longer covers the refs==0 path", refs)
	}

	m.Register("7", a, 0) // the panel re-registers on every request: same config
	st.touch()
	time.Sleep(300 * time.Millisecond)
	if n := count("/a.ts"); n != 1 {
		t.Fatalf("re-registering an identical source reconnected (%d connections to /a.ts)", n)
	}

	b := a
	b.URLs = []string{origin.URL + "/b.ts"}
	m.Register("7", b, 0) // the operator edits the source
	st.touch()            // ...and the HLS audience goes on polling
	if !waitFor(func() bool { return count("/b.ts") >= 1 }) {
		t.Fatalf("the edited source was never pulled for an HLS-only audience (a=%d b=%d)", count("/a.ts"), count("/b.ts"))
	}
}
