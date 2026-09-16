// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAStalePlaylistIsNotAnEncoderRestart: a sequence that goes backwards is
// read as an encoder restarting on the same URL, which rejoins at the live edge
// and streams it again. But a CDN edge answering one poll from a slightly older
// cached copy — or a second origin behind the same hostname running a couple of
// segments behind — sends the sequence backwards too, over a window that is
// still the SAME stream. Rejoining there replays segments already on the wire:
// PTS and PCR go backwards into the ring, which is the very thing the ordered
// pull exists to prevent.
//
// The two are told apart by the numbering: a restart begins again far below the
// window it replaced, while a lagging copy still overlaps it.
func TestAStalePlaylistIsNotAnEncoderRestart(t *testing.T) {
	var polls, segGets atomic.Int64
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			segGets.Add(1)
			mu.Lock()
			seen[r.URL.Path]++
			mu.Unlock()
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		// Every other poll is answered by the copy that is two segments behind.
		first := int64(100)
		if polls.Add(1)%2 == 0 {
			first = 98
		}
		var b strings.Builder
		fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n", first)
		for i := first; i < first+6; i++ {
			fmt.Fprintf(&b, "#EXTINF:2.0,\ns%d.ts\n", i)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	drain(rc)
	time.Sleep(4500 * time.Millisecond) // ~4 polls at the target/2 cadence
	rc.Close()

	if p := polls.Load(); p < 4 {
		t.Fatalf("only %d manifest polls: the test never exercised the flip", p)
	}
	mu.Lock()
	defer mu.Unlock()
	for path, n := range seen {
		if n > 1 {
			t.Fatalf("%s was fetched %d times over %d polls: a playlist that lags the one "+
				"before it is the same stream, and rejoining it puts content already on "+
				"the wire back into the ring", path, n, polls.Load())
		}
	}
	if n := segGets.Load(); n > hlsLiveStartSegments {
		t.Fatalf("%d segment fetches over %d polls, want the %d joined at the live edge",
			n, polls.Load(), hlsLiveStartSegments)
	}
}
