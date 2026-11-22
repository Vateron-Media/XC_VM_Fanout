// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestClosingAnHLSReaderStopsThePuller pins a goroutine leak that compounded
// once per reconnect for the life of a stream.
//
// The puller ran on the CALLER's ctx — the stream's whole lifetime — while
// Close only shut the pipe. Against a frozen playlist (an encoder's m3u8 left
// on disk after the encoder died, still served 200) the abandoned goroutine
// never writes again, so its segment-failure counter never grows, and every
// manifest poll succeeds, so its manifest counter keeps resetting. It polls
// until the stream stops. Each idle-timeout reconnect leaves another one
// behind, so after an hour ~100 goroutines and their transports are hammering
// the upstream — which also trips the per-account connection limits IPTV
// providers enforce.
func TestClosingAnHLSReaderStopsThePuller(t *testing.T) {
	var polls atomic.Int64
	seg := tsSegment(0)
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n" +
		"#EXTINF:2.0,\ns0.ts\n#EXTINF:2.0,\ns1.ts\n#EXTINF:2.0,\ns2.ts\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(seg)
			return
		}
		polls.Add(1) // the playlist never changes: a frozen live window
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(pl))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel() // the STREAM outlives this reader, exactly as in puller.Run

	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Drain the whole first window so the puller is back in its poll loop
	// rather than blocked writing into the pipe when we close.
	if _, err := io.ReadFull(rc, make([]byte, 3*len(seg))); err != nil {
		t.Fatalf("read first window: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	before := polls.Load()
	time.Sleep(3 * time.Second) // >= 3 polls at the target/2 == 1s cadence
	if after := polls.Load() - before; after > 1 {
		t.Fatalf("the puller fetched the manifest %d more times after its reader was closed: "+
			"the goroutine outlives the reader and leaks once per reconnect", after)
	}
}
