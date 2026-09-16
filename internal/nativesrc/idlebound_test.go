// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHLSFromAnExtensionlessURLKeepsItsIdleBound pins the reintroduction of the
// exact bug hlsReader.IdleBound was added to fix.
//
// Provider URLs routinely carry no .m3u8 — `/live/user/pass/123`,
// `/playlist.php?id=9` — and still serve a playlist. Those go through openHTTP,
// which wraps the reader to tie the HTTP client's pool to the body's lifetime.
// That wrapper embeds the io.ReadCloser INTERFACE, so only Read and Close are
// promoted and the live pull's own stall bound became invisible. remux.Run then
// armed the 8s default against a source that is legitimately silent for a whole
// 10s segment, so the watcher closed the pipe between two healthy segments,
// remux died with "io: read/write on closed pipe", and the supervisor
// restart-looped the channel.
func TestHLSFromAnExtensionlessURLKeepsItsIdleBound(t *testing.T) {
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:10.0,\ns0.ts\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(pl))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// No .m3u8 anywhere in the path: this is the openHTTP + AdoptHTTP route.
	rc, err := Open(ctx, srv.URL+"/live/user/pass/123", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	// What remux.Run asks, with its configured default.
	want := 30 * time.Second // 3 x TARGETDURATION
	if got := IdleBound(rc, DefaultSourceIdleTimeout); got != want {
		t.Fatalf("IdleBound = %s, want %s: the client-bound wrapper hides the live "+
			"pull's own bound, so the stall watcher fires between healthy segments", got, want)
	}
}

// TestIdleBoundFallsBackThroughTheWrapper: forwarding the bound must not invent
// one. A wrapped source that has no bound of its own still gets the caller's
// default, or a plain TS body would inherit a nonsense bound of zero.
func TestIdleBoundFallsBackThroughTheWrapper(t *testing.T) {
	body := tsSegment(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	rc, err := Open(context.Background(), srv.URL+"/live", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	if got := IdleBound(rc, 7*time.Second); got != 7*time.Second {
		t.Fatalf("IdleBound = %s, want the caller's 7s default", got)
	}
}
