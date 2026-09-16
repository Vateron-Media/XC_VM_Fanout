// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestHostOverrideStopsAtTheSourcesOwnHost: the reason anyone configures a
// "Host:" line is a vhost-routed origin reached by IP. That origin can list its
// segments as absolute URLs on a different CDN name, and sending the origin's
// vhost to the CDN is exactly the 404 the line was configured to avoid. The
// override has to reach every fetch to the SOURCE's host — the playlist polls
// and the relative segments under it — and stop there.
func TestHostOverrideStopsAtTheSourcesOwnHost(t *testing.T) {
	var mu sync.Mutex
	var cdnHost string
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cdnHost = r.Host
		mu.Unlock()
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(tsSegment(0))
	}))
	defer cdn.Close()

	var originHost string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		originHost = r.Host
		mu.Unlock()
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, fmt.Sprintf(
			"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,\n%s/s0.ts\n#EXT-X-ENDLIST\n",
			cdn.URL))
	}))
	defer origin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, origin.URL+"/index.m3u8", Options{Headers: []string{"Host: vhost.example"}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if len(got) == 0 {
		t.Fatal("the segment on the second host was never delivered")
	}

	mu.Lock()
	defer mu.Unlock()
	if originHost != "vhost.example" {
		t.Fatalf("origin saw Host %q, want the configured vhost.example", originHost)
	}
	want := strings.TrimPrefix(cdn.URL, "http://")
	if cdnHost != want {
		t.Fatalf("the segment host saw Host %q, want its own %q: the override belongs to "+
			"the source's host, and sending it to another CDN name is the 404 it was "+
			"configured to avoid", cdnHost, want)
	}
}

// TestHostOverrideAppliesToTheSourcesOwnSegments is the other side of the same
// bound: a relative segment under the configured origin must still carry it.
func TestHostOverrideAppliesToTheSourcesOwnSegments(t *testing.T) {
	var mu sync.Mutex
	hosts := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hosts[r.URL.Path] = r.Host
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n"+
			"#EXTINF:2.0,\ns0.ts\n#EXT-X-ENDLIST\n")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{Headers: []string{"Host: cdn.provider.tv"}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _ = io.ReadAll(rc)
	rc.Close()

	mu.Lock()
	defer mu.Unlock()
	for _, p := range []string{"/index.m3u8", "/s0.ts"} {
		if hosts[p] != "cdn.provider.tv" {
			t.Fatalf("%s saw Host %q, want cdn.provider.tv: an upstream that gates on the "+
				"vhost gates on every fetch of the source, not just the manifest", p, hosts[p])
		}
	}
}

// TestHostOverrideWithNoScopeStillApplies keeps the header honest for a caller
// that builds a request itself: with no source host to measure against, the
// configured line is the most specific thing anyone said.
func TestHostOverrideWithNoScopeStillApplies(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://198.51.100.7/live/ch1.m3u8", nil)
	if err != nil {
		t.Fatal(err)
	}
	Options{Headers: []string{"Host: cdn.provider.tv"}}.apply(req)
	if req.Host != "cdn.provider.tv" {
		t.Fatalf("req.Host = %q, want cdn.provider.tv", req.Host)
	}
	u, _ := url.Parse("http://198.51.100.7/live/ch1.m3u8")
	req2, _ := http.NewRequest(http.MethodGet, "http://other.cdn.example/s0.ts", nil)
	Options{Headers: []string{"Host: cdn.provider.tv"}}.scopedTo(u).apply(req2)
	if req2.Host == "cdn.provider.tv" {
		t.Fatal("the override reached a request to another host")
	}
}
