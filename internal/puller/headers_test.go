// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// TestConfiguredHostHeaderReachesTheWire: Source.Headers says a configured
// header travels down every path — the probe, the native reader and the ffmpeg
// child — and an upstream that serves several vhosts off one address needs
// "Host:" most of all. net/http ignores Header["Host"] and sends URL.Host
// instead, while ffmpeg's -headers block honours it, so the probe and the
// ffmpeg fallback asked for DIFFERENT sites: the probe could 404 a URL out of
// the candidate list before ffmpeg was ever given it.
func TestConfiguredHostHeaderReachesTheWire(t *testing.T) {
	var mu sync.Mutex
	var gotHost, gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHost, gotToken = r.Host, r.Header.Get("X-Token")
		mu.Unlock()
		w.Header().Set("Content-Type", "video/mp2t")
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	applyHeaders(req, []string{"Host: cdn.example.com", "X-Token: shh"})
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	mu.Lock()
	host, token := gotHost, gotToken
	mu.Unlock()
	if host != "cdn.example.com" {
		t.Errorf("the request asked for vhost %q, want cdn.example.com", host)
	}
	if token != "shh" {
		t.Errorf("X-Token = %q, want shh", token)
	}

	// The ffmpeg child is given the same line, so all three paths agree about
	// which site they are pulling.
	if blk := ffmpegHeaderBlock(Source{Headers: []string{"Host: cdn.example.com"}}); !strings.Contains(blk, "Host: cdn.example.com\r\n") {
		t.Errorf("ffmpeg -headers block = %q, want it to carry the Host line", blk)
	}
}
