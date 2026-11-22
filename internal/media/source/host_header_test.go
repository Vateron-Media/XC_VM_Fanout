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
	"sync"
	"testing"
)

// TestConfiguredHostHeaderIsSent: a vhost-routed origin reached by IP is a
// normal panel source — the URL is http://198.51.100.7/live/ch1.ts and a
// configured "Host: cdn.provider.tv" line is what makes the origin answer with
// the right channel instead of 404. apply set it with req.Header.Set, but
// net/http's client takes the request line's Host from req.Host and drops
// Header["Host"] entirely, so the origin saw the IP. ffmpeg's -headers sends the
// configured Host, so the same source played on one backend and not the other.
func TestConfiguredHostHeaderIsSent(t *testing.T) {
	var mu sync.Mutex
	var gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHost = r.Host
		mu.Unlock()
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(tsSegment(0))
	}))
	defer srv.Close()

	rc, err := Open(context.Background(), srv.URL+"/live/ch1.ts", Options{
		Headers: []string{"Host: cdn.provider.tv"},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, _ = io.Copy(io.Discard, rc)
	rc.Close()

	mu.Lock()
	defer mu.Unlock()
	if gotHost != "cdn.provider.tv" {
		t.Fatalf("origin saw Host %q, want the configured cdn.provider.tv", gotHost)
	}
}

// TestConfiguredHostHeaderSetsRequestHost is the same defect one layer down,
// where every fetch in this package goes through: the playlist poll, each
// segment, and the adopted body's own request.
func TestConfiguredHostHeaderSetsRequestHost(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://198.51.100.7/live/ch1.m3u8", nil)
	if err != nil {
		t.Fatal(err)
	}
	Options{Headers: []string{"host:  cdn.provider.tv  ", "X-Token: abc"}}.apply(req)

	if req.Host != "cdn.provider.tv" {
		t.Fatalf("req.Host = %q, want cdn.provider.tv — net/http writes this, not the header map", req.Host)
	}
	if v := req.Header.Get("Host"); v != "" {
		t.Fatalf("Header[Host] = %q, want it gone: net/http ignores it and it only misleads a reader", v)
	}
	if got := req.Header.Get("X-Token"); got != "abc" {
		t.Fatalf("X-Token = %q, want abc — other headers must be untouched", got)
	}
}

// TestHostHeaderWithoutAValueIsIgnored: an empty value would otherwise blank
// req.Host and make the request unroutable.
func TestHostHeaderWithoutAValueIsIgnored(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://198.51.100.7/live/ch1.m3u8", nil)
	if err != nil {
		t.Fatal(err)
	}
	Options{Headers: []string{"Host:   "}}.apply(req)
	if req.Host != "" && !strings.Contains(req.Host, "198.51.100.7") {
		t.Fatalf("req.Host = %q, want the URL's own host to stand", req.Host)
	}
}
