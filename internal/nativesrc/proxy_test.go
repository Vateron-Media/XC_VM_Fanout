// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestABadProxyFailsClosed: a source configured to go through a proxy must
// never quietly go direct. The parse error was dropped on the floor, so
// `-http_proxy http://user:p%zz@10.0.0.1:3128` (a bad percent-escape in the
// credentials — '/', '#' and '%' in a password all do it) left the transport
// with no proxy at all, and every playlist and segment fetch went out from the
// node's own IP. The operator configured that proxy for geo or anonymity and
// got no error, no log line, and no proxy.
func TestABadProxyFailsClosed(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(tsSegment(0))
	}))
	defer srv.Close()

	opt := Options{Proxy: "user:s3cr3t%zz@10.0.0.1:3128"}
	for _, path := range []string{"/live.ts", "/index.m3u8"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rc, err := Open(ctx, srv.URL+path, opt)
		cancel()
		if err == nil {
			rc.Close()
			t.Fatalf("%s: Open succeeded with an unusable proxy", path)
		}
		if !strings.Contains(err.Error(), "proxy") {
			t.Errorf("%s: err = %v, want it to name the proxy", path, err)
		}
		if strings.Contains(err.Error(), "s3cr3t") {
			t.Errorf("%s: err = %v leaks the proxy password into the log", path, err)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("the source was fetched %d times with no proxy: a misconfigured proxy "+
			"must not become a direct connection from the node's own IP", n)
	}
}

// TestProxyWithASchemeIsNotDoubled: the panel sends "host:port", but the remux
// flag and hand-edited configs carry "http://host:port". Prefixing that blindly
// makes "http://http://host:port", which either fails to parse or aims at a
// host named "http".
func TestProxyWithASchemeIsNotDoubled(t *testing.T) {
	for _, raw := range []string{"127.0.0.1:3128", "http://127.0.0.1:3128"} {
		tr := Options{Proxy: raw}.transport(time.Second)
		if tr.Proxy == nil {
			t.Fatalf("proxy %q: transport has no proxy", raw)
		}
		req, _ := http.NewRequest(http.MethodGet, "http://example.invalid/live.ts", nil)
		pu, err := tr.Proxy(req)
		if err != nil {
			t.Fatalf("proxy %q: %v", raw, err)
		}
		if pu == nil || pu.Host != "127.0.0.1:3128" || pu.Scheme != "http" {
			t.Fatalf("proxy %q resolved to %v, want http://127.0.0.1:3128", raw, pu)
		}
	}
}

// TestNoProxyStaysDirect: the common case must not grow a proxy.
func TestNoProxyStaysDirect(t *testing.T) {
	if tr := (Options{}).transport(time.Second); tr.Proxy != nil {
		t.Fatal("a source with no proxy configured got one")
	}
}
