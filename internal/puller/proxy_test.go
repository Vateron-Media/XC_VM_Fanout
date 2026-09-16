// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// argValue returns the value that follows flag in a recorded argv.
func argValue(t *testing.T, args []string, flag string) string {
	t.Helper()
	for i, a := range args {
		if a == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	t.Fatalf("%s missing from ffmpeg args %v", flag, args)
	return ""
}

// TestProxyValueWithASchemeIsNotDoubled: the panel's proxy field is free text an
// operator types, and "http://10.0.0.5:3128" is what they naturally type. The
// daemon prefixed "http://" unconditionally, which PARSES — the host becomes
// "http:" — so the stream was permanently off air with "dial tcp: lookup http::
// no such host", an error that names neither the proxy nor the stream.
func TestProxyValueWithASchemeIsNotDoubled(t *testing.T) {
	cases := []struct {
		name, proxy, want string
	}{
		{"host-port", "10.0.0.5:3128", "http://10.0.0.5:3128"},
		{"with-scheme", "http://10.0.0.5:3128", "http://10.0.0.5:3128"},
		{"with-scheme-uppercase", "HTTP://10.0.0.5:3128", "http://10.0.0.5:3128"},
		{"https-scheme", "https://10.0.0.5:3128", "https://10.0.0.5:3128"},
		{"trailing-slash", "http://10.0.0.5:3128/", "http://10.0.0.5:3128"},
		{"padded", "  10.0.0.5:3128  ", "http://10.0.0.5:3128"},
	}
	req, err := http.NewRequest(http.MethodGet, "http://origin.invalid/live.ts", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := httpClient(Source{Proxy: tc.proxy})
			if err != nil {
				t.Fatalf("httpClient(proxy=%q): %v", tc.proxy, err)
			}
			tr, ok := c.Transport.(*http.Transport)
			if !ok || tr.Proxy == nil {
				t.Fatalf("proxy %q produced no transport proxy", tc.proxy)
			}
			u, err := tr.Proxy(req)
			if err != nil {
				t.Fatalf("transport proxy func: %v", err)
			}
			if got := u.String(); got != tc.want {
				t.Fatalf("proxy %q dials %q, want %q", tc.proxy, got, tc.want)
			}

			// And the ffmpeg child must be given the same address, not the same
			// value with a second scheme glued to the front of it.
			bin, args := argRecorder(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			const raw = "http://origin.invalid/live.ts"
			_ = convert(ctx, Source{URLs: []string{raw}, FfmpegBin: bin, Backend: BackendFfmpeg, Proxy: tc.proxy, Label: "t"},
				raw, nil, 12032, func([]byte) {})
			if got := argValue(t, args(), "-http_proxy"); got != tc.want {
				t.Fatalf("ffmpeg -http_proxy = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestProxySchemeSurvivesTheWayNativesrcKeepsIt: the daemon and the native
// reader read the SAME operator value out of the same config field, so they
// have to agree about what it may look like.
//
// nativesrc.Options.proxyURL prefixes "http://" only when the value carries no
// "://" at all, so "socks5://10.0.0.5:1080" reaches Go's transport as the
// socks5 proxy it says it is. The puller prefixed unless the value began
// http:// or https://, producing "http://socks5://10.0.0.5:1080" — which
// PARSES, as the host "socks5:" — so every probe dialled a host literally
// called socks5, the URL was skipped before nativesrc was ever consulted, and
// the stream stayed off air on a value the native reader handles correctly.
func TestProxySchemeSurvivesTheWayNativesrcKeepsIt(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://origin.invalid/live.ts", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, proxy, want string
	}{
		{"socks5", "socks5://10.0.0.5:1080", "socks5://10.0.0.5:1080"},
		{"socks5-with-credentials", "socks5://joe:pw@10.0.0.5:1080", "socks5://joe:pw@10.0.0.5:1080"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := httpClient(Source{Proxy: tc.proxy})
			if err != nil {
				t.Fatalf("httpClient(proxy=%q): %v", tc.proxy, err)
			}
			tr, ok := c.Transport.(*http.Transport)
			if !ok || tr.Proxy == nil {
				t.Fatalf("proxy %q produced no transport proxy", tc.proxy)
			}
			u, err := tr.Proxy(req)
			if err != nil {
				t.Fatalf("transport proxy func: %v", err)
			}
			if got := u.String(); got != tc.want {
				t.Fatalf("proxy %q dials %q, want %q (nativesrc dials %q for the same value)", tc.proxy, got, tc.want, tc.want)
			}
		})
	}
}

// TestProxyWithoutAHostIsRejectedByName: a value that cannot be a proxy must
// still say which value it was — the puller retires on it.
func TestProxyWithoutAHostIsRejectedByName(t *testing.T) {
	for _, bad := range []string{"10.0.0.5 :3128", "http://"} {
		_, err := httpClient(Source{Proxy: bad})
		if err == nil {
			t.Fatalf("proxy %q was accepted", bad)
		}
		if !strings.Contains(err.Error(), "proxy") {
			t.Errorf("proxy %q failed with %q, which does not name the proxy", bad, err)
		}
	}
}
