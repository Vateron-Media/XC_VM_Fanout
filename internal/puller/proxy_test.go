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
		// ffmpegRefuses: libavformat's http protocol only uses -http_proxy when
		// the value starts with "http://" and drops anything else without a
		// word, so the fallback must refuse rather than pull direct.
		ffmpegRefuses bool
	}{
		{name: "host-port", proxy: "10.0.0.5:3128", want: "http://10.0.0.5:3128"},
		{name: "with-scheme", proxy: "http://10.0.0.5:3128", want: "http://10.0.0.5:3128"},
		{name: "with-scheme-uppercase", proxy: "HTTP://10.0.0.5:3128", want: "http://10.0.0.5:3128"},
		{name: "https-scheme", proxy: "https://10.0.0.5:3128", want: "https://10.0.0.5:3128", ffmpegRefuses: true},
		{name: "trailing-slash", proxy: "http://10.0.0.5:3128/", want: "http://10.0.0.5:3128"},
		{name: "padded", proxy: "  10.0.0.5:3128  ", want: "http://10.0.0.5:3128"},
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

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			const raw = "http://origin.invalid/live.ts"

			// A proxy ffmpeg cannot honour must stop the attempt, not be
			// handed over for ffmpeg to drop on the floor: it drops it
			// silently and pulls the source direct from the node's own IP,
			// which is the one outcome a proxy is configured to prevent.
			if tc.ffmpegRefuses {
				bin, marker := markerFfmpeg(t)
				err := convert(ctx, Source{URLs: []string{raw}, FfmpegBin: bin, Backend: BackendFfmpeg, Proxy: tc.proxy, Label: "t"},
					raw, nil, 12032, func([]byte) {})
				if err == nil {
					t.Fatalf("proxy %q was accepted for the ffmpeg path, which ignores it and pulls direct", tc.proxy)
				}
				if !strings.Contains(err.Error(), "proxy") {
					t.Errorf("proxy %q failed with %q, which does not name the proxy", tc.proxy, err)
				}
				if ran(marker) {
					t.Fatalf("ffmpeg was spawned with proxy %q: it ignores anything but http:// and pulls the source direct from the node's own IP", tc.proxy)
				}
				return
			}

			// And the ffmpeg child must be given the same address, not the same
			// value with a second scheme glued to the front of it.
			bin, args := argRecorder(t)
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

// TestFfmpegRefusesAProxyItWouldSilentlyIgnore: libavformat's http protocol
// uses -http_proxy only when the value starts with "http://" (use_proxy =
// av_strstart(proxy_path, "http://", NULL)) and drops anything else without a
// word. Measured with ffmpeg n7.1.5 against a closed port as the proxy:
// "-http_proxy http://127.0.0.1:9" fails dialling 127.0.0.1:9, while
// "-http_proxy https://127.0.0.1:9" fails dialling the ORIGIN — byte for byte
// the same output as passing no proxy at all.
//
// So a source an operator put behind an https or socks5 proxy went through it
// on the probe and the native path and BYPASSED it on the ffmpeg fallback,
// pulling from the node's own IP. That is the one outcome a proxy is configured
// to prevent, and nativesrc refuses the fetch rather than connect direct for
// exactly this hazard. Refuse here too, naming the value.
func TestFfmpegRefusesAProxyItWouldSilentlyIgnore(t *testing.T) {
	for _, proxy := range []string{"https://10.0.0.5:3128", "socks5://10.0.0.5:1080"} {
		t.Run(proxy, func(t *testing.T) {
			bin, marker := markerFfmpeg(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			const raw = "http://origin.invalid/live.ts"
			err := convert(ctx, Source{URLs: []string{raw}, FfmpegBin: bin, Backend: BackendFfmpeg, Proxy: proxy, Label: "t"},
				raw, nil, 12032, func([]byte) {})
			if err == nil {
				t.Fatalf("proxy %q was accepted for the ffmpeg path", proxy)
			}
			if !strings.Contains(err.Error(), "proxy") {
				t.Errorf("proxy %q failed with %q, which does not name the proxy", proxy, err)
			}
			if ran(marker) {
				t.Fatalf("ffmpeg was spawned with proxy %q, which it ignores: the source is pulled direct from the node's own IP", proxy)
			}
		})
	}
}

// TestFfmpegProxyIsIrrelevantToANonHTTPSource: the proxy only governs an http
// fetch, so a udp:// or file:// source must not be refused over it — those
// never reach a proxy in the first place, and refusing them would take a
// perfectly good multicast channel off air over a setting it cannot use.
func TestFfmpegProxyIsIrrelevantToANonHTTPSource(t *testing.T) {
	bin, args := argRecorder(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const raw = "udp://239.0.0.1:1234"
	_ = convert(ctx, Source{URLs: []string{raw}, FfmpegBin: bin, Backend: BackendFfmpeg, Proxy: "socks5://10.0.0.5:1080", Label: "t"},
		raw, nil, 12032, func([]byte) {})
	got := args()
	if hasArg(got, "-http_proxy") {
		t.Fatalf("a udp source was given -http_proxy: %v", got)
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
