// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"net/http"
	"net/url"
	"testing"
)

// A source's credentials belong to the source's origin, and to nothing else.
//
// The playlist decides what is fetched next, and an upstream is free to list a
// segment, a key or a variant on any host it likes. Sending the configured
// Cookie and auth headers to whatever host it names hands the provider account
// to a third party on the upstream's say-so — and a hostile or compromised
// playlist is all it takes. Host was already scoped for a different reason; the
// secrets were not.
func TestCredentialsDoNotLeaveTheSourceHost(t *testing.T) {
	opt := Options{
		UserAgent: "VLC/3.0",
		Cookie:    "provider_session=SECRET",
		Headers:   []string{"X-Provider-Token: SECRET-TOKEN", "Referer: http://provider/portal"},
	}.scopedTo(mustParse(t, "http://provider.example/live.m3u8"))

	t.Run("the source's own host gets everything", func(t *testing.T) {
		req := mustReq(t, "http://provider.example/seg0.ts")
		opt.apply(req)
		if got := req.Header.Get("Cookie"); got != "provider_session=SECRET" {
			t.Errorf("Cookie = %q, want the configured one", got)
		}
		if got := req.Header.Get("X-Provider-Token"); got != "SECRET-TOKEN" {
			t.Errorf("X-Provider-Token = %q, want the configured one", got)
		}
		if got := req.Header.Get("Referer"); got != "http://provider/portal" {
			t.Errorf("Referer = %q, want the configured one", got)
		}
	})

	t.Run("another host gets none of them", func(t *testing.T) {
		req := mustReq(t, "http://attacker.example/seg0.ts")
		opt.apply(req)
		if got := req.Header.Get("Cookie"); got != "" {
			t.Errorf("Cookie = %q went to a third-party host", got)
		}
		if got := req.Header.Get("X-Provider-Token"); got != "" {
			t.Errorf("X-Provider-Token = %q went to a third-party host", got)
		}
		if got := req.Header.Get("Referer"); got != "" {
			t.Errorf("Referer = %q went to a third-party host", got)
		}
		// The User-Agent is not a credential and identifies the player, which a
		// CDN may legitimately gate on, so it still travels.
		if got := req.Header.Get("User-Agent"); got != "VLC/3.0" {
			t.Errorf("User-Agent = %q, want it to travel cross-host", got)
		}
	})

	t.Run("a CDN on another port of the same name is still another host", func(t *testing.T) {
		req := mustReq(t, "http://provider.example:8080/seg0.ts")
		opt.apply(req)
		if got := req.Header.Get("Cookie"); got != "" {
			t.Errorf("Cookie = %q crossed to a different port", got)
		}
	})

	t.Run("an unscoped Options still sends them", func(t *testing.T) {
		// Nothing has said what the source host is (a direct Open with no
		// scoping): the configured values are still the most specific thing
		// anyone said, so they apply.
		req := mustReq(t, "http://anywhere.example/seg0.ts")
		(Options{Cookie: "c=1"}).apply(req)
		if got := req.Header.Get("Cookie"); got != "c=1" {
			t.Errorf("Cookie = %q, want it applied when no scope is known", got)
		}
	})
}

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func mustReq(t *testing.T, raw string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}
