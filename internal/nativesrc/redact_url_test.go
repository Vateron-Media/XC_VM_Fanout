// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"net/url"
	"strings"
	"testing"
)

// Every error this package builds around a source URL is logged on every retry.
// An XC source carries its account in the path, which url.URL.Redacted() — what
// this used to be — leaves untouched.
func TestRedactURLMasksPathCredentials(t *testing.T) {
	for _, raw := range []string{
		"http://bob:s3cr3t@host:8080/live.ts",
		"http://host:8080/live/bob/s3cr3t/1234.ts",
		"http://host:8080/hls/bob/s3cr3t/1234/3.ts",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		got := redactURL(u)
		if strings.Contains(got, "s3cr3t") {
			t.Errorf("redactURL(%q) = %q: the credential survived", raw, got)
		}
		if !strings.Contains(got, "host:8080") {
			t.Errorf("redactURL(%q) = %q: the host must stay, or the error names no source", raw, got)
		}
	}
	if got := redactURL(nil); got != "source" {
		t.Errorf("redactURL(nil) = %q, want %q", got, "source")
	}
}
