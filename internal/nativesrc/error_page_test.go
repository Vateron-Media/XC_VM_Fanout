// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestAnErrorPageIsNotAFormatRefusal: the commoner half of the blip an empty
// 200 body is the other half of. An IPTV origin over its connection limit, or
// with an account that has just expired, answers the stream URL with 200 and an
// HTML page. Calling that a FORMAT refusal moves the channel onto its ffmpeg
// fallback for the life of the spec (remux exits ExitUnsupported and the
// supervisor's fallback is sticky) — and ffmpeg cannot play an HTML page
// either, so the move buys nothing and costs the channel its native path once
// the account comes back.
func TestAnErrorPageIsNotAFormatRefusal(t *testing.T) {
	cases := []struct {
		name, ct, body string
	}{
		{"connection limit", "text/html", "<html><body>Session limit reached.</body></html>"},
		{"login page", "text/html; charset=UTF-8", "<!DOCTYPE html>\n<html><head><title>Sign in</title></head></html>"},
		{"plain text notice", "text/plain", "MAX CONNECTIONS REACHED FOR THIS ACCOUNT"},
		{"page with no content-type", "application/octet-stream", "<html><body>account expired</body></html>"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", c.ct)
				// Long enough that the sniff reads a full probe window.
				_, _ = io.WriteString(w, c.body+strings.Repeat(" ", 1024))
			}))
			defer srv.Close()

			rc, err := Open(context.Background(), srv.URL+"/live/user/pass/1", Options{})
			if err == nil {
				rc.Close()
				t.Fatal("Open accepted an error page as a stream")
			}
			if !errors.Is(err, ErrUnsupportedSource) {
				t.Fatalf("err = %v, want ErrUnsupportedSource", err)
			}
			if IsFormat(err) {
				t.Fatalf("err = %v reads as a format refusal, which pins the channel onto "+
					"ffmpeg for the life of its spec over an account blip", err)
			}
		})
	}
}
