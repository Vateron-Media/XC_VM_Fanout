// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAnEmptyBodyIsNotAFormatRefusal: an upstream that answers 200 and then
// sends nothing is DOWN, not a different container. IPTV origins do it all the
// time — a blip, a backend restart, an account momentarily over its connection
// limit — and ErrFormat is the one signal that moves a stream onto its ffmpeg
// fallback for the life of its spec (`xc_fanout remux` maps it to
// ExitUnsupported and the supervisor's fallback is sticky). ErrFormat's own
// contract says that must never happen to a source that is merely down, so a
// body with no bytes to classify has to stay an ordinary failure.
func TestAnEmptyBodyIsNotAFormatRefusal(t *testing.T) {
	for _, ct := range []string{"application/octet-stream", "", "video/x-unknown"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if ct != "" {
				w.Header().Set("Content-Type", ct)
			}
			w.WriteHeader(http.StatusOK) // 200, no body at all
		}))
		rc, err := Open(context.Background(), srv.URL+"/live", Options{})
		srv.Close()
		if err == nil {
			rc.Close()
			t.Fatalf("content-type %q: Open accepted a body with no bytes", ct)
		}
		if !errors.Is(err, ErrUnsupportedSource) {
			t.Errorf("content-type %q: err = %v, want ErrUnsupportedSource", ct, err)
		}
		if IsFormat(err) {
			t.Errorf("content-type %q: err = %v reads as a format refusal, which pins the "+
				"channel onto ffmpeg permanently for one bad minute upstream", ct, err)
		}
	}
}
