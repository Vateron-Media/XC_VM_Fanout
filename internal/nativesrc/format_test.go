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
	"time"
)

// TestFormatRefusalsAreFormat: every refusal about WHAT a source is carries
// ErrFormat — it is the one signal that moves a supervised stream onto its
// ffmpeg fallback, and it must also still read as the generic refusal.
func TestFormatRefusalsAreFormat(t *testing.T) {
	fmp4 := newHLSServer(t, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2.0,\ns0.m4s\n", nil)
	enc := newHLSServer(t, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-KEY:METHOD=AES-128,URI=\"k.bin\"\n#EXTINF:2.0,\ns0.ts\n", nil)
	html := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, strings.Repeat("<html>not a stream</html>", 100))
	}))
	defer html.Close()

	for _, c := range []struct{ name, url string }{
		{"rtmp scheme", "rtmp://host/app/stream"},
		{"srt scheme", "srt://host:9000"},
		{"fmp4 hls", fmp4.URL + "/index.m3u8"},
		{"encrypted hls", enc.URL + "/index.m3u8"},
		{"not a stream", html.URL + "/live"},
	} {
		_, err := Open(context.Background(), c.url, Options{})
		if !IsFormat(err) {
			t.Errorf("%s: err = %v, want a format refusal", c.name, err)
		}
		if !errors.Is(err, ErrUnsupportedSource) {
			t.Errorf("%s: err = %v no longer reads as ErrUnsupportedSource", c.name, err)
		}
	}
}

// TestUnavailableIsNotFormat: a source that is merely DOWN must never read as a
// format refusal — that would permanently move a healthy channel onto ffmpeg
// because its upstream had a bad minute.
func TestUnavailableIsNotFormat(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer down.Close()

	for _, c := range []struct{ name, url string }{
		{"http 503", down.URL + "/live.ts"},
		{"hls 503", down.URL + "/index.m3u8"},
		{"refused", "http://127.0.0.1:1/live.ts"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := Open(ctx, c.url, Options{})
		cancel()
		if err == nil {
			t.Fatalf("%s: opened a source that is down", c.name)
		}
		if IsFormat(err) {
			t.Errorf("%s: err = %v reads as a format refusal", c.name, err)
		}
	}
}

// TestKeyMethodNoneIsClear: METHOD=NONE is an explicit "not encrypted", and a
// playlist that says it must still be served.
func TestKeyMethodNoneIsClear(t *testing.T) {
	pl, err := parseHLSPlaylist([]byte("#EXTM3U\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:2.0,\ns0.ts\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pl.Encrypted || servable(pl) != nil {
		t.Error("METHOD=NONE was treated as encryption")
	}
	pl, _ = parseHLSPlaylist([]byte("#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"k\"\n#EXTINF:2.0,\ns0.ts\n"), nil)
	if !errors.Is(servable(pl), ErrHLSEncrypted) {
		t.Error("SAMPLE-AES was not refused")
	}
}

// TestPlaylistTurningEncryptedEndsThePull: a live playlist that switches on
// encryption mid-life ends the pull with the format refusal rather than
// streaming ciphertext to viewers.
func TestPlaylistTurningEncryptedEndsThePull(t *testing.T) {
	calls := 0
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, ".m3u8"):
			calls++
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			if calls == 1 {
				_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\ns0.ts\n")
				return
			}
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-KEY:METHOD=AES-128,URI=\"k\"\n#EXTINF:1.0,\ns1.ts\n")
		default:
			_, _ = w.Write(tsSegment(0))
		}
	}))
	defer srv.Close()

	rc, err := Open(context.Background(), srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rc.Close()
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, rc); done <- err }()
	select {
	case err := <-done:
		if !IsFormat(err) || !errors.Is(err, ErrHLSEncrypted) {
			t.Errorf("pull ended with %v, want the encryption refusal", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pull kept going after the playlist turned encrypted")
	}
}
