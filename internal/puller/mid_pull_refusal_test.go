// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// packedAudioFromTSNames serves an HLS playlist whose segments are NAMED .ts
// and are not MPEG-TS at all — ID3 + ADTS packed audio, which is how radio
// channels ship. nativesrc cannot refuse this at classification time (the name
// says TS), so it takes the source and only notices when it reads the first
// segment body: the refusal is raised INSIDE the puller goroutine, after the
// pull has started, and arrives at the caller out of ingest.Copy.
func packedAudioFromTSNames(t *testing.T) *httptest.Server {
	t.Helper()
	// Long enough that nativesrc reads it as a statement about the format
	// rather than as a truncated fetch (under one 188-byte packet stays an
	// ordinary transport failure).
	seg := append([]byte("ID3\x04\x00\x00\x00\x00\x00\x00"), bytes.Repeat([]byte{0xff, 0xf1, 0x4c, 0x80}, 128)...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(seg)
			return
		}
		// Served the way IPTV upstreams routinely serve a playlist: no .m3u8
		// in the path, a generic content type, found by sniffing #EXTM3U.
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXTINF:4.0,\ns0.ts\n"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestNativeFormatRefusalMidPullFallsBackToFfmpeg: a format refusal the native
// reader raises AFTER the pull has started must reach the ffmpeg fallback, the
// same as one raised by AdoptHTTP/Open.
//
// convert() only chose the fallback on the error Open/AdoptHTTP RETURNED. A
// refusal raised inside the HLS puller goroutine — a segment that turns out not
// to be MPEG-TS, or a live playlist that changes flavour mid-life — comes back
// through the pipe instead, out of ingest.Copy, and was handed to Run(), which
// only logs it and reconnects. Nothing recorded that the source was
// format-refused, so the stream looped connect → refuse → back off forever with
// zero bytes published while the panel showed it running.
func TestNativeFormatRefusalMidPullFallsBackToFfmpeg(t *testing.T) {
	srv := packedAudioFromTSNames(t)
	bin, marker := deliveringFfmpeg(t)

	var mu sync.Mutex
	var paths []string
	var published int

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := pullOnce(ctx, mustClient(t, Source{}), Source{
		URLs:      []string{srv.URL + "/play/abc123"},
		FfmpegBin: bin,
		Backend:   BackendAuto,
		Label:     "t",
		OnPath:    func(p string) { mu.Lock(); paths = append(paths, p); mu.Unlock() },
	}, 12032, func(b []byte) { mu.Lock(); published += len(b); mu.Unlock() })

	mu.Lock()
	got := append([]string(nil), paths...)
	n := published
	mu.Unlock()

	if !ran(marker) {
		t.Fatalf("a source the native reader refused mid-pull never reached ffmpeg: routes=%v err=%v", got, err)
	}
	if len(got) == 0 || got[len(got)-1] != PathFfmpegBack {
		t.Fatalf("route settled on %v, want the last one to be %q: nothing tells the panel the source was format-refused (err=%v)",
			got, PathFfmpegBack, err)
	}
	// And the point of the fallback is bytes: the native reader's wrapper is
	// closed before ffmpeg starts, so this also pins that closing it does not
	// take the attempt down with it.
	if n == 0 {
		t.Fatalf("the fallback ran but published nothing: routes=%v err=%v", got, err)
	}
}

// deliveringFfmpeg is a stand-in ffmpeg that records that it ran AND serves a
// TS payload, so a test can tell the fallback apart from a fallback that only
// spawned something.
func deliveringFfmpeg(t *testing.T) (bin, marker string) {
	t.Helper()
	dir := t.TempDir()
	payload := filepath.Join(dir, "p.ts")
	if err := os.WriteFile(payload, tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0)), 0o644); err != nil {
		t.Fatal(err)
	}
	bin, marker = filepath.Join(dir, "fakeffmpeg"), filepath.Join(dir, "ran")
	script := "#!/bin/sh\ntouch " + marker + "\ncat " + payload + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, marker
}

// TestNativeFormatRefusalMidPullIsNotFallenBackOnWhenPinnedNative: backend=native
// means no ffmpeg, and a refusal raised mid-pull is still a refusal.
func TestNativeFormatRefusalMidPullIsNotFallenBackOnWhenPinnedNative(t *testing.T) {
	srv := packedAudioFromTSNames(t)
	bin, marker := markerFfmpeg(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := pullOnce(ctx, mustClient(t, Source{}), Source{
		URLs:      []string{srv.URL + "/play/abc123"},
		FfmpegBin: bin,
		Backend:   BackendNative,
		Label:     "t",
	}, 12032, func([]byte) {})

	if ran(marker) {
		t.Error("backend=native fell back to ffmpeg on a mid-pull refusal")
	}
	if err == nil {
		t.Fatal("backend=native silently succeeded on a source the native reader refuses mid-pull")
	}
}
