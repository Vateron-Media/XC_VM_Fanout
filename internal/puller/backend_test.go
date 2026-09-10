package puller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// hlsSource serves a tiny finite HLS playlist of real MPEG-TS, i.e. exactly the
// case the native reader exists for.
func hlsSource(t *testing.T) *httptest.Server {
	t.Helper()
	seg := tsfixture.Concat(
		tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101),
		tsfixture.KeyframePCR(0x101, 0, 0), tsfixture.Fill(0x101),
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(seg)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\na.ts\n#EXT-X-ENDLIST\n"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// markerFfmpeg is a stand-in ffmpeg that records that it ran, so a test can tell
// which backend actually served the source rather than inferring it from bytes.
func markerFfmpeg(t *testing.T) (bin, marker string) {
	t.Helper()
	dir := t.TempDir()
	bin = filepath.Join(dir, "fakeffmpeg")
	marker = filepath.Join(dir, "ran")
	script := "#!/bin/sh\ntouch " + marker + "\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, marker
}

func ran(path string) bool { _, err := os.Stat(path); return err == nil }

// TestBackendAutoPrefersNative: an HLS source must be served in-process, with no
// ffmpeg child — that is the whole point of the feature.
func TestBackendAutoPrefersNative(t *testing.T) {
	srv := hlsSource(t)
	bin, marker := markerFfmpeg(t)

	var got []byte
	err := convert(context.Background(),
		Source{URLs: []string{srv.URL + "/i.m3u8"}, FfmpegBin: bin, Backend: BackendAuto},
		srv.URL+"/i.m3u8", nil, 12032, func(b []byte) { got = append(got, b...) })
	if err != nil && err.Error() != "EOF" {
		t.Logf("convert returned %v (EOF is the normal end)", err)
	}
	if ran(marker) {
		t.Error("ffmpeg was spawned for a source the native reader handles")
	}
	if len(got) == 0 || got[0] != 0x47 {
		t.Fatalf("native path delivered %d bytes, first=0x%02x; want packet-aligned TS", len(got), got[0])
	}
}

// TestBackendFfmpegForcesChild is the kill-switch: even a natively-eligible
// source must go to ffmpeg when the operator says so.
func TestBackendFfmpegForcesChild(t *testing.T) {
	srv := hlsSource(t)
	bin, marker := markerFfmpeg(t)

	_ = convert(context.Background(),
		Source{URLs: []string{srv.URL + "/i.m3u8"}, FfmpegBin: bin, Backend: BackendFfmpeg},
		srv.URL+"/i.m3u8", nil, 12032, func([]byte) {})
	if !ran(marker) {
		t.Error("backend=ffmpeg did not spawn ffmpeg")
	}
}

// TestBackendAutoFallsBackToFfmpeg: a source the native reader declines (here an
// fMP4 HLS) must still be served, via ffmpeg, exactly as before the feature.
func TestBackendAutoFallsBackToFfmpeg(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2.0,\ns0.m4s\n"))
	}))
	defer srv.Close()
	bin, marker := markerFfmpeg(t)

	_ = convert(context.Background(),
		Source{URLs: []string{srv.URL + "/i.m3u8"}, FfmpegBin: bin, Backend: BackendAuto},
		srv.URL+"/i.m3u8", nil, 12032, func([]byte) {})
	if !ran(marker) {
		t.Error("an fMP4 source was not handed to ffmpeg — the fallback is what makes auto safe")
	}
}

// TestBackendNativeDoesNotFallBack: backend=native must surface the refusal
// instead of quietly doing the thing the operator switched off.
func TestBackendNativeDoesNotFallBack(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2.0,\ns0.m4s\n"))
	}))
	defer srv.Close()
	bin, marker := markerFfmpeg(t)

	err := convert(context.Background(),
		Source{URLs: []string{srv.URL + "/i.m3u8"}, FfmpegBin: bin, Backend: BackendNative},
		srv.URL+"/i.m3u8", nil, 12032, func([]byte) {})
	if err == nil {
		t.Fatal("backend=native silently succeeded on a source the native reader declines")
	}
	if ran(marker) {
		t.Error("backend=native fell back to ffmpeg")
	}
	if !strings.Contains(err.Error(), "no fallback") {
		t.Errorf("error should say why it did not fall back, got %q", err)
	}
}

// TestDirectTSNeverConverts: a source already served as mp2t bypasses the whole
// question — it never reached a converter before and must not now.
func TestDirectTSNeverConverts(t *testing.T) {
	payload := tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.KeyframePCR(0x101, 0, 0))
	srv := serveTS("video/mp2t", payload)
	defer srv.Close()
	bin, marker := markerFfmpeg(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var got []byte
	err := pullOnce(ctx, mustClient(t, Source{}),
		Source{URLs: []string{srv.URL}, FfmpegBin: bin, Backend: BackendFfmpeg}, 12032,
		func(b []byte) { got = append(got, b...) })
	if err != nil && !errors.Is(err, os.ErrClosed) && err.Error() != "EOF" {
		t.Logf("pullOnce: %v", err)
	}
	if ran(marker) {
		t.Error("a direct mp2t source was sent through ffmpeg")
	}
	if len(got) != len(payload) {
		t.Errorf("direct pull delivered %d bytes, want %d", len(got), len(payload))
	}
}
