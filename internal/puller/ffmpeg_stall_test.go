// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package puller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/nativesrc"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// encryptedHLS serves an AES-128 HLS playlist — the canonical thing the native
// reader refuses and hands to ffmpeg — classified by content-type rather than
// by a ".m3u8" extension. nativesrc.AdoptHTTP exists exactly because upstreams
// serve playlists under arbitrary paths.
func encryptedHLS(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n" +
			"#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n" +
			"#EXTINF:10.0,\ns0.ts\n#EXTINF:10.0,\ns1.ts\n"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// burstyFfmpeg is a stand-in that models ffmpeg at an HLS live edge: it emits
// one segment's worth of bytes, says nothing for gap, then emits the next. That
// silence is HEALTHY — it is how long the upstream takes to publish a segment.
func burstyFfmpeg(t *testing.T, gap time.Duration) (bin string, payload []byte) {
	t.Helper()
	dir := t.TempDir()
	payload = tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.Keyframe(0x101, 0))
	payloadPath := filepath.Join(dir, "seg.ts")
	if err := os.WriteFile(payloadPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	bin = filepath.Join(dir, "fakeffmpeg")
	script := "#!/bin/sh\ncat " + payloadPath +
		"\nsleep " + strconv.Itoa(int(gap/time.Second)) +
		"\ncat " + payloadPath + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, payload
}

// TestFfmpegHLSStallBoundWithoutM3U8Extension: an HLS source the native reader
// declined must get the HLS stall bound on the ffmpeg path too, even when its
// URL carries no ".m3u8".
//
// Keying the bound off the URL TEXT gave such a source the 8s continuous-source
// bound, so the daemon killed ffmpeg between two perfectly healthy segments and
// reconnected — every segment duration, forever, with a ring discontinuity each
// time, while the panel still reported a running stream.
func TestFfmpegHLSStallBoundWithoutM3U8Extension(t *testing.T) {
	// Long enough to trip the 8s bound (whose watchdog ticks every 8s/3), short
	// enough to sit well inside the HLS bound.
	const gap = 12 * time.Second
	srv := encryptedHLS(t)
	bin, payload := burstyFfmpeg(t, gap)

	var mu sync.Mutex
	var got []byte
	var paths []string

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	err := pullOnce(ctx, mustClient(t, Source{}), Source{
		URLs:      []string{srv.URL + "/play/abc123"},
		FfmpegBin: bin,
		Label:     "t",
		OnPath:    func(p string) { mu.Lock(); paths = append(paths, p); mu.Unlock() },
	}, 12032, func(b []byte) { mu.Lock(); got = append(got, b...); mu.Unlock() })

	mu.Lock()
	gotLen := len(got)
	gotPaths := append([]string(nil), paths...)
	mu.Unlock()

	// The route matters: if this stopped being the ffmpeg fallback, the test is
	// no longer exercising the thing it is named for.
	if len(gotPaths) == 0 || gotPaths[len(gotPaths)-1] != PathFfmpegBack {
		t.Fatalf("expected an ffmpeg fallback after a native refusal, routes=%v (err=%v)", gotPaths, err)
	}
	if gotLen < 2*len(payload) {
		t.Fatalf("delivered %d bytes, want both segments (%d): the stall bound killed ffmpeg between two healthy segments (err=%v)",
			gotLen, 2*len(payload), err)
	}
}

// TestFfmpegStallBoundClassification pins WHY each source gets the bound it
// gets, so the continuous-source bound is not quietly widened for everything:
// eight seconds of silence really is a dead source when the source is not one
// that arrives a segment at a time.
func TestFfmpegStallBoundClassification(t *testing.T) {
	withCT := func(ct string) *http.Response {
		return &http.Response{Header: http.Header{"Content-Type": []string{ct}}}
	}
	cases := []struct {
		name    string
		raw     string
		resp    *http.Response
		refusal error
		wantHLS bool
	}{
		{"m3u8 in the url", "http://h/live.m3u8", nil, nil, true},
		{"m3u8 uppercase", "http://h/LIVE.M3U8", nil, nil, true},
		{"mpegurl content-type, no extension", "http://h/play/abc123", withCT("application/vnd.apple.mpegurl"), nil, true},
		{"x-mpegurl content-type", "http://h/play/abc123", withCT("application/x-mpegURL"), nil, true},
		{"encrypted-hls refusal", "http://h/play/abc123", nil, nativesrc.ErrHLSEncrypted, true},
		{"fmp4-hls refusal", "http://h/play/abc123", nil, nativesrc.ErrHLSIsFMP4, true},
		{"wrapped hls refusal", "http://h/play/abc123", nil, fmt.Errorf("native declined: %w", nativesrc.ErrHLSEncrypted), true},
		{"continuous mp2t", "http://h/live.ts", withCT("video/mp2t"), nil, false},
		{"continuous, no hint at all", "http://h/live", nil, nil, false},
		{"udp", "udp://239.0.0.1:1234", nil, nil, false},
		{"non-hls refusal", "http://h/live", nil, nativesrc.ErrUnsupported, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isHLSSource(tc.raw, tc.resp, tc.refusal)
			if got != tc.wantHLS {
				t.Fatalf("isHLSSource = %v, want %v", got, tc.wantHLS)
			}
			want := nativesrc.DefaultSourceIdleTimeout
			if tc.wantHLS {
				want = 3 * nativesrc.DefaultSourceIdleTimeout
			}
			if bound := ffmpegStallBound(got); bound != want {
				t.Fatalf("stall bound = %s, want %s", bound, want)
			}
		})
	}
}
