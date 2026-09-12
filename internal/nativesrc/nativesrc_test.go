// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package nativesrc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// tsSegment builds a small but structurally real MPEG-TS segment.
func tsSegment(pts int64) []byte {
	parts := [][]byte{tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101), tsfixture.KeyframePCR(0x101, pts, pts)}
	for i := 0; i < 20; i++ {
		parts = append(parts, tsfixture.Fill(0x101))
	}
	return tsfixture.Concat(parts...)
}

// hlsServer serves a media playlist plus its segments, and records the requests
// it saw so a test can assert on what the source actually received.
type hlsServer struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []*http.Request
}

func newHLSServer(t *testing.T, playlist string, segs map[string][]byte) *hlsServer {
	t.Helper()
	h := &hlsServer{}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.reqs = append(h.reqs, r.Clone(context.Background()))
		h.mu.Unlock()
		name := strings.TrimPrefix(r.URL.Path, "/")
		if body, ok := segs[name]; ok {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(body)
			return
		}
		if name == "index.m3u8" {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte(playlist))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(h.Close)
	return h
}

func (h *hlsServer) seen() []*http.Request {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*http.Request(nil), h.reqs...)
}

// TestOpenHLSConcatenatesSegments is the whole point of the package: an HLS
// source becomes one continuous MPEG-TS byte stream, packet-aligned, which is
// exactly what ingest.Copy consumes on the direct-mp2t path.
func TestOpenHLSConcatenatesSegments(t *testing.T) {
	segs := map[string][]byte{
		"a.ts": tsSegment(0),
		"b.ts": tsSegment(90000),
		"c.ts": tsSegment(180000),
	}
	pl := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n" +
		"#EXTINF:1.0,\na.ts\n#EXTINF:1.0,\nb.ts\n#EXTINF:1.0,\nc.ts\n#EXT-X-ENDLIST\n"
	srv := newHLSServer(t, pl, segs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{UserAgent: "test-ua"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	want := len(segs["a.ts"]) + len(segs["b.ts"]) + len(segs["c.ts"])
	if len(got) != want {
		t.Fatalf("got %d bytes, want %d (all three segments, in order)", len(got), want)
	}
	if len(got)%188 != 0 {
		t.Fatalf("output is not packet-aligned: %d %% 188 = %d", len(got), len(got)%188)
	}
	for off := 0; off < len(got); off += 188 {
		if got[off] != 0x47 {
			t.Fatalf("packet at %d lost its sync byte (0x%02x)", off, got[off])
		}
	}
}

// TestOpenRefusesFMP4 pins the fallback signal: fMP4 HLS must be refused
// cleanly, never half-served, so the caller can run ffmpeg instead.
func TestOpenRefusesFMP4(t *testing.T) {
	pl := "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n" +
		"#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2.0,\ns0.m4s\n"
	srv := newHLSServer(t, pl, nil)
	_, err := Open(context.Background(), srv.URL+"/index.m3u8", Options{})
	if !errors.Is(err, ErrHLSIsFMP4) {
		t.Fatalf("fMP4 source: err = %v, want ErrHLSIsFMP4", err)
	}
}

// TestOpenRefusesUnsupported: every refusal must funnel through ErrUnsupported
// so the caller has one thing to test for.
func TestOpenRefusesUnsupported(t *testing.T) {
	for _, u := range []string{
		"rtmp://host/app/stream",
		"rtsp://host/stream",
		"srt://host:9000",
		"",
		"not a url at all::::",
	} {
		if _, err := Open(context.Background(), u, Options{}); !errors.Is(err, ErrUnsupported) {
			t.Errorf("Open(%q): err = %v, want ErrUnsupported", u, err)
		}
	}
}

// TestOptionsReachTheSource is the reason the upstream package-level clients had
// to go: a panel source needs ITS OWN cookie and User-Agent, on the segment
// fetches as well as the playlist.
func TestOptionsReachTheSource(t *testing.T) {
	segs := map[string][]byte{"a.ts": tsSegment(0)}
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\na.ts\n#EXT-X-ENDLIST\n"
	srv := newHLSServer(t, pl, segs)

	rc, err := Open(context.Background(), srv.URL+"/index.m3u8",
		Options{UserAgent: "XC-Fanout/1", Cookie: "sid=abc123"})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, _ = io.ReadAll(rc)
	rc.Close()

	reqs := srv.seen()
	if len(reqs) < 2 {
		t.Fatalf("expected a playlist and a segment request, saw %d", len(reqs))
	}
	for _, r := range reqs {
		if ua := r.Header.Get("User-Agent"); ua != "XC-Fanout/1" {
			t.Errorf("%s: User-Agent = %q, want the source's", r.URL.Path, ua)
		}
		if c := r.Header.Get("Cookie"); c != "sid=abc123" {
			t.Errorf("%s: Cookie = %q, want the source's", r.URL.Path, c)
		}
	}
}

// TestOpenDirectTSStreamsThrough: a plain mp2t body is handed back untouched.
func TestOpenDirectTSStreamsThrough(t *testing.T) {
	body := tsSegment(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	rc, err := Open(context.Background(), srv.URL+"/live.ts", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) != len(body) || got[0] != 0x47 {
		t.Fatalf("direct TS mangled: %d bytes, first=0x%02x", len(got), got[0])
	}
}

// TestOpenFollowsLiveWindow: a live playlist whose window rolls forward must be
// followed without re-sending a segment already delivered.
func TestOpenFollowsLiveWindow(t *testing.T) {
	var mu sync.Mutex
	window := 0 // advances as the "encoder" produces segments
	segs := map[string][]byte{}
	for i := 0; i < 6; i++ {
		segs[fmt.Sprintf("s%d.ts", i)] = tsSegment(int64(i) * 90000)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if body, ok := segs[name]; ok {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(body)
			return
		}
		mu.Lock()
		start := window
		window++ // each poll advances the window by one segment
		mu.Unlock()
		if start > 3 {
			start = 3
		}
		var b strings.Builder
		fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n", start)
		for i := start; i < start+2 && i < 6; i++ {
			fmt.Fprintf(&b, "#EXTINF:1.0,\ns%d.ts\n", i)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(io.LimitReader(rc, 1<<20))
	if len(got) == 0 {
		t.Fatal("live pull delivered nothing")
	}
	if len(got)%188 != 0 {
		t.Fatalf("live output not packet-aligned: %d", len(got))
	}
	// It must have advanced past the first window, i.e. followed the roll.
	if len(got) <= len(segs["s0.ts"]) {
		t.Fatalf("only %d bytes — the puller did not follow the rolling window", len(got))
	}
}

// TestOpenRefusesNonTSBody is the guard that had to be added on the way in.
// Upstream, a body with an unrecognised content-type was handed back unchecked —
// safe there because a TS demuxer sat behind it. Here the bytes go straight to
// viewers, so an MP4 (or an HTML error page) served with a generic type would be
// fanned out as if it were video. It must refuse instead.
func TestOpenRefusesNonTSBody(t *testing.T) {
	cases := []struct {
		name, ct string
		body     []byte
	}{
		{"mp4", "application/octet-stream", []byte("\x00\x00\x00\x20ftypisom\x00\x00\x02\x00isomiso2avc1")},
		{"html error page", "text/html", []byte("<html><body>404 not found</body></html>")},
		{"empty", "application/octet-stream", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", c.ct)
				_, _ = w.Write(c.body)
			}))
			defer srv.Close()
			rc, err := Open(context.Background(), srv.URL+"/stream", Options{})
			if err == nil {
				rc.Close()
				t.Fatalf("accepted a %s body as MPEG-TS", c.name)
			}
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("err = %v, want ErrUnsupported so the caller falls back to ffmpeg", err)
			}
		})
	}
}

// TestOpenAcceptsTSWithGenericContentType: the flip side — IPTV upstreams
// routinely serve real MPEG-TS as application/octet-stream, and sniffing must
// accept those rather than bounce them to ffmpeg for no reason. The sniffed
// bytes must also be replayed, not swallowed.
func TestOpenAcceptsTSWithGenericContentType(t *testing.T) {
	body := tsSegment(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	rc, err := Open(context.Background(), srv.URL+"/live", Options{})
	if err != nil {
		t.Fatalf("refused real MPEG-TS served as octet-stream: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) != len(body) {
		t.Fatalf("got %d bytes, want %d — the sniffed prefix was not replayed", len(got), len(body))
	}
	if got[0] != 0x47 {
		t.Fatalf("first byte 0x%02x, want the sync byte", got[0])
	}
}

// TestOpenJoinsALiveWindowNearItsEdge: a live playlist is joined three
// segments from its end, as ffmpeg does, not from its oldest entry — which put a
// whole window of old content into the pipeline at download speed, and again
// on every reconnect.
func TestOpenJoinsALiveWindowNearItsEdge(t *testing.T) {
	segs := map[string][]byte{}
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n")
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("s%d.ts", i)
		segs[name] = tsSegment(int64(i) * 6 * 90000)
		fmt.Fprintf(&b, "#EXTINF:6.0,\n%s\n", name)
	}
	srv := newHLSServer(t, b.String(), segs) // live: no ENDLIST

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	_, _ = io.ReadAll(io.LimitReader(rc, int64(4*len(segs["s0.ts"]))))

	var fetched []string
	for _, r := range srv.seen() {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			fetched = append(fetched, strings.TrimPrefix(r.URL.Path, "/"))
		}
	}
	if len(fetched) == 0 || fetched[0] != "s7.ts" {
		t.Fatalf("a live pull fetched %v first, want it to start three from the edge (s7.ts)", fetched)
	}
}

// TestHLSSourceCarriesItsOwnIdleBound: a live HLS source arrives a segment at a
// time and is silent in between, so its stall bound must outlast a segment. The
// default bound (8 s) closed any source with longer segments between two healthy
// ones — here, 1 s segments against a 0.5 s default, the same shape.
func TestHLSSourceCarriesItsOwnIdleBound(t *testing.T) {
	started := time.Now()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsSegment(0))
			return
		}
		n := int(time.Since(started)/time.Second) + 3
		var b strings.Builder
		fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n", n-3)
		for i := n - 3; i < n; i++ {
			fmt.Fprintf(&b, "#EXTINF:1.0,\ns%d.ts\n", i)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(b.String()))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if b := IdleBound(rc, 500*time.Millisecond); b < DefaultSourceIdleTimeout {
		t.Fatalf("an HLS source's idle bound is %s, want at least the %s floor", b, DefaultSourceIdleTimeout)
	}
	rc = WrapIdleTimeout(rc, IdleBound(rc, 500*time.Millisecond))
	defer rc.Close()

	buf := make([]byte, 64<<10)
	for time.Since(started) < 3*time.Second {
		if _, err := rc.Read(buf); err != nil {
			t.Fatalf("a healthy HLS source was closed after %s: %v", time.Since(started).Round(100*time.Millisecond), err)
		}
	}
	if b := IdleBound(io.NopCloser(strings.NewReader("")), 7*time.Second); b != 7*time.Second {
		t.Errorf("a source with no bound of its own gets %s, want the default", b)
	}
}
