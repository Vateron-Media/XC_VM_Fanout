package nativesrc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAdoptHTTPReusesTheOpenResponse: the puller GETs every source URL to read
// its content-type, and used to throw that response away so this package could
// open a SECOND connection to the same URL — two round trips per connect, the
// first one's connection unusable for the pool because it was closed undrained.
// AdoptHTTP takes the open response instead, so an HLS source costs exactly one
// manifest fetch before the polling loop starts.
func TestAdoptHTTPReusesTheOpenResponse(t *testing.T) {
	var manifests atomic.Int64
	segs := map[string][]byte{"a.ts": tsSegment(0)}
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\na.ts\n#EXT-X-ENDLIST\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if body, ok := segs[name]; ok {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(body)
			return
		}
		manifests.Add(1)
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write([]byte(pl))
	}))
	defer srv.Close()

	// Stand in for the puller's probe: one GET, then hand the response over.
	resp, err := http.Get(srv.URL + "/index.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	if n := manifests.Load(); n != 1 {
		t.Fatalf("setup fetched the manifest %d times", n)
	}

	rc, err := AdoptHTTP(context.Background(), resp, Options{})
	if err != nil {
		t.Fatalf("AdoptHTTP refused a playlist response: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()

	if len(got) != len(segs["a.ts"]) {
		t.Fatalf("adopted pull delivered %d bytes, want %d", len(got), len(segs["a.ts"]))
	}
	if n := manifests.Load(); n != 1 {
		t.Errorf("manifest was fetched %d times; adopting the probe's response must cost no extra fetch", n)
	}
}

// TestAdoptHTTPStreamsDirectTS: a plain mp2t response is handed straight back.
func TestAdoptHTTPStreamsDirectTS(t *testing.T) {
	body := tsSegment(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/live.ts")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := AdoptHTTP(context.Background(), resp, Options{})
	if err != nil {
		t.Fatalf("AdoptHTTP: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if len(got) != len(body) || got[0] != 0x47 {
		t.Fatalf("adopted direct TS mangled: %d bytes, first=0x%02x", len(got), got[0])
	}
}

// TestAdoptHTTPRefusesAndClosesBody: every refusal must funnel through
// ErrUnsupported AND close the body, because the caller's next move is to spawn
// ffmpeg and it will never look at this response again.
func TestAdoptHTTPRefusesAndClosesBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>not on air</body></html>"))
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := AdoptHTTP(context.Background(), resp, Options{})
	if err == nil {
		rc.Close()
		t.Fatal("AdoptHTTP accepted an HTML error page as MPEG-TS")
	}
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported so the caller falls back to ffmpeg", err)
	}
	// A second Close must be harmless, and a read must not still be live.
	if _, rerr := resp.Body.Read(make([]byte, 1)); rerr == nil {
		t.Error("AdoptHTTP left the body open on a refusal")
	}
}

// TestAdoptHTTPSniffsPlaylistWithGenericContentType: IPTV upstreams serve real
// playlists as text/plain or octet-stream often enough that the header alone is
// not a classifier. The body sniff must catch those rather than bouncing a
// perfectly good HLS source to ffmpeg.
func TestAdoptHTTPSniffsPlaylistWithGenericContentType(t *testing.T) {
	segs := map[string][]byte{"a.ts": tsSegment(0)}
	pl := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\na.ts\n#EXT-X-ENDLIST\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if body, ok := segs[strings.TrimPrefix(r.URL.Path, "/")]; ok {
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(body)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte(pl))
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/chan")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := AdoptHTTP(context.Background(), resp, Options{})
	if err != nil {
		t.Fatalf("a playlist served as octet-stream was refused: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if len(got) != len(segs["a.ts"]) {
		t.Fatalf("got %d bytes, want the segment's %d", len(got), len(segs["a.ts"]))
	}
}

// TestHLSPullFailsOverOnUnparseableManifest pins a silent-death bug. The manifest
// fetch branch and the segment branch both closed the pipe after a run of
// failures; the manifest PARSE branch incremented its counter and looped with no
// threshold check at all. So an upstream that answered every poll with something
// that was not a playlist spun forever at the poll cadence: the pipe never
// closed, so the reader never errored, so puller.Run never backed off and never
// rotated to the next source URL. The channel was dead and looked healthy.
func TestHLSPullFailsOverOnUnparseableManifest(t *testing.T) {
	if testing.Short() {
		// The threshold is maxHLSConsecutiveFails polls at the 1s floor, so this
		// one genuinely has to wait ~5s.
		t.Skip("waits out the consecutive-failure threshold")
	}
	var mu sync.Mutex
	polls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n := polls
		polls++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		if n == 0 {
			// A valid live playlist so the pull starts (no ENDLIST → it polls).
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n"))
			return
		}
		_, _ = w.Write([]byte("")) // empty body: parseHLSPlaylist rejects it
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rc, err := Open(ctx, srv.URL+"/index.m3u8", Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	done := make(chan error, 1)
	go func() {
		_, e := io.ReadAll(rc)
		done <- e
	}()

	select {
	case e := <-done:
		if e == nil {
			t.Fatal("pull ended cleanly; an upstream serving garbage must surface an error so the puller backs off")
		}
		if !strings.Contains(e.Error(), "unparseable") {
			t.Errorf("error should name the cause, got %q", e)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("an unparseable manifest never closed the pipe: the puller would poll it forever, " +
			"never erroring and never rotating to the next source URL")
	}
}
