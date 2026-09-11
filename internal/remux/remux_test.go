package remux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsseg"
)

const vpid = 0x101

// gopBytes is one GOP of a clean live source: tables, a keyframe at sec, fill.
func gopBytes(sec float64) []byte {
	t := int64(sec * 90000)
	parts := [][]byte{tsfixture.PAT(0x100), tsfixture.PMT(0x100, vpid), tsfixture.KeyframePCR(vpid, t, t)}
	for i := 0; i < 30; i++ {
		parts = append(parts, tsfixture.Fill(vpid))
	}
	return tsfixture.Concat(parts...)
}

// liveTS serves an endless MPEG-TS body, one GOP per keyframe interval, faster
// than realtime so the test does not wait on the clock.
func liveTS(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		fl, _ := w.(http.Flusher)
		for g := 0; ; g++ {
			if _, err := w.Write(gopBytes(float64(g) * 2)); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakeIngest is the daemon's ingest socket: it accepts producers and records
// what they sent.
type fakeIngest struct {
	path string
	ln   net.Listener
	mu   sync.Mutex
	got  bytes.Buffer
}

func newFakeIngest(t *testing.T) *fakeIngest {
	t.Helper()
	path := filepath.Join(t.TempDir(), "9.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeIngest{path: path, ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 32<<10)
				for {
					n, err := c.Read(buf)
					f.mu.Lock()
					f.got.Write(buf[:n])
					f.mu.Unlock()
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeIngest) bytes() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.got.Bytes()...)
}

func baseConfig(dir, url string) Config {
	return Config{
		Input: url,
		Seg: tsseg.Config{
			Playlist:   filepath.Join(dir, "9_.m3u8"),
			SegPattern: filepath.Join(dir, "9_%d.ts"),
			TargetSec:  4,
			ListSize:   3,
			KeepExtra:  1,
		},
		ProgressPath: filepath.Join(dir, "9_.progress"),
	}
}

func waitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never appeared", path)
}

// TestRemuxProducesBothOutputs: the two things the panel's ffmpeg tee produced —
// the on-disk HLS and the daemon feed — both come out, the feed packet-aligned
// and byte-identical to the source.
func TestRemuxProducesBothOutputs(t *testing.T) {
	dir := t.TempDir()
	src := liveTS(t)
	ing := newFakeIngest(t)
	cfg := baseConfig(dir, src.URL+"/live.ts")
	cfg.IngestSock = ing.path

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()

	waitFile(t, filepath.Join(dir, "9_1.ts"))
	waitFile(t, cfg.Seg.Playlist)
	deadline := time.Now().Add(5 * time.Second)
	for len(ing.bytes()) < 100*188 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("a cancelled run returned %v, want nil", err)
	}

	pl, _ := os.ReadFile(cfg.Seg.Playlist)
	if !strings.Contains(string(pl), "9_0.ts") && !strings.Contains(string(pl), "9_1.ts") {
		t.Errorf("playlist lists none of the written segments:\n%s", pl)
	}
	fed := ing.bytes()
	if len(fed) == 0 || len(fed)%188 != 0 {
		t.Fatalf("daemon feed is %d bytes, want a non-empty multiple of 188", len(fed))
	}
	one := gopBytes(0)
	if !bytes.Equal(fed[:188*2], one[:188*2]) {
		t.Error("daemon feed does not start with the source's own packets")
	}
	prog, _ := os.ReadFile(cfg.ProgressPath)
	if !strings.Contains(string(prog), "progress=") || !strings.Contains(string(prog), "speed=") {
		t.Errorf("progress file lacks the ffmpeg keys the panel reads:\n%s", prog)
	}
}

// TestRemuxPullsHLS: an HLS source with TS segments is the common IPTV case.
func TestRemuxPullsHLS(t *testing.T) {
	var segs []byte
	for g := 0; g < 8; g++ {
		segs = append(segs, gopBytes(float64(g)*2)...)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".m3u8") {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:16\n#EXTINF:16.0,\ns0.ts\n")
			return
		}
		_, _ = w.Write(segs)
	}))
	defer srv.Close()

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, baseConfig(dir, srv.URL+"/index.m3u8")) }()
	waitFile(t, filepath.Join(dir, "9_1.ts"))
	cancel()
	<-done
}

// TestRemuxUnsupportedIsDistinct: the outcome the supervisor keys the ffmpeg
// fallback on — and only for sources that can never work natively.
func TestRemuxUnsupportedIsDistinct(t *testing.T) {
	fmp4 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2.0,\ns0.m4s\n")
	}))
	defer fmp4.Close()
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer down.Close()

	for _, c := range []struct {
		name, url   string
		unsupported bool
	}{
		{"fmp4 hls", fmp4.URL + "/i.m3u8", true},
		{"rtmp", "rtmp://example/app/s", true},
		{"local file", "/srv/media/film.mp4", true},
		{"upstream 503", down.URL + "/live.ts", false},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := Run(ctx, baseConfig(t.TempDir(), c.url))
		cancel()
		if err == nil {
			t.Errorf("%s: no error", c.name)
			continue
		}
		if got := errors.Is(err, ErrUnsupported); got != c.unsupported {
			t.Errorf("%s: unsupported=%v (%v), want %v", c.name, got, err, c.unsupported)
		}
	}
}

// TestRemuxSourceEndIsAFailure: a live source that closes is a failure the
// supervisor restarts, never a clean exit.
func TestRemuxSourceEndIsAFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(gopBytes(0))
	}))
	defer srv.Close()
	err := Run(context.Background(), baseConfig(t.TempDir(), srv.URL+"/live.ts"))
	if !errors.Is(err, ErrSourceEnded) {
		t.Fatalf("err = %v, want ErrSourceEnded", err)
	}
}

// TestRemuxSurvivesDaemonRestart: the feed reconnects when the daemon's ingest
// comes back, which ffmpeg's tee slave never did.
func TestRemuxSurvivesDaemonRestart(t *testing.T) {
	dir := t.TempDir()
	src := liveTS(t)
	sock := filepath.Join(t.TempDir(), "9.sock")
	cfg := baseConfig(dir, src.URL+"/live.ts")
	cfg.IngestSock = sock

	// A daemon going away takes its producer connections with it (it is a
	// process exit, or stopIngestLocked closing them); a listener closed on its
	// own would leave the old connection happily accepting writes.
	var conns []net.Conn
	var connsMu sync.Mutex
	listen := func() (net.Listener, *bytes.Buffer, *sync.Mutex) {
		ln, err := net.Listen("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		buf := &bytes.Buffer{}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				connsMu.Lock()
				conns = append(conns, c)
				connsMu.Unlock()
				go func() {
					defer c.Close()
					b := make([]byte, 32<<10)
					for {
						n, err := c.Read(b)
						mu.Lock()
						buf.Write(b[:n])
						mu.Unlock()
						if err != nil {
							return
						}
					}
				}()
			}
		}()
		return ln, buf, &mu
	}
	size := func(b *bytes.Buffer, mu *sync.Mutex) int { mu.Lock(); defer mu.Unlock(); return b.Len() }

	ln, first, mu1 := listen()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out: %s", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor("first daemon fed", func() bool { return size(first, mu1) > 0 })

	_ = ln.Close() // the daemon goes away…
	connsMu.Lock()
	for _, c := range conns {
		_ = c.Close()
	}
	connsMu.Unlock()
	_ = os.Remove(sock)
	time.Sleep(100 * time.Millisecond)
	ln2, second, mu2 := listen() // …and comes back at the same path
	defer ln2.Close()
	waitFor("second daemon fed", func() bool { return size(second, mu2) > 0 })

	cancel()
	<-done
}

// TestAlignerResyncs: bytes ahead of the first sync byte, and junk between
// packets, are skipped; every packet handed on is whole and in order.
func TestAlignerResyncs(t *testing.T) {
	var a aligner
	p1, p2, p3 := tsfixture.FillGen(vpid, 1), tsfixture.FillGen(vpid, 2), tsfixture.FillGen(vpid, 3)
	stream := tsfixture.Concat([]byte{1, 2, 3, 0x47, 9}, p1, p2, []byte{0xff, 0xfe}, p3, p1[:100])
	var seen []uint16
	var out []byte
	// Feed it in awkward pieces, as a network read would.
	for off := 0; off < len(stream); off += 77 {
		end := off + 77
		if end > len(stream) {
			end = len(stream)
		}
		out = append(out, a.push(stream[off:end], func(p []byte) { seen = append(seen, tsfixture.ReadGen(p)) })...)
	}
	if fmt.Sprint(seen) != "[1 2 3]" {
		t.Errorf("packets seen %v, want [1 2 3]", seen)
	}
	if len(out) != 3*188 || !bytes.Equal(out, tsfixture.Concat(p1, p2, p3)) {
		t.Errorf("aligned output is %d bytes / not the three packets", len(out))
	}
}
