// Command origin is a fake live-stream source for exercising the xc_fanout
// daemon end-to-end. It serves one synthetic "channel" (generated from a short
// looping sample.ts) in the two shapes the daemon's puller distinguishes:
//
//   - GET /ts/<ch>        Content-Type: video/mp2t — an endless MPEG-TS stream
//     the daemon consumes directly (no ffmpeg on the daemon side).
//   - GET /hls/<ch>/...   an HLS playlist + segments the daemon must remux to
//     MPEG-TS with ffmpeg.
//
// Plus deliberately-broken endpoints for failover/off-air tests:
//
//   - GET /ts/dead        always 404 (a source URL that never works).
//   - GET /ts/stall       200 video/mp2t but never sends a byte (a source that
//     connects yet produces no data — the "not on air" case).
//
// The stream is produced by ffmpeg looping sample.ts; nothing here decodes or
// re-encodes, so a machine with a stock ffmpeg keeps up easily.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	sample := flag.String("sample", "/data/sample.ts", "looping MPEG-TS sample fed to every endpoint")
	ffmpeg := flag.String("ffmpeg", "ffmpeg", "ffmpeg binary path")
	hlsDir := flag.String("hlsdir", "/tmp/hls", "directory the persistent HLS writer emits into")
	channelsCSV := flag.String("channels", "ch1", "comma-separated channel ids to publish")
	flag.Parse()

	if _, err := os.Stat(*sample); err != nil {
		log.Fatalf("origin: sample %s not found: %v (build the image or pass -sample)", *sample, err)
	}
	channels := splitCSV(*channelsCSV)
	if len(channels) == 0 {
		log.Fatal("origin: no channels")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// One persistent HLS writer per channel: ffmpeg loops the sample and keeps a
	// small sliding window of segments on disk that we serve as static files.
	for _, ch := range channels {
		dir := filepath.Join(*hlsDir, ch)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("origin: mkdir %s: %v", dir, err)
		}
		go runHLSWriter(ctx, *ffmpeg, *sample, dir)
	}

	srv := &origin{ffmpeg: *ffmpeg, sample: *sample, hlsDir: *hlsDir, channels: set(channels)}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("/stats", srv.serveStats)
	mux.HandleFunc("/ts/", srv.serveTS)
	mux.HandleFunc("/hls/", srv.serveHLS)

	httpSrv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sc)
	}()
	log.Printf("origin: listening on %s, channels=%v", *addr, channels)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("origin: %v", err)
	}
}

type origin struct {
	ffmpeg   string
	sample   string
	hlsDir   string
	channels map[string]bool

	tsActive atomic.Int64 // concurrent /ts pull connections right now
	tsTotal  atomic.Int64 // /ts pulls started since boot
}

// serveStats reports how many source pulls the daemon has opened — the fan-out
// invariant check: N viewers on one channel must still be a single upstream pull.
func (o *origin) serveStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int64{
		"ts_active": o.tsActive.Load(),
		"ts_total":  o.tsTotal.Load(),
	})
}

// serveTS handles /ts/<ch> and the two fault endpoints /ts/dead and /ts/stall.
func (o *origin) serveTS(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/ts/")
	switch name {
	case "dead":
		http.Error(w, "dead source", http.StatusNotFound)
		return
	case "stall":
		// Connect, promise TS, then never send a byte: this is the daemon's
		// "puller running, has_data=false" off-air signal.
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		flush(w)
		<-r.Context().Done()
		return
	}
	if !o.channels[name] {
		http.Error(w, "no such channel", http.StatusNotFound)
		return
	}
	o.tsActive.Add(1)
	o.tsTotal.Add(1)
	defer o.tsActive.Add(-1)
	w.Header().Set("Content-Type", "video/mp2t")
	w.WriteHeader(http.StatusOK)
	flush(w)
	o.pipeFfmpeg(r.Context(), w)
}

// pipeFfmpeg loops the sample as real-time MPEG-TS straight to the client and
// stops as soon as the client disconnects.
func (o *origin) pipeFfmpeg(ctx context.Context, w http.ResponseWriter) {
	cmd := exec.CommandContext(ctx, o.ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-re", "-stream_loop", "-1", "-i", o.sample,
		"-c", "copy", "-mpegts_flags", "+initial_discontinuity",
		"-f", "mpegts", "pipe:1",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("origin: ts stdout pipe: %v", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Printf("origin: ts ffmpeg start: %v", err)
		return
	}
	defer cmd.Wait()
	buf := make([]byte, 188*64)
	for {
		n, rerr := stdout.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return // client gone
			}
			flush(w)
		}
		if rerr != nil {
			return
		}
	}
}

// serveHLS serves the on-disk sliding-window playlist and segments.
func (o *origin) serveHLS(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/hls/")
	ch, file, ok := strings.Cut(rest, "/")
	if !ok || !o.channels[ch] || strings.Contains(file, "..") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	full := filepath.Join(o.hlsDir, ch, filepath.Clean("/"+file))
	if strings.HasSuffix(file, ".m3u8") {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	} else if strings.HasSuffix(file, ".ts") {
		w.Header().Set("Content-Type", "video/mp2t")
	}
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, full)
}

// runHLSWriter keeps a live HLS window on disk, restarting ffmpeg if it dies.
func runHLSWriter(ctx context.Context, ffmpeg, sample, dir string) {
	for ctx.Err() == nil {
		cmd := exec.CommandContext(ctx, ffmpeg,
			"-hide_banner", "-loglevel", "error", "-nostdin",
			"-re", "-stream_loop", "-1", "-i", sample, "-c", "copy",
			"-f", "hls", "-hls_time", "2", "-hls_list_size", "6",
			"-hls_flags", "delete_segments+omit_endlist",
			"-hls_segment_filename", filepath.Join(dir, "seg_%05d.ts"),
			filepath.Join(dir, "index.m3u8"),
		)
		if err := cmd.Run(); err != nil && ctx.Err() == nil {
			log.Printf("origin: hls writer %s: %v (restart in 1s)", dir, err)
			time.Sleep(time.Second)
		}
	}
}

func flush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func set(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}
