// Command xc_fanout is the native live-stream fan-out daemon (ADR 0002, P2–P3).
//
// One process serves many streams: it pulls each live source once and fans it
// out to many viewers over GET /live/<id>, and produces HLS in memory. This
// keeps PHP out of the byte path — a viewer is a cheap connection, not a pinned
// worker.
//
// Two HTTP surfaces on separate unix sockets:
//   - -sock (client, nginx-facing): /live/<id>, /hls/<id>/index.m3u8, /hls/<id>/<seq>.ts
//   - -ctl  (control, PHP-only):    PUT/DELETE /streams/<id>  → register/unregister a
//     source; the puller starts on the first viewer and stops after the last leaves.
//
// -id/-source and -id/-in remain for isolated testing (feed one stream at launch).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/dlog"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/ingest"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/puller"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/server"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	sock := flag.String("sock", "/home/xc_vm/bin/xc_fanout/sockets/http.sock", "client unix socket (nginx-facing)")
	ctl := flag.String("ctl", "", "control unix socket (PHP-only), e.g. /home/xc_vm/bin/xc_fanout/sockets/control.sock; empty = no control API")
	ingestDir := flag.String("ingestdir", "", "dir for per-stream push-fed ingest sockets (non-proxy tee); empty = <sock dir>/ingest")
	grace := flag.Int("grace", 10, "seconds to keep a control-managed puller alive after the last viewer")
	writeTimeout := flag.Int("write-timeout", 15, "seconds a single write to a live-TS viewer may stall before the viewer is dropped (stalled/half-open client cleanup)")
	id := flag.String("id", "", "stream id to feed at launch (testing; empty = serve only)")
	in := flag.String("in", "", "input file for -id, or - for stdin (testing)")
	source := flag.String("source", "", "comma-separated source URLs for -id (testing)")
	ua := flag.String("ua", "", "source User-Agent")
	proxy := flag.String("proxy", "", "source HTTP proxy host:port")
	cookie := flag.String("cookie", "", "source Cookie header")
	ffmpeg := flag.String("ffmpeg", "ffmpeg", "ffmpeg binary path")
	maxGOP := flag.Int("maxgop", 10528000, "max join-snapshot size in bytes")
	prebufferMax := flag.Int("prebuffer-max", 20, "ceiling (seconds) of live TS history retained per stream for client_prebuffer; a viewer's ?prebuffer= is clamped to this")
	chunk := flag.Int("chunk", defaults.IngestChunk, "ingest read size (aligned down to 188)")
	hlsTarget := flag.Float64("hlstarget", 6, "HLS target segment duration (seconds)")
	hlsWindow := flag.Int("hlswindow", 6, "HLS segments kept in the sliding window")
	font := flag.String("font", "", "font file for the admin \"send message\" drawtext overlay; empty disables the overlay")
	sourceInsecure := flag.Bool("source-insecure", true, "skip TLS certificate verification when pulling HTTPS sources (default true: the panel commonly pulls self-signed/mismatched-cert upstreams; set false to require valid certs)")
	debug := flag.Bool("debug", false, "verbose debug log: narrate stream/puller/viewer/HLS/ingest activity and periodic per-stream state (also enabled by XC_FANOUT_DEBUG=1)")
	statsEvery := flag.Int("debug-stats", 5, "seconds between periodic per-stream state snapshots in debug mode (0 disables the snapshot)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(buildVersion())
		return
	}

	// Debug mode: -debug or XC_FANOUT_DEBUG=1. When on, switch the standard logger
	// to microsecond timestamps so the timing of events (a slow probe, a stalled
	// viewer, reconnect backoff) is legible.
	if *debug || isTruthy(os.Getenv("XC_FANOUT_DEBUG")) {
		dlog.Enable(true)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		dlog.Logf("boot", "debug mode on; version=%s pid=%d", buildVersion(), os.Getpid())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mgr := server.NewManager(*maxGOP, int64(*prebufferMax)*1000, *hlsTarget, *hlsWindow, time.Duration(*grace)*time.Second)
	mgr.SetWriteTimeout(time.Duration(*writeTimeout) * time.Second)
	idir := *ingestDir
	if idir == "" {
		idir = filepath.Join(filepath.Dir(*sock), "ingest")
	}
	_ = os.MkdirAll(idir, 0o755)
	mgr.SetIngestDir(idir)
	mgr.SetSourceInsecure(*sourceInsecure)
	mgr.SetOverlay(*ffmpeg, *font) // admin "send message" drawtext overlay (no font ⇒ disabled)
	mgr.StartReaper(ctx)           // idle-stop sweep for control-managed streams (TS + HLS)
	dlog.Logf("boot", "config: sock=%s ctl=%s ingestdir=%s grace=%ds write-timeout=%ds prebuffer-max=%ds hls=%.1fs/%dseg overlay=%v",
		*sock, *ctl, idir, *grace, *writeTimeout, *prebufferMax, *hlsTarget, *hlsWindow, *font != "")
	mgr.StartDebugStats(ctx, time.Duration(*statsEvery)*time.Second) // periodic per-stream snapshot (debug only)

	clientSrv, cleanupClient := serveUnix(*sock, mgr.ClientHandler())
	ctlSrv, cleanupCtl := (*http.Server)(nil), func() {}
	if *ctl != "" {
		ctlSrv, cleanupCtl = serveUnix(*ctl, mgr.ControlHandler())
	}

	// Optional launch-time feed for isolated testing.
	if *id != "" {
		st := mgr.GetOrCreate(*id)
		switch {
		case *source != "":
			// Pinned launch feed: runs through the Manager like a control-managed
			// source (so status/running/reaper stay consistent), just without
			// waiting for a viewer.
			src := puller.Source{
				URLs:      splitCSV(*source),
				UserAgent: *ua,
				Proxy:     *proxy,
				Cookie:    *cookie,
				FfmpegBin: *ffmpeg,
			}
			mgr.RunPinned(ctx, *id, src, *chunk)
		case *in != "":
			go func() {
				r := os.Stdin
				if *in != "-" {
					f, err := os.Open(*in)
					if err != nil {
						log.Fatalf("open %s: %v", *in, err)
					}
					defer f.Close()
					_ = ingest.Copy(f, *chunk, st.Publish)
				} else {
					_ = ingest.Copy(r, *chunk, st.Publish)
				}
				log.Printf("ingest for id=%s finished", *id)
			}()
		}
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = clientSrv.Shutdown(shutdownCtx)
	if ctlSrv != nil {
		_ = ctlSrv.Shutdown(shutdownCtx)
	}
	cleanupClient()
	cleanupCtl()
}

// serveUnix starts an HTTP server on a fresh unix socket and returns it with a
// cleanup that removes the socket file.
func serveUnix(path string, h http.Handler) (*http.Server, func()) {
	if dir := filepath.Dir(path); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		log.Fatalf("listen %s: %v", path, err)
	}
	_ = os.Chmod(path, 0o660)

	srv := &http.Server{Handler: h, ReadHeaderTimeout: defaults.HTTPReadHeaderTimeout}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve %s: %v", path, err)
		}
	}()
	log.Printf("xc_fanout listening on unix:%s", path)
	return srv, func() { _ = os.Remove(path) }
}

// buildVersion returns the ldflags-stamped version, or—when the binary was
// built without -ldflags "-X main.version=…" (a plain `go build`, so version is
// still "dev")—falls back to the VCS revision the Go toolchain embeds, so a
// hand-built binary still identifies its commit instead of a bare "dev".
func buildVersion() string {
	if version != "dev" {
		return version
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	var rev, dirty string
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	if rev == "" {
		return version
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	return "dev+" + rev + dirty
}

// isTruthy reports whether an env var value means "on" (1/true/yes/on).
func isTruthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
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
