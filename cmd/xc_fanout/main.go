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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/config"
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
	id := flag.String("id", "", "stream id to feed at launch (testing; empty = serve only)")
	in := flag.String("in", "", "input file for -id, or - for stdin (testing)")
	source := flag.String("source", "", "comma-separated source URLs for -id (testing)")
	ua := flag.String("ua", "", "source User-Agent")
	proxy := flag.String("proxy", "", "source HTTP proxy host:port")
	cookie := flag.String("cookie", "", "source Cookie header")
	ffmpeg := flag.String("ffmpeg", "ffmpeg", "ffmpeg binary path (for the admin \"send message\" overlay)")
	font := flag.String("font", "", "font file for the admin \"send message\" drawtext overlay; empty disables the overlay")
	debug := flag.Bool("debug", false, "verbose debug log: narrate stream/puller/viewer/HLS/ingest activity and periodic per-stream state (also enabled by XC_FANOUT_DEBUG=1)")
	statsEvery := flag.Int("debug-stats", 5, "seconds between periodic per-stream state snapshots in debug mode (0 disables the snapshot)")
	// Operator tuning (prebuffer, HLS window, grace, write timeout, chunk, maxgop,
	// TLS) is NOT flags any more — the panel never set them. It lives in the JSON
	// config below, which the daemon self-creates, self-heals (backfills missing
	// keys), and polls. See internal/config.
	configPath := flag.String("config", "/home/xc_vm/bin/xc_fanout/config.json", "operator-tuning JSON (prebuffer_max_sec, hls_target_sec, hls_window, grace_sec, write_timeout_sec, chunk_bytes, max_gop_bytes, source_insecure). Self-created with defaults if absent; missing keys backfilled; polled and applied live. Empty disables the file (built-in defaults are used)")
	configInterval := flag.Int("config-interval", 60, "seconds between config-file reloads (re-read only when the file's mtime changes)")
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

	// Bound the runtime's memory footprint: a soft heap limit derived from the
	// box's RAM (so growth under load degrades into harder GC, not unbounded RSS)
	// plus a periodic idle-heap scavenge (StartMemoryScavenger below) so freed
	// pages actually return to the OS. Both are independent of the stream count.
	applyAutoMemLimit()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Operator tuning comes from the panel-editable JSON config (the CLI no longer
	// carries these knobs). Load self-creates a missing file with the built-in
	// defaults and backfills any key an older panel omitted; an empty -config or a
	// malformed file falls back to the built-in defaults. The daemon then polls the
	// file and applies changes live (prebuffer/HLS retune existing streams).
	cfg := config.Defaults()
	if *configPath != "" {
		if v, wrote, err := config.Load(*configPath); err != nil {
			log.Printf("config: %v (using built-in defaults)", err)
		} else {
			cfg = v
			if wrote {
				log.Printf("config: seeded/backfilled %s", *configPath)
			}
		}
	}

	mgr := server.NewManager(cfg.MaxGOPBytes, int64(cfg.PrebufferMaxSec)*1000, cfg.HLSTargetSec, cfg.HLSWindow, time.Duration(cfg.GraceSec)*time.Second)
	mgr.ApplyConfig(cfg) // also stamps write-timeout, source-insecure and chunk from the config
	idir := *ingestDir
	if idir == "" {
		idir = filepath.Join(filepath.Dir(*sock), "ingest")
	}
	_ = os.MkdirAll(idir, 0o755)
	mgr.SetIngestDir(idir)
	mgr.SetOverlay(*ffmpeg, *font) // admin "send message" drawtext overlay (no font ⇒ disabled)
	mgr.StartReaper(ctx)           // idle-stop sweep for control-managed streams (TS + HLS)
	mgr.StartMemoryScavenger(ctx, defaults.MemScavengeInterval, defaults.MemScavengeIdleMin) // return idle heap to the OS
	dlog.Logf("boot", "config: sock=%s ctl=%s ingestdir=%s prebuffer-max=%ds hls=%.1fs/%dseg grace=%ds write-timeout=%ds chunk=%dB maxgop=%dB insecure=%v overlay=%v",
		*sock, *ctl, idir, cfg.PrebufferMaxSec, cfg.HLSTargetSec, cfg.HLSWindow, cfg.GraceSec, cfg.WriteTimeoutSec, cfg.ChunkBytes, cfg.MaxGOPBytes, cfg.SourceInsecure, *font != "")
	mgr.StartDebugStats(ctx, time.Duration(*statsEvery)*time.Second) // periodic per-stream snapshot (debug only)

	if *configPath != "" {
		go pollConfig(ctx, *configPath, time.Duration(*configInterval)*time.Second, mgr)
	}

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
			mgr.RunPinned(ctx, *id, src, cfg.ChunkBytes)
		case *in != "":
			go func() {
				r := os.Stdin
				if *in != "-" {
					f, err := os.Open(*in)
					if err != nil {
						log.Fatalf("open %s: %v", *in, err)
					}
					defer f.Close()
					_ = ingest.Copy(f, cfg.ChunkBytes, st.Publish)
				} else {
					_ = ingest.Copy(r, cfg.ChunkBytes, st.Publish)
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

// pollConfig re-reads the operator-tuning file every `every` and applies it when
// the file changes. It is mtime-gated, so a steady file costs one stat per tick.
// A read or parse error is logged and the current tuning kept — a bad config
// (or a mid-write torn read) never interrupts streaming; the next tick retries.
// If the file is deleted out from under a running daemon it is recreated,
// preserving the running tuning (or the built-in defaults if nothing has loaded
// yet), so the self-healing contract holds at runtime, not just at startup.
func pollConfig(ctx context.Context, path string, every time.Duration, mgr *server.Manager) {
	if every < time.Second {
		every = time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	var lastMod time.Time
	var current *config.Values // last successfully applied tuning, or nil
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fi, err := os.Stat(path)
			if err != nil {
				if !os.IsNotExist(err) {
					continue // unreadable (perms, etc.): keep current, retry next tick
				}
				// Deleted at runtime — recreate it so the self-healing contract
				// holds. Preserve the running tuning if we have it; otherwise fall
				// back to the built-in defaults (config.Load self-creates them).
				if current != nil {
					if werr := config.Save(path, *current); werr != nil {
						log.Printf("config: recreate %s failed: %v", path, werr)
						continue
					}
					dlog.Logf("config", "recreated %s after deletion (kept running tuning)", path)
				} else {
					v, _, lerr := config.Load(path)
					if lerr != nil {
						log.Printf("config: recreate %s failed: %v", path, lerr)
						continue
					}
					current = &v
					mgr.ApplyConfig(v)
					dlog.Logf("config", "recreated %s after deletion (defaults)", path)
				}
				if nfi, serr := os.Stat(path); serr == nil {
					lastMod = nfi.ModTime() // re-arm the mtime gate on the recreated file
				}
				continue
			}
			if fi.ModTime().Equal(lastMod) {
				continue
			}
			lastMod = fi.ModTime()
			v, _, err := config.Load(path)
			if err != nil {
				log.Printf("config reload: %v (keeping current tuning)", err)
				continue
			}
			current = &v
			mgr.ApplyConfig(v)
			dlog.Logf("config", "applied %s: prebuffer-max=%ds hls=%.1fs/%dseg grace=%ds write-timeout=%ds",
				path, v.PrebufferMaxSec, v.HLSTargetSec, v.HLSWindow, v.GraceSec, v.WriteTimeoutSec)
		}
	}
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

// applyAutoMemLimit gives the Go runtime a soft memory limit derived from the
// box's memory, so the daemon's footprint degrades into more-aggressive GC under
// load instead of growing RSS without bound — the memory-safety knob that scales
// with the machine, not the stream count. It is a ceiling, not a reservation: a
// working set well under it never triggers it. Skipped (leaving the runtime
// default / any operator value) when GOMEMLIMIT is set in the environment, so an
// explicit override always wins. Best-effort: if the budget can't be detected it
// does nothing rather than guess.
func applyAutoMemLimit() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return // operator set it explicitly; the runtime already applied it
	}
	budget := systemMemoryBytes()
	if budget <= 0 {
		log.Printf("mem: could not detect system memory; no soft limit set")
		return
	}
	limit := int64(float64(budget) * defaults.MemLimitFraction)
	debug.SetMemoryLimit(limit)
	log.Printf("mem: soft limit %d MiB (%.0f%% of %d MiB detected); periodic idle-heap scavenge on",
		limit>>20, defaults.MemLimitFraction*100, budget>>20)

	// Tighten GC growth so the heap tracks the working set instead of ballooning to
	// ~2× live between collections (a large RSS swing on a many-stream fan-out). An
	// explicit GOGC in the environment wins, mirroring the GOMEMLIMIT handling above.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(defaults.GCPercent)
		log.Printf("mem: GC target set to %d%% (GOGC) for a tighter heap", defaults.GCPercent)
	}
}

// systemMemoryBytes returns the memory budget to size the soft limit against: the
// cgroup v2 limit when the daemon runs under a finite one (a container), else the
// physical RAM from /proc/meminfo. Returns 0 if neither can be read.
func systemMemoryBytes() int64 {
	// cgroup v2: a finite memory.max is the real ceiling the OOM killer enforces.
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if s := strings.TrimSpace(string(b)); s != "" && s != "max" {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
				return v
			}
		}
	}
	// Physical RAM: MemTotal is in kB.
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if !strings.HasPrefix(line, "MemTotal:") {
				continue
			}
			f := strings.Fields(line) // ["MemTotal:", "4004156", "kB"]
			if len(f) >= 2 {
				if kb, err := strconv.ParseInt(f[1], 10, 64); err == nil && kb > 0 {
					return kb * 1024
				}
			}
		}
	}
	return 0
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
