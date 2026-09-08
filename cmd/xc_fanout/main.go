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
	"sync/atomic"
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

	// Tighten GC growth so the heap tracks the working set instead of ballooning
	// between collections. The soft memory limit is applied once the config is
	// loaded below, since an operator may pin it there (mem_limit_mb).
	applyGCPercent()

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

	// Bound the runtime's memory footprint: a soft heap limit from the operator's
	// budget (or the cgroup/host one), plus the periodic idle-heap scavenge below
	// so freed pages actually return to the OS. Both are O(1) in stream count.
	applyMemLimit(cfg.MemLimitMB)

	mgr := server.NewManager(cfg.MaxGOPBytes, int64(cfg.PrebufferMaxSec)*1000, cfg.HLSTargetSec, cfg.HLSWindow, time.Duration(cfg.GraceSec)*time.Second)
	mgr.ApplyConfig(cfg) // also stamps write-timeout, source-insecure and chunk from the config
	idir := *ingestDir
	if idir == "" {
		idir = filepath.Join(filepath.Dir(*sock), "ingest")
	}
	_ = os.MkdirAll(idir, 0o755)
	mgr.SetIngestDir(idir)
	mgr.SetOverlay(*ffmpeg, *font)                                                           // admin "send message" drawtext overlay (no font ⇒ disabled)
	mgr.StartReaper(ctx)                                                                     // idle-stop sweep for control-managed streams (TS + HLS)
	mgr.StartMemoryScavenger(ctx, defaults.MemScavengeInterval, defaults.MemScavengeIdleMin) // return idle heap to the OS
	dlog.Logf("boot", "config: sock=%s ctl=%s ingestdir=%s prebuffer-max=%ds hls=%.1fs/%dseg grace=%ds write-timeout=%ds viewer-idle=%ds chunk=%dB maxgop=%dB insecure=%v overlay=%v",
		*sock, *ctl, idir, cfg.PrebufferMaxSec, cfg.HLSTargetSec, cfg.HLSWindow, cfg.GraceSec, cfg.WriteTimeoutSec, cfg.ViewerIdleTimeoutSec, cfg.ChunkBytes, cfg.MaxGOPBytes, cfg.SourceInsecure, *font != "")
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
			applyMemLimit(v.MemLimitMB)
			dlog.Logf("config", "applied %s: prebuffer-max=%ds hls=%.1fs/%dseg grace=%ds write-timeout=%ds viewer-idle=%ds",
				path, v.PrebufferMaxSec, v.HLSTargetSec, v.HLSWindow, v.GraceSec, v.WriteTimeoutSec, v.ViewerIdleTimeoutSec)
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

// applyGCPercent tightens GC growth (GOGC): the heap may grow this percent over
// the live set before a collection. The default 100 lets the heap reach ~2× live
// between GCs, which on a many-stream fan-out is a large RSS swing on top of a
// working set already inflated by GOP append-growth capacity. An explicit GOGC in
// the environment always wins. Set once at boot; it is not a per-config knob.
func applyGCPercent() {
	if os.Getenv("GOGC") != "" {
		return
	}
	debug.SetGCPercent(defaults.GCPercent)
	log.Printf("mem: GC target set to %d%% (GOGC) for a tighter heap", defaults.GCPercent)
}

// applyMemLimit sets the Go soft memory limit (debug.SetMemoryLimit / GOMEMLIMIT).
// It is a ceiling, not a reservation: the GC only intensifies as usage nears it,
// so a working set well under it never feels it. Skipped entirely when GOMEMLIMIT
// is set in the environment, so an explicit operator override always wins.
//
// The budget, in order: mem_limit_mb from the config when set, else the cgroup
// limit this process actually runs under, else a share of the box's RAM. The
// host fallback deliberately claims a smaller share than a cgroup one — a cgroup
// limit is this daemon's own budget, whereas the machine is shared with nginx,
// MySQL, PHP-FPM and one ffmpeg per stream, and taking 80% of it lets the fan-out
// grow until it starves the processes feeding it. Safe to call again on a config
// reload; the limit is re-applied live.
func applyMemLimit(explicitMB int) {
	if os.Getenv("GOMEMLIMIT") != "" {
		return // operator set it explicitly; the runtime already applied it
	}
	if explicitMB > 0 {
		if appliedMemLimit.Swap(int64(explicitMB)<<20) != int64(explicitMB)<<20 {
			debug.SetMemoryLimit(int64(explicitMB) << 20)
			log.Printf("mem: soft limit %d MiB (mem_limit_mb)", explicitMB)
		}
		return
	}
	budget, source, fraction := memoryBudget()
	if budget <= 0 {
		log.Printf("mem: could not detect a memory budget; no soft limit set")
		return
	}
	limit := int64(float64(budget) * fraction)
	if appliedMemLimit.Swap(limit) == limit {
		return // unchanged: every config poll re-applies, but only a change is news
	}
	debug.SetMemoryLimit(limit)
	log.Printf("mem: soft limit %d MiB (%.0f%% of %d MiB %s); periodic idle-heap scavenge on",
		limit>>20, fraction*100, budget>>20, source)
}

// appliedMemLimit is the limit currently in force, so a config reload that does
// not change it is a no-op rather than a repeated log line.
var appliedMemLimit atomic.Int64

// memoryBudget returns the memory this process should size its soft limit
// against, a label for it, and the fraction of it to claim: the cgroup limit when
// the daemon runs under a finite one (v2 first, then v1 — a v1-only host was
// previously missed entirely and silently fell back to the whole machine's RAM),
// else the physical RAM from /proc/meminfo. Returns 0 if neither can be read.
func memoryBudget() (int64, string, float64) {
	host := physicalMemoryBytes()

	// cgroup v2: a finite memory.max is the real ceiling the OOM killer enforces.
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if s := strings.TrimSpace(string(b)); s != "" && s != "max" {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
				return v, "cgroup v2 limit", defaults.MemLimitFraction
			}
		}
	}
	// cgroup v1: "unlimited" is expressed as a sentinel near the int64 ceiling
	// rather than a keyword, so treat any limit at or above physical RAM as no
	// limit at all instead of reading it as a budget.
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && v > 0 {
			if host <= 0 || v < host {
				return v, "cgroup v1 limit", defaults.MemLimitFraction
			}
		}
	}
	if host > 0 {
		return host, "system RAM", defaults.MemLimitHostFraction
	}
	return 0, "", 0
}

// physicalMemoryBytes reads MemTotal (kB) from /proc/meminfo, or 0.
func physicalMemoryBytes() int64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
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
