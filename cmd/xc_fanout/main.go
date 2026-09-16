// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

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
//
// `xc_fanout remux -i <url> … <playlist>` is a separate mode: the native remuxer
// the panel runs, under this daemon's supervisor, in place of a copy-only ffmpeg.
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
	"path"
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

// attribution is the notice required by LICENSE-ADDITIONAL-TERMS.md (§7(b)/(c)
// of the AGPL): printed by -version and at the top of -h, and it must be
// preserved in modified versions.
const attribution = "XC_VM_Fanout — Copyright (C) 2026 Vateron Media — https://github.com/Vateron-Media/XC_VM_Fanout — Licensed under AGPL-3.0"

func main() {
	// `xc_fanout remux …` is the native remuxer the panel runs in place of a
	// copy-only ffmpeg (see internal/remux). It is a separate process with its
	// own flags, dispatched before the daemon's are parsed.
	if len(os.Args) > 1 && os.Args[1] == "remux" {
		os.Exit(runRemux(os.Args[2:]))
	}

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
	debug := flag.Bool("debug", false, "verbose debug log: narrate stream/puller/viewer/HLS/ingest/monitor activity and periodic per-stream state (also enabled by XC_FANOUT_DEBUG=1)")
	debugCats := flag.String("debug-cats", "", "limit the debug log to these categories, comma-separated (e.g. \"puller,monitor\"); empty means every category. Also settable as XC_FANOUT_DEBUG=puller,monitor")
	statsEvery := flag.Int("debug-stats", 5, "seconds between periodic per-stream state snapshots in debug mode (0 disables the snapshot)")
	// Operator tuning (prebuffer, HLS window, grace, write timeout, chunk, maxgop,
	// TLS) is NOT flags any more — the panel never set them. It lives in the JSON
	// config below, which the daemon self-creates, self-heals (backfills missing
	// keys), and polls. See internal/config.
	configPath := flag.String("config", "/home/xc_vm/bin/xc_fanout/config.json", "operator-tuning JSON (prebuffer_max_sec, hls_target_sec, hls_window, grace_sec, write_timeout_sec, chunk_bytes, max_gop_bytes, source_insecure, source_backend). Self-created with defaults if absent; missing keys backfilled; polled and applied live. Empty disables the file (built-in defaults are used)")
	configInterval := flag.Int("config-interval", 60, "seconds between config-file reloads (re-read only when the file's mtime changes)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = func() {
		out := flag.CommandLine.Output()
		fmt.Fprintln(out, attribution)
		fmt.Fprintf(out, "\nUsage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// A leftover positional word is never a daemon launch — refuse it instead of
	// silently ignoring it. flag.Parse stops at the first non-flag argument, so
	// `xc_fanout version`, `xc_fanout status`, or the panel's
	// `xc_fanout remux -i … <playlist>` line handed to a binary from BEFORE the
	// native remuxer, all used to fall straight through into a full daemon on the
	// DEFAULT -sock path. On a production node that second instance took the
	// running daemon's client socket (see listenUnix) and the whole node went
	// dark. Exit 2, the same "bad usage" status the remux mode uses.
	if flag.NArg() > 0 {
		out := flag.CommandLine.Output()
		fmt.Fprintf(out, "%s: unknown command %q — this binary takes flags only (and `remux`, see -h)\n", os.Args[0], flag.Arg(0))
		flag.Usage()
		os.Exit(2)
	}

	if *showVersion {
		fmt.Println(buildVersion())
		return
	}

	// Debug mode: -debug/-debug-cats, or XC_FANOUT_DEBUG. When on, switch the
	// standard logger to microsecond timestamps so the timing of events (a slow
	// probe, a stalled viewer, reconnect backoff) is legible.
	//
	// The environment variable carries either meaning: "1" is every category, as
	// it always was, and a category list narrows it. That matters on a node with
	// hundreds of channels, where the full narration is too much to read and the
	// interesting subsystem is buried in it.
	if cats := debugSpec(*debug, *debugCats, os.Getenv("XC_FANOUT_DEBUG")); cats != "" {
		dlog.EnableCats(cats)
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		if on := dlog.Cats(); len(on) > 0 {
			dlog.Logf("boot", "debug mode on (categories: %s); version=%s pid=%d", strings.Join(on, ","), buildVersion(), os.Getpid())
		} else {
			dlog.Logf("boot", "debug mode on (all categories); version=%s pid=%d", buildVersion(), os.Getpid())
		}
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
	var startupCfg *config.Values // the boot tuning, once it is known to be real
	if *configPath != "" {
		if v, wrote, err := config.Load(*configPath); err != nil {
			log.Printf("config: %v (using built-in defaults)", err)
		} else {
			cfg = v
			startupCfg = &cfg
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
	mgr.SetOverlay(*ffmpeg, *font) // admin "send message" drawtext overlay (no font ⇒ disabled)
	// Encoder supervision is always wired, and does nothing on its own: a stream
	// is supervised only once the panel hands it over, and the daemon accepts a
	// hand-over only while config.json says `supervise: true` — a live setting,
	// so the panel can turn it on without a daemon restart dropping every viewer.
	mgr.EnableSupervision()
	mgr.StartReaper(ctx)                                                                     // idle-stop sweep for control-managed streams (TS + HLS)
	mgr.StartMemoryScavenger(ctx, defaults.MemScavengeInterval, defaults.MemScavengeIdleMin) // return idle heap to the OS
	dlog.Logf("boot", "config: supervise=%v sock=%s ctl=%s ingestdir=%s prebuffer-max=%ds hls=%.1fs/%dseg grace=%ds write-timeout=%ds viewer-idle=%ds chunk=%dB maxgop=%dB insecure=%v backend=%s overlay=%v",
		cfg.Supervise, *sock, *ctl, idir, cfg.PrebufferMaxSec, cfg.HLSTargetSec, cfg.HLSWindow, cfg.GraceSec, cfg.WriteTimeoutSec, cfg.ViewerIdleTimeoutSec, cfg.ChunkBytes, cfg.MaxGOPBytes, cfg.SourceInsecure, cfg.SourceBackend, *font != "")
	mgr.StartDebugStats(ctx, time.Duration(*statsEvery)*time.Second) // periodic per-stream snapshot (debug only)

	if *configPath != "" {
		go pollConfig(ctx, *configPath, time.Duration(*configInterval)*time.Second, mgr, startupCfg)
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

	shutdownDaemon(ctlSrv, clientSrv, func() {
		// Stop watching the encoders but LEAVE THEM RUNNING. They are orphaned, not
		// killed, and the next daemon adopts them (internal/supervisor/adopt.go), so a
		// restart or an upgrade costs the viewers nothing. Killing them here would take
		// every channel on this node off air for the length of the restart.
		if n := mgr.DetachSupervision(); n > 0 {
			log.Printf("monitor: detached %d encoder(s), left running for the next daemon to adopt", n)
		}
	}, shutdownGrace)
	cleanupClient()
	cleanupCtl()
}

// shutdownGrace is how long each HTTP surface is given to drain on the way out.
const shutdownGrace = 2 * time.Second

// shutdownDaemon stops the two HTTP surfaces and detaches encoder supervision,
// in the one order that cannot orphan an encoder: the CONTROL surface first,
// then supervision, then the client surface.
//
// Detaching first left the control socket accepting for as long as the rest of
// the shutdown took — the full grace period on a node with viewers, because a
// live-TS viewer is never idle. A DELETE /monitor/<id> arriving in that window
// found an already-emptied process table, so Release returned false and the
// handler still answered 204. The encoder is its own process group and is not
// tied to the daemon's context, so it kept running, with its pid file; the
// panel took the 204 for a stop and never handed the channel to the next
// daemon, and nothing adopted or reaped the process while it held the provider
// connection and went on writing HLS for a "stopped" channel.
//
// Each surface gets its own grace: the control API drains in milliseconds (the
// panel's requests are short), and giving it a share of the client's budget
// would only cut the drain viewers actually benefit from.
func shutdownDaemon(ctlSrv, clientSrv *http.Server, detach func(), grace time.Duration) {
	shutdown := func(srv *http.Server) {
		if srv == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
	shutdown(ctlSrv)
	detach()
	shutdown(clientSrv)
}

// pollConfig re-reads the operator-tuning file every `every` and applies it when
// the file changes. It is gated on (mtime, size), so a steady file costs one
// stat per tick. A read or parse error is logged and the current tuning kept —
// a bad config (or a mid-write torn read) never interrupts streaming — and the
// gate is left where it was so the next tick really does retry.
// If the file is deleted out from under a running daemon it is recreated,
// preserving the running tuning (or the built-in defaults if nothing has loaded
// yet), so the self-healing contract holds at runtime, not just at startup.
// `startup` is the tuning main loaded at boot, so that contract also holds in
// the first interval, before any tick has run.
// configApplier is the part of the Manager the poll loop drives. It is an
// interface so the loop's gating can be tested without a live stream registry.
type configApplier interface {
	ApplyConfig(config.Values)
}

func pollConfig(ctx context.Context, path string, every time.Duration, mgr configApplier, startup *config.Values) {
	if every < time.Second {
		every = time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	var lastMod time.Time
	var lastSize int64
	current := startup // last applied tuning (the boot one to begin with), or nil
	if current != nil {
		// Seed the gate from the file the daemon booted with, so the first tick
		// is not a pointless re-apply of what is already in force.
		if fi, err := os.Stat(path); err == nil {
			lastMod, lastSize = fi.ModTime(), fi.Size()
		}
	}
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
					lastMod, lastSize = nfi.ModTime(), nfi.Size() // re-arm the gate on the recreated file
				}
				continue
			}
			if fi.ModTime().Equal(lastMod) && fi.Size() == lastSize {
				continue
			}
			v, _, err := config.Load(path)
			if err != nil {
				// Do NOT arm the gate here. The panel writes this file
				// non-atomically (truncate, then write) and Linux stamps mtime
				// from the coarse clock, so a poll landing mid-write read an
				// empty file, failed, and armed the gate with the very mtime the
				// completed write then carried — every later tick saw "no
				// change" and the admin's edit was lost until the next save.
				log.Printf("config reload: %v (keeping current tuning)", err)
				continue
			}
			lastMod, lastSize = fi.ModTime(), fi.Size()
			current = &v
			mgr.ApplyConfig(v)
			applyMemLimit(v.MemLimitMB)
			dlog.Logf("config", "applied %s: supervise=%v prebuffer-max=%ds hls=%.1fs/%dseg grace=%ds write-timeout=%ds viewer-idle=%ds backend=%s",
				path, v.Supervise, v.PrebufferMaxSec, v.HLSTargetSec, v.HLSWindow, v.GraceSec, v.WriteTimeoutSec, v.ViewerIdleTimeoutSec, v.SourceBackend)
		}
	}
}

// serveUnix starts an HTTP server on a fresh unix socket and returns it with a
// cleanup that removes the socket file. A socket the daemon cannot bind is
// fatal: without it nginx has nothing to reach.
func serveUnix(path string, h http.Handler) (*http.Server, func()) {
	srv, cleanup, err := listenUnix(path, h)
	if err != nil {
		log.Fatal(err)
	}
	return srv, cleanup
}

// listenUnix binds path and serves h on it, returning the server and a cleanup
// that removes the socket file.
//
// Binding a unix socket means unlinking whatever is at the path first, which is
// how the daemon heals after its own unclean exit: a SIGKILLed instance leaves
// the file behind and nothing else will ever remove it. Done blind, though, that
// unlink also takes the socket of a daemon that is still RUNNING and serving
// nginx — the whole node then reaches an instance with an empty registry, every
// /live and /hls request 404s, and when the impostor stops it takes the path
// with it. So probe first: if something ANSWERS on the path, another daemon owns
// it and this one refuses rather than evicting it. Only a dead file is removed.
func listenUnix(path string, h http.Handler) (*http.Server, func(), error) {
	if dir := filepath.Dir(path); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	if socketAnswers(path) {
		return nil, nil, fmt.Errorf("listen %s: another xc_fanout is already listening there; refusing to take its socket", path)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, fmt.Errorf("listen %s: %w", path, err)
	}
	_ = os.Chmod(path, 0o660)
	owned := &ownedListener{Listener: ln}

	srv := &http.Server{Handler: h, ReadHeaderTimeout: defaults.HTTPReadHeaderTimeout}
	go func() {
		if err := srv.Serve(owned); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve %s: %v", path, err)
		}
	}()
	log.Printf("xc_fanout listening on unix:%s", path)
	// Closing the listener is what unlinks the socket — it owns the file it
	// created — so once Shutdown has run there is nothing left to remove, and
	// removing the path BY NAME anyway is how an exiting daemon used to delete
	// its REPLACEMENT's socket: Shutdown gets two seconds, and a daemon still
	// inside it when the new instance binds the same path took the new socket
	// with it, leaving nginx with nothing to reach and a healthy daemon behind
	// it. So clean up only while we still hold the listener, when the path can
	// only be our own socket.
	return srv, func() {
		if !owned.closed.Load() {
			_ = os.Remove(path)
		}
	}, nil
}

// ownedListener records that the socket file has been unlinked — which is what
// closing a unix listener does — before it can happen, so the cleanup above can
// never race the close and delete a path that by then belongs to someone else.
type ownedListener struct {
	net.Listener
	closed atomic.Bool
}

func (l *ownedListener) Close() error {
	l.closed.Store(true)
	return l.Listener.Close()
}

// socketAnswers reports whether a connection can be made to path right now, i.e.
// whether a live daemon is serving it. A socket file with no listener behind it
// (an unclean exit) refuses the connection immediately and reads as free, which
// is what keeps the self-heal working.
func socketAnswers(path string) bool {
	c, err := net.DialTimeout("unix", path, defaults.SocketProbeTimeout)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
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
// against, a label for it, and the fraction of it to claim: the cgroup limit the
// daemon actually runs under when there is a finite one, else the physical RAM
// from /proc/meminfo. Returns 0 if neither can be read.
func memoryBudget() (int64, string, float64) {
	return memoryBudgetFrom(cgroupRoot, procSelfCgroup, physicalMemoryBytes())
}

// cgroupRoot and procSelfCgroup are where the cgroup facts live on a running
// node; a test points memoryBudgetFrom at a fixture tree instead.
const (
	cgroupRoot     = "/sys/fs/cgroup"
	procSelfCgroup = "/proc/self/cgroup"
)

func memoryBudgetFrom(root, procCgroup string, host int64) (int64, string, float64) {
	if v, src := cgroupMemoryLimit(root, procCgroup, host); v > 0 {
		return v, src, defaults.MemLimitFraction
	}
	if host > 0 {
		return host, "system RAM", defaults.MemLimitHostFraction
	}
	return 0, "", 0
}

// cgroupMemoryLimit is the tightest finite memory ceiling in force for this
// process, found by walking from its OWN cgroup up to the root.
//
// Reading only the root cgroup's files, as this used to, sees a limit only
// inside a container namespace where the root IS the container's cgroup. On an
// ordinary cgroup v2 host the root has no memory.max at all and a unit's
// MemoryMax lives at /sys/fs/cgroup/system.slice/<unit>/memory.max, so a daemon
// run under `MemoryMax=2G` on a 16 GB box was given a soft limit of 8 GB: the
// GC never tightened, and the kernel OOM-killed the daemon at 2 GB with every
// viewer on the node attached. A v1 parent-slice limit was missed the same way.
func cgroupMemoryLimit(root, procCgroup string, host int64) (int64, string) {
	v2, v1 := cgroupPaths(procCgroup)
	best, src := int64(0), ""
	take := func(v int64, label string) {
		if v > 0 && (best == 0 || v < best) {
			best, src = v, label
		}
	}
	// cgroup v2: a finite memory.max is the real ceiling the OOM killer enforces.
	for _, dir := range cgroupAncestors(v2) {
		if v, ok := readCgroupV2Max(filepath.Join(root, dir, "memory.max")); ok {
			take(v, "cgroup v2 limit")
		}
	}
	for _, dir := range cgroupAncestors(v1) {
		if v, ok := readCgroupV1Limit(filepath.Join(root, "memory", dir, "memory.limit_in_bytes"), host); ok {
			take(v, "cgroup v1 limit")
		}
	}
	return best, src
}

// cgroupPaths reads /proc/self/cgroup for this process's cgroup path on the
// unified (v2) hierarchy and on v1's memory controller. Either falls back to
// "/", which is the root-only lookup this did before and the right answer
// inside a cgroup namespace.
func cgroupPaths(procCgroup string) (v2, v1 string) {
	v2, v1 = "/", "/"
	b, err := os.ReadFile(procCgroup)
	if err != nil {
		return v2, v1
	}
	for _, line := range strings.Split(string(b), "\n") {
		// hierarchy-ID:controller-list:path — "0::/…" is the unified hierarchy.
		f := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(f) != 3 || f[2] == "" {
			continue
		}
		if f[1] == "" {
			v2 = f[2]
			continue
		}
		for _, c := range strings.Split(f[1], ",") {
			if c == "memory" {
				v1 = f[2]
			}
		}
	}
	return v2, v1
}

// cgroupAncestors lists a cgroup path and every parent up to the root, nearest
// first. A limit set on a parent slice binds this process just as much as one
// on its own cgroup.
func cgroupAncestors(p string) []string {
	if p == "" || p[0] != '/' {
		p = "/" + p
	}
	p = path.Clean(p)
	out := []string{p}
	for p != "/" {
		p = path.Dir(p)
		out = append(out, p)
	}
	return out
}

// readCgroupV2Max reads a v2 memory.max, where no limit is the word "max".
func readCgroupV2Max(path string) (int64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(b))
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

// readCgroupV1Limit reads a v1 memory.limit_in_bytes. "unlimited" there is a
// sentinel near the int64 ceiling rather than a keyword, so treat any limit at
// or above physical RAM as no limit at all instead of reading it as a budget.
func readCgroupV1Limit(path string, host int64) (int64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	if host > 0 && v >= host {
		return 0, false
	}
	return v, true
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
// debugSpec resolves the three ways debug can be asked for into one category
// spec for dlog.EnableCats, or "" for off.
//
// Precedence runs most-specific first: an explicit -debug-cats list, then a
// category list in the environment, then the plain on/off forms. That way a
// systemd drop-in carrying XC_FANOUT_DEBUG=puller is not silently widened to
// everything by a -debug that someone left on the command line.
func debugSpec(debug bool, cats, env string) string {
	if s := strings.TrimSpace(cats); s != "" {
		return s
	}
	if s := strings.TrimSpace(env); s != "" && !isTruthy(s) {
		// A non-empty value that is not a boolean is a category list. An
		// explicitly false one ("0", "off") stays off unless -debug says
		// otherwise, which is what the flag is for.
		if !isFalsey(s) {
			return s
		}
	}
	if debug || isTruthy(env) {
		return "all"
	}
	return ""
}

func isFalsey(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "0", "false", "no", "off":
		return true
	}
	return false
}

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
