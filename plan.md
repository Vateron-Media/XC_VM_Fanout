# Plan — the load balancer as one standalone binary

Oct 15, 2026 · draft for review

**Verdict: doable, but only in stages.** The first stage works today. The full
goal, an LB with no PHP and no nginx, is a big port (a rough guess is 6–9
person-months). It also needs three decisions before it can start (see
*Blockers*).

## What an LB runs today

- **Go (already single static binaries):** `xc_fanout` handles live fan-out,
  in-memory HLS, the segment gateway and the `remux` command that replaces
  copy-only ffmpeg. `xc_agent` handles the cluster API, the config copy from
  MAIN, the viewer registry, the lease and telemetry.
- **PHP:** about 102k lines of PHP ship to an LB, across 477 files. They cover:
  - the stream endpoints: `live`, `vod`, `timeshift`, `thumb`, `subtitle`,
    `rtmp`, `xfile`, `progress`, `status`;
  - the stream lifecycle commands: `MonitorCommand`, `Ondemand`, `Llod`,
    `Delay`, `Loopback`, `Created`, `Record`, `Archive`, `Thumbnail`, `Queue`;
  - 12 crons with role `all`;
  - the background loops `signals`, `watchdog`, `queue` and `fanout_sync`;
  - the cluster commands `cluster:apply`, `cluster:exec` and `cluster:root`.
- **Other binaries:** nginx, nginx_rtmp, PHP-FPM, ffmpeg/ffprobe, yt-dlp,
  `network.py` and certbot.

## Check run (Oct 15, 2026)

A scratch prototype of a single `xc_node` binary was built. It turns both
`main()` functions into packages and picks one by subcommand or by the name it
is called under:

```
xc_fanout 7.8 MB + xc_agent 7.6 MB  →  xc_node 8.7 MB, static, CGO_ENABLED=0
./xc_node fanout -version   ✓      ./xc_node agent version   ✓
```

Neither program registers package-level flags, so they merge with no changes.

## Plan

| Phase | What | Rough effort |
|---|---|---|
| **1. One Go binary** ✅ checked | Ship `xc_node` (fanout + agent + remux) as one release asset, with `xc_fanout`/`xc_agent` as symlinks so PHP's process-name checks and `run.sh` keep working. Merge `fanout_binary` and `agent_binary`. | 1–2 weeks |
| **2. Go serves every HTTP request** | Extend the gateway from segments, keys, refreshes and TS first requests to also cover HLS first requests, `vod` (file + Range), `timeshift`, `thumb`, `subtitle`, `xfile`, `progress`/`status` and RTMP auth. Use the existing "shadow, then 7 clean days" rollout with shared test vectors for each endpoint. PHP-FPM is then off the request path. | 6–8 weeks |
| **3. Replace nginx** | A Go front server: ports, TLS with Go's built-in Let's Encrypt support (replaces certbot), real IP, limits and sendfile. Separately, decide on RTMP ingest (`nginx_rtmp`): use a Go library or keep it. | 4–6 weeks |
| **4. Stream lifecycle in Go** | The fanout supervisor already starts and restarts producers. Port the ffmpeg command builder (`StreamProcess`) and run on-demand, LLOD, delay, loopback, created channels, recording/archive, thumbnails and the VOD transcode queue from the config copy MAIN sends. | 8–12 weeks |
| **5. Crons and background loops** | Move the watchdog (the agent already samples the host every second), signals, queue, cleanup/tmp/errors/logs, the streams cron, maxmind and ipset into goroutines. Root-only work (ipset, binding ports below 1024, updates, `cluster:root`) goes to a small root helper mode or Linux capabilities. | 4–6 weeks |
| **6. Packaging** | The installer becomes "copy the binary, then `xc_node install`". PHP, nginx and python are gone from the LB. The PHP LB stays as a fallback for a few releases. | 2 weeks |

## Blockers to decide on

1. **Mode 2 only.** Nodes in mode 0 or 1 still read MAIN's MySQL and Redis
   directly. A Go-only LB should support mode 2 only. That makes the cutover
   (`api_mode_allowed`, still false) a prerequisite.
2. **The `fanout_enabled = 0` fallback.** ADR 0003's amendment requires the
   pre-fanout delivery paths, which live in PHP. Either port those paths or
   drop that requirement for LBs.
3. **Licence enforcement.** On an LB it now sits in the closed `xcvm_core`
   Rust extension so it "cannot be commented out". In open-source Go it would
   be easy to patch out. The alternative is linking the Rust core in through
   cgo, which ends `CGO_ENABLED=0` and the "runs on any distro" property.

## Hard limits

- **ffmpeg stays.** It's still needed for transcoding, logos and profiles, and
  ffprobe is still used. "Standalone" realistically means one Go binary plus a
  managed ffmpeg, which nodes already download per distro. Embedding it would
  need a static ffmpeg build and would make the binary about 80–100 MB.
- **No PHP fallback once PHP is gone.** The current safety rule, "anything not
  proven equivalent goes to PHP", disappears. Every endpoint needs parity
  vectors and a shadow period before PHP is removed.
- **MAIN keeps PHP**, so streaming logic exists twice. The shared vector files
  already used by the gateway are how both sides stay in step.

## Repackaging PHP instead

Bundling the existing PHP into one binary (FrankenPHP or static-php-cli) is
not a real option. `xcvm_core.so` is a closed, dynamically loaded extension,
and a static PHP build can't load it. The result would be a self-extracting
archive, not a standalone binary.

## Recommendation

Do Phase 1 now, since it's cheap and checked. Then do Phase 2: it removes
PHP-FPM from the request path, which is the measured bottleneck. Decide on
blockers 1–3 before starting Phases 4–5.
