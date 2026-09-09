# 06. Parameters and configuration

> What the daemon consumes as input. Since **0.11.1** this comes from **two** places:
> the **command-line flags** (placement, identity, debug — set once at launch), and a
> **JSON config file** (the operator tuning — buffer sizes, timeouts, HLS — edited by the
> panel and applied live, no restart). The entry point is
> [`cmd/xc_fanout/main.go`](../../cmd/xc_fanout/main.go).

Historically (≤ 0.10.0) the daemon had **no config file** and every knob was a launch flag.
The problem: the panel never set those flags, so a value like the prebuffer depth or the HLS
window could not be changed without a rebuild. In **0.11.1** the operator tuning moved into a
JSON file the panel edits and the daemon polls; the flags that carried those knobs were retired
(see [Migration from 0.10.0](#migration-from-0100) below).

The dynamic part (which streams to serve) still arrives at runtime via the
[control API](03-endpoints.md#control-surface--php-only).

## Command-line flags

Flags set what the daemon *is* (its sockets, identity, debug), not the tunable numbers. They
are fixed for the lifetime of the process.

| Flag | Default | Purpose |
|------|---------|---------|
| `-sock` | `/home/xc_vm/bin/xc_fanout/sockets/http.sock` | Client unix socket (this is where nginx/viewers connect). |
| `-ctl` | `""` (off) | Control unix socket (PHP only). Empty = no control API. |
| `-ingestdir` | `<dir of -sock>/ingest` | Directory for per-stream push sockets (for ingest mode). |
| `-config` | `/home/xc_vm/bin/xc_fanout/config.json` | Path to the operator-tuning JSON (see [The config file](#the-config-file)). Self-created with defaults if absent; missing keys backfilled; polled and applied live. Empty = disable the file (the built-in defaults are used). |
| `-config-interval` | `60` | Seconds between config-file reloads. The file is re-read only when its mtime changes. |
| `-ffmpeg` | `ffmpeg` | Path to the ffmpeg binary (for remuxing non-mp2t sources, and for the "send message" `drawtext` overlay). Must be a build that has the `drawtext` filter for the overlay to work. |
| `-font` | `""` (overlay off) | Path to a `.ttf` font for the "send message" overlay. Empty, or an ffmpeg without `drawtext`, disables the overlay (signals become no-ops). |
| `-debug` | `false` | Verbose debug log: narrate stream/puller/viewer/HLS/ingest activity plus a periodic per-stream state snapshot. Also enabled by `XC_FANOUT_DEBUG=1`. See ["Debug mode"](#debug-mode) below. |
| `-debug-stats` | `5` | Seconds between the periodic per-stream state snapshots in debug mode (`0` disables just the snapshot; the event log stays on). |
| `-version` | — | Print the version and exit. |

### "launch / test" mode flags

For isolated testing — feed a single stream directly at startup, without the control API:

| Flag | Purpose |
|------|---------|
| `-id` | Identifier of the stream to feed at startup (empty = serve only). |
| `-source` | Source(s) for `-id`, comma-separated (pull mode). |
| `-in` | Input file for `-id`, or `-` for stdin. |
| `-ua` | Source User-Agent. |
| `-proxy` | Source HTTP proxy `host:port`. |
| `-cookie` | Value of the `Cookie` header for the source. |

`-source` and `-in` are mutually exclusive: the former pulls from a URL, the latter reads from a file/stdin.

## The config file

The operator tuning lives in a small JSON file (`-config`, default
`/home/xc_vm/bin/xc_fanout/config.json`), owned by [`internal/config`](../../internal/config/config.go).
An admin edits the values in the panel; the panel writes them to this file; the daemon polls it
and applies the changes **at runtime — no restart, no viewer drop**.

### Self-maintaining

The file never has to be created or migrated by hand:

- **Absent** → the daemon writes it with the built-in defaults ([`internal/defaults`](../../internal/defaults/defaults.go),
  the `Cfg*` constants), so a fresh node starts from a real, editable file instead of nothing.
  This holds not just at startup: if the file is **deleted while the daemon runs**, the next
  poll recreates it — re-saving the values currently in memory (so deleting it never silently
  resets live tuning), or the built-in defaults if none are known yet.
- **Missing a key** → the daemon backfills that key with its default **and rewrites the file**,
  so the on-disk file always carries the daemon's full current schema. An **older** panel that
  writes only a subset never makes the daemon throw, and a key a **newer** daemon added appears
  in the file (with its default) for the panel to pick up.
- **Malformed** → the daemon keeps the running values and does **not** overwrite the file (it may
  be a torn mid-write or a hand-edit in progress); the next poll retries. Streaming never breaks
  on a bad config.

Writes are **atomic** (temp file + rename), so a reader — the daemon polling, or a concurrent
panel write — never sees a half-written file.

### Polled and applied live

The daemon re-reads the file every `-config-interval` seconds, but only when the file's **mtime**
changed since the last check (a stat, not a full parse, on a quiet tick). A change is applied to
**existing** streams in place within one poll:

- the **prebuffer ring** and **HLS window** shrink and free memory immediately;
- `write_timeout_sec` / `source_insecure` become atomic (they are read on hot paths);
- `chunk_bytes` / `max_gop_bytes` take effect for **newly created** streams.

No stream is dropped and no viewer is disconnected when the config changes.

### The keys

Every value is validated (clamped) on load, so a typo in the panel or a hand-edited file can
never push the daemon into a pathological state.

| Key | Default | Range | Affects |
|-----|---------|-------|---------|
| `prebuffer_max_sec` | `40` | `0…120` | The buffer/ring size per stream (seconds of TS history). This **is** the buffer — it drives both the TS prebuffer and the HLS depth. |
| `default_prebuffer_sec` | `0` | `0…120` | Fallback per-viewer join burst **only** when a `/live/` request carries no `?prebuffer=`. `0` = the daemon imposes nothing (current GOP only). See [Per-viewer prebuffer](#per-viewer-prebuffer). |
| `hls_target_sec` | `6` | `1…30` | Target HLS segment duration (seconds). Actual length floats (cut on keyframes). |
| `hls_window` | `6` | `1…20` | How many HLS segments the playlist lists — a **display cap**, not a ring driver. |
| `grace_sec` | `10` | `1…3600` | Idle-stop grace for control-managed streams: how long a source stays alive after the last viewer leaves (the reaper window). |
| `write_timeout_sec` | `15` | `1…600` | Per-write deadline for a live-TS viewer before a stalled connection is dropped. |
| `chunk_bytes` | `12032` | `188…4 MiB` | Source read size for daemon-pulled streams (rounded down to a multiple of 188). |
| `max_gop_bytes` | `10528000` | `188…256 MiB` | Cap on a single ring block. A source that gives no keyframe within this many bytes has a block **cut** here (see [Sources without keyframes](04-internals.md#sources-without-keyframes)). |
| `source_insecure` | `true` | — | Skip upstream TLS verification when pulling HTTPS sources. `true` because the panel commonly pulls upstreams with self-signed / mismatched certs; set `false` to require valid certificates. |
| `idle_buffer_grace_sec` | `30` | `0…3600` | No-viewer window before the ring collapses (`0` = the idle gate is off). See [The idle-buffer gate](#the-idle-buffer-gate). |
| `idle_buffer_ratio` | `0.5` | `0.1…1` | Fraction of the buffer kept while a stream is unwatched. HLS is still cut from the reduced ring, so the channel stays openable. |
| `viewer_idle_timeout_sec` | `30` | `0`, or `5…3600` | Drop a live-TS viewer that has received **nothing** for this long (`0` = never). This is what bounds a ghost on an off-air stream — `write_timeout_sec` can only fire while there are bytes to write. See [Guarding against stalled and idle viewers](04-internals.md#guarding-against-stalled-viewers). |
| `source_backend` | `auto` | `auto`, `ffmpeg`, `native` | How a **non-mp2t** source becomes MPEG-TS. See [The source backend](#the-source-backend). An unknown value falls back to `auto`. |
| `mem_limit_mb` | `0` (auto) | `0…1 TiB` | Explicit ceiling (MiB) for the Go soft memory limit. `0` derives it from the cgroup limit, else a share of the box's RAM. See [The memory budget](#the-memory-budget). |

A minimal file the daemon writes on a fresh node:

```json
{
  "prebuffer_max_sec": 40,
  "hls_target_sec": 6,
  "hls_window": 6,
  "grace_sec": 10,
  "write_timeout_sec": 15,
  "chunk_bytes": 12032,
  "max_gop_bytes": 10528000,
  "source_insecure": true,
  "default_prebuffer_sec": 0,
  "idle_buffer_grace_sec": 30,
  "idle_buffer_ratio": 0.5,
  "viewer_idle_timeout_sec": 30,
  "mem_limit_mb": 0,
  "source_backend": "auto"
}
```

## What the key parameters affect

### The buffer / ring

Since 0.11.1 there is **one** per-stream buffer, not two: the keyframe-aligned TS ring in
[`tsjoin`](04-internals.md#clean-join-and-prebuffer--tsjoin). `prebuffer_max_sec` sizes it, and
HLS is a **view** over it (see [ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md)). So this
one number drives both the live-TS prebuffer and how deep the HLS window can reach.

- **`prebuffer_max_sec`** — the cap on the TS history a stream holds. Directly the per-stream RAM
  ceiling. Must be ≥ the HLS window (`hls_window · hls_target_sec`) for HLS to reach its full
  depth; the default 40 covers a 6×6 window comfortably.
- **`max_gop_bytes`** — the size limit of a single ring block. Normally never reached: blocks are
  cut on keyframes, which arrive far more often. It is what bounds a source whose keyframes are
  rare or absent — reaching it forces a block boundary, so the ring keeps rolling instead of
  freezing. (Before 0.11.4 it was a discard threshold: everything past it was thrown away and the
  stream stalled at one block. See
  [04, "Sources without keyframes"](04-internals.md#sources-without-keyframes).)
- **`chunk_bytes`** — the size of source read chunks. Rounded down to a multiple of 188.

### The idle-buffer gate

To stop idle channels holding a full buffer with no audience, a stream with **no live viewer and
no viewer touch** (a TS attach or an HLS request) for `idle_buffer_grace_sec` has its ring
collapsed to `prebuffer_max_sec × idle_buffer_ratio` by the reaper sweep, and pumped back to the
full buffer the instant a viewer returns. HLS **keeps being cut** from the reduced ring (with a
`2 · hls_target` floor so at least one segment remains), so an idle HLS channel's playlist stays
non-empty and opens immediately. Set `idle_buffer_grace_sec = 0` to disable the gate entirely.
See [05. Lifecycle](05-lifecycle.md) and [ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md).

### Per-viewer prebuffer

The join-burst depth a viewer gets on connect is the **panel's** call, not the daemon's: the
panel knows whether the viewer is a client or a restreamer and passes the chosen depth in the
`?prebuffer=<sec>` parameter of the internal `/live/<id>` URL. The daemon honors that value
**as-is, including an explicit `0`** (= current GOP only). `default_prebuffer_sec` is the fallback
used **only** when a request carries no `?prebuffer=` at all — its default `0` means the daemon
imposes nothing on its own.

### Lifecycle timings

- **`grace_sec`** — how "inertial" a source is. A larger value = the source lives longer after
  viewers leave (faster pickup when they return, but longer idle hang time). The reaper ticks
  every `grace/2`. See [05. Lifecycle](05-lifecycle.md).
- **`write_timeout_sec`** — the threshold for dropping a "stalled" viewer. Lower = we clean up
  dead connections faster, but with a higher risk of hitting a slow-but-alive one. 15 s has
  headroom, since a healthy realtime viewer accumulates ≤ 1 s of lag per second. See
  [04, "Guarding against stalled viewers"](04-internals.md#guarding-against-stalled-viewers).
- **`viewer_idle_timeout_sec`** — the threshold for dropping an **idle** viewer, i.e. one being
  sent nothing at all. `write_timeout_sec` covers a viewer that will not *read*; this covers a
  stream that has nothing to *write*. Must stay comfortably above a source blip (reconnect backoff
  tops out at 8 s, plus an ffmpeg cold start), or a recovering source costs its viewers their
  connections; `0` disables the drop and restores the pre-0.11.4 behaviour of holding such a
  viewer forever.

### The source backend

A source already served as `video/mp2t` is streamed straight through and never touched by this
setting. Everything else — an HLS playlist, a udp feed — has to be converted, and this chooses how:

| Value | Behaviour |
|-------|-----------|
| `auto` (default) | Convert **in-process** where [`nativesrc`](04-internals.md#native-conversion--nativesrc) can, **ffmpeg** for everything it declines. |
| `ffmpeg` | Always spawn ffmpeg. The pre-0.12 behaviour, kept as the kill-switch. |
| `native` | Native only — a source the native reader declines **fails** instead of falling back. For finding out what is actually eligible on a node; **not for production**, where a declined source means a dead channel rather than a slightly more expensive one. |

`auto` is the default because the fallback makes it strictly safer than `ffmpeg`: anything the
native reader will not take runs exactly the pipeline it ran before, while the common case (HLS
with TS segments) stops costing a child process per stream. The win is one fewer ~27 MB process
per proxy stream, plus no process spawn on each on-demand join.

Applied live on a config reload, and it takes effect on the **next** pull — a stream already
connected keeps the path it started on until it reconnects.

**Per-stream override.** `PUT /streams/<id>` accepts a
[`backend`](03-endpoints.md#put--post-streamsid--register-a-pull-source) field that pins one
channel, so a single troublesome source can be forced to ffmpeg without changing the node. An
unknown value there is ignored (the stream takes the node-wide setting) rather than pinning
something that does not exist.

### The memory budget

The daemon gives the Go runtime a **soft** memory limit (`GOMEMLIMIT`): a ceiling, not a
reservation — the GC only intensifies as usage approaches it, and a working set well below it
never feels it. The budget is taken, in order:

1. **`mem_limit_mb`**, when set. Use this on a shared panel node: you know the split between the
   daemon, nginx, MySQL, PHP-FPM and the streams' ffmpeg processes, and the daemon does not.
2. the **cgroup limit** this process runs under (v2 `memory.max`, then v1
   `memory.limit_in_bytes`) — its own budget, so **80%** of it is claimed;
3. otherwise the box's **physical RAM**, of which only **50%** is claimed — the machine is shared,
   and taking most of it lets the fan-out grow until it starves the very processes feeding it.

`GOMEMLIMIT` in the environment overrides all of this (the daemon then sets nothing). The chosen
limit and its source are logged at startup, and `mem_limit_mb` is re-applied live on a config
reload.

### HLS

- **`hls_target_sec`** — the target segment length (the actual length floats, since we cut on
  keyframes). Affects HLS latency and segment frequency.
- **`hls_window`** — how many recent segments the playlist lists. A larger window = a longer
  "tail" for seeking/catch-up players. It is only a **display cap** now: the ring
  (`prebuffer_max_sec`) is what actually holds the bytes, so a window larger than the ring can
  hold is capped by the ring.

### Paths and access

- **`-sock` / `-ctl` / `-ingestdir`** — the placement of the unix sockets. At startup the daemon
  creates the directories, removes old socket files, and listens with `0660` permissions. If
  `-ctl` is empty, the control API is not brought up at all (the daemon only serves).

## What happens at startup

1. Parse flags (`-version` → print the version and exit).
2. Load the config file (`-config`): self-create it with defaults if absent, backfill any missing
   keys. An empty `-config` skips the file and uses the built-in defaults.
3. Create the `Manager` with the resolved tuning; start the config poller (every `-config-interval`).
4. Create the ingest-socket directory, start the reaper.
5. Bring up the client HTTP server on `-sock`; if `-ctl` is set — the control one too.
6. If `-id` is set — a one-off feed of the test stream.
7. Wait for a signal; on `SIGINT`/`SIGTERM` — graceful shutdown (2 s) and removal of the sockets.

## Startup examples

```bash
# Production mode: both surfaces, sources arrive via the control API.
# Tuning comes from the config file (self-created on first run), not flags.
xc_fanout \
  -sock   /home/xc_vm/bin/xc_fanout/sockets/http.sock \
  -ctl    /home/xc_vm/bin/xc_fanout/sockets/control.sock \
  -config /home/xc_vm/bin/xc_fanout/config.json

# Test a single stream from a file, without the control API
xc_fanout -id test -in ./sample.ts

# Test a single stream from an external URL
xc_fanout -id test -source https://example/live.m3u8 -ua "Mozilla/5.0"
```

## Migration from 0.10.0

The following flags existed in ≤ 0.10.0 and were **removed** in 0.11.1. Their values are now keys
in the [config file](#the-keys), editable from the panel and applied live:

| Retired flag (≤ 0.10.0) | Now (0.11.1) |
|-------------------------|--------------|
| `-prebuffer-max` | `prebuffer_max_sec` (default raised `20 → 40`) |
| `-hlstarget` | `hls_target_sec` |
| `-hlswindow` | `hls_window` |
| `-grace` | `grace_sec` |
| `-write-timeout` | `write_timeout_sec` |
| `-chunk` | `chunk_bytes` |
| `-maxgop` | `max_gop_bytes` |
| `-source-insecure` | `source_insecure` |

New in 0.11.1 and not present as a flag before: `default_prebuffer_sec`, `idle_buffer_grace_sec`,
`idle_buffer_ratio`, and the two flags that drive the file itself — `-config` and
`-config-interval`. No action is needed on upgrade: on first run the daemon writes the file with
these defaults; an existing panel that only knows the older keys still works (the daemon backfills
the rest).

## Debug mode

By default the daemon is nearly silent — it logs only startup, fatal errors, and
source reconnects. When you need to see *what it is doing right now* and *where it
is stuck*, turn on debug mode with `-debug` (or `XC_FANOUT_DEBUG=1`). Log
timestamps switch to microsecond precision so the timing of a slow probe, a
stalled viewer, or reconnect backoff is legible.

Debug mode narrates every interesting event, each line tagged `[dbg <category>]`:

| Category | What it reports |
|----------|-----------------|
| `boot` | Version, pid, and the resolved configuration at startup. |
| `stream` | Stream created; puller starting/stopping (with the current viewer refcount). |
| `puller` | Source pull start, which URL was chosen and how (direct mpegts vs ffmpeg remux), probe failures, clean source ends, reconnect backoff, and stop. |
| `viewer` | Live-TS attach, and a single detach line per viewer with the **cause** — client closed, dropped as too slow (hub buffer full), or write stalled past `write_timeout_sec` — plus session duration and KB delivered. |
| `hls` | Segments served (seq + size), and anomalies: playlist requested while still warming up / off-air, or a segment that rolled out of the window. |
| `ingest` | Push-fed listener up, producer connect/disconnect. |
| `ctl` | Control-API actions: register/unregister, probe prewarm and its result. |
| `signal` | "Send message" overlay queued and applied. |
| `reaper` | Idle-stop of a control-managed stream after the grace window, and idle-buffer gating (ring collapse / restore). |
| `stats` | A periodic per-stream snapshot (every `-debug-stats` seconds): `running`, `ingest`, viewer `refs`, hub `subs`, tracked `conns`, `data_age` (a growing `data_age` on a running stream is the **off-air** signal) and `nokf` (ring blocks cut on the byte cap for want of a keyframe — non-zero means this source carries no random-access points, and so can never produce HLS segments). |

```bash
# Full debug, per-stream snapshot every 2 s
xc_fanout -sock … -ctl … -debug -debug-stats 2

# Same via the environment (handy for systemd drop-ins)
XC_FANOUT_DEBUG=1 xc_fanout -sock … -ctl …
```

When debug is off, the instrumentation costs a single atomic load per call site and
formats nothing, so it is safe to leave the calls in the hot paths (per-chunk
publish, per-viewer write). Filter the output by category with `grep`, e.g.
`journalctl -u xc_fanout | grep '\[dbg viewer'` to watch only viewer churn.
