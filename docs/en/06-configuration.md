# 06. Parameters and configuration

> What the daemon consumes as input: command-line flags, their default values, and what
> each one affects. The entry point is [`cmd/xc_fanout/main.go`](../../cmd/xc_fanout/main.go).

The daemon has **no config file** — everything is set through command-line flags at launch.
The dynamic part (which streams to serve) arrives at runtime via the
[control API](03-endpoints.md#управляющая-поверхность-только-для-php).

## Full list of flags

| Flag | Default | Purpose |
|------|---------|---------|
| `-sock` | `/home/xc_vm/bin/xc_fanout/sockets/http.sock` | Client unix socket (this is where nginx/viewers connect). |
| `-ctl` | `""` (off) | Control unix socket (PHP only). Empty = no control API. |
| `-ingestdir` | `<dir of -sock>/ingest` | Directory for per-stream push sockets (for ingest mode). |
| `-grace` | `10` | Seconds to keep a source alive after the last viewer leaves (the reaper window). |
| `-write-timeout` | `15` | Seconds a single write to a viewer may stall before the connection is dropped. |
| `-prebuffer-max` | `20` | Cap (seconds) on the TS history held per stream; the client `?prebuffer=` is clamped to this value. |
| `-chunk` | `12032` | Ingest read size in bytes (rounded down to a multiple of 188). |
| `-maxgop` | `10528000` | Maximum join-snapshot size (bytes) — memory protection for streams without keyframes. |
| `-hlstarget` | `6` | Target HLS segment duration (seconds). |
| `-hlswindow` | `6` | How many HLS segments to keep in the sliding window. |
| `-ffmpeg` | `ffmpeg` | Path to the ffmpeg binary (for remuxing non-mp2t sources). |
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

## What the key parameters affect

### Lifecycle timings

- **`-grace`** — how "inertial" a source is. A larger value = the source lives longer
  after viewers leave (faster pickup when they return, but longer idle hang time). The reaper
  ticks every `grace/2`. See [05. Lifecycle](05-lifecycle.md).
- **`-write-timeout`** — the threshold for dropping a "stalled" viewer. Lower = we clean up dead
  connections faster, but with a higher risk of hitting a slow-but-alive one. 15 s has headroom, since a healthy
  realtime viewer accumulates ≤1 s of lag per second. See
  [04, "Guarding against stalled viewers"](04-internals.md#guarding-against-stalled-viewers).

### Memory and buffering

- **`-prebuffer-max`** — the cap on the TS history a stream keeps in memory in case of
  client prebuffer. Directly affects RAM consumption per stream. The client
  `?prebuffer=<sec>` is clamped to this value.
- **`-maxgop`** — the size limit of a single GOP snapshot. Protection against memory growth on streams
  where keyframes are rare or absent.
- **`-chunk`** — the size of source read chunks. Rounded down to a multiple of 188
  (the TS packet length).

### HLS

- **`-hlstarget`** — the target segment length (the actual length floats, since we cut on
  keyframes). Affects HLS latency and segment frequency.
- **`-hlswindow`** — how many recent segments are available. A larger window = a longer "tail"
  for seeking/catch-up players, but more RAM.

> The segmenter guards against invalid values: `-hlstarget ≤ 0` is treated as
> `6`, and `-hlswindow < 1` as `3`. When launched from `main.go` the values are always valid, so
> this fallback only fires when a `Segmenter` is created programmatically with different numbers.

### Paths and access

- **`-sock` / `-ctl` / `-ingestdir`** — the placement of the unix sockets. At startup the daemon creates
  the directories, removes old socket files, and listens with `0660` permissions. If `-ctl` is empty,
  the control API is not brought up at all (the daemon only serves).

## What happens at startup

1. Parse flags (`-version` → print the version and exit).
2. Create the `Manager` with sizes/timeouts from the flags.
3. Create the ingest-socket directory, start the reaper.
4. Bring up the client HTTP server on `-sock`; if `-ctl` is set — the control one too.
5. If `-id` is set — a one-off feed of the test stream.
6. Wait for a signal; on `SIGINT`/`SIGTERM` — graceful shutdown (2 s) and removal of the sockets.

## Startup examples

```bash
# Production mode: both surfaces, sources arrive via the control API
xc_fanout \
  -sock /home/xc_vm/bin/xc_fanout/sockets/http.sock \
  -ctl  /home/xc_vm/bin/xc_fanout/sockets/control.sock \
  -grace 10 -write-timeout 15 -prebuffer-max 20 \
  -hlstarget 6 -hlswindow 6

# Test a single stream from a file, without the control API
xc_fanout -id test -in ./sample.ts

# Test a single stream from an external URL
xc_fanout -id test -source https://example/live.m3u8 -ua "Mozilla/5.0"
```
