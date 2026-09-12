# 03. HTTP endpoints

> A complete reference for both of the daemon's HTTP interfaces. For why there are two of them and which
> sockets they live on, see [02. Architecture](02-architecture.md#two-http-surfaces-on-separate-unix-sockets).

The daemon serves two surfaces:

- **Client** (`-sock`, for nginx/viewers) — video delivery.
- **Control** (`-ctl`, PHP-only) — stream registration and status.

Everywhere, `<id>` is the identifier of the stream (a string) under which it is registered.

---

## Client surface (for nginx)

Routing is handled by [`ClientHandler`](../../internal/server/server.go). This is where nginx proxies
viewer requests, usually via `X-Accel-Redirect` from `live.php`.

| Method and path | Purpose |
|--------------|------------|
| `GET /live/<id>` | Live MPEG-TS fan-out. |
| `GET /hls/<id>/index.m3u8` | HLS media playlist. |
| `GET /hls/<id>/<seq>.ts` | HLS segment by number. |
| `GET /healthz` | Health check. |

### `GET /live/<id>` — live TS

Keeps the connection open and continuously streams MPEG-TS to the viewer. The handler is
[`serveLive`](../../internal/server/server.go).

**Query parameters:**

| Parameter | Type | Meaning |
|----------|-----|-------|
| `prebuffer` | seconds (integer) | How much history to "catch up" on entry. The **panel** chooses this (client vs restreamer) and the daemon honors it **as-is, including an explicit `0`** (= current GOP only), bounded only by what the ring holds (`prebuffer_max_sec`). **Absent** = fall back to the `default_prebuffer_sec` [config key](06-configuration.md#per-viewer-prebuffer) (`0` by default). |
| `c` | uuid | The viewer's connection identifier (passed through by `live.php`). `fanout_sync` uses it to track disconnects and close the `lines_live` row; a queued [`/signal/<uuid>`](#post-signaluuid--admin-send-message-overlay) overlay is matched against it. |
| `vc` | codec | Source video codec, forwarded by `live.php`. Used only if a "send message" overlay is active for `?c=`, to keep the codec on the transient re-encode; ignored otherwise. |

**What happens on connect:**

1. Finds the stream by `<id>` (otherwise `404`).
2. Reads the prebuffer from `?prebuffer=` and honors it as-is (falling back to
   `default_prebuffer_sec` when the param is absent).
3. Places a cursor at the start of its prebuffer (PAT/PMT header first, then the needed tail of
   GOPs) and follows the ring forward into the live tail — without a gap or duplication (ADR 0004).
4. `attach()` — accounts for the viewer and **starts the puller on the first viewer**
   (for streams managed via the control API).
5. If `?c=<uuid>` is given — registers the connection for reconciliation.
6. Sends the snapshot, then streams live chunks in a loop.

**Response:** `200`, `Content-Type: video/mp2t`, `Cache-Control: no-store`, body — an
infinite TS stream. `404` if the stream is not registered.

**Protection against "stalled" viewers:** each write is bounded by the `write_timeout_sec` deadline
(15 s by default). A viewer that stops reading the socket without a clean close
(a minimized player, a dropped mobile link) is torn down — otherwise it would permanently
block the delivery goroutine.

**Protection against "idle" viewers:** a viewer that is sent **nothing** for
`viewer_idle_timeout_sec` (30 s by default) is likewise dropped. The write deadline only fires
while there are bytes to write, so it does not cover an off-air stream — where nothing is written,
nothing times out, and a half-open client would linger forever. `0` disables it. For both, see
[04, "Guarding against stalled and idle viewers"](04-internals.md#guarding-against-stalled-and-idle-viewers).

### `GET /hls/<id>/index.m3u8` — HLS playlist

Returns the current media playlist, generated in memory. The handler is
[`serveHLS`](../../internal/server/server.go).

- If there are no segments yet — `404 no segments yet`.
- **Response:** `200`, `Content-Type: application/vnd.apple.mpegurl`, `Cache-Control: no-store`.
- Segment URIs in the playlist are relative `<seq>.ts`.

### `GET /hls/<id>/<seq>.ts` — HLS segment

Returns the bytes of the segment numbered `<seq>`.

- If the segment has already slid out of the sliding window — `404`.
- If an encryption key is set for the stream, the segment is served **AES-128-CBC-encrypted**,
  otherwise plain.
- **Response:** `200`, `Content-Type: video/mp2t`.

> HLS is **poll-based** and does not hold a "ref" on the stream. Every HLS request updates
> the access mark so the source does not stop under an HLS-only audience — see
> [05. Lifecycle](05-lifecycle.md).

### `GET /healthz` — health check

Always `200` with body `ok`. For monitoring/keepalive.

---

## Control surface (PHP-only)

Enabled by the `-ctl` flag. Routing is handled by [`ControlHandler`](../../internal/server/server.go).
Only the **PHP panel** talks to this surface.

| Method and path | Purpose |
|--------------|------------|
| `PUT`/`POST` `/streams/<id>` | Register/update a **pull** source. |
| `GET /streams/<id>` | Stream status (off-air detection). |
| `DELETE /streams/<id>` | Remove the stream, stop the puller/ingest. |
| `PUT`/`POST` `/ingest/<id>` | Switch the stream into **push** mode, return the socket path. |
| `DELETE /ingest/<id>` | Tear down a push stream. |
| `GET /probe/<id>?wait=<ms>` | Warm up the source and wait for data. |
| `GET /connections` | All uuids of active live-TS viewers. |
| `DELETE /connections/<uuid>` | Disconnect a live-TS viewer (panel kick / connection-limit eviction). |
| `GET /rates` | Per-viewer average delivery rate (KB/s), keyed by uuid. |
| `POST /signal/<uuid>` | Queue a one-shot admin "send message" text overlay for one viewer. |

### `PUT` / `POST` `/streams/<id>` — register a pull source

Registers a source that the daemon will pull **itself**. The body is JSON:

```json
{
  "urls":   ["http://src1/live.ts", "http://src2/..."],
  "ua":     "Mozilla/5.0",
  "proxy":  "host:port",
  "cookie": "...",
  "ffmpeg": "/usr/bin/ffmpeg",
  "chunk":  12032,
  "key":    "<hex 16 bytes, optional>",
  "iv":     "<hex 16 bytes, optional>",
  "backend": "auto | ffmpeg | native (optional)"
}
```

| Field | Req. | Meaning |
|------|:----:|-------|
| `urls` | yes | Candidate source URLs, tried in order. An empty list → `400 bad config`. |
| `ua` | no | User-Agent for the request to the source. |
| `proxy` | no | HTTP proxy `host:port`. |
| `cookie` | no | Value of the `Cookie` header. |
| `ffmpeg` | no | Path to ffmpeg (for remuxing non-mp2t sources). |
| `backend` | no | Pins how **this** stream's non-mp2t source is converted, overriding `source_backend`: `auto`, `ffmpeg` or `native`. Omitted (the usual case) takes the node-wide setting — so send it only for a channel that needs pinning. An unknown value is ignored rather than honoured, so a typo can never take a channel off air. See [The source backend](06-configuration.md#the-source-backend). |
| `chunk` | no | Ingest read size (aligned down to 188). |
| `key`, `iv` | no | Hex, 16 bytes each — enable HLS encryption. TS fan-out is always plain. |

If viewers are already waiting on this stream, the puller starts immediately. **Response:** `204 No Content`.

### `GET /streams/<id>` — stream status

Returns enough JSON for the PHP authorizer to decide the off-air question:

```json
{
  "running":       true,
  "refs":          3,
  "has_data":      true,
  "since_data_ms": 120
}
```

| Field | Meaning |
|------|-------|
| `running` | The puller is running. |
| `refs` | How many live-TS viewers are connected right now. |
| `has_data` | Whether the stream is on air **now**: data within the viewer idle timeout (`viewer_idle_timeout_sec`, 30 s by default). Before 0.13.2 it meant "has ever had data", so a channel whose source had died still answered on air. |
| `since_data_ms` | Milliseconds since the last non-empty chunk; `-1` if there has been no data. |

"`running=true` but `has_data=false`, or a large `since_data_ms`" = the source is dead → PHP
shows a "not on air" page. `404` if the stream is not registered.

### `DELETE /streams/<id>` — remove a stream

Clears the config, stops the puller, closes the ingest listener **and every producer connected to
it**, **drops the viewers still attached**, and removes the stream from the registry.
**Response:** `204`.

> **Why the teardown is that thorough** (0.11.4). Removing the stream from the registry makes its
> viewers invisible to [`/connections`](#get-connections--viewer-reconciliation), so `fanout_sync`
> closes their `lines_live` rows — while their handler goroutines would sit forever on a hub that
> will never publish again, pinning the stream, its hub and its whole ring as an unreachable
> orphan. Likewise, closing only the ingest *listener* left an already-connected producer (the
> stream's ffmpeg tee) feeding that orphan. Both are now hung up on, so a viewer reconnects (and
> re-authorises) instead of freezing on a dead stream, and the memory is actually released.

### `PUT` / `POST` `/ingest/<id>` — push mode

Switches the stream into a mode where **the producer pushes** data to the daemon itself (for example, an ffmpeg-tee
stream). The body is optional:

```json
{ "chunk": 12032, "key": "<hex, optional>", "iv": "<hex, optional>" }
```

The daemon brings up a per-stream unix listener and returns the socket path the producer
should connect to:

```json
{ "socket": "/home/xc_vm/bin/xc_fanout/sockets/ingest/<id>.sock" }
```

Called by the panel (`StreamProcess`) **before** launching ffmpeg-tee. `key`/`iv` likewise
enable HLS encryption. **Response:** `200` with the JSON above.

### `DELETE /ingest/<id>` — tear down a push stream

The same as `DELETE /streams/<id>` (shared `Unregister`). **Response:** `204`.

### `GET /probe/<id>?wait=<ms>` — off-air warm-up

Warms up a registered stream (starts the puller, just as a viewer would) and
waits for data to appear, then returns the same JSON as `GET /streams/<id>`.
The handler is [`serveProbe`](../../internal/server/server.go).

| Parameter | Default | Maximum | Meaning |
|----------|:-----------:|:--------:|-------|
| `wait` | 5000 | 30000 | How many milliseconds to wait for the first data. |

PHP calls this after registering a proxy source: if there is no data by the time `wait` elapses
(`has_data=false`), it shows "not on air" instead of letting the viewer hang on a dead
source. It answers at once only for a stream whose data is flowing (newer than 2 s); otherwise it
waits for data newer than the probe itself — a channel that once had a picture is not on air for
that alone. The warmed-up puller keeps running, so the viewer's real connection is
picked up by it; if no viewer arrives, the reaper stops the puller.

### `GET /connections` — viewer reconciliation

Returns a JSON array of the uuids of **all** currently connected live-TS viewers (the values
of `?c=`) across all streams. The handler is [`serveConnections`](../../internal/server/server.go).

> A uuid is deduplicated **within a single stream** (refcounted per connection). There is no global
> deduplication across streams: the same uuid connected to two different streams would
> theoretically appear in the list twice. In practice `?c=` is unique per connection
> (`md5(uniqid())` on every auth request), so this is harmless.

The `fanout_sync` daemon reconciles this set against the `lines_live` rows and closes those whose uuid
no longer appears here — because under X-Accel PHP cannot see a viewer disconnect on its own.

**Response:** `200`, `Content-Type: application/json`, body — for example `["uuid-1","uuid-2"]`.

### `DELETE /connections/<uuid>` — disconnect a viewer

Ends every live-TS connection carrying `<uuid>` (the `?c=` value), on whichever stream it is
attached to. The handler is [`serveDropConnection`](../../internal/server/server.go).

Under X-Accel the PHP worker that admitted a viewer returns at hand-off, so the panel cannot
kill a process to end the session the way it did on the legacy byte path. It calls this instead
when a line exceeds `max_connections` (the oldest connection is evicted), when an admin kills a
connection, and from `fanout_sync` for a viewer whose `lines_live` row is gone. The viewer then
reconnects through `live.php`, where its expired token and the line's limits apply again.

**Response:** `204` when the uuid was connected, `404` when it was not (already gone, or served by
another node), `405` for any method but `DELETE`. Advertised as `drop_connection` in
`GET /monitors/state` → `features`.

### `GET /rates` — per-viewer transfer telemetry

Returns a JSON object mapping each active live-TS viewer's uuid to its **average delivery rate
in KB/s** since the connection attached (`bytes / elapsed / 1024`). The handler is
[`serveRates`](../../internal/server/server.go); it sums bytes the daemon actually wrote to each
`?c=<uuid>` connection.

This is the daemon-side replacement for the legacy `live.php` chase-read loop, which measured the
same rate itself and wrote it to `DIVERGENCE_TMP_PATH/<uuid>`. Under X-Accel PHP is out of the byte
path and can no longer see it, so the `fanout_sync` daemon polls this endpoint, compares each rate to
the stream's expected bitrate (`streams_servers.bitrate / 8 * 0.92`) and records the shortfall as the
viewer's `divergence` in `lines_live` / `lines_divergence` (ADR 0003, P4). On the rare chance a uuid
is live on more than one stream, the higher rate wins.

> Because the daemon **drops** a viewer that falls behind (its cursor is pruned off the ring's tail,
> plus the write deadline), a sustained-slow reading rarely appears here — a healthy realtime viewer's average
> converges to the stream bitrate, so divergence for daemon-served viewers is normally ~0. The value
> is the connection-speed signal and admin display, not a fast-pull fraud check (a viewer cannot pull
> faster than the live tail is fanned out).

**Response:** `200`, `Content-Type: application/json`, body — for example `{"uuid-1":512,"uuid-2":498}`.

### `GET /memory` — where the memory is

Read-only. The daemon's memory by what holds it: the per-stream join rings — the dominant term by
design — against the Go heap as a whole, so an operator whose node runs hot can tell "the rings are
as big as the config says" from "something else is growing" without a profiler.

```json
{
  "streams": 155,
  "ring_bytes": 2147483648,
  "heap_in_use_bytes": 2415919104,
  "heap_goal_bytes": 3623878656,
  "mapped_bytes": 3865470566,
  "released_bytes": 402653184,
  "largest": [
    { "id": "412", "bytes": 41943040, "seconds": 40, "gops": 21, "viewers": 3, "gated": false }
  ]
}
```

| Field | Meaning |
|------|-------|
| `ring_bytes` | Every stream's ring together. |
| `heap_in_use_bytes` | Heap objects, live and not yet swept. |
| `heap_goal_bytes` | The size the GC lets the heap reach before collecting (`GOGC`, soft limit). |
| `mapped_bytes` / `released_bytes` | Memory the runtime holds from the OS, and how much of it is already returned (not resident). |
| `largest` | The ten biggest rings: bytes, the stream time they span, GOPs, live-TS viewers, and whether the idle gate has collapsed them. |

A ring should span about `prebuffer_max_sec` seconds while watched and `× idle_buffer_ratio`
while gated. One far bigger than that, at the byte backstop (`prebuffer × 24 Mbit/s`), is a stream
whose clock the ring cannot read. The heap figures come from `runtime/metrics`, which — unlike
`runtime.ReadMemStats` — does not stop the world, so this is safe to poll on a busy node:

```bash
curl -s --unix-socket /home/xc_vm/bin/xc_fanout/sockets/control.sock http://localhost/memory | jq
```

### `POST /signal/<uuid>` — admin "send message" overlay

Queues a **one-shot** text banner to be burned onto the video of the single viewer whose
connection uuid is `<uuid>` (the `?c=` value). This is the daemon-side replacement for the
legacy PHP byte-path overlay that Phase E removed — the admin "Send Message" feature. The panel
posts it via [`FanoutClient::sendSignal`](../../../XC_VM/src/Streaming/Fanout/FanoutClient.php);
the handler is [`serveSignal`](../../internal/server/server.go). The body is JSON:

```json
{
  "message":     "Your subscription expires tomorrow",
  "font_size":   24,
  "font_color":  "white",
  "xy_offset":   "150x110",
  "ttl":         30
}
```

| Field | Req. | Meaning |
|------|:----:|-------|
| `message` | yes | The text to draw. Escaped for ffmpeg `drawtext` (no shell involved). Empty → `400`. |
| `font_size` | no | Point size (default 24). |
| `font_color` | no | ffmpeg colour — a name (`white`) or `#RRGGBB`, optionally `name@0.8`. Sanitised; invalid → `white`. |
| `xy_offset` | no | Position as `"<x>x<y>"`. Absent/malformed → a random position (150–380 × 110–250). |
| `ttl` | no | Seconds the queued signal stays valid before it expires unshown. `0`/absent = no expiry. |

**How it is consumed** (best-effort, one-shot, and it **never** breaks playback):

- **HLS** (`serveHLS`): the next segment requested with a matching `?c=<uuid>` is re-encoded in
  memory with the `drawtext` banner; on any ffmpeg error the plain segment is served.
- **Live TS** (`serveLive`): the banner is burned onto a short (~5 s) transient window — a fresh
  clean-join snapshot re-seeds a per-viewer ffmpeg, then the viewer rejoins the raw fan-out.

Both consumers pass `?vc=<codec>` (the source video codec, forwarded by `live.php`) so the
re-encode keeps the stream's codec; absent → `h264`. Requires the daemon to have been started with
`-ffmpeg` **and** `-font` pointing at a `drawtext`-capable ffmpeg — without them the signal is a
no-op. **Response:** `204 No Content` once queued (`400` on empty message, `405` on non-POST).

> **Fixed in 0.11.4:** the re-encode passed the `drawtext` graph as `-filter_complex` with an
> unlabeled input pad. ffmpeg 7 refuses to resolve that alongside `-map 0` ("Cannot find a matching
> stream for unlabeled input pad"), so the encode failed — and because the overlay is best-effort,
> the failure surfaced as the signal doing **nothing at all** on any ffmpeg 7 host, with the plain
> segment served instead. It is now passed as `-vf`, which binds on every version.

---

## Response codes at a glance

| Situation | Code |
|----------|-----|
| Successful delivery / status / playlist / segment | `200` |
| Successful registration/removal on control | `204` |
| Stream not found / no such segment / no segments | `404` |
| Empty `urls` in `/streams` | `400 bad config` |
| Wrong method on a control endpoint | `405` |
| Failed to bring up the ingest socket | `500` |
