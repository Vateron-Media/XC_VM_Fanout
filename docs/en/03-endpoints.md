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
| `prebuffer` | seconds (integer) | How much history to "catch up" on entry. Clamped by the `-prebuffer-max` ceiling (20 s by default). Absent/0 = minimal clean entry (current GOP only). |
| `c` | uuid | The viewer's connection identifier (passed through by `live.php`). `fanout_sync` uses it to track disconnects and close the `lines_live` row. |

**What happens on connect:**

1. Finds the stream by `<id>` (otherwise `404`).
2. Reads the prebuffer from `?prebuffer=` and clamps it to the ceiling.
3. Atomically takes a "clean-entry snapshot" (PAT/PMT + the needed tail of GOPs) and
   subscribes to the live tail — without a gap or duplication.
4. `attach()` — accounts for the viewer and **starts the puller on the first viewer**
   (for streams managed via the control API).
5. If `?c=<uuid>` is given — registers the connection for reconciliation.
6. Sends the snapshot, then streams live chunks in a loop.

**Response:** `200`, `Content-Type: video/mp2t`, `Cache-Control: no-store`, body — an
infinite TS stream. `404` if the stream is not registered.

**Protection against "stalled" viewers:** each write is bounded by the `-write-timeout` deadline
(15 s by default). A viewer that stops reading the socket without a clean close
(a minimized player, a dropped mobile link) is torn down — otherwise it would permanently
block the delivery goroutine. For details, see [04, "Guarding against stalled viewers"](04-internals.md#guarding-against-stalled-viewers).

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
  "iv":     "<hex 16 bytes, optional>"
}
```

| Field | Req. | Meaning |
|------|:----:|-------|
| `urls` | yes | Candidate source URLs, tried in order. An empty list → `400 bad config`. |
| `ua` | no | User-Agent for the request to the source. |
| `proxy` | no | HTTP proxy `host:port`. |
| `cookie` | no | Value of the `Cookie` header. |
| `ffmpeg` | no | Path to ffmpeg (for remuxing non-mp2t sources). |
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
| `has_data` | Whether at least one non-empty chunk of data has arrived. |
| `since_data_ms` | Milliseconds since the last non-empty chunk; `-1` if there has been no data. |

"`running=true` but `has_data=false`, or a large `since_data_ms`" = the source is dead → PHP
shows a "not on air" page. `404` if the stream is not registered.

### `DELETE /streams/<id>` — remove a stream

Clears the config, stops the puller, closes the ingest listener, and removes the stream from
the registry. **Response:** `204`.

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
source. The warmed-up puller keeps running, so the viewer's real connection is
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
