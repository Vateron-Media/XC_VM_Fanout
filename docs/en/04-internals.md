# 04. Internal mechanisms

> How the "guts" are built: source acquisition, packet alignment, TS fan-out,
> "clean join" and prebuffer, HLS segmentation and encryption. The big picture is in
> [02. Architecture](02-architecture.md).

A bit of context about the format. **MPEG-TS** is a stream of fixed-length packets of
**188 bytes**, each starting with the sync byte `0x47`. Inside are service tables
(**PAT** — the list of programs, **PMT** — the makeup of a program: which PIDs are video/audio) and
the actual video/audio itself, spread across PIDs. For a player to start showing a picture from the
middle of a stream, it needs PAT, PMT and a **video keyframe** (keyframe / random access point).
Almost the entire "guts" of the daemon revolve around these facts.

---

## Data entry point — `Stream.Publish`

Everything the daemon receives from the source enters the stream through a single method,
[`Publish`](../../internal/server/server.go):

```
Publish(chunk):
    if chunk is non-empty → record time (lastData)      // liveness marker for off-air
    Hub.Publish(chunk)                                  // into the single TS ring
```

Since 0.11.0 there is **one** buffer: `Hub.Publish` folds the chunk into the `tsjoin` ring, and
both the live-TS fan-out and HLS are served from that ring (HLS as a metadata view — see
["HLS is a view over the ring"](#hls-is-a-view-over-the-ring--hls-from-tsjoin)). The `lastData`
marker is later read by the status (`GET /streams/<id>`) to determine off-air.

---

## Source acquisition — `puller`

> **Important: the `puller` runs ONLY for proxy streams** (`direct_proxy=1`). Proxy
> streams have no panel-side ffmpeg (they are passthrough — no transcode/logo), so
> the daemon pulls the source itself. **Regular (non-proxy) streams reach the daemon
> NOT through the puller but through `ingest`** — that is the panel ffmpeg's already
> prepared output, emitted as a second `-f tee` output (one ffmpeg → both on-disk HLS
> and the daemon); see [05, power modes](05-lifecycle.md#the-three-power-modes-of-a-stream). So
> the "mp2t directly, without ffmpeg" below is **proxy-passthrough only**, not the
> normal case: for normal streams the daemon always consumes the ffmpeg output, never
> the raw source link.

[`puller.Run`](../../internal/puller/puller.go) pulls the source for as long as the stream is needed by someone,
and **reconnects with exponential backoff** on a break: the pause grows `1s → 2 → 4 → 8s`
(capped at 8 s), and resets on cancellation.

On each attempt the `URLs` are tried in order, and for each one a **probe
(`probe`)** is done — a GET request is opened (with `User-Agent`/`Cookie`/`Proxy`, TLS without
certificate verification, **with no timeout** — the stream is long-lived) and the response's
`Content-Type` is inspected:

- **`video/mp2t`** → the bytes are **streamed directly** (`ingest.Copy`), without ffmpeg — this is
  the cheapest path;
- **anything else** (HLS playlist, etc.) → **ffmpeg** is launched, which remuxes the
  source into MPEG-TS.

This is a port of the `ProxyCommand::getActiveStream` logic from the legacy panel.

### The ffmpeg invocation

When the source is not ready-made mp2t, `runFfmpeg` launches (reproducing the invocation from
`ProxyCommand.php`):

```
ffmpeg -copyts -vsync 0 -nostats -nostdin -hide_banner -loglevel quiet -y
       -user_agent <ua> [-headers "Cookie: …"] [-http_proxy http://host:port]
       -i <url> -map 0 -c copy -mpegts_flags +initial_discontinuity
       -pat_period 2 -f mpegts -
```

The key point: `-c copy` — **no transcoding**, only repackaging the container into TS
(cheap on CPU). ffmpeg's stdout is read by that same `ingest.Copy` and fed into `Publish`.

---

## Packet alignment — `ingest`

[`ingest.Copy`](../../internal/ingest/ingest.go) reads the source and calls `publish`
**in chunks that are multiples of 188 bytes**. If a given read broke off in the middle of a packet,
the "tail" is carried over to the next read — so all the parsers downstream (the join ring)
always receive only whole TS packets. The read size is set by the `chunk_bytes`
[config key](06-configuration.md#the-keys) (default 12032; rounded down to a multiple of 188).

The same `ingest.Copy` is used in three places: a direct mp2t source, ffmpeg's
output, and push mode (data from the ingest socket).

---

## TS fan-out — `hub`

[`Hub`](../../internal/hub/hub.go) is the point of distribution of a single stream to many subscribers.
One `Hub` per stream.

- **`Publish(chunk)`**: **copies** the chunk (so the source can reuse the buffer),
  folds it into the join state (see below) and **non-blockingly** broadcasts it to all
  subscribers through buffered channels.
- **A slow subscriber is dropped.** Each subscriber has a buffer of `subQueue = 256`
  chunks. If it overflows (the viewer can't keep up reading) — the subscriber is closed and
  removed. This way one lagging viewer **never stalls the source or the others**.
- **`Subscribe(prebufMS)`**: under a single mutex it simultaneously takes a snapshot of the history and
  registers the subscriber. Atomicity matters: the live tail continues exactly where the
  snapshot ended — **with no gap and no duplication**.

### Guarding against stalled viewers

Dropping on buffer overflow doesn't catch every case. If a viewer stops
reading the socket altogether but **doesn't close the connection** (a minimized player, a dropped mobile
channel), its OS send buffer fills up, and the write system call blocks
**forever** — neither the hub's dropping nor cancellation of the HTTP context interrupts a write that has
already begun.

The solution is in [`serveLive`](../../internal/server/server.go): each write to a viewer is
capped by a **deadline** `write_timeout_sec` (default 15 s). A stalled write
turns into an error, the deferred `detach`/`removeConn` fire, and `fanout_sync`
can close the `lines_live` row. A healthy realtime viewer accumulates no more than ~1 s of
lag per second, so the deadline fires only on truly dead connections.

---

## Clean join and prebuffer — `tsjoin`

The problem: a viewer connects to a live stream at an arbitrary moment. If you start pouring
bytes to them "from the current position", their player will see the middle of a video frame with no PAT/PMT and no
keyframe — and will show nothing. A "clean join" is needed.

[`tsjoin.State`](../../internal/tsjoin/tsjoin.go) honestly **parses the MPEG-TS structure**
(rather than matching by fixed offsets, like the legacy `ProxyCommand`) and holds exactly what
a new viewer needs to start:

- the last **PAT** and the last **PMT** (the PMT PID is computed from the PAT);
- a **ring of GOPs** — blocks "from one keyframe to the next keyframe", each tagged
  with a **monotonic id** and a time from the PCR clock (90 kHz).

Since **0.11.0** this ring is the daemon's **single per-stream buffer**: it serves the live-TS
clean join and prebuffer, *and* HLS is derived from it (see
["HLS is a view over the ring"](#hls-is-a-view-over-the-ring--hls-from-tsjoin) below and
[ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md)). The separate HLS byte store that used
to exist (`hlsseg`) is gone.

How it works:

- on a `random_access_indicator` (keyframe) a new GOP is opened; between keyframes
  packets are appended to the current GOP;
- `prune()` discards old GOPs: by **duration** (capped at `prebuffer_max_sec`
  seconds, if PCR parses) or by a **byte backstop** (~24 Mbit/s estimate — in
  case PCR can't be read), so that the ring doesn't grow without bound. When the stream is
  **idle** the ring is collapsed to `prebuffer_max_sec × idle_buffer_ratio` (see
  ["the idle-buffer gate"](06-configuration.md#the-idle-buffer-gate));
- **`Snapshot(reqMS)`** collects what is handed to the viewer before the live tail:
  - `reqMS = 0` (or a stream without PCR) → `PAT + PMT + only the current GOP` — the minimal
    clean join;
  - `reqMS > 0` → the ring is rewound to a keyframe ~N seconds back, and the viewer gets
    more history, so that their player starts with an already filled cache.

This reproduces `client_prebuffer` from the legacy `live.php`, which the transfer via X-Accel
otherwise bypasses. The prebuffer depth is the **panel's** call: it passes the chosen value in
the `?prebuffer=` parameter of the [`/live/<id>` URL](03-endpoints.md#get-liveid--live-ts), and
the daemon honors it as-is — **including an explicit `0`** (current GOP only). Only when a request
carries no `?prebuffer=` at all does the daemon fall back to the `default_prebuffer_sec`
[config key](06-configuration.md#per-viewer-prebuffer) (itself `0` by default).

---

## HLS is a view over the ring — HLS from `tsjoin`

Before 0.11.0 HLS lived in a separate package (`hlsseg`) that re-parsed the same TS and stored a
**second copy** of the bytes as finished segments. Two buffers holding essentially the same data
drove ~80 GB of RAM at 350 channels. **0.11.0 removes that store**: the [`tsjoin`](#clean-join-and-prebuffer--tsjoin)
ring is the single source, and HLS is a **lightweight metadata view** over it — no second byte
store. See [ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md) for the full rationale.

How it works now:

- alongside the GOP ring, `tsjoin` maintains a **segment index** — one entry per HLS segment,
  holding `seq`, the `startID..endID` range of the GOP ids it spans, and its duration (ms). No
  segment bytes are stored;
- a **new segment boundary** is cut at a **video-PID keyframe carrying a PES PTS**, once at least
  `hls_target_sec` have accumulated since the previous boundary (that PTS is the segment clock).
  A `random_access_indicator` on a *non-video* PID still opens a ring GOP for the clean live join,
  but it does **not** drive HLS — treating it as a boundary mixed two different 90 kHz offsets and
  segments never closed;
- the playlist is rendered **from the index** (`hls_window` most recent segments, a display cap);
  a `<seq>.ts` request **assembles the bytes on the fly** from the ring (`PAT + PMT + the GOPs'
  data`), encrypting on the way out if the stream has a key;
- because `seq` is anchored to monotonic GOP ids (not slice positions), it stays **stable** as the
  ring slides. `HLSPlaylist` reserves the single oldest in-ring segment as a **fetch margin**, so
  a listed segment can't age out between the playlist render and the client fetch; a segment whose
  GOPs have left the ring returns nil and the client re-polls;
- when the stream is **idle** and the ring is collapsed, HLS keeps being cut from the reduced
  ring (with a `2 · hls_target` floor guaranteeing ≥1 segment), so an idle channel's playlist
  stays non-empty and opens immediately. See
  ["the idle-buffer gate"](06-configuration.md#the-idle-buffer-gate).

The playlist is a standard `#EXTM3U` version 3 with `EXT-X-TARGETDURATION`, `EXT-X-MEDIA-SEQUENCE`
and `#EXTINF` lines (durations from PCR/PTS deltas); the segment URIs are `<seq>.ts`. The `Hub`
exposes this as `Configure` / `HLSPlaylist` / `HLSSegment`.

---

## HLS encryption — `hlscrypt`

[`hlscrypt.EncryptCBC`](../../internal/hlscrypt/hlscrypt.go) encrypts HLS segments with
**AES-128-CBC + PKCS#7**, byte-for-byte compatible with the PHP panel:
`openssl_encrypt(data, "aes-128-cbc", key, OPENSSL_RAW_DATA, iv)`.

- One fixed `key`+`iv` per stream (the panel's files `<id>_.key` / `<id>_.iv`).
- The key/IV are passed to the daemon in the body of `/streams` or `/ingest` (hex, 16 bytes each).
- The presence of a key is declared by the `#EXT-X-KEY` line in the playlist (which the panel forms).
- **Only the HLS segments** are encrypted; the live TS fan-out (`/live/<id>`) is always plain.
- Encryption is applied **when a segment is assembled from the ring on request** — on the fly,
  no ciphertext is ever stored.

---

## Where to next

- When the puller starts and when it stops — [05. Lifecycle](05-lifecycle.md).
- All the config keys affecting the sizes/timeouts above — [06. Configuration](06-configuration.md).
- Why the two buffers were collapsed into one — [ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md).
