# 02. Architecture and data flow

> Here is what parts the daemon is made of and how bytes travel the path from source to
> viewer. The details of each mechanism are moved to [04. Internals](04-internals.md).

## Overview diagram

```
                     source (mp2t / HLS / any)
                                │
                    ┌───────────▼───────────┐
                    │   puller  OR  ingest   │   (we pull OR it is pushed to us)
                    └───────────┬───────────┘
                                │  188-byte-aligned MPEG-TS chunks
                         Stream.Publish(chunk)
                                │
                       ┌────────▼─────────┐
                       │   Hub → tsjoin   │   the single per-stream buffer
                       │  GOP ring + HLS  │   (HLS is a metadata view over the ring)
                       │  segment index   │
                       └────────┬─────────┘
                          ┌─────┴─────┐
              GET /live/<id>        GET /hls/<id>/index.m3u8
              (video/mp2t)          GET /hls/<id>/<seq>.ts  (assembled from the ring)
                         │                 │
                     ┌───▼─────────────────▼───┐
                     │      nginx (X-Accel)     │  → viewer
                     └──────────────────────────┘
```

## Two HTTP surfaces on separate unix sockets

The daemon brings up **two independent HTTP servers**, each on its own unix socket (a file
with `0660` permissions). The split is by purpose and trust:

| Surface | Socket (flag) | Who talks to it | What it does |
|---------|---------------|-----------------|--------------|
| **Client** | `-sock` | nginx (viewers) | Serves video: live TS and HLS. |
| **Control** | `-ctl` | PHP panel only | Registers streams, serves status, reconciliation. |

The control surface is enabled only if the `-ctl` flag is set. A full reference of the
addresses is in [03. Endpoints](03-endpoints.md).

Why unix sockets rather than TCP ports: they are local to the machine, have file
permissions (`0660`), are not visible from outside, and exchange with nginx/PHP on the same
machine is faster.

## Internal parts (the `internal/…` packages)

| Package | File | Role |
|---------|------|------|
| `server` | [server.go](../../internal/server/server.go) | Registry `id → Stream`, both HTTP surfaces, on-demand stream lifecycle. |
| `hub` | [hub.go](../../internal/hub/hub.go) | Fan-out of a single TS stream to many subscribers; drops the slow ones. |
| `tsjoin` | [tsjoin.go](../../internal/tsjoin/tsjoin.go) | The single per-stream buffer: PAT/PMT + a GOP ring for the "clean entry" and prebuffer, **and** the HLS segment index derived from that ring. |
| `hlscrypt` | [hlscrypt.go](../../internal/hlscrypt/hlscrypt.go) | AES-128-CBC encryption of HLS segments (compatible with the panel), applied on the fly as a segment is assembled. |
| `puller` | [puller.go](../../internal/puller/puller.go) | Source acquisition (direct mp2t or ffmpeg remux), reconnect with backoff. |
| `ingest` | [ingest.go](../../internal/ingest/ingest.go) | Copying a TS stream into the publish callback in 188-byte-aligned chunks. |
| `config` | [config.go](../../internal/config/config.go) | The panel↔daemon tuning bridge: a self-healing JSON file, polled and applied live. |
| `defaults` | [defaults.go](../../internal/defaults/defaults.go) | The built-in operational tuning constants (the config-file seeds and other invariants). |
| `cmd/xc_fanout` | [main.go](../../cmd/xc_fanout/main.go) | Entry point: flags, config load/poll, socket setup, graceful shutdown. |

## Key entities

### Manager

The registry of all streams, keyed by `id` (a string). It holds the default settings (sizes,
timeouts), creates a `Stream` on demand (`GetOrCreate`), routes HTTP requests of both
surfaces, and runs the **reaper** (the background cleanup of idle streams).

### Stream

A single live stream. It combines:

- **`Hub`** — fan-out plus the single `tsjoin` buffer that serves **both** live TS and HLS
  (HLS derived from the ring — no separate segmenter);
- **lifecycle state** — pulling/not pulling, the number of viewers (`refs`), timestamps of
  the last data and the last access, the encryption key, the idle-buffer gate.

All data enters `Stream` through a single point — the `Publish` method.

## Data flow: from source to viewer

1. **Acquisition.** Bytes reach the daemon one of two ways: **`puller`** pulls the source
   itself — that is **proxy streams only** (`direct_proxy=1`, which have no panel ffmpeg);
   **`ingest`** accepts a push from a producer — that is **regular (non-proxy) streams**,
   where the panel ffmpeg emits its prepared output via a second `-f tee`. So a direct
   source pull is proxy-only; regular streams reach the daemon as the ffmpeg output. More
   detail — [04, "Source acquisition"](04-internals.md#source-acquisition--puller).
2. **Alignment.** The bytes are cut into chunks that are multiples of 188 (the TS packet
   length), so that the parsers downstream always see whole packets (`ingest.Copy`).
3. **Publishing.** `Stream.Publish(chunk)` feeds the chunk into the `Hub`, which folds it into
   the single `tsjoin` ring, and updates the liveness marker `lastData` (for off-air detection).
   There is one buffer — both TS and HLS are served from it.
4. **Serving TS.** Every viewer `GET /live/<id>` first receives from the `Hub` a
   "clean-entry snapshot" (PAT/PMT + the current GOP, optionally a prebuffer), then the
   live tail. A slow viewer is dropped so as not to hold back the rest.
5. **Serving HLS.** HLS is a **view over the ring**: a segment index (metadata only) drives the
   playlist, and each `<seq>.ts` request is assembled from the ring's GOPs on the fly (encrypted
   if the stream has a key). The player polls `index.m3u8` and downloads `<seq>.ts`.
6. **Outward.** Live TS and HLS **segments** are served by nginx via `X-Accel`, proxying to
   the client socket. The HLS **playlist** (`index.m3u8`) is the exception: it is fetched
   and rewritten to authorized URLs by PHP itself (see
   [07. Integration](07-integration.md#nginx--carries-the-bytes-out)).

## Design principles

- **One source — many viewers.** The source is pulled once (`Hub` copies each chunk to the
  subscribers).
- **PHP outside the byte path.** Video does not pass through PHP — only control commands.
- **Nothing to disk.** Both TS and HLS live in RAM.
- **One buffer, not two.** A single TS ring per stream serves live-TS *and* HLS (HLS is a
  metadata view over it); an unwatched stream collapses that ring. See
  [ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md).
- **On-demand.** The source is pulled only while there are viewers; otherwise it stops
  (see [05. Lifecycle](05-lifecycle.md)).
- **The slow don't hold back the fast.** A viewer that can't keep up reading the data is
  dropped rather than blocking the stream.
- **A static binary.** No dependencies — copy it and run.
