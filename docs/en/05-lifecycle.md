# 05. Stream lifecycle

> When the daemon starts pulling a source, when it stops, and what power modes a
> stream can be in. The mechanisms these rules rely on are covered in
> [04. Internal mechanisms](04-internals.md).

The core idea: the daemon pulls a source **on-demand**. As long as nobody is watching
a stream, its source is not pulled (we don't burn traffic and ffmpeg processes). A
viewer arrives — the source starts; the last one leaves — after a short delay it stops.

## The three power modes of a stream

| Mode | How it is set up | Who supplies the data |
|------|------------------|-----------------------|
| **pull / proxy** | `PUT /streams/<id>` | The daemon **pulls the source itself** (`puller`). On-demand start/stop. |
| **push / ingest** | `PUT /ingest/<id>` | **The producer pushes it itself** into a unix socket (ffmpeg-tee of the stream). Data flows as long as the producer is alive. |
| **launch / test** | flags `-id` + (`-source` \| `-in`) | A one-off feed of a single stream at daemon startup — for isolated testing. |

The start/stop rules below (`attach`/`touch`/`startLocked` and the reaper) apply
specifically to **pull mode** — that is, to the lifecycle of the *puller*. A **push
stream has no puller**: it "starts" when a producer connects to its ingest socket and
"stops" when the producer disconnects or the panel calls `DELETE /ingest`. **The reaper
does not touch push streams** (see below — `idleStopLocked` requires a source `cfg` to be
set, and a push stream has none).

## Tracking interest: refs and lastAccess

A stream has two "interest sensors":

- **`refs`** — a counter of **live-TS viewers** connected right now (`GET /live/<id>`).
  It grows on `attach()`, drops on `detach()`.
- **`lastAccess`** — the time of the last touch of the stream. Updated by live-TS
  viewers, by **every HLS request**, and by `probe`.

Why two sensors: HLS is **poll-based** (the player periodically hits `index.m3u8` and
segments), with no active connection between requests — so HLS **does not hold `refs`**.
If stopping depended only on `refs`, the source would stop under an HLS audience between
polls. That is why HLS "marks itself" via `lastAccess`, and stopping looks at both sensors.

## Start

The source (puller) starts on the **first** signal of interest in the stream, under the
control of the control API:

- the first viewer `GET /live/<id>` → `attach()` → `startLocked()`;
- the first HLS request or `probe` → `touch()` → `startLocked()`;
- if the source config arrived (`PUT /streams`) **while viewers are already waiting** — the
  puller starts immediately.

`startLocked()` is idempotent: if the puller is already running or there is no config yet — it
does nothing.

## Stop: the reaper

The source is stopped by a background cleanup — the **reaper**
([`StartReaper`](../../internal/server/server.go)), launched once at daemon startup.

- **How often:** it ticks every `grace/2` (but no less than once a second).
- **What it does:** it walks all streams and calls `idleStopLocked` for each.
- **Stop condition** (`idleStopLocked`): the stream is **pull-managed** (a source `cfg` is
  set — i.e. the puller is running) **and** `refs == 0` (no live-TS viewers) **and** at least
  `-grace` (10 s by default) has passed since the last `lastAccess`.

This is a **single idle-stop path** for both TS and HLS audiences: HLS-only viewers no longer
let the source "fall asleep" under them, because every one of their requests moves `lastAccess`.

> Push/ingest streams are **not subject** to this rule: they have no `cfg`, so `idleStopLocked`
> exits immediately for them. Their power is determined by the producer (as long as the
> ffmpeg-tee is alive, data flows), and teardown comes only from `DELETE /ingest` or stopping
> the daemon.

## Warm-up for off-air detection

`GET /probe/<id>?wait=<ms>` is a special case of a start. It launches the puller, as a viewer
would, and **waits for the first data** (up to `wait` ms). The warmed-up puller **keeps
running** (because `probe` moved `lastAccess`), so the viewer's real connection, arriving right
after, is picked up by the already-running source. If the viewer never arrives — the reaper
stops the puller under the usual idle rule. More on the endpoint itself —
[03, `/probe`](03-endpoints.md#get-probeidwaitms--off-air-warm-up).

## Full scenario (pull stream, step by step)

```
1. PHP: PUT /streams/<id> {urls:[…]}      → config registered, puller not running yet
2. Viewer: GET /live/<id>                 → attach(): refs=1, puller starts
3. puller: probe → mp2t directly / ffmpeg → Publish(chunk) → Hub + Segmenter
4. More viewers: GET /live / GET /hls/…    → refs grows / lastAccess moves
5. Viewers leave                           → detach(): refs drops; HLS moves lastAccess
6. refs=0 and silence ≥ grace              → reaper: puller stopped
7. New viewer                              → puller starts again (step 2)
8. PHP: DELETE /streams/<id>               → config cleared, puller stopped, stream removed
```

## Daemon shutdown

The daemon catches `SIGINT`/`SIGTERM` and performs a **graceful shutdown**: it closes both
HTTP servers with a 2-second timeout and removes the socket files. The external keepalive of
the XC_VM service restarts the process if needed.

## Restart and state recovery

The entire `id → Stream` registry lives **in memory only**. A restart of the daemon
(crash+keepalive, `SIGTERM`, a binary update via `console.php fanout_binary`) **wipes it
completely** — after startup the daemon knows about no stream at all until the panel registers
them anew.

What this means in practice:

- **pull/proxy streams** recover on their own: the very first viewer hits `live.php` again, which
  repeats `PUT /streams/<id>` + `GET /probe`, and the puller starts. The pause is on the order of
  a single viewer's round trip.
- **push/ingest streams do NOT recover automatically.** Their ingest socket is created only at the
  moment of `PUT /ingest/<id>` (when the stream's ffmpeg-tee starts in `StreamProcess`). After a
  daemon restart, the already-running ffmpeg keeps writing into the **vanished** ingest sockets —
  and `onfail=ignore` in tee silently suppresses that slave output. Such a stream is **not fed** by
  the daemon (its `GET /streams/<id>` returns `404`/`has_data=false`), and delivery quietly falls
  back to the legacy path (on-disk HLS / chase-read) until **that stream's ffmpeg restarts** and
  calls `PUT /ingest` again (e.g. `console.php monitor <id> 0`, restarting the stream, or the
  natural on-demand cycle).

Operational takeaway: after a daemon update/restart, push streams go around the daemon for a while
(not broken, but without acceleration). If you need to bring them back into the daemon immediately —
you have to re-warm their ffmpeg, not just restart the daemon.
