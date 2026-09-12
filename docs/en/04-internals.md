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

Since 0.11.1 there is **one** buffer: `Hub.Publish` folds the chunk into the `tsjoin` ring, and
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
- **anything else** (HLS playlist, etc.) → converted to MPEG-TS, **natively where possible and by
  ffmpeg otherwise** — see below.

This is a port of the `ProxyCommand::getActiveStream` logic from the legacy panel.

### Native conversion — `nativesrc`

Converting a non-mp2t source used to mean one **ffmpeg child process per stream**, which for the
overwhelmingly common case — HLS whose segments are already MPEG-TS — was a process doing little
more than concatenating bytes the daemon could concatenate itself. Since 0.12.0
[`nativesrc`](../../internal/nativesrc) does that in-process: it fetches the playlist, follows the
live window, and yields the segments as one continuous packet-aligned TS stream, which is exactly
what `ingest.Copy` already consumes on the direct path.

Measured on the same 720p source, `ffmpeg -c copy` sat at **27 MB RSS across 3 threads, per
stream**; natively it is a goroutine and a pipe. It also removes the process spawn and the
`-probesize` analysis window from every on-demand join and every reconnect.

**What it takes, and what it refuses:**

| Source | Path |
|--------|------|
| `http(s)` serving MPEG-TS | native (and already was — this is the direct path) |
| `http(s)` serving `m3u8` with **TS** segments | **native** |
| `udp://`, `rtp://` | native |
| HLS with **fMP4/CMAF** segments | ffmpeg |
| RTMP / SRT / RTSP | ffmpeg |
| AES-128 encrypted HLS **source** | ffmpeg |
| anything it cannot positively identify | ffmpeg |

Refusing loudly is the whole contract. Every refusal returns `ErrUnsupported` **before a single
byte is published**, so the caller runs ffmpeg and an unsupported source degrades to exactly the
pipeline it had before — never to garbage on the wire. That is also why a body whose
`Content-Type` is neither `mp2t` nor `mpegurl` must **prove** it is MPEG-TS (four consecutive sync
bytes at 188-byte spacing) before it is accepted: IPTV upstreams routinely serve real TS as
`application/octet-stream`, so the header alone is neither sufficient nor necessary, and an MP4 or
an HTML error page served with a generic type would otherwise be fanned out as if it were video.

Which path a stream took is in the debug log — `connected native (no ffmpeg child)` or
`connected via ffmpeg remux`. Choosing between them is
[`source_backend`](06-configuration.md#the-source-backend).

> The daemon still needs ffmpeg installed: the fallback path, and the admin "send message"
> overlay, both use it. What changes is how often it is *spawned*.

### The source connection

All the pulls of one stream share **one** `http.Client`, created when the puller starts and whose
idle connections are closed when it stops. It used to build a fresh `http.Transport` per probe
attempt, which meant no connection was ever reused — a full TCP (and TLS) handshake on every
reconnect — and, worse, each abandoned transport kept whatever it had pooled: a source that ended
cleanly had its connection returned to that pool where, a zero-value transport having no idle
timeout, it stayed open **with its reader goroutine** for the life of the process, unreachable
because the transport itself was garbage. A source reconnecting on the 8 s backoff ceiling leaked
a socket and two goroutines every 8 s.

The transport also carries real bounds, which a zero-value one does not have: a **dial** timeout,
a **TLS handshake** timeout, a **response-header** timeout, and an **idle-connection** timeout
(see `defaults.Pull*`). Without the header timeout a source that accepted the connection and then
said nothing pinned its puller until the stream was stopped — never erroring, so never trying the
next URL and never backing off, while the panel saw a "running" stream with no data. The response
**body** stays unbounded, and there is no `Client.Timeout`, for the obvious reason: it is a live
stream that should never end.

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
- **`Subscribe(prebufMS)`**: under a single mutex it simultaneously **captures** the history to send
  and registers the subscriber. Atomicity matters: the live tail continues exactly where the
  snapshot ended — **with no gap and no duplication**. Nothing is copied: the viewer is written
  straight out of the ring's pinned GOP buffers, with the lock **released** — see below.

#### Why the join burst is neither copied nor written under the lock

The snapshot a viewer gets on connect is up to the whole ring: **14 MB at a 40 s prebuffer**,
~3 ms to copy. Doing that under the hub lock stalled the stream's producer for that long on
*every* join, and serialised a join storm — a channel going live, an EPG event, a restreamer fleet
reconnecting — into one stall after another.

`SnapshotPin` captures under the lock (the header, which is mutated in place as new PAT/PMT
arrive, is copied there and then; the GOP byte slices are captured with the lengths they have at
that instant) and **pins** those buffers. The caller then releases the lock, appends the parts,
and `Unpin`s. While anything is pinned, `prune` stops **recycling** dropped GOP arrays — it leaves
them to GC, which the in-flight reader's own slices keep alive for exactly as long as it needs
them. Without that pin a recycled array gets handed to a new GOP and rewritten mid-copy, and the
joining viewer receives a splice of two different points in the stream.

Atomicity survives because the capture fixes the content at the moment of registration: the open
GOP growing afterwards is invisible (the captured length does not move), and every chunk published
from that point reaches the viewer through its channel instead.

**Since 0.13.2 the burst is not copied at all.** `Subscribe` returns a `Burst` — the header, plus
the pinned GOP slices themselves — and `serveLive` writes them to the socket one GOP at a time
(each with its own write deadline, like every chunk of the live tail) and then releases the pin.
The copy had bought nothing the pin did not already provide, and it was the daemon's largest
transient: every join allocated up to the whole client prebuffer (the panel's `client_prebuffer`
defaults to **30 s** — ~30 MB of an 8 Mbit/s channel; buffers over 16 MB were not even pooled) and
held it for as long as the viewer took to drain it. Measured on 30 channels at 6.6 Mbit/s, 90
viewers joining with `prebuffer=30`:

```
heap after the storm:   2,669 MB → 1,440 MB peak
retained afterwards:    +1,650 MB (join copies) → +0 (only the ungated rings)
```

```
lock held per join:      3.13 ms → 1.30 µs
192 joins at 40 s:        604 ms → 337 ms
```

The storm is now bound by **memory bandwidth**, not the lock: 192 joins × 14 MB is 2.7 GB to copy,
however it is scheduled. The producer's worst-case wait during one is no longer the serialised
copies (a tight ~77 ms before) but scheduler contention with the copying goroutines (26–81 ms,
variable). If join storms hurt, the lever is **prebuffer depth**, which the panel sets per viewer —
not the lock.

> **Why `Publish` still copies.** The copy is made once per chunk and **shared by every
> subscriber**, so it is already O(1) in audience size — a subscriber holds the buffer in its
> channel across later publishes, so it cannot be handed the producer's reusable one. Measured, it
> costs ~2.3 µs per 12 KB chunk, i.e. under 5% of one core at 500 streams pulling simultaneously.
> Removing it would take either per-stream slab allocation (which parks unused slab tail on every
> stream — paying RAM, the scarcer resource here, to save CPU that is not scarce) or recycling
> buffers on a fixed cycle, which races with a dropped subscriber's handler that may still be
> writing the buffer it already pulled from its channel. It stays.

### Guarding against stalled and idle viewers

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

**The other half of the same problem** (fixed in 0.11.4): that deadline can only fire *while
there are bytes to write*. When a stream goes **off-air** nothing is written at all, so nothing
ever times out — and a client whose socket is half-open (a dropped mobile link that sent neither
FIN nor RST) is invisible to the daemon and to nginx alike, since nginx is equally idle. Such a
viewer sat in the serve loop **forever**, holding its uuid in `/connections` as a ghost
`fanout_sync` could never clear.

So `serveLive` also bounds a viewer that receives **nothing**: after
`viewer_idle_timeout_sec` (default 30 s) with no chunk delivered, the viewer is dropped and its
cleanup runs, exactly as for a stalled write. It is polled on a coarse ticker (a quarter of the
timeout) rather than a per-chunk timer reset, so the hot path stays a timestamp store. The default
sits comfortably above a source blip — reconnect backoff tops out at 8 s plus an ffmpeg cold start
— so a recovering source does not cost its viewers their connections; `0` disables the drop.

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
  with a **monotonic id** and a time on the **ring clock**: the PCR (90 kHz), made monotonic —
  see below.

Since **0.11.1** this ring is the daemon's **single per-stream buffer**: it serves the live-TS
clean join and prebuffer, *and* HLS is derived from it (see
["HLS is a view over the ring"](#hls-is-a-view-over-the-ring--hls-from-tsjoin) below and
[ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md)). The separate HLS byte store that used
to exist (`hlsseg`) is gone.

How it works:

- on a **video** random-access point a new GOP is opened; everything else — including a
  `random_access_indicator` on another PID — is appended to the current GOP. The indicator is set
  on the **audio** PID too (ffmpeg's own muxer does it: every audio frame is a random access
  point), and until 0.13.1 a GOP opened there, so a viewer joining on it received video slices
  from the middle of a GOP whose SPS/PPS it never got — `non-existing PPS 0 referenced`, a black
  picture until the next real keyframe, and with `prebuffer=0` (only the current GOP is kept) that
  was **every** join. A stream with no video at all (radio) keeps the plain random-access rule;
- `prune()` discards old GOPs: by **duration** on the ring clock (capped at `prebuffer_max_sec`
  seconds, if PCR parses) or by a **byte backstop** (~24 Mbit/s estimate — in
  case PCR can't be read), so that the ring doesn't grow without bound. When the stream is
  **idle** the ring is collapsed to `prebuffer_max_sec × idle_buffer_ratio` (see
  ["the idle-buffer gate"](06-configuration.md#the-idle-buffer-gate));
- **`Snapshot(reqMS)`** collects what is handed to the viewer before the live tail:
  - `reqMS = 0` (or a stream without PCR) → `PAT + PMT + only the current GOP` — the minimal
    clean join. The start is always moved to a GOP a decoder can begin on: the first video
    random-access block at or after it, else the newest one before it;
  - `reqMS > 0` → the ring is rewound to a keyframe ~N seconds back, and the viewer gets
    more history, so that their player starts with an already filled cache.

#### The ring clock

Retention used to subtract raw PCRs — newest minus oldest — and **the PCR is not monotonic**. It
starts over near zero with every producer restart (a fresh ffmpeg starts its own clock), jumps with
a source failover, and wraps at 2³³ every 26.5 hours. Across any of those the difference went
**negative**, which `prune` read as "still inside the window", so it stopped dropping anything:
the ring grew to its byte backstop — `prebuffer × 24 Mbit/s`, **120 MB at the default 40 s**
(60 MB when idle-gated) whatever the stream's real bitrate — and stayed there until every
pre-splice GOP had been pushed out. A source carrying a second, unrelated PCR on another PID flipped
the sign constantly and never pruned at all.

Since 0.13.2 each GOP is stamped by `ringClock()`: it advances by the PCR step when that step is
plausible (0–60 s), unwraps the 33-bit rollover, and across anything else carries on at the last
good cadence — so the window stays a window. And once the PMT's declared **PCR PID** has shown a
PCR, only that PID drives the clock. Measured on real ffmpeg output across a producer restart, a
20 s window held:

```
before:  35 GOPs (~70 s of video)
after:   11 GOPs (20.0 s)
```

On a daemon with 30 supervised channels, restarting every producer once took the heap from 602 MB
to 1,963 MB and kept it there for minutes; at 155 channels that is the difference between a node
at a few GB and one at 9 GB. `GET /memory` on the control socket shows each ring's bytes and span,
so an operator can see this directly — see [03. HTTP endpoints](03-endpoints.md#get-memory--where-the-memory-is).

This reproduces `client_prebuffer` from the legacy `live.php`, which the transfer via X-Accel
otherwise bypasses. The prebuffer depth is the **panel's** call: it passes the chosen value in
the `?prebuffer=` parameter of the [`/live/<id>` URL](03-endpoints.md#get-liveid--live-ts), and
the daemon honors it as-is — **including an explicit `0`** (current GOP only). Only when a request
carries no `?prebuffer=` at all does the daemon fall back to the `default_prebuffer_sec`
[config key](06-configuration.md#per-viewer-prebuffer) (itself `0` by default).

---

## HLS is a view over the ring — HLS from `tsjoin`

Before 0.11.1 HLS lived in a separate package (`hlsseg`) that re-parsed the same TS and stored a
**second copy** of the bytes as finished segments. Two buffers holding essentially the same data
drove ~80 GB of RAM at 350 channels. **0.11.1 removes that store**: the [`tsjoin`](#clean-join-and-prebuffer--tsjoin)
ring is the single source, and HLS is a **lightweight metadata view** over it — no second byte
store. See [ADR 0001](../adr/0001-single-ts-cache-hls-on-demand.md) for the full rationale.

How it works now:

- alongside the GOP ring, `tsjoin` maintains a **segment index** — one entry per HLS segment,
  holding `seq`, the `startID..endID` range of the GOP ids it spans, and its duration (ms). No
  segment bytes are stored;
- a **new segment boundary** is cut at a **video-PID keyframe carrying a PES PTS**, once at least
  `hls_target_sec` have accumulated since the previous boundary (that PTS is the segment clock).
  A `random_access_indicator` on a *non-video* PID neither cuts a segment nor opens a ring GOP
  (see above) — treating it as a boundary mixed two different 90 kHz offsets and segments never
  closed;
- the playlist is rendered **from the index** (`hls_window` most recent segments, a display cap);
  a `<seq>.ts` request **assembles the bytes on the fly** from the ring (`PAT + PMT + the GOPs'
  data`), encrypting on the way out if the stream has a key. Since 0.11.4 that assembly happens
  **once per segment, not once per viewer** — see below;
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

### Sources without keyframes

The ring is cut on **`random_access_indicator`**. Some sources never set it. Such a stream used
to grow a single block to `max_gop_bytes` and then **silently discard every packet after it**:
pruning needs two blocks, so the ring froze — the stream held `max_gop_bytes` forever, and every
joining viewer was served that same stale block, captured whenever the cap was first reached,
ahead of the live tail.

Since 0.11.4 reaching the cap **cuts a new block** instead. The ring then rolls and prunes like
any other stream, memory is bounded by `prebuffer_max_sec` as usual, and a joiner gets recent
bytes. Each such cut increments a counter surfaced as `nokf=` in the debug per-stream snapshot —
a non-zero value is the signal that a source carries no random-access points.

**HLS still yields nothing for these sources, and cannot**: a segment has to begin at a
random-access point or the player cannot decode it. `nokf=` growing alongside an empty playlist
is the explanation for "this channel works on live TS but never opens over HLS".

### The playlist cache

The playlist is rendered from the segment index, which changes only when a segment closes or
ages out — once per `hls_target_sec`. But every HLS viewer polls `index.m3u8` on its own
schedule, so an audience of a few hundred re-rendered an identical string hundreds of times a
second, each render holding the hub lock against the producer. The rendered string is now cached
and invalidated exactly when the segment list changes (a segment closing, segments pruned, or an
`hls_window` / `hls_target_sec` change).

```
before: 3225 ns/op   560 B/op   11 allocs/op
after:     3.7 ns/op   0 B/op    0 allocs/op
```

### The segment cache

Every viewer of a channel fetches **byte-identical** segments, so assembling one out of the ring
and running AES over it *per request* was work repeated once per viewer. Measured on a 2.26 MB
segment: **6.2 ms of CPU and 6.8 MB of garbage, per viewer, per segment** — and an HLS audience
rolls onto each new segment at the same moment, so the cost arrived as a burst. At a few hundred
viewers that is a core spent, and hundreds of MB/s of allocation, producing bytes that were
already computed.

`Stream.hlsSegment` keeps the last `HLSSegCacheEntries` (3) segments **ready to send**, already
encrypted when the stream has a key. A cache hit is **58 ns and zero allocations**. The first
requester assembles while the rest **wait on that one result** (a `ready` channel per entry)
rather than each starting their own — without that single-flight the simultaneous join of an
audience onto a fresh segment would still let the whole herd through.

The cache is dropped when the key changes (the cached bytes carry the old ciphertext) and when the
stream is **gated idle**, so a channel nobody watches holds no segment copies on top of its
already-collapsed ring. A **miss is never cached**: the `seq` may simply not have closed yet, and
a pinned `nil` would hide the segment once it does exist. The per-viewer
["send message" overlay](#hls-encryption--hlscrypt) bypasses the cache in both directions — those
bytes belong to one viewer.

> **What is still done under the hub lock.** Assembling a segment out of the ring (~0.7 ms)
> happens while the lock is held, because a GOP's buffer is recycled by `prune` the moment it
> leaves the ring — a reader outside the lock could be copying an array already handed to a new
> GOP. Encryption, the expensive half, is done outside it. With the cache above this assembly now
> runs **once per segment per stream** rather than once per viewer, so the producer is held for
> ~0.7 ms roughly once per `hls_target_sec` — about 0.01% of the time. Lifting it out would mean
> refcounting GOP buffers in the hottest data structure the daemon has, which is not worth that.

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
