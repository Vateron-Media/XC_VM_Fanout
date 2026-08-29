# ADR 0001 — One TS cache, HLS cut on demand

Status: **accepted** (2026-08-29) — implementation in progress.

## Context

The daemon keeps **two independent per-stream buffers**, both filled on every
`Stream.Publish`, unconditionally, for every fed channel regardless of viewers:

- **TS ring** (`internal/tsjoin`) — keyframe-aligned GOPs with PCR timing, kept
  `prebuffer_max` seconds. Serves `GET /live/<id>` (MPEG-TS fan-out): clean join
  (PAT/PMT + keyframe) plus client prebuffer.
- **HLS store** (`internal/hlsseg`) — re-parses the same TS and holds the
  **finished bytes** of the last `hls_window` segments (each ≈ `hls_target` s).
  Serves `/hls/<id>/index.m3u8` and `/hls/<id>/<seq>.ts`.

Consequences measured in production and reproduced on the test node:

- Memory ≈ `(prebuffer_max + hls_window·hls_target) · bitrate` **per fed channel**
  (defaults 20 s + 36 s = 56 s), held even with **zero viewers**. Measured
  ~107 MB/channel on the test node (5 channels, 0 viewers → 534 MB). At 350
  always-fed channels this is 40–80 GB — it scales with **channel count, not
  viewers**, the opposite of what fan-out promises.
- The HLS bytes are a **second copy** of data the TS ring already holds.

This was not the original intent. The design was always: **cache the TS stream
once, and cut HLS segments (keyframe-aligned, hence of varying length) out of
that cache on demand, when an HLS client appears.**

## Decision

Collapse the two buffers into **one TS cache** and derive HLS from it:

1. The **TS ring is the single source of truth**. Its retention grows to
   `max(prebuffer_max, hls_window·hls_target) + margin` so it always covers the
   HLS window as well as the TS prebuffer. It stays one buffer, not two.
2. HLS is a **view over the ring**: a lightweight segment index (metadata only —
   no segment bytes) maps `seq → GOP-id range + duration`, built incrementally as
   GOPs arrive. The playlist renders from the index; a `<seq>.ts` request
   assembles its bytes on the fly (`PAT + PMT + the GOPs' data`) from the ring.
3. `internal/hlsseg` as a parallel byte store is **removed**. Segment boundaries
   are GOP boundaries (a keyframe where the accumulated duration since the last
   boundary ≥ `hls_target`), so segments are keyframe-aligned and varying-length,
   with `EXTINF` computed from PCR deltas.

Phase 2b (done): when a channel has **no live viewer and no viewer touch** (TS
attach or HLS request) for `idle_buffer_grace_sec`, the reaper collapses its TS
prebuffer to `idle_buffer_sec` (default 2 s) **but keeps cutting HLS with a
reduced window** (`idle_hls_window`, default 3 segments); the instant a viewer
returns (`attach`/`touch`) the ring is pumped back to the full prebuffer + HLS
window and refills over the next window. All knobs are panel-editable via the JSON
config (`idle_buffer_grace_sec = 0` disables the gate).

**Why HLS stays alive on idle.** An HLS channel's playlist *is* its buffer: to
serve any playlist the ring must still hold ~one window. Collapsing an idle HLS
channel to a bare 2 s floor makes the playlist empty and the stream un-openable —
the very failure this refactor must not cause. So the idle floor for HLS keeps a
short window (ring ≥ `(idle_hls_window+1)·hls_target`), which opens instantly with
a short playlist. The consequence is that idle **HLS** channels save only
modestly (they still hold ~a window); the deep idle win is for TS-prebuffer-heavy
channels, or with `idle_hls_window = 0` (drops HLS while idle: max saving, but the
first HLS viewer waits ~one window for the playlist to rebuild). The structural
win for HLS at scale is Phase 3 (stop feeding idle channels at all).

Two supporting details make the gate observable and useful in production:

- **Forced OS release.** Collapsing a ring turns its dropped GOP bytes into GC
  garbage, but Go returns freed pages to the OS only lazily (the background
  scavenger paces itself), so RSS would sit flat for minutes and the win would be
  invisible. The reaper calls `debug.FreeOSMemory()` once per sweep *iff* it
  gated at least one stream, so idle RAM actually drops (e.g. ~200 MB → ~32 MB for
  5 idle channels with `idle_hls_window = 0`; a smaller drop when HLS is kept)
  without costing anything on a quiet sweep.
- **Client prebuffer is a daemon setting.** The per-viewer join burst
  (`client_prebuffer_sec`, default = the full ring) is served from the ring on
  every `/live/` connect *without* the panel passing a `?prebuffer=` URL param
  (a per-request param > 0 still overrides). This fixes the "TS only hands out
  ~1 GOP" symptom: the panel never set the param, so viewers got the minimal
  clean join; now they get the configured cache.

## Why this is safe here

- **Keyframe detection.** The daemon's inputs are TS produced by the panel's
  `ffmpeg -f tee [f=mpegts…]`, whose muxer sets `random_access_indicator` on
  video keyframes. `tsjoin` already cuts GOPs on that flag, so GOP boundaries are
  real keyframes for every real input — no PES-level keyframe parse needed for
  segment cutting. (`hlsseg`'s PES fallback existed for arbitrary sources the
  daemon does not actually ingest.)
- **Clean join / overlay** keep the current GOP always, so TS clean-join and the
  "send message" overlay re-seed are unaffected.

## Risks & mitigations

| Risk | Mitigation |
| --- | --- |
| **HLS seq must be stable** as the ring slides (GOPs age out). | Assign each GOP a monotonic id at creation; the seg index references GOP ids, not slice positions. `seq` is committed when a boundary forms and never recomputed. |
| **A listed segment ages out before the client fetches it → 404.** | Ring retention ≥ `hls_window·hls_target + margin`; the index drops a segment only once its GOPs have left the ring, so anything still in the playlist is still assemblable. |
| **EXTINF accuracy** from PCR, incl. the 33-bit PCR wrap. | Compute durations from PCR deltas with wrap handling (the ring already stores per-GOP PCR). |
| **Encrypted HLS** (`hlscrypt` AES-CBC). | Applied when a segment is assembled/served — on the fly, no stored ciphertext. |
| **HLS cold-start** on a fully idle channel (2b). | The ring already holds the window when there are TS viewers → instant playlist. Cold-start only for a channel with literally no audience; bounded by one window build. |

## Impact

- Watched channel: **≈ −50 %** memory (one buffer, not two).
- Idle **TS** channel (with 2b): **≈ `idle_buffer_sec`** — its prebuffer burst
  collapses from ~12.7 MB (full 15 s) to ~2–3 MB when unwatched, restored on the
  next viewer.
- Idle **HLS** channel (with 2b): from the full ring (~`hls_window·hls_target`)
  down to `(idle_hls_window+1)·hls_target` — a modest cut (e.g. 42 s → 24 s at the
  defaults), because HLS must keep ~a window buffered to stay openable. `idle_hls_
  window = 0` unlocks the deep cut at the cost of an HLS rebuild on first view.
- HLS latency: same or better (segments cut from an already-warm ring); an idle
  HLS channel opens instantly on its short kept window.

## Plan

1. **Config tuning** (done, ADR-independent): `prebuffer_max`/`hls_window` are
   now panel-editable and applied live — a linear stopgap.
2. **This ADR:** HLS-from-ring. Extend `tsjoin` with GOP ids + a segment index +
   `Playlist`/`Segment`; expose via `Hub`; rewire `serveHLS`; delete `hlsseg` as
   a byte store. Existing `hlsseg_test` / `server_test` / integration tests are
   the regression net; port the segment-cutting assertions onto the ring.
3. **Phase 2b (done):** viewer-gated ring/index (idle → `idle_buffer_sec` floor,
   restored on the returning viewer), driven by the existing reaper sweep.
4. **Phase 3 (separate):** on-demand ingest so the panel stops pushing idle
   channels 24/7 (idle channels then have no ingest at all).
