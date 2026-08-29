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

Phase 2b (follow-up): when a channel has **no audience** (`Hub.Count()==0` and no
HLS access within `grace`), collapse the ring to the current GOP and drop the HLS
index. First viewer regrows it. Idle channels then cost ≈ one GOP.

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
- Idle channel (with 2b): **≈ current GOP** only — memory scales with *watched*
  channels, not fed channels.
- HLS latency: same or better (segments cut from an already-warm ring).

## Plan

1. **Config tuning** (done, ADR-independent): `prebuffer_max`/`hls_window` are
   now panel-editable and applied live — a linear stopgap.
2. **This ADR:** HLS-from-ring. Extend `tsjoin` with GOP ids + a segment index +
   `Playlist`/`Segment`; expose via `Hub`; rewire `serveHLS`; delete `hlsseg` as
   a byte store. Existing `hlsseg_test` / `server_test` / integration tests are
   the regression net; port the segment-cutting assertions onto the ring.
3. **Phase 2b:** viewer-gated ring/index (idle → current GOP).
4. **Phase 3 (separate):** on-demand ingest so the panel stops pushing idle
   channels 24/7 (idle channels then have no ingest at all).
