# ADR 0005 — Switchable buffer source (RAM ring vs on-disk segments)

Status: **proposed** (2026-09-12) — design only. **Sequenced after ADR 0004**
(the ring-as-live-tail refactor); see "Why after 0004".

## Context

The daemon's premise (ADR 0001/0002, README) is *pull once, fan out from a RAM
ring, HLS in memory, no on-disk segment store*. The request here is to make the
**source of a stream's buffered bytes switchable**: keep today's RAM path, but
also offer a mode that holds nothing in memory until a channel is requested, then
reads the channel's already-on-disk HLS segments to fill the buffer (or serves
them straight), the way the legacy `live.php` byte path did.

Three facts shape the decision:

1. **On XC_VM, "disk" is usually tmpfs.** The panel writes `<id>_*.ts` /
   `<id>_.m3u8` to a RAM disk to spare the SSD from constant segment churn. So
   "read from disk instead of RAM" often **saves no memory** — the segments are
   already in RAM (page cache) and reading them into a buffer *adds a copy*. A real
   memory win needs segments on a **real disk**, paying IOPS + join latency + wear.

2. **Idle channels already cost ~0 RAM.** On the last viewer leaving, the reaper
   idle-stops the puller and **flushes the ring** (ADR 0001 gate + idle-stop). The
   RAM this proposal would reclaim is therefore mostly for *actively watched*
   channels — which any mode must buffer regardless.

3. **A local-playlist source already works.** `nativesrc.Open` accepts a bare
   path, `file://`, and a local `.m3u8` (→ `OpenHLSPull`); the puller's
   `nativeOnlyScheme` routes those past the HTTP probe; and the on-demand lifecycle
   already means "nothing buffered until requested, flushed after the audience
   leaves." So *disk-fed-ring* is largely a matter of formalising and documenting
   what the source layer can already do.

## Decision

Add a **`source_mode`** knob — node-wide in `config.json`, per-stream override on
the control PUT, exactly like the existing `source_backend` — selecting how a
stream's bytes are buffered and served:

| `source_mode` | Bytes come from | RAM held | Serve path |
|---|---|---|---|
| `ring` (**default, today**) | daemon pulls the upstream once | ring (watched only; idle→0) | ring cursor (post-0004) |
| `disk-ring` | a local `<id>_.m3u8` written by a separate encoder | ring, filled from the local playlist | ring cursor (post-0004) |
| `disk-passthrough` | the on-disk `<id>_*.ts` segments directly | ~none | segments streamed straight off disk |

`ring` and `disk-ring` share the whole existing pipeline — `disk-ring` is just a
stream whose configured source URL is the local playlist path, so it needs no new
byte path, only: (a) accept a `source_mode`/local-source registration cleanly,
(b) document it, (c) a small guard that a `disk-ring` stream never *also* tries to
pull the upstream. `disk-passthrough` is the only genuinely new serve path.

## Why after 0004

ADR 0004 collapses `serveLive`'s two paths (join cursor + live-tail channel) into
**one ring-cursor loop**. `disk-passthrough` is a *second* serve path that bypasses
the ring entirely. Designing it against the post-0004 single-cursor `serveLive` is
far cleaner than bolting a third mode onto today's dual-path code: after 0004 the
mode switch is "cursor over the ring" vs "reader over the on-disk playlist", one
branch at the top of `serveLive`/`serveHLS`, rather than a change threaded through
join-then-subscribe. `disk-ring` could land independently (it rides existing
`nativesrc`), but formalising the knob belongs with `disk-passthrough`.

## Design sketch (per mode)

- **`ring`** — unchanged.
- **`disk-ring`** — registration sets the stream's source to the local playlist
  (`file:///…/<id>_.m3u8`) and marks it disk-fed so `startLocked` never falls back
  to pulling the upstream. Everything downstream (ring, HLS view, fan-out, idle
  gate) is identical to `ring`. The producing encoder (the panel's ffmpeg or
  `xc_fanout remux`) is assumed already running per the panel's model.
- **`disk-passthrough`** — on a viewer request, `serveHLS` serves the on-disk
  playlist and `<seq>.ts` files directly (with the existing AES-encrypt-once cache
  in front, since encryption is still the daemon's), and `serveLive` reconstructs
  the TS by concatenating segments as they appear (a disk "cursor": last segment +
  offset, tailing the playlist). No ring, no `Publish`, no puller. Idle handling is
  trivial — there is nothing resident to reap. The daemon holds only the small
  per-stream registry entry + the segment cache while watched.

## Consequences / when to use which

- **Use `ring` (default)** for the project's core case: the daemon owns the pull
  and serves from memory. Best latency, finest keyframe-aligned join, one copy
  after 0004.
- **Use `disk-ring`** when a separate encoder already produces segments and you
  want the daemon to fan those out with its full RAM-ring behaviour (prebuffer,
  in-memory HLS, idle gate) without pulling the upstream itself.
- **Use `disk-passthrough` only when segments live on a real disk** and the
  channel-to-concurrent-viewer ratio is very high — then the OS page cache is the
  buffer and cold channels cost no daemon RAM. **On tmpfs it saves little and adds
  a copy; do not use it there.** Costs: coarser (whole-segment) join granularity,
  higher join latency, disk IOPS/wear.

## Open question (answered by the operator, via the knob)

Whether `disk-passthrough` pays off depends on **where segments live (tmpfs vs
real disk) and the channel:viewer ratio** — which is exactly why this is an
operator-selectable knob rather than a global change: the daemon can't know, so it
exposes the choice and defaults to `ring`.

## Rollout

After ADR 0004 is merged:

1. Add the `source_mode` config key + per-stream override (clamped to a known
   value, unknown → `ring`, mirroring `normalizeBackend`).
2. Wire `disk-ring` (local-playlist source + no-upstream-pull guard) with a test.
3. Add `disk-passthrough` as the single top-of-`serveLive`/`serveHLS` branch
   against the post-0004 cursor code, with the segment-tailing reader + tests.
4. Document all three modes in `docs/en/06-configuration.md` and the source-mode
   section of the architecture doc.

## Alternatives considered

- **Make it automatic** (daemon picks disk vs ring by detecting tmpfs / load).
  Rejected: the daemon cannot reliably tell tmpfs from disk or predict the
  viewer ratio, and a silent mode switch on the byte path is exactly the kind of
  invisible behaviour this project avoids. Keep it an explicit knob.
- **Drop the RAM ring entirely and always serve from disk** (revert to the legacy
  model). Rejected: it discards the core value of ADRs 0001/0002 for the common
  case; the switch keeps both.
