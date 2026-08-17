# xc_fanout documentation

**Language:** [Русский](../README.md) · English

`xc_fanout` is XC_VM's native live-stream **fan-out** daemon:
**one process pulls each source exactly once** and fans it out to many viewers
through nginx, keeping PHP out of the byte path, and produces HLS
(including AES-128 encrypted) **entirely in memory**.

The docs are split by topic — each file covers one thing. If you're new to the
project, read in order: 01 → 02 → then whatever interests you.

| # | File | About |
|---|------|-------|
| 01 | [what-is-it.md](01-what-is-it.md) | **What it is and why.** The problem the daemon solves, in plain terms. Start here. |
| 02 | [architecture.md](02-architecture.md) | **Architecture and data flow.** What it's built from, how bytes travel from source to viewer. |
| 03 | [endpoints.md](03-endpoints.md) | **HTTP endpoints.** Full reference for both surfaces: paths, methods, parameters, bodies, responses. |
| 04 | [internals.md](04-internals.md) | **Internal mechanisms.** How the TS fan-out, clean join, prebuffer, HLS segmentation, encryption and source acquisition work. |
| 05 | [lifecycle.md](05-lifecycle.md) | **Stream lifecycle.** On-demand start/stop, pull/push/launch modes, the reaper. |
| 06 | [configuration.md](06-configuration.md) | **Parameters and flags.** What the daemon consumes at startup: CLI flags, defaults, what tunes what. |
| 07 | [integration.md](07-integration.md) | **Integration with the XC_VM panel.** How nginx, PHP, `fanout_sync` and binary install work with it. |
| 08 | [build-release.md](08-build-release.md) | **Build, test, release.** How to build, run the tests, cut a version. |

## Terms in one line

- **Stream** — one live source with an `<id>`, which the daemon fans out.
- **Fan-out** — serving one incoming stream to many viewers at once.
- **Puller** — the part that pulls the source (fetches the data itself).
- **Ingest** — the reverse mode: a producer pushes data to the daemon.
- **Client surface** — HTTP for viewers (via nginx).
- **Control surface** — HTTP for the PHP panel only (stream registration, status).
- **MPEG-TS** — the transport video format, made of 188-byte packets; the daemon's base byte-path format.
- **HLS** — streaming by cutting the stream into short segments + an `.m3u8` playlist.
