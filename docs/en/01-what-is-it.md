# 01. What it is and why

> Read this file first if you are seeing the project for the first time. Here — no code, in
> plain words: what this program is and what problem it solves.

## What it is

`xc_fanout` is a background program (a daemon) written in Go. It is built into the
**XC_VM** IPTV panel system and is responsible for one narrow but heavily loaded area:
delivering **live video** (TV channels, live streams) from the source to the viewers.

The daemon compiles into **a single static binary** with no external dependencies
(`CGO_ENABLED=0`) — you can simply copy it onto any Linux server and run it.

## What problem it solves

Imagine a TV channel that 500 people are watching. This channel's video sits on an
external source (a URL). It needs to be delivered to all 500 viewers.

### How it was before (the legacy PHP scheme)

In the old XC_VM scheme, a separate PHP process was started for **each viewer**
(`live.php` / `ProxyCommand`), which:

1. went to the external source itself and downloaded the video from there;
2. relayed the bytes to its own viewer.

Consequences:

- **The source is downloaded 500 times** — once per viewer. This is wasted traffic and
  load on the source (and sometimes the source outright limits the number of connections).
- **Each viewer "pins" a heavy PHP process** for the entire duration of viewing.
  500 viewers = 500 busy processes. The server hits a wall not on the network, but on the number of workers.

### How it works now (with xc_fanout)

The daemon changes the model to "one pulls — many distribute":

1. The daemon goes to the external source **exactly once** per channel (regardless of the number of
   viewers).
2. It keeps the received video in memory and **distributes it to all viewers at once** (fan-out).
3. The bytes are sent outward by **nginx** (via the `X-Accel-Redirect` mechanism), not by PHP.
   To the system, a viewer is a cheap network connection, not a busy process.

Result:

- the source is downloaded **once** instead of 500 times;
- PHP **does not carry video bytes at all** — it stays only on the "control"
  circuit (tell the daemon "start this channel", ask "is the channel alive?");
- scaling is now limited by network and memory, not by the number of PHP workers.

## What exactly it serves

The daemon serves live video in two formats simultaneously:

- **Live MPEG-TS** — a continuous byte stream (for players that can handle TS directly).
- **HLS** — split into short `.ts` segments + an `.m3u8` playlist (for browsers,
  mobile players). HLS is assembled **directly in RAM**, nothing is written
  to disk. When needed, segments are served **encrypted with AES-128**.

## Where it sits in the big picture

```
external source → [ xc_fanout ] → nginx → viewers (many)
                       ▲
                       │ control commands (channel registration, status)
                     XC_VM PHP panel
```

- **On the left** — the video source (the TV channel URL).
- **In the center** — this daemon: pulls the source once, distributes it to many.
- **On the right** — nginx, which serves the bytes to the viewers.
- **At the bottom** — the PHP panel, which only *controls* the daemon but does not carry video itself.

## Historical note

This is an implementation of the design decisions **ADR 0002** (phases P2–P3) and **ADR 0003** (off-air detection,
encrypted HLS) from the XC_VM panel repository. The logic for selecting and capturing the source is a
direct port of the legacy `ProxyCommand` class from PHP, but with honest parsing of the
MPEG-TS structure instead of reading at fixed offsets.

## Next

- How it is built internally and how the bytes flow — [02. Architecture](02-architecture.md).
- The full list of HTTP addresses — [03. Endpoints](03-endpoints.md).
