# 10. Segment gateway

> An optional third HTTP surface that answers a viewer's routine requests in Go, without
> PHP-FPM, on behalf of the panel: HLS segments and keys, a known viewer's playlist refresh, and
> a known MPEG-TS viewer's reconnect. The panel's design record is XC_VM's
> `docs/adr/0005-segment-gateway.md`; the code is [`internal/gateway`](../../internal/gateway).

## Why

With fanout delivering, PHP still answered every request a viewer makes after the first one,
although none of them carries video: PHP checks a token, then hands the bytes to the daemon
(`X-Accel-Redirect`). An HLS viewer polls its playlist and fetches a segment and a key every few
seconds, so 10,000 viewers at 4-second segments are about 5,000 PHP-FPM requests a second on one
node. The gateway answers those requests itself, from the daemon's memory, with the same tokens,
the same checks and the same answers as PHP.

## The rule: anything not proven equivalent goes to PHP

For each request the gateway answers in one of four ways:

| Answer | When |
| --- | --- |
| **The bytes** | It owns the request: a segment from fanout's ring, a key, a tokenized playlist, a TS stream. |
| **PHP's 404** | PHP would refuse it the same way (a token that does not open, another address, an ended session). |
| **PHP's 302** | A segment minted by another server goes to that server, as `segment.php` sends it. |
| **`X-Accel-Redirect: @gw_<kind>_php`** | Anything else. nginx runs the original request through PHP exactly as before. |

The hand-back happens **before any side effect**, so PHP never acts twice. Among what is handed
back: a token in a form PHP's lenient base64 might read otherwise, a value PHP reads with
`intval()`, a non-ASCII user agent (`htmlentities`' table), an unknown server, a stopped agent,
a stale or missing policy, a node lease past its deadline. A gateway that does not answer at all
(nginx 502/504) sends the request to PHP too.

## What it answers

The panel's `gateway_mode` setting picks how much, per node:

| Mode | The gateway answers |
| --- | --- |
| `off` | nothing; nginx sends every request to PHP |
| `shadow` | nothing; nginx mirrors each request to it, and it only judges and counts (see [Shadow](#shadow-compared-with-php)) |
| `segments` | live daemon segments (`/hls/<token>`, from the ring), catch-up minutes (from their file and offset) and keys (`/key/<token>`) |
| `segments+playlist` | and, on a node whose viewers are in its cluster agent: a known viewer's playlist refresh, and a known TS viewer's reconnect (both `/auth/<token>`) |

The first request of each viewer (`/auth/<token>` with no connection yet) always stays PHP's:
admission, the connection's creation, an on-demand start, a proxy channel's source registered
with the daemon.

### Segments and keys

- **Live segment** (`<id>_d<seq>.ts` in the token): the token opens, the server is this one (or
  302 to its owner), the lease holds, the viewer's marker exists, the address matches; then
  fanout's own `/hls/<id>/<seq>.ts` in-process, with a pending "send message" overlay.
- **Catch-up minute**: the viewer is heard through the agent (as PHP's heartbeat) and refused when
  its session ended; the minute is served from its offset, with ranges, by fanout's file server.
- **Key**: the token opens and the address matches; the key file `STREAMS_PATH/<id>_.key`.

### Playlist refresh (HLS)

A known viewer's `/auth/<token>` with `extension: m3u8`: the gateway computes the HLS
connection id as the panel does (`md5("hls#<identity>#<stream>#<ip>#<ua>")`), finds the open
record in the agent, checks the address and the second-address rule, re-opens the record,
spools the line's `conn.limit` for MAIN, and serves the daemon's playlist with every segment
line tokenized and the media sequence re-anchored under the panel's shared state file.

### TS reconnect

A known viewer's `/auth/<token>` with `extension: ts`, which a player sends when it reconnects
to the URL it was redirected to. The gateway answers it as `live.php`'s TS arm does:

- **Judged**: the same checks as a refresh, then the record named by the token's `uuid` (TS keeps
  MAIN's uuid; only HLS is keyed on the viewer), container `ts`, open, and fanout's (`pid` 0);
  the stream fed (`has_data`, in-process: `Manager.Fed`).
- **Served**: the record re-opened in the agent with `pid` 0 and `hls_last_read`, `conn.limit`
  spooled, then fanout's own `/live/<id>?c=<uuid>&prebuffer=<s>&vc=<codec>` in-process: the URL
  `live.php` hands nginx (`/xc_fanout/<id>`), without the hop. The viewer is counted under its
  uuid as before, so `/connections`, the agent and `fanout_sync` see no difference. No viewer
  marker is written (`live.php`'s hand-off leaves none).
- **Prebuffer** as `live.php` picks it: `client_prebuffer`; a restreamer's `restreamer_prebuffer`,
  or `seg_time` when its link asks for a prebuffer.
- **PHP's**: a direct-proxy channel (only `live.php` registers its source with the daemon, so a
  source edit reaches it there), an ended connection (PHP re-opens it), a record a PHP worker
  still feeds, and a policy without `live.ts` (a panel from before this change).

## Inputs

### The policy file (`-gw-policy`)

`/home/xc_vm/tmp/gateway/policy.json`, `0600`, written every minute by the panel's
`cron:cache` (`Core/Gateway/GatewayPolicy`) from the same settings and servers caches its
stream endpoints read. The gateway never reads PHP's caches.

It carries the mode, the keys with their windows (viewer keys, the stream secret, the token
context, as hex of the exact bytes), the address rules, the headers, the lease's deadline, the
paths (markers, streams, archive, signals, the agent's socket and spool), where the viewers are
kept (`conn_store`: `agent` or `php`), and what `live.php` reads (`live`: buffering, instant off,
the second-address rule, and for TS `ts`, `client_prebuffer`, `restreamer_prebuffer`,
`seg_time`).

The gateway re-reads the file when its mtime or size changes, looking at most once a second.
Missing, unreadable, of another version, or older than ten minutes: every request goes to PHP.

### The tokens

The panel's sealed tokens (AES-256-GCM, key `HMAC-SHA256(secret, "xc_vm stream token v2|" +
context)`) and, when accepted, the legacy CBC ones, opened in the panel's order: viewer keys,
the stream secret, the replaced context, the replaced secret. The test vectors in
`internal/gateway/testdata` are generated from the panel's own code and passed by both sides
(XC_VM's `GatewayTokenVectorsTest` and `GatewayLiveVectorsTest`).

### The cluster agent

The viewer records live in the node's agent (`xc_agent`, `/v1/conn` on its socket): the gateway
reads, touches or replaces a record exactly as PHP does, and spools `conn.limit` lines into the
agent's spool with the panel's file naming (`CLOCK_MONOTONIC`, PHP's `hrtime`), so they reach
MAIN in order with PHP's.

## Endpoints (`-gw` socket, nginx only)

| Path | Who calls it | What it does |
| --- | --- | --- |
| `/stream/segment`, `/stream/key`, `/stream/live` | nginx, serving modes | Answers the request nginx describes: its original URI in `X-XC-Original-URI`, the viewer's address in `X-XC-Client-IP`, the host in `X-XC-Host`. |
| `POST /shadow` | nginx's mirror, `shadow` | Judges the mirrored request and records the verdict. `204`. |
| `POST /shadow/php` | the panel, `shadow` | What PHP answered the same request (`X-XC-Request-ID`, `X-XC-Kind`, `X-XC-Outcome`). `204`. |
| `GET /stats` | the panel's audit | `{"counts": {"<kind> <action> <reason>": n}, "judge_us_le": {…}, "shadow": {…}}`. |

Each verdict is counted as `<kind> <action> <reason>`: for example `segment serve daemon`,
`key serve key`, `live serve playlist`, `live serve ts`, `live php new-connection` (a first
request), `live php proxy`. `judge_us_le` sorts the time each decision took (not the bytes served
after it) under the first bound it fits: 100, 500, 1000, 5000, 20000, 100000 µs, `inf`.

```bash
curl --unix-socket /home/xc_vm/bin/xc_fanout/sockets/gw.sock http://gw/stats
```

## nginx

The panel renders the include `bin/nginx/conf/gateway.conf` from the node's mode
(`Core/Gateway/GatewayNginxConfig`), and only while the gateway's socket exists. Its locations
are exact matches for what the server-level rewrites make of `/hls/`, `/key/` and `/auth/`
(`/stream/segment`, `/stream/key`, `/stream/live`), each passing to the `xc_gateway` upstream
(keepalive) with buffering off and `error_page 502 504 = @gw_<kind>_php`. Rollback is
`gateway_mode = off`: the root cron renders an empty include and reloads nginx within a minute.

A TS stream answered by the gateway runs through `/stream/live`'s location, whose
`proxy_read_timeout` is 60 s; the daemon's `viewer_idle_timeout_sec` (30 s by default) ends a
silent stream first.

## Shadow: compared with PHP

In `shadow`, nginx gives PHP and the mirrored copy the same request id. The gateway judges the
copy (read-only: it touches nothing), PHP reports what it answered once the viewer has it, and
the gateway's book pairs both, whichever comes first:

- **agree** / **disagree** (with a sample: kind, both answers, stream; never a token);
- **deferred**: the gateway would have handed the request to PHP anyway;
- **unmatched**: one side never came (30 s).

The comparison is kept in `-gw-state` across restarts. The panel reports it with the node's audit
and shows on Cluster Nodes when a node is ready to serve: seven days, and a hundred requests,
since its last disagreement. Switching a node from shadow to serving stays the operator's call.

## Measured

On a one-vCPU test LB: a segment decision 9 µs, a key 2.3 µs, a playlist refresh's checks 92 µs
plus 65 µs to tokenize six segments; 99% of decisions in ≤ 500 µs at 1,500 req/s. Through
nginx: p50 2.4 ms / p99 5.1 ms with the gateway, 8.3 ms / 23 ms with PHP. With `xc_fanout`
killed under load every key request was still answered (PHP, while the daemon restarted).

## Limits

- A request behind an XC_VM_Proxy route (`/<route>/hls/…`) stays PHP's.
- A TS reconnect on a direct-proxy channel stays PHP's (see above).
- The verdict counts restart with the daemon; the shadow comparison does not.
- A node whose viewers are in MAIN's Redis or MySQL (`conn_store: php`) gets segments and keys
  only; its refreshes and reconnects stay PHP's.
