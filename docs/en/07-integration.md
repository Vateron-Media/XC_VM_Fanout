# 07. Integration with the XC_VM panel

> How the daemon is embedded in the system: who talks to it, who carries the bytes, how the
> binary is installed and updated. The address reference lives in [03. Endpoints](03-endpoints.md).

The daemon is not a standalone product but a load-bearing "muscle" inside the XC_VM panel.
Around it are four participants: nginx, the PHP panel, the `fanout_sync` daemon, and the
binary-installation mechanism.

## Who talks to whom

```
                    ┌─────────────── PHP panel ─────────────────┐
                    │  live.php (authorization, X-Accel)         │
                    │  StreamProcess (source registration)       │
                    │  console.php fanout_binary (installation)   │
                    └───────────────┬───────────────────────────┘
                                    │ control socket (-ctl)
                                    │ PUT/GET/DELETE /streams,/ingest,/probe
                                    ▼
   viewer ──HTTP──► nginx ──X-Accel──► [ xc_fanout ] ──pulls/receives──► source
                       ▲   client socket (-sock)        │
                       │   /live, /hls                  │ GET /connections
                       └──────────  fanout_sync ────────┘  (viewer reconciliation)
```

## nginx — carries the bytes out

nginx proxies viewer requests to the **client socket** (`-sock`). The key trick is
`X-Accel-Redirect`: `live.php` authorizes the viewer and **hands nginx an internal URL** of the form

```
/xc_fanout/<id>?c=<uuid>&prebuffer=<sec>
```

It is precisely `/xc_fanout/<id>` that PHP emits; in an internal `location` nginx rewrites this
path into the daemon's `/live/<id>` and proxies it to `-sock`. After that **nginx carries the
bytes**, and the PHP process is freed: it made the "let in / don't let in" decision and exited.

> ⚠️ The X-Accel target must be **2-segment** (`/xc_fanout/<id>`). The server-wide catch-all
> `rewrite ^/(user)/(pass)/(stream)` in nginx fires on internal redirects too — and a
> 3-segment path (e.g. `/xc_fanout/live/<id>`) it would intercept and parse as
> `user=xc_fanout`/`pass=live` → `INVALID_CREDENTIALS`.

**HLS works differently — the playlist does NOT go through X-Accel.** Segments are
`X-Accel`-redirected to the daemon (`segment.php` → internal `/xc_fanout_hls/<id>_<seq>`), but
the **playlist `index.m3u8` is built by PHP**: `live.php` fetches it from the daemon's client
socket itself (`FanoutClient::hlsPlaylist`), rewrites the relative `<seq>.ts` into tokenized
authorized URLs (`HLSGenerator::tokenizeDaemonPlaylist`, adding `#EXT-X-KEY` when encryption is
in play) and **serves** the result itself. The playlist is polled and small, so PHP is involved
on every poll here — only the segments leave the byte path.

## The PHP panel — manages, but doesn't carry video

PHP talks to the **control socket** (`-ctl`):

- **Source registration.** For a pull/proxy channel — `PUT /streams/<id>` with the URL,
  User-Agent, proxy, cookie, and, if needed, the HLS encryption key. For a push channel —
  `PUT /ingest/<id>` (the panel's `StreamProcess` obtains the ingest-socket path before starting
  the ffmpeg-tee stream).
- **Off-air detection.** `GET /streams/<id>` (status) and `GET /probe/<id>?wait=<ms>` (warming
  up the source and waiting for data). If there is no data, the panel shows a "not on air" page,
  replicating the legacy `startProxy` behavior but without letting the viewer hang on a dead
  source.
- **Tearing down channels.** `DELETE /streams/<id>` or `DELETE /ingest/<id>`.

## fanout_sync — reconciles the viewer list

Under X-Accel, PHP **does not see the moment a viewer disconnects** (the connection is held by
nginx, not PHP). Because of this, `lines_live` rows (the accounting of active views) could
"hang" open.

The solution: `live.php` creates a `lines_live` row with `pid=0` and passes the viewer a
connection-uuid via `?c=<uuid>`. The `fanout_sync` daemon periodically reads
`GET /connections` (the list of uuids of all active live-TS viewers across all streams) and
**closes rows** whose uuid is no longer in that list. The daemon, for its part, reliably detects
disconnects — including by dropping "stuck" viewers on `-write-timeout`
(see [04](04-internals.md#guarding-against-stalled-viewers)).

> **Closing is not instantaneous.** Between a viewer's actual departure and the closing of the
> row, the following accumulates: up to `-write-timeout` (15 s) — until the daemon drops the
> stuck connection and removes the uuid from `/connections`; + the `fanout_sync` reconciliation
> interval (~10 s); and the row is not closed earlier than the connect-grace of `fanout_sync`
> itself (~20 s from creation — protection against a race on connect). Net result: a "ghost"
> after an unclean disconnect lives up to ~25–35 s, then the slot is freed. On a clean
> connection close the daemon removes the uuid immediately, without waiting for
> `-write-timeout`.

## Installing and updating the binary

The sources live in this repository; **compiled binaries are GitHub Release assets** (they are
not committed to the tree, `dist/` is in `.gitignore`). The panel installs and updates the
daemon with the command:

```
console.php fanout_binary          # install/update to the latest release
console.php fanout_binary force     # reinstall the same version
```

What happens inside:

1. the version of the installed binary is read (`xc_fanout -version`);
2. it is compared against the tag of the latest release on GitHub;
3. the asset for the machine's architecture is downloaded;
4. its **SHA-256** is verified;
5. the binary is installed **atomically**, and the service keepalive restarts the daemon.

> ⚠️ Restarting the daemon (this update included) **resets its in-memory stream registry**.
> pull streams will be restored on the next viewer, but push/ingest streams will not — not until
> their ffmpeg restarts and calls `PUT /ingest` again. Details and how to force a re-warm —
> [05, "Restart and state recovery"](05-lifecycle.md#restart-and-state-recovery).

More on version releases — [08. Build and release](08-build-release.md).

## Related project documentation

- `XC_VM/docs/xc_fanout.md` in the panel repository — the overall design and the phased cutover
  plan.
- ADR 0002 — phases P2–P3 (fan-out TS, in-memory HLS).
- ADR 0003 — off-air detection and encrypted HLS.
