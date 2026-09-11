# 09. Encoder supervision (runbook)

The daemon can run and watch a stream's encoder instead of XC_VM's per-stream PHP
watchdog (`console.php monitor <id>`, `MonitorCommand.php`). One supervisor per node
replaces one resident PHP process per channel, and the health checks are made against
bytes the daemon is already fanning out rather than by ffprobing segments on disk.

Design and rationale: [`docs/adr/0002-monitor-in-daemon.md`](../adr/0002-monitor-in-daemon.md).

**It is off everywhere until you turn it on, in two places.**

---

## What the daemon does and does not own

| | Owner |
|---|---|
| Starting, watching, restarting the encoder | **daemon** |
| Choosing between sources (failover, priority backup, forced switch) | **daemon** |
| Health: stalled output, audio loss, frame-rate drop, scheduled restart | **daemon** |
| Building the command line — ffmpeg, or the [native remuxer](#the-native-remuxer) | **PHP** (`StreamProcess::buildLive` / `buildNativeLive`) |
| Every database write | **PHP** |
| Delay streams | **PHP** (unchanged, excluded from supervision) |

The daemon **never composes a command**. It runs the one the panel hands it, over the
PHP-only control socket. That is the security boundary for the whole feature.

The daemon has **no database access** and cannot get one — XC_VM's credentials live inside
its compiled C extension. This is why the split falls where it does, and why events reach
the panel through `<logs>/stream_log.log` (the file `StreamProcess::streamLog` already
writes and a cron already drains) rather than through SQL.

---

## Turning it on

Admin → Settings → **Fanout Encoder Supervision** (`fanout_supervise`, migration
`018_add_fanout_supervise.sql`). It is **on by default**.

**The node** follows it by itself: the panel's `fanout_sync` daemon writes `supervise` into the
daemon's `config.json`, and the daemon applies it on its next config poll — no restart, so no
viewer is dropped. While it is off, a hand-over is answered `501` and the panel runs the PHP
watchdog, exactly as before any of this existed. Confirm from the reload line:

```
config: applied …/config.json: supervise=true …
```

**Streams** move over without a flag day and without a restart:

- a stream that **starts** (admin start/restart, an on-demand viewer, `cron:streams` reviving it)
  is handed over instead of getting a PHP watchdog;
- a stream **already running** under a PHP watchdog is handed over by the next `cron:streams`
  pass, and its encoder is **adopted** — kept running — rather than restarted. A producer the
  daemon cannot adopt (the PHP LLOD segmenter, the PHP loopback relay) is replaced.

The PHP watchdog remains the fallback for a daemon that cannot be reached, and for what the
supervisor does not take: delay streams, created channels, and sources that must be resolved to a
short-lived URL at each start (YouTube and the other `yt-dlp` platforms).

### Verifying

```bash
# what this node is supervising
curl --unix-socket /home/xc_vm/bin/xc_fanout/sockets/control.sock http://localhost/monitors

# every supervised stream's state in one call — what the panel reconciles from
curl --unix-socket /home/xc_vm/bin/xc_fanout/sockets/control.sock http://localhost/monitors/state

# one stream: state plus what it turned out to be
curl --unix-socket /home/xc_vm/bin/xc_fanout/sockets/control.sock http://localhost/monitor/123
```

A supervised stream answers with `running`, `confirmed` (the running producer has delivered
bytes — `running` without it is a start in progress), `pid`, `source`, `restarts`,
`uptime_ms`, `adopted`, `fallback` (it is running its source's fallback command),
`last_error`, `daemon_pid`, and a `meta` block carrying the codecs, picture size and
measured bitrate. A stream this node is not supervising answers `404`.

The PHP watchdog stands down on its own for a supervised stream — it asks the daemon rather
than assuming, and logs `Stream is supervised by the fanout daemon; monitor standing down.`

---

## Rolling back

Clear **Fanout Encoder Supervision** in the panel. That is the whole rollback:

- PHP stops handing streams over, so nothing new is supervised.
- `MonitorCommand` resumes for those streams, because its stand-down check asks the daemon
  and an unreachable-or-not-supervising daemon answers "no".
- Streams already supervised stay with the daemon until they next restart, and come back
  under PHP then: a start that the daemon is not taking releases the stream from it before the
  PHP watchdog starts.

To take a single stream back immediately, `DELETE /monitor/<id>` — but note that **kills its
encoder**, so the channel restarts under PHP. There is no way to hand a running encoder back
to PHP without a restart.

If the daemon is stopped or removed entirely, its encoders keep running (see below) and the
PHP watchdog picks them up by pid on its next start.

---

## Restarts and upgrades

**Stopping the daemon does not stop the encoders.** They are detached, not killed, and left
running. The next daemon adopts them and resumes watching, so a daemon upgrade costs the
viewers nothing.

Adoption requires two things to agree, because pids are recycled:

- the pid in `<streams>/<id>_.pid` is alive, **and**
- its command line contains the stream's own HLS path (`adopt_match`, supplied by the panel)

If either fails, the daemon starts a fresh encoder instead. An adopted stream reports
`"adopted": true` on `GET /monitor/<id>`.

The panel re-hands its streams over after a daemon restart; `FanoutClient::supervisedIDs()`
is how it notices there is anything to re-register. That call returns `null` rather than an
empty list when the daemon is unreachable, so a dead socket is never mistaken for "nothing
is supervised".

---

## What the health checks do, and what turns them off

Each is derived from the stream's existing panel settings, and each is simply **not made**
when the panel has it switched off.

| Check | From | Fires when |
|---|---|---|
| Stalled output | `seg_time × 6` | no bytes published for that long |
| Audio loss | `audio_restart_loss` | audio PID silent for 30s, **only if the stream ever had audio** |
| Frame-rate drop | `fps_restart`, `fps_threshold`, `fps_delay` | rate falls below the threshold fraction of the peak seen since warm-up |
| Scheduled restart | `auto_restart` | the configured weekday and HH:MM, once per window |

Three deliberate differences from the PHP watchdog, each because the original behaviour was
wrong rather than merely slow:

- **A video-only channel is never restarted for losing audio.** It has none to lose.
- **The frame-rate baseline tracks the peak** rather than freezing at the first reading, so a
  stream that legitimately ramps up is not measured against its own worst moment.
- **The scheduled restart fires once per window.** Matching on HH:MM and re-checking within
  that minute restarts the stream in a loop until the clock moves on.

---

## Events

Every restart reason is written to `<logs>/stream_log.log` in the panel's own format, so the
existing drain records them with no change: `STREAM_START`, `STREAM_RESTART`, `STREAM_FAILED`,
`STREAM_START_FAIL`, `AUTO_RESTART`, `FFMPEG_ERROR` (a stall), `AUDIO_LOSS`,
`FPS_DROP_THRESHOLD`, `PRIORITY_SWITCH`, `FORCE_SOURCE`.

ffmpeg's stderr still lands in `<streams>/<id>.errors`, and the pid still in
`<streams>/<id>_.pid`, so existing tooling that reads either is unaffected.

---

## How the panel stays in step

`cron:streams` asks the daemon once per pass which streams it is supervising, then for each
of those reads `GET /monitor/<id>` and writes what it learns back into `streams_servers`:
`stream_status`, `pid`, `current_source`, and the codecs, resolution and measured bitrate.
The daemon has no database access, so this is the only path by which its view reaches the
panel.

Only fields the daemon could actually determine are written — it reports an unknown as
unknown, and overwriting a correct value with a blank would be worse than leaving it.

Two places that ask "is anything watching this stream?" now ask the daemon before concluding
nobody is, because `monitor_pid` names a PHP process and a supervised stream has none:

- `cron:streams` would otherwise start a PHP monitor on every pass, which would immediately
  stand down again.
- `admin/live.php` (the on-demand connect path) would otherwise start one and then wait its
  full three seconds for a `_.monitor` file that never appears — latency paid on the
  viewer's connect.

If the daemon cannot be reached, both fall back to exactly the old behaviour. That matters:
treating an unreachable daemon as "supervising nothing" would start a PHP monitor for every
stream on the node the moment the socket blinked.

Three more panel actions had to learn who owns the encoder, because each of them assumed
PHP did:

- **Stopping a stream** releases supervision *before* killing anything. Killing the encoder
  first is exactly the event the supervisor exists to react to, so the daemon would start a
  replacement and the stream would refuse to stop.
- **The rogue-ffmpeg sweep** is given the daemon's own reported pid. Its allow-list is built
  from the database pid read at the top of the pass, so an encoder the daemon restarted a
  moment later would not be on it and the sweep would shoot a healthy stream — logged as
  having killed a rogue.
- **Forcing a source** goes through the control socket. The `<id>.force` file it used to
  write was only ever read by `MonitorCommand`, which stands down for supervised streams, so
  the request silently did nothing.

## The native remuxer

`xc_fanout remux` is the native replacement for the ffmpeg process a **copy-only** live stream
used to need. The panel builds its command line exactly as it builds an ffmpeg one
(`StreamProcess::buildNativeLive`, next to `buildLive`), hands it over like any other, and the
supervisor runs it like any other. There is no command-line analysis on the daemon side: the
panel decides which command a stream gets, and the daemon runs what it is given.

```
xc_fanout remux -loglevel error -i 'http://provider/live/u/p/1234.ts' \
    -user_agent 'Mozilla/5.0' -insecure -ingest 'unix:…/ingest/42.sock' \
    -hls_time 10 -hls_init_time 2 -hls_list_size 6 -hls_delete_threshold 4 \
    -progress '…/streams/42_.progress' -hls_segment_filename '…/streams/42_%d.ts' \
    '…/streams/42_.m3u8'
```

It produces the same two outputs as the panel's ffmpeg `-f tee` line: the on-disk HLS the rest
of the panel reads (`<id>_.m3u8` + `<id>_<n>.ts`, numbered from 0 — for the tv_archive worker,
loopback children and the on-demand start checks), and the MPEG-TS feed into this daemon's ingest
socket. Bytes pass through unchanged: no demux of the codec payload, no re-encode. It is built from
the P2PTV project's native remuxer — its source layer (`internal/nativesrc`), its passthrough
segmenter (`internal/tsseg`) and its keyframe detection (`internal/tspes`, which also lets the
daemon cut HLS from sources that never set `random_access_indicator`).

Unlike ffmpeg's tee slave, whose feed into the daemon stays broken once a daemon restart breaks
it, the remuxer **redials** the ingest socket — after a daemon restart the stream is adopted and
carries on without restarting.

### Which streams run it

Only with supervision on, and [`source_backend`](06-configuration.md#the-source-backend) `auto` or
`native` — `ffmpeg` keeps ffmpeg for everything. A stream runs it when it is a live stream with
none of: a transcode profile, a custom ffmpeg command, a custom map, RTMP output, an external push
from this server, "generate timestamps", "read native", or a forced input audio codec. Each of its
sources is judged on its own: `http(s)`, `udp` and `rtp` run the remuxer; anything else (RTMP,
SRT, a local file, a `yt-dlp` platform) gets ffmpeg.

### When it cannot read a source

Some sources only show what they are once connected: an HLS playlist with fMP4 segments, an
encrypted one, a body that is neither MPEG-TS nor a playlist, a source with no detectable
keyframes. For those the remuxer exits with status **3** within a second or two. What happens
then is the panel's choice, carried in the spec:

- **`auto`** — each native source carries the panel's ffmpeg command as `fallback_cmd`. Exit 3
  switches that source to it at once: no fail sleep, no charge against `stop_failures`, and it
  sticks for the life of the spec. `GET /monitor/<id>` reports `"fallback": true`.
- **`native`** — no fallback; exit 3 is an ordinary failed start.

Any other exit — the upstream is down, slow or closed the stream — is an ordinary failure and
walks the source list exactly as an ffmpeg failure would. A `503` never selects the fallback:
switching pipelines would not make an unreachable upstream answer.

| Exit | Meaning |
|------|---------|
| 0 | stopped (a signal) |
| 1 | the source failed or ended |
| 2 | bad command line |
| 3 | the source cannot be served natively — run the fallback |

### Verifying

```bash
# remuxers this node is running, one per copy-only channel
pgrep -af 'xc_fanout remux'

# the channels that fell back to ffmpeg, and why
curl -s --unix-socket …/control.sock http://localhost/monitors/state | jq '.streams | map_values(.fallback)'
tail /home/xc_vm/content/streams/<id>.errors
```

The remuxer writes its errors where ffmpeg's went, `<streams>/<id>.errors`, and its progress
where ffmpeg's `-progress` went, so the panel shows speed and frame rate as before.

## Known gaps

- **Excluded, still on the PHP watchdog:** delay streams (their delay worker runs off the
  encoder's playlist), created channels (they resume at an offset computed at each start), and
  sources resolved through `yt-dlp` (the resolved URL expires).
- **`streams_servers.monitor_pid` names the daemon** for a supervised stream. The panel paths that
  ask "is anything watching this stream?" ask the daemon too (`StreamProcess::isWatched`); anything
  else reading the column should treat a pid that is not an `XC_VM[<id>]` process as the daemon.
- **Not yet exercised on a live node.** The launcher, process groups, adoption, the remuxer
  pipeline and the fallback are covered by tests against real processes, sockets and HTTP
  sources, but no production channel has run through this. Stage it before enabling it widely.

---

## Troubleshooting

**`/monitor/<id>` returns 501 "supervision not enabled on this node"** — the daemon's
`supervise` key is false. Check the panel setting, then that `fanout_sync` rewrote
`config.json` and the daemon re-read it.

**A copy-only channel still runs ffmpeg** — `"fallback": true` means the remuxer could not read
its source; the reason is at the end of `<streams>/<id>.errors`. Otherwise check that the source
backend is not `ffmpeg` and that the stream has none of the options listed under
[Which streams run it](#which-streams-run-it).

**Two encoders on one source** — should be impossible; adoption exists to prevent it. Check
whether `adopt_match` is reaching the daemon (`GET /monitor/<id>` and the spec the panel
sent) and whether `<id>_.pid` was correct at the time of the restart. Report it.

**A channel flaps** — check `last_error` on `GET /monitor/<id>` for the verdict, and the
event trail in the stream log. A health check firing repeatedly usually means its window is
too tight for that source rather than that the source is bad; the stall window follows
`seg_time`, so a stream with long segments needs a larger one.

**Nothing is supervised after enabling** — streams move over as they start, and running ones on
the next `cron:streams` pass once the daemon reports it is accepting hand-overs.
