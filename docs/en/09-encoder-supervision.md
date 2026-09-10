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
| Building the ffmpeg command line | **PHP** (`StreamProcess::buildLive`) |
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

Both halves are required. Either one off means nothing changes.

**1. The node.** `supervise: true` in the daemon's `config.json`. In practice you do not
edit this by hand — the panel writes the file from the setting below, and the daemon picks
it up on its next config poll. Confirm from the daemon's boot line:

```
config: supervise=true sock=… ctl=… ingestdir=…
```

**2. The panel.** Admin → Settings → **Fanout Encoder Supervision** (`fanout_supervise`).
Requires migration `018_add_fanout_supervise.sql`.

**3. Per stream.** Nothing is supervised until the panel hands it over, which happens the
next time each stream starts. Existing streams keep running under the PHP watchdog until
they restart, so a rollout is gradual by construction rather than a flag day.

### Verifying

```bash
# what this node is supervising
curl --unix-socket /home/xc_vm/bin/xc_fanout/sockets/control.sock http://localhost/monitors

# one stream: state plus what it turned out to be
curl --unix-socket /home/xc_vm/bin/xc_fanout/sockets/control.sock http://localhost/monitor/123
```

A supervised stream answers with `running`, `pid`, `source`, `restarts`, `uptime_ms`,
`adopted`, `last_error`, and a `meta` block carrying the codecs, picture size and measured
bitrate. A stream this node is not supervising answers `404`.

The PHP watchdog stands down on its own for a supervised stream — it asks the daemon rather
than assuming, and logs `Stream is supervised by the fanout daemon; monitor standing down.`

---

## Rolling back

Clear **Fanout Encoder Supervision** in the panel. That is the whole rollback:

- PHP stops handing streams over, so nothing new is supervised.
- `MonitorCommand` resumes for those streams, because its stand-down check asks the daemon
  and an unreachable-or-not-supervising daemon answers "no".
- Streams already supervised stay with the daemon until they next restart.

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

## Known gaps

- **`streams_servers.monitor_pid` changes meaning.** For a supervised stream it becomes the
  daemon's pid rather than a per-stream monitor's. Anything reconciling on it needs updating.
- **Delay streams are excluded** and still run the legacy way.
- **`GET /monitor/<id>` is not yet consumed by the panel** to update `streams_servers`. The
  metadata is available and correct; wiring it into the reconcile is outstanding, so codec
  and resolution columns are still filled by PHP's start-up ffprobe.

---

## Troubleshooting

**`/monitor/<id>` returns 501 "supervision not enabled on this node"** — the daemon's
`supervise` key is false. Check the panel setting, then that the daemon re-read its config.

**Two encoders on one source** — should be impossible; adoption exists to prevent it. Check
whether `adopt_match` is reaching the daemon (`GET /monitor/<id>` and the spec the panel
sent) and whether `<id>_.pid` was correct at the time of the restart. Report it.

**A channel flaps** — check `last_error` on `GET /monitor/<id>` for the verdict, and the
event trail in the stream log. A health check firing repeatedly usually means its window is
too tight for that source rather than that the source is bad; the stall window follows
`seg_time`, so a stream with long segments needs a larger one.

**Nothing is supervised after enabling** — supervision is applied when a stream *starts*.
Restart a stream, or wait for its next restart.
