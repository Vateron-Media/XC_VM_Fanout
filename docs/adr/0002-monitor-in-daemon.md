# ADR 0002 — Move the per-stream monitor into the daemon

Status: **implemented** (M1-M5 landed; off by default, see Rollout) (supersedes the "PHP retains per-stream process monitoring" line in
XC_VM's `docs/adr/0003-full-daemon-cutover.md`)

Date: 2026-09-10

## Context

`XC_VM/src/Cli/Commands/MonitorCommand.php` is a per-stream watchdog: one long-lived PHP
process per stream, started by `StreamProcess::startMonitor()` via
`console.php monitor <id> [restart]`. It does eleven distinct jobs in one loop:

1. Single-instance enforcement (`_.monitor` pid file, kill any prior monitor)
2. Start/restart the stream's ffmpeg (`startStream` / `startLLOD` / `startLoopback`)
3. Wait for the HLS playlist to appear, with a bounded retry count
4. Probe the first segment with ffprobe for codec/duration metadata
5. Persist that metadata to `streams_servers` (stream_info, compatible, audio_codec,
   video_codec, resolution, bitrate, stream_status, stream_started)
6. Detect a stalled output (md5 of the on-disk playlist unchanged for `seg_time * 6`)
7. Detect an FPS drop (reads ffmpeg's `_.progress_check` file, compares to a baseline)
8. Detect audio loss (ffprobe a segment every 300s)
9. Priority backup: probe higher-priority sources every 300s and switch back to one
10. Forced source switch (a `SIGNALS_TMP_PATH/<id>.force` file)
11. Scheduled auto-restart, delay-process supervision, failure-limit give-up

Nine of the eleven are things the daemon is better placed to do, because it already has
the bytes in RAM. It parses PAT/PMT/PES today (`internal/tsjoin`), tracks per-stream
liveness (`Stream.lastData`), owns the source URL list (`puller.Source.URLs`), and already
reconnects with backoff. PHP is doing all of this the hard way — shelling out to ffprobe
against files on disk, md5-ing a playlist, and reading a progress file ffmpeg writes.

## The constraint that shapes the design

The obvious version of this change — "give the daemon a MySQL connection and port
`StreamProcess.php` to Go" — **is not available**, and this is the central finding.

XC_VM's database credentials live in `config.enc` (AES-256-GCM). The key is derived from
`install_id` and the whole thing is handled inside the compiled `XC_VM` C extension.
`src/Core/Config/ConfigReader.php` states it outright:

> Учётные данные БД и Redis расширение никогда не возвращает в PHP;
> для подключений используйте `\XC_VM::db_connect()` / `\XC_VM::redis_connect()`.

PHP itself never sees the credentials. `LbInstallFlow` confirms `config.ini` is deleted
once `config.enc` exists. For the daemon to reach MySQL it would have to either

- reimplement the vendor's protected key derivation — adversarial to a deliberate security
  control, and it would break on any extension update; or
- link the C extension — which ends `CGO_ENABLED=0`, and with it the single static binary
  per architecture that is the daemon's defining property and its whole install story.

Neither is acceptable. **The daemon does not get a database connection.**

## Decision

Split on *ownership of the process* vs *ownership of the configuration*:

- **The daemon owns the process and the monitoring.** It spawns the stream's ffmpeg,
  supervises it, restarts it, selects between sources, runs every health check, and applies
  the auto-restart schedule. This is a full supervisor, not an advisor.
- **PHP owns the configuration and stays the system of record.** It continues to build the
  ffmpeg command line (`StreamProcess::buildLive`) from the database, transcode profiles and
  settings, and it continues to perform every database write.
- **The two talk over the control socket that already exists.** PHP pushes a *stream spec*
  down; the daemon pushes *state and events* back up.

`buildLive` is therefore **not** ported. That is deliberate: it is 211 lines of
DB-and-settings-driven string assembly at the centre of a live system, its inputs
(transcode profiles, per-protocol arguments, GPU options, logo filters, external push
targets) are all in tables the daemon cannot read, and reimplementing it in Go would buy
nothing except a large surface for silent behavioural drift.

### What moves

| Monitor job | Moves? | Where it lands |
|---|---|---|
| ffmpeg spawn / kill / restart | **yes** | daemon `internal/supervisor` |
| single-instance enforcement | **yes** | daemon owns the stream, so it is structural |
| playlist-appears wait | **replaced** | daemon watches its own ingest socket for first bytes |
| stalled output | **yes, improved** | `lastData`, continuous, not an md5 poll |
| audio loss | **yes, improved** | audio-PID flow from the TS it already parses |
| FPS drop | **yes, improved** | PES timestamps, not ffmpeg's progress file |
| codec / resolution / bitrate | **yes, improved** | measured from the ring, not one ffprobe |
| priority backup + force source | **yes** | daemon owns the URL list already |
| scheduled auto-restart | **yes** | trivial in the reaper |
| delay-process supervision | **no** | stays in PHP (`console.php delay`) |
| every DB write | **no** | stays in PHP |

### The seam

Extends the existing control API (`internal/server.ControlHandler`) rather than inventing
a channel:

```
PUT    /monitor/<id>     register a stream spec: the built ffmpeg command, the source
                         list, and monitor policy (fps_restart, fps_threshold,
                         audio_restart_loss, auto_restart, priority_backup, stop_failures,
                         stream_fail_sleep, on_demand, llod, parent_id)
DELETE /monitor/<id>     stop supervising and kill the process
GET    /monitor/<id>     current state: running, pid, current_source, restarts, uptime,
                         plus derived metadata (codecs, resolution, bitrate, compatible)
POST   /monitor/<id>/source   force a source switch (replaces the .force signal file)
GET    /monitor/events?since=<seq>   drain the event log (see below)
```

Events are drained by PHP and written to the DB there. The daemon *also* appends them to
`LOGS_TMP_PATH/stream_log.log` in the panel's existing format (base64-encoded JSON, one per
line) — `StreamProcess::streamLog` already writes exactly that file and a cron drains it,
so every existing event (`STREAM_START`, `STREAM_RESTART`, `AUTO_RESTART`, `FPS_DROP_THRESHOLD`,
`AUDIO_LOSS`, `PRIORITY_SWITCH`, `FORCE_SOURCE`, `STREAM_FAILED`, `STREAM_START_FAIL`) keeps
flowing with **no PHP change at all**. That file is the reason the daemon needs no DB for
logging.

## Consequences

**Good.** One supervisor process per node instead of one PHP process per stream — on a
500-channel node that is 500 fewer long-lived PHP processes. Health checks become continuous
and free (the bytes are already parsed) instead of periodic ffprobe subprocesses against
disk. Source failover gets faster and gains the "switch back to a higher-priority source"
behaviour for daemon-pulled streams. The `_.monitor`, `_.progress_check`, `_.dur` and
`.force` file-based IPC all disappear.

**Costs and risks.** The daemon gains the right to spawn processes, which is a real change
in its threat surface — it must never build a command string itself, only execute the one
PHP hands it, and the spec endpoint is on the PHP-only control socket.

A daemon crash does NOT take the encoders with it: they are orphaned, not killed, and M5
adopts them back rather than either duplicating them or killing them on sight (which would
turn every daemon upgrade into a node-wide outage). Adoption requires the pid to be alive
AND its command line to carry the panel-supplied `adopt_match`, because pids are recycled.
`service`'s keepalive is still what brings the daemon back, but it is no longer the only
thing standing between a crash and dead channels.

`monitor_pid` in `streams_servers` changes meaning — it becomes the daemon's pid for every
supervised stream, so anything reconciling on it needs updating. That is the one piece of
panel bookkeeping this work does not yet touch.

**Rollback.** The daemon is only asked to supervise a stream when PHP `PUT`s a spec for it.
If PHP stops doing that, `MonitorCommand` runs exactly as it does today. The cutover is
therefore per-stream and reversible without a daemon deploy.

## Phases

- **M1** — supervisor core: `PUT`/`DELETE /monitor/<id>`, spawn/kill/restart with the
  failure-limit and backoff semantics of the current loop, `stream_log.log` emission.
  No health checks yet; the daemon just runs the process PHP used to run.
- **M2** — health: stalled output, audio loss, FPS drop, auto-restart schedule.
- **M3** — sources: priority backup and forced switch; retire the `.force` file.
- **M4** — metadata: codecs, resolution and bitrate from the ring; retire the per-segment
  ffprobe. `GET /monitor/<id>` becomes the source PHP reads for `streams_servers`.
- **M5** — re-adoption of running ffmpeg across a daemon restart; the panel-side health
  policy and supervision reconcile.

Each phase is independently shippable and independently revertible.

## Rollout

Nothing changes until an operator opts in, twice over:

- Per node: the daemon only supervises when `EnableSupervision` has been called.
- Per install: PHP only hands a stream over when the `daemon_supervise` setting is truthy.
  It is absent on every existing install, needs no migration, and clearing it is the
  rollback — `MonitorCommand` then runs exactly as it always did, because its stand-down
  check asks the daemon rather than assuming, and an unreachable daemon answers no.
- Per stream: the daemon supervises only what the panel PUTs a spec for, and forms no
  opinion about output for a spec that carries no `health` block.

## What is deliberately NOT here

- **`buildLive` is not ported.** 211 lines of DB-and-settings-driven string assembly whose
  inputs are all in tables the daemon cannot read. Reimplementing it in Go would buy nothing
  but a large surface for silent behavioural drift.
- **The daemon never composes a command.** It runs the one it is handed. That is the
  security boundary for the whole feature.
- **Delay streams are excluded.** They write their own playlist and are still run the legacy
  way.
- **Metadata is reported as unknown when it cannot be read**, never guessed, so the panel
  keeps a correct value rather than having it overwritten by a blank.
