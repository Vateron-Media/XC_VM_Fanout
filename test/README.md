# xc_fanout — end-to-end test bench

A self-contained bench that exercises the [`xc_fanout`](../cmd/xc_fanout) daemon against a
realistic live source, covering every mode the daemon branches on: source acquisition
(direct MPEG-TS, HLS-remux, failover, off-air, push, launch), delivery (live TS, plain HLS,
encrypted HLS) and lifecycle (grace reaping, viewer reconciliation, rate telemetry, stalled-viewer
eviction, teardown).

Nothing here touches the production module — `test/` is a **separate Go module**, so
`go build ./...` / `go test ./...` at the repo root are unaffected.

## Layout

| Path | Role |
|------|------|
| [`origin/`](origin/) | Fake live source over **TCP** — serves the daemon's *upstream*. Streams a synthetic looping sample as `video/mp2t` and as HLS, plus fault endpoints (`/ts/dead`, `/ts/stall`) and a `/stats` fan-out counter. |
| [`integration/`](integration/) | Go integration suite (build tag `integration`). Spawns one daemon per scenario, talks to the client/control surfaces over **unix sockets**, pulls the source over TCP. |
| [`fanout/Dockerfile`](fanout/Dockerfile) | Builds the daemon and runs the suite. |
| [`docker-compose.yml`](docker-compose.yml) | Wires `origin` + `fanout` together. |

Why two transports: the daemon serves viewers/PHP on **unix sockets** (`-sock`, `-ctl`) but pulls
its source over **HTTP/TCP**. The bench mirrors that exactly — `origin` is TCP, the suite dials the
daemon's sockets.

## Run everything (Docker)

```bash
cd test
docker compose up --build --abort-on-container-exit --exit-code-from fanout
```

`origin` comes up first (health-gated); `fanout` builds the daemon, runs the suite, and the
compose exits with the suite's status. Rebuild the daemon by re-running with `--build`.

## Run locally (no Docker)

Requires Go ≥ 1.21 and ffmpeg (with `libx264` + `aac`).

```bash
# 0. build the daemon (from the repo root)
CGO_ENABLED=0 go build -o /tmp/xc_fanout ./cmd/xc_fanout

# 1. generate the sample the origin serves
ffmpeg -y -f lavfi -i testsrc2=size=640x360:rate=25 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset veryfast -tune zerolatency -g 25 -keyint_min 25 -pix_fmt yuv420p \
  -c:a aac -b:a 64k -t 20 -f mpegts /tmp/sample.ts

# 2. start the origin (any free port)
go run ./test/origin -addr 127.0.0.1:18080 -sample /tmp/sample.ts -hlsdir /tmp/hls -channels ch1 &

# 3. run the suite against it
cd test
XC_FANOUT_BIN=/tmp/xc_fanout ORIGIN=http://127.0.0.1:18080 FFMPEG=ffmpeg \
  go test -tags integration -v -count=1 ./integration/
```

## Environment knobs (read by the suite)

| Var | Default | Meaning |
|-----|---------|---------|
| `XC_FANOUT_BIN` | `xc_fanout` (PATH) | Daemon binary the tests spawn. |
| `ORIGIN` | `http://localhost:8080` | Base URL of the fake source. |
| `FFMPEG` | `ffmpeg` (PATH) | ffmpeg used by the daemon's remux path and by push/launch fixtures. |

## Origin endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /ts/<ch>` | Endless `video/mp2t` — the **direct-pull** path (no ffmpeg on the daemon). |
| `GET /hls/<ch>/index.m3u8`, `…/<seg>.ts` | HLS source — forces the daemon's **ffmpeg remux** path. |
| `GET /ts/dead` | Always `404` — a dead candidate for **failover** tests. |
| `GET /ts/stall` | `200 video/mp2t` but never sends a byte — the **off-air** signal. |
| `GET /stats` | `{"ts_active","ts_total"}` — proves N viewers stay **one** upstream pull. |
| `GET /healthz` | Health gate for compose. |

## Coverage

| Test | What it proves |
|------|----------------|
| `TestPullDirectTS_FanoutSingleUpstream` | Direct TS streams through; 2 viewers → 1 upstream pull. |
| `TestPullHLSSource_Remux` | HLS source is remuxed to TS by ffmpeg before reaching a viewer. |
| `TestFailover` | A dead first URL falls through to the next candidate. |
| `TestOffAir` | A connected-but-silent source reads `running` + `has_data=false`. |
| `TestDaemonHLS_Plaintext` | In-memory HLS yields a playlist + a valid TS segment. |
| `TestDaemonHLS_Encrypted` | With key/iv, segments come back AES-128-CBC and decrypt to TS. |
| `TestPrebuffer` | A joining viewer receives the retained history burst. |
| `TestConnectionsAndRates` | `?c=<uuid>` shows in `/connections` and `/rates`. |
| `TestGraceReaper` | The puller idle-stops after the last viewer + grace. |
| `TestDeleteTeardown` | `DELETE` makes status/live `404`. |
| `TestStalledViewerEviction` | A non-draining viewer is dropped after `-write-timeout`. |
| `TestIngestPush` | Push mode: a producer feeds the ingest socket, a viewer receives it. |
| `TestLaunchModeSource` / `TestLaunchModeFile` | `-id/-source` and `-id/-in` launch feeds without the control API. |
