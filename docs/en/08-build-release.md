# 08. Build, test and release

> How to build the daemon, run the tests and cut a new version. For how the finished binary
> reaches the panel servers, see [07. Integration](07-integration.md#installing-and-updating-the-binary).

## Requirements

- **Go ≥ 1.21** (module `github.com/Vateron-Media/XC_VM_Fanout`, see `go.mod`).
- There are no external C dependencies: the build runs with `CGO_ENABLED=0` → a **static binary**
  that works on any Linux distribution.
- To run (not build) on non-mp2t sources you need `ffmpeg` on the target machine.

## Development

```bash
go run ./cmd/xc_fanout -h   # list of flags
go vet ./...                # static analysis
```

## Testing

Tests live next to the code (`*_test.go`) in every internal package. The common commands:

```bash
go test ./...                       # all unit tests
go test -race ./...                 # with the race detector (do this before a release)
go test -cover ./...                # per-package statement coverage
go test -v ./internal/server -run TestOverlay   # one package / one test, verbose
```

The suite is fast (seconds) and needs no network, no sockets set up by hand, and no
running daemon — each test wires up exactly what it needs in-process.

### What each package tests

| Package | What its tests cover |
|---------|----------------------|
| `hub` | Fan-out correctness (every subscriber gets the identical byte stream), slow-subscriber drop, snapshot/unsubscribe. |
| `tsjoin` | Clean-join parsing (PAT/PMT + keyframe), the prebuffer ring rewind, and the HLS-from-ring view (segment cutting on keyframes, the display window, playlist/segment assembly). |
| `config` | The self-healing JSON contract: self-create when absent, backfill missing keys, keep defaults on malformed, atomic write, clamp ranges. |
| `hlscrypt` | AES-128-CBC segment encryption round-trips. |
| `ingest` | 188-byte packet alignment across read boundaries. |
| `puller` | Source classification (direct mp2t vs ffmpeg remux), cold-start ffmpeg args, reconnect, and ffmpeg-failure surfacing (see below). |
| `server` | The HTTP surfaces end to end: control API (register / ingest / signal / probe), live-TS and HLS serving, connection & rate tracking, the idle-stop reaper, the debug-stats snapshot, and a dedicated stalled-viewer reset (`serve_live_stall_test.go`). |
| `dlog` | The debug log toggles on/off and stays silent when disabled. |

Synthetic MPEG-TS inputs come from the `internal/tsfixture` helper (hand-built PAT/PMT/keyframe
packets), so most tests are pure and deterministic with no external tools.

### How the ffmpeg paths are tested

ffmpeg is exercised two ways, on purpose:

- **Fake ffmpeg** — a tiny shell stand-in written into a temp dir and passed as the ffmpeg
  binary. It makes the branch deterministic: the remux happy path, a forced non-zero exit
  (asserting the failure is surfaced, not swallowed as a clean stream end), a cancel that must
  *not* be reported as a fault, and the overlay's graceful-fallback / disabled / start-failure
  branches.
- **Real system ffmpeg** — the overlay drawtext re-encode is also run end to end against the
  machine's actual `ffmpeg`: the test synthesizes a real H.264 MPEG-TS with `lavfi` and burns a
  real banner using a system TTF font, then checks the result is a valid, changed stream. It
  **auto-skips** (with a log line) when `ffmpeg` or a known font is absent, and **logs which
  binary and font it used** when present — so a test run always makes clear whether the real
  encode path was exercised or skipped, rather than silently passing.

Debug logging is off during tests; a test that wants to assert on log output enables it and
captures the log writer (see `internal/dlog/*_test.go` for the pattern).

### End-to-end bench

Beyond the unit tests, [`test/`](../../test/) is a self-contained **end-to-end bench** (a separate
Go module, so it never affects `go test ./...` at the root): a fake live origin server plus the real
daemon wired together under Docker Compose, covering every operational mode — direct pull, HLS remux,
failover, off-air, push/ingest, encrypted HLS, grace reaping, rate telemetry, stalled-viewer
eviction and teardown. See [`test/README.md`](../../test/README.md) for how to run it.

## Cutting a release

The version number lives in one place — the [`VERSION`](../../VERSION) file at the repo root.
Everything else reads it from there, so a release only touches that file.

```bash
# 1. Bump the version number — the single source of truth
echo 0.9.1 > VERSION                        # or edit VERSION by hand
VERSION="$(cat VERSION)"

# 2. Build the artifacts. With no argument release.sh reads VERSION;
#    with an argument it writes that into VERSION and uses it.
./release.sh                                # == ./release.sh "$VERSION"

# 3. Commit and tag with the same version
git commit -am "release $VERSION"
git tag "$VERSION" && git push --tags        # tag WITHOUT the "v" prefix — project convention
```

What `release.sh` does:

1. takes the version from its argument or from `VERSION` (`VERSION="${1:-$(cat VERSION)}"`);
2. runs `go test ./...`;
3. builds static binaries for the target architectures into `dist/xc_fanout-linux-*`;
4. computes the checksums `dist/SHA256SUMS`.

The version is stamped into the binary at build time via `-ldflags "-X main.version=…"`, so
`xc_fanout -version` prints exactly the version from `VERSION` (the panel uses it to decide whether
it needs to update).

## What happens after pushing a tag

Pushing a tag of the form `[0-9]*` (the value from `VERSION`) triggers the workflow
[`.github/workflows/release.yml`](../../.github/workflows/release.yml):

1. checkout of the full history with tags (`fetch-depth: 0`), install Go 1.21;
2. `./release.sh "${GITHUB_REF_NAME}"` — rebuild of binaries and checksums;
3. changelog generation — the commits between the previous and current tag are collected into the release body;
4. publish the assets (`dist/xc_fanout-linux-*` and `dist/SHA256SUMS`) to a **GitHub Release**
   via `softprops/action-gh-release`, with that changelog as the description.

### Changelog

Each release's description is assembled automatically from the git history — the list of commits
between the previous and current tag (`--match '[0-9]*'` skips non-release tags):

```bash
PREV=$(git describe --tags --abbrev=0 --match '[0-9]*' "$VERSION^")
git log --no-merges --pretty='- %s (%h)' "$PREV..$VERSION"
```

So releases stay "alive" without a hand-kept CHANGELOG: meaningful commit subjects
(`feat:`, `fix:`, `docs:` …) become the changelog on the GitHub Release page directly.

Manual alternative (GitHub generates the notes itself):

```bash
gh release create "$VERSION" dist/* --generate-notes
```

## Project conventions

- **Version tags — without the `v` prefix** (`0.7.0`, not `v0.7.0`). The workflow listens on the
  `[0-9]*` pattern, and `release.sh` prints its messages to match this same format.
- **Binaries are not committed** to the tree — `dist/` is in `.gitignore`; distribution happens only
  through Release assets.
- **The source of truth is the version in the binary.** The panel reads it via `-version` and compares
  it against the latest release tag.
