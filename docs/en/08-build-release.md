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
go test ./...            # unit tests of all packages
go vet ./...             # static analysis
go run ./cmd/xc_fanout -h   # list of flags
```

Every internal package has tests next to the code (`*_test.go`): `hub`, `tsjoin`,
`hlsseg`, `hlscrypt`, `ingest`, `puller`, `server` (including a dedicated test for resetting
a stalled viewer, `serve_live_stall_test.go`).

## Cutting a release

```bash
./release.sh 0.7.0                       # go test + build dist/xc_fanout-linux-* + SHA256SUMS
git commit -am "release 0.7.0"
git tag 0.7.0 && git push --tags          # tag WITHOUT the "v" prefix — project convention
```

What `release.sh` does:

1. runs `go test ./...`;
2. builds static binaries for the target architectures into `dist/xc_fanout-linux-*`;
3. computes the checksums `dist/SHA256SUMS`.

The version is stamped into the binary at build time via `-ldflags "-X main.version=…"`, so
`xc_fanout -version` prints exactly the release version (the panel uses it to decide whether it
needs to update).

## What happens after pushing a tag

Pushing a tag of the form `[0-9]*` (for example `0.7.0`) triggers the workflow
[`.github/workflows/release.yml`](../../.github/workflows/release.yml):

1. checkout, install Go 1.21;
2. `./release.sh "${GITHUB_REF_NAME}"` — rebuild of binaries and checksums;
3. publish the assets (`dist/xc_fanout-linux-*` and `dist/SHA256SUMS`) to a **GitHub Release**
   via `softprops/action-gh-release`.

Manual alternative:

```bash
gh release create 0.7.0 dist/*
```

## Project conventions

- **Version tags — without the `v` prefix** (`0.7.0`, not `v0.7.0`). The workflow listens on the
  `[0-9]*` pattern, and `release.sh` prints its messages to match this same format.
- **Binaries are not committed** to the tree — `dist/` is in `.gitignore`; distribution happens only
  through Release assets.
- **The source of truth is the version in the binary.** The panel reads it via `-version` and compares
  it against the latest release tag.
