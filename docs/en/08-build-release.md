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
