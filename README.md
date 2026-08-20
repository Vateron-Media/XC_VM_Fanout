# XC_VM_Fanout

Native **live-stream fan-out daemon** for XC_VM — one process pulls each live
source once and fans it out to many viewers via nginx, keeping PHP out of the
byte path, and produces HLS (incl. AES-128 encrypted) entirely in memory. See
`XC_VM/docs/xc_fanout.md` and `docs/adr/0002`/`0003` in the panel repo for the
design and the phased cutover.

Pure Go, `CGO_ENABLED=0` → one **static** binary per architecture runs on any
Linux distro.

## Documentation

Full docs live in [`docs/`](docs/README.md), split by topic.

## This repo ships binaries as releases

The **source** lives here; the compiled binaries are **GitHub Release assets**,
never committed to the tree (`dist/` is gitignored). The panel installs and
updates the daemon from the latest release:

```
console.php fanout_binary          # install/update to the latest release
console.php fanout_binary force     # reinstall the same version
```

It reads the installed version straight from the binary (`xc_fanout -version`),
compares it to the latest release tag, downloads the arch-matched asset, verifies
its SHA-256, installs it atomically and lets the `service` keepalive respawn the
daemon.

## Cut a release

The version number lives in the `VERSION` file — bump it there, then everything reads it back:

```bash
echo 0.9.1 > VERSION && VERSION="$(cat VERSION)"   # single source of truth
./release.sh              # no arg → version from VERSION; runs go test, builds dist/* + SHA256SUMS
git commit -am "release $VERSION" && git tag "$VERSION" && git push --tags
```

Pushing the version tag (no `v` prefix) triggers `.github/workflows/release.yml`, which rebuilds,
generates a changelog from the commits since the previous tag, and attaches `dist/*` to the GitHub
Release. (Or do it manually: `gh release create "$VERSION" dist/* --generate-notes`.)

## Develop

```bash
go test ./...
go vet ./...
go run ./cmd/xc_fanout -h
```

Requires Go >= 1.21.
