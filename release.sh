#!/usr/bin/env bash
#
# Build the xc_fanout daemon for every target arch into ./dist as flat,
# release-ready assets (+ SHA256SUMS). Binaries are NOT committed to the repo —
# they are attached to a GitHub Release (see .github/workflows/release.yml, or
# upload dist/* manually with `gh release create <tag> dist/*`).
#
# Usage:
#   ./release.sh            # version from VERSION
#   ./release.sh 0.7.0      # explicit version (also writes VERSION)
set -euo pipefail
cd "$(dirname "$0")"

VERSION="${1:-$(cat VERSION)}"
[ -n "${1:-}" ] && echo "$VERSION" > VERSION

OUT=dist
rm -rf "$OUT"; mkdir -p "$OUT"

# arch label -> GOARCH[/GOARM]
TARGETS=(amd64 arm64 armv7 386)

echo ">>> xc_fanout $VERSION  ($(go version | awk '{print $3}'))"
echo ">>> go test ./..."
go test ./...

for T in "${TARGETS[@]}"; do
  GOARM=""
  case "$T" in
    amd64) GOARCH=amd64 ;;
    arm64) GOARCH=arm64 ;;
    armv7) GOARCH=arm; GOARM=7 ;;
    386)   GOARCH=386 ;;
  esac
  BIN="$OUT/xc_fanout-linux-$T"
  echo ">>> build linux-$T"
  CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" GOARM="$GOARM" \
    go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o "$BIN" ./cmd/xc_fanout
  # abort if not fully static
  if command -v file >/dev/null && file "$BIN" | grep -qv "statically linked"; then
    echo "ERROR: $BIN is not statically linked"; exit 1
  fi
done

( cd "$OUT" && sha256sum xc_fanout-linux-* > SHA256SUMS )
echo ">>> done — $OUT (tag as $VERSION and attach these as release assets)"
ls -la "$OUT"
