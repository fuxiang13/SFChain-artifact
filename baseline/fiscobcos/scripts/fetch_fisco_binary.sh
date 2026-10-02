#!/bin/bash
# Fetch the official FISCO BCOS v3.17.1 node binary into bin/.
#
# The replication package intentionally does not ship the 53 MB official
# binary; this script downloads the release tarball, verifies its SHA-256
# against the value published on the GitHub release, and extracts the node
# executable. A mirror fallback is tried when github.com is unreachable.
#
# Usage (from anywhere; resolves the package layout relative to this script):
#   bash scripts/fetch_fisco_binary.sh
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"        # baseline/fiscobcos
BIN="$BASE/bin"
mkdir -p "$BIN"

VERSION="v3.17.1"
TARBALL="fisco-bcos-linux-x86_64.tar.gz"
TARBALL_SHA256="28eb054bb222b805e116bf7d2462709f99d1f460a732e978555abe61973b3cbb"
# checksum of the extracted executable (informational; the tarball is what we verify)
BINARY_SHA256="e67cd8f869264cbf78e7b2155a420dd7b71ec747bdb9b1fbf7d464484a070541"

URLS=(
  "https://github.com/FISCO-BCOS/FISCO-BCOS/releases/download/$VERSION/$TARBALL"
)

if [ -f "$BIN/fisco-bcos" ]; then
    echo "bin/fisco-bcos already exists, skipping download ($(du -h "$BIN/fisco-bcos" | cut -f1))"
    exit 0
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ok=0
for url in "${URLS[@]}"; do
  echo "Downloading: $url"
  if curl -fSL --retry 2 --connect-timeout 20 -o "$TMP/$TARBALL" "$url"; then ok=1; break; fi
  echo "  Failed, trying the next source..."
done
[ "$ok" = "1" ] || { echo "All download sources failed; please download $TARBALL manually, place it in $BIN/, and retry" >&2; exit 1; }

echo "Verifying sha256..."
echo "$TARBALL_SHA256  $TMP/$TARBALL" | sha256sum -c - || {
  echo "sha256 verification failed (expected $TARBALL_SHA256)" >&2; exit 1; }

echo "Extracting..."
tar xzf "$TMP/$TARBALL" -C "$BIN" fisco-bcos
chmod +x "$BIN/fisco-bcos"

echo "Done: $BIN/fisco-bcos"
sha256sum "$BIN/fisco-bcos"   # should be $BINARY_SHA256
echo "FISCO BCOS $VERSION node binary ready."
