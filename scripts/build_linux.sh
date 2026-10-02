#!/usr/bin/env bash
# Linux/WSL + Go >=1.26 + C/C++ toolchain. Native Windows BLS/CGO is not tested.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/prototype"
export CGO_ENABLED=1
mkdir -p "$ROOT/bin"
go test -mod=readonly ./...
for name in sfchain endorsement_service loadgen auditverify rq3proxy user_service generate_logs genkeys seedusers preparepool; do
  go build -mod=readonly -trimpath -buildvcs=false -o "$ROOT/bin/$name" "./cmd/$name"
done
cd "$ROOT/bin"
find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
