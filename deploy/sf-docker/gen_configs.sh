#!/bin/bash
# One-time: generate docker-variant node configs from the native ones.
# - node addresses 127.0.0.1:908x -> compose service names
# - MySQL: native localhost -> sf-mysql service
set -e
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OUT="$BASE/deploy/sf-docker/configs"
mkdir -p "$OUT"
for n in management development test operations; do
    src="$BASE/prototype/configs/${n}-node.yaml"
    dst="$OUT/${n}-d.yaml"
    sed -e 's/127\.0\.0\.1:9080/sf-mgmt:9080/g' \
        -e 's/127\.0\.0\.1:9081/sf-dev:9081/g' \
        -e 's/127\.0\.0\.1:9082/sf-test:9082/g' \
        -e 's/127\.0\.0\.1:9083/sf-ops:9083/g' \
        -e 's/host: "localhost"/host: "sf-mysql"/' \
        -e 's/user: "root"/user: "root"/' \
        -e 's/password: "qwer@123"/password: "qwer@123"/' \
        "$src" > "$dst"
    echo "wrote $dst"
done
