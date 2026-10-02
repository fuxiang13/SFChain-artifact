#!/bin/bash
# Generate the FISCO BCOS four-node configuration and certificates (runs
# build_chain.sh inside a Docker container, so the Windows host needs no
# WSL/Linux environment)
#
# Usage (run from the fiscobcos/ directory; Git Bash / WSL both work):
#   bash scripts/generate_nodes.sh
#
# Output: nodes/172.25.0.{11,12,13,14}/node0/{config.ini,config.genesis,...}
#         nodes/172.25.0.11/sdk/{ca.crt,sdk.crt,sdk.key}   <- load-test tool certs

set -e

# fiscobcos/ directory (this script lives under scripts/)
BASE="$(cd "$(dirname "$0")/.." && pwd)"
# Under Git Bash, convert to a Windows-style path for docker -v; keep as-is under WSL/Linux
DIR="$(cd "$BASE" && (pwd -W 2>/dev/null || pwd))"

GEN_IMAGE="fisco-chain-gen:latest"
NODE_IPS="172.25.0.11:1,172.25.0.12:1,172.25.0.13:1,172.25.0.14:1"

cd "$BASE"

# ---------- 0. Ensure the official node binary exists (the 53 MB binary is not shipped in the package; download and verify on demand) ----------
if [ ! -f "$BASE/bin/fisco-bcos" ]; then
    echo "[0/3] bin/fisco-bcos not found, downloading the official binary automatically (scripts/fetch_fisco_binary.sh)..."
    bash "$BASE/scripts/fetch_fisco_binary.sh"
fi

# ---------- 1. Build the config-generation image ----------
if ! docker image inspect "$GEN_IMAGE" >/dev/null 2>&1; then
    echo "[1/3] Building config-generation image $GEN_IMAGE ..."
    docker build -f gen.Dockerfile -t "$GEN_IMAGE" .
else
    echo "[1/3] Config-generation image already exists"
fi

# ---------- 2. Run build_chain.sh ----------
if [ -d "$BASE/nodes" ]; then
    echo "nodes/ directory already exists, skipping generation (delete nodes/ first if you need to regenerate)"
else
    echo "[2/3] Generating 4-node configuration (IPs: $NODE_IPS)..."
    # MSYS_NO_PATHCONV=1 stops Git Bash from rewriting container paths like /w into Windows paths
    MSYS_NO_PATHCONV=1 docker run --rm \
        -v "$DIR":/w -w /w \
        -v fisco-tassl-cache:/root/.fisco \
        --entrypoint bash "$GEN_IMAGE" \
        bin/build_chain.sh \
        -l "$NODE_IPS" \
        -o nodes \
        -e bin/fisco-bcos
fi

# ---------- 3. Verify the outputs ----------
echo "[3/3] Verifying generated files ..."
fail=0
for ip in 172.25.0.11 172.25.0.12 172.25.0.13 172.25.0.14; do
    for f in "$BASE/nodes/$ip/node0/config.ini" "$BASE/nodes/$ip/node0/config.genesis"; do
        if [ ! -f "$f" ]; then
            echo "  Missing $f"
            fail=1
        fi
    done
done
for f in ca.crt sdk.crt sdk.key; do
    if [ ! -f "$BASE/nodes/172.25.0.11/sdk/$f" ]; then
        echo "  Missing nodes/172.25.0.11/sdk/$f"
        fail=1
    fi
done
[ "$fail" = "0" ] && echo "  Node configuration and SDK certificates all present" || { echo "Verification failed"; exit 1; }

cat <<'EOF'

Node configuration generated. Next steps:

  1. Build the node image:   docker build -f node.Dockerfile -t fisco-bcos-node:v3.17.1 .
  2. Start the four nodes:   docker compose up -d
  3. Check consensus status: docker logs fisco-node0 --tail 20   (Report=== lines mean block production is normal)
  4. Load test:              see the "First-time setup" section of README.md
EOF
