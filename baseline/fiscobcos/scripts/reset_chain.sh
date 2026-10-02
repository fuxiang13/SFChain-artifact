#!/bin/bash
# Reset the FISCO BCOS chain (wipe block data and restart the chain)
#
# Purpose:
#   - Required after modifying config.genesis (e.g. block_tx_count_limit)
#   - A fresh chain (clean baseline) is needed before storage-efficiency experiments
#
# Usage (from the fiscobcos/ directory):
#   bash scripts/reset_chain.sh            # stop chain -> wipe data -> start chain
#   bash scripts/reset_chain.sh --set-block-limit 200   # also set the per-block tx limit to 200
#                                            (= SFChain block_size; symmetric parameter for experiments A/B)
#
# Note: after a reset the contract address becomes invalid; re-run:
#   docker compose run --rm loadgen -action deploy

set -e
BASE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE"

BLOCK_LIMIT=""
if [ "$1" = "--set-block-limit" ] && [ -n "$2" ]; then
    BLOCK_LIMIT="$2"
fi

echo "[1/4] Stopping nodes..."
docker compose down 2>/dev/null || true

echo "[2/4] Wiping node data directories..."
for ip in 172.25.0.11 172.25.0.12 172.25.0.13 172.25.0.14; do
    rm -rf "nodes/$ip/node0/data"
done
rm -f results/contract_address

if [ -n "$BLOCK_LIMIT" ]; then
    echo "[3/4] Setting block_tx_count_limit=$BLOCK_LIMIT (applied to all 4 nodes in sync)..."
    for ip in 172.25.0.11 172.25.0.12 172.25.0.13 172.25.0.14; do
        sed -i "s/block_tx_count_limit=.*/block_tx_count_limit=$BLOCK_LIMIT/" "nodes/$ip/node0/config.genesis"
    done
    grep -h block_tx_count_limit nodes/*/node0/config.genesis
else
    echo "[3/4] --set-block-limit not given, keeping the current genesis configuration"
    grep -h block_tx_count_limit nodes/172.25.0.11/node0/config.genesis || true
fi

echo "[4/4] Restarting..."
docker compose up -d

echo "Waiting for consensus to become ready..."
sleep 8
for n in 0 1 2 3; do
    if docker logs "fisco-node$n" 2>&1 | tail -20 | grep -q "Report"; then
        echo "  fisco-node$n "
    else
        echo "  fisco-node$n (no Report seen yet; check docker logs fisco-node$n again later)"
    fi
done
echo "Done. Remember to redeploy the contract: docker compose run --rm loadgen -action deploy"
