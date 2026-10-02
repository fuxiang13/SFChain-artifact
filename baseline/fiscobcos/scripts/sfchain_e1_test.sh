#!/bin/bash
# SFChain E1 single test: cold start 4 nodes (no endorsement), wait drain, stop, save logs
# Usage: bash sfchain_e1_test.sh <block_size>
set -u
BASE="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$BASE"
BIN=$BASE/bin
CFG=$BASE/prototype/configs
LOGDIR=$BASE/logs
MYSQL="mysql -uroot -pqwer@123 -N -s"
mkdir -p "$LOGDIR"

BS="${1:?need block_size}"
TAG="${2:-e1_bs${BS}}"
if [[ ! "$TAG" =~ ^[A-Za-z0-9_-]+$ ]]; then echo "Invalid run tag"; exit 1; fi
if [ -e "$LOGDIR/management_${TAG}.log" ]; then echo "Run log already exists: $TAG"; exit 1; fi

echo "=== SFChain E1 bs=$BS ==="

# 0. stop old procs
bash "$BASE/prototype/scripts/wsl/stop_all.sh" >/dev/null 2>&1 || true
for f in "$LOGDIR"/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null && rm -f "$f"; done
pkill -f 'bin/sfchain -config' 2>/dev/null || true
pkill -f 'bin/endorsement_service' 2>/dev/null || true
sleep 2

# 1. set block_size
sed -i "s/^  block_size: .*/  block_size: $BS/" $CFG/management-node.yaml
sed -i "s/^  tps_priority: .*/  tps_priority: false/" $CFG/management-node.yaml
echo "block_size: $(sed -n '38p' $CFG/management-node.yaml)"
echo "tps_priority: $(sed -n '39p' $CFG/management-node.yaml)"

# 2. clear block tables (keep transactions + software_factory_logs)
for t in man_blocks_management man_blocks_development man_blocks_test man_blocks_operations \
         man_headers_management man_headers_development man_headers_test man_headers_operations \
         dev_blocks dev_headers_management dev_headers_development dev_headers_test dev_headers_operations \
         test_blocks test_headers_management test_headers_development test_headers_test test_headers_operations \
         ops_blocks ops_headers_management ops_headers_development ops_headers_test ops_headers_operations \
         man_blockchain_info dev_blockchain_info test_blockchain_info ops_blockchain_info \
         processed_transactions; do
    $MYSQL -e "TRUNCATE sfchain.$t" 2>/dev/null || true
done
echo "block tables cleared"

# 3. reset tx status to endorsed (reusable)
$MYSQL -e "UPDATE sfchain.transactions SET status='endorsed' WHERE status!='endorsed';" 2>/dev/null
endorsed=$($MYSQL -e "SELECT COUNT(*) FROM sfchain.transactions WHERE status='endorsed';" 2>/dev/null)
echo "endorsed=$endorsed (pre-endorsed workload ready)"

# 4. cold start 4 nodes (SFCHAIN_PACK_TICK (default 100ms), no endorsement_service)
MGMT_ENV="SFCHAIN_PACK_TICK=${SFCHAIN_PACK_TICK:-100ms} SFCHAIN_NET=http"
# DTO nodes first, management LAST (pool pre-loaded: mgmt generates blocks on boot)
nohup env SFCHAIN_NET=http "$BIN/sfchain" -config $CFG/development-node.yaml > "$LOGDIR/development_${TAG}.log" 2>&1 &
echo $! > "$LOGDIR/development.pid"
nohup env SFCHAIN_NET=http "$BIN/sfchain" -config $CFG/test-node.yaml > "$LOGDIR/test-node_${TAG}.log" 2>&1 &
echo $! > "$LOGDIR/test-node.pid"
nohup env SFCHAIN_NET=http "$BIN/sfchain" -config $CFG/operations-node.yaml > "$LOGDIR/operations_${TAG}.log" 2>&1 &
echo $! > "$LOGDIR/operations.pid"
sleep 3
nohup env $MGMT_ENV "$BIN/sfchain" -config $CFG/management-node.yaml > "$LOGDIR/management_${TAG}.log" 2>&1 &
echo $! > "$LOGDIR/management.pid"

# wait ports
ok=0
for i in $(seq 1 30); do
    ok=1
    for port in 9080 9081 9082 9083; do
        (echo > /dev/tcp/127.0.0.1/$port) 2>/dev/null || ok=0
    done
    [ "$ok" = "1" ] && break
    sleep 1
done
echo "ports ready: $ok (i=$i)"

# 5. wait drain (endorsed -> 0 + blocks stable)
prev=-1
stable=0
last_endor=""
last_blocks=""
MLOG="$LOGDIR/management_${TAG}.log"
for i in $(seq 1 120); do
    endor=$($MYSQL -e "SELECT COUNT(*) FROM sfchain.transactions WHERE status='endorsed';" 2>/dev/null)
    total_blocks=$($MYSQL -e "SELECT (SELECT COUNT(*) FROM sfchain.man_blocks_management)+(SELECT COUNT(*) FROM sfchain.man_blocks_development)+(SELECT COUNT(*) FROM sfchain.man_blocks_test)+(SELECT COUNT(*) FROM sfchain.man_blocks_operations)" 2>/dev/null)
    seal_n=$(grep -c "block created successfully, txType" "$MLOG" 2>/dev/null || echo 0)
    cons_n=$(grep -c "consensus completed, height=" "$MLOG" 2>/dev/null || echo 0)
    last_endor=$endor
    last_blocks=$total_blocks
    if [ "$total_blocks" = "$prev" ]; then
        stable=$((stable+1))
        # Completion requires consensus to catch up (cons==seal); if blocks are stable but consensus has not caught up, wait at most 8 more rounds (40s) as a fallback
        if [ "$seal_n" -gt 0 ] && [ "$cons_n" -ge "$seal_n" ] && [ "$stable" -ge 2 ]; then
            echo "[drained] blocks=$total_blocks endorsed=$endor seals=$seal_n cons=$cons_n (i=$i)"; break
        fi
        [ "$stable" -ge 10 ] && { echo "[drained-timeout] blocks=$total_blocks endorsed=$endor seals=$seal_n cons=$cons_n (i=$i)"; break; }
    else
        stable=0
    fi
    prev=$total_blocks
    sleep 5
done
echo "final: endorsed=$last_endor, blocks=$last_blocks (i=$i)"

# 6. stop
sleep 3
bash "$BASE/prototype/scripts/wsl/stop_all.sh" >/dev/null 2>&1 || true
for f in "$LOGDIR"/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null && rm -f "$f"; done
pkill -f 'bin/sfchain -config' 2>/dev/null || true
sleep 2
echo "=== bs=$BS done ==="
