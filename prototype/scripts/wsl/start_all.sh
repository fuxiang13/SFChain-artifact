#!/bin/bash
# SFChain one-click startup script (WSL2 Ubuntu environment)
# Usage: wsl -d <distro> -- bash <package-root>/scripts/wsl/start_all.sh

BASE="$(cd "$(dirname "$0")/../../../.." && pwd)"
BIN=$BASE/bin
LOGDIR=$BASE/logs
mkdir -p "$LOGDIR"

# ---------- 1. Ensure MySQL is running ----------
if ! mysqladmin -uroot -pqwer@123 status >/dev/null 2>&1; then
    echo "[MySQL not running, starting it...]"
    service mysql start >/dev/null 2>&1
    sleep 3
    if mysqladmin -uroot -pqwer@123 status >/dev/null 2>&1; then
        echo "[MySQL started]"
    else
        echo "[MySQL failed to start; check manually: service mysql start]"
        exit 1
    fi
else
    echo "[MySQL running]"
fi

# ---------- 2. Start the 4 nodes ----------
start_node() {
    local name=$1 config=$2
    if [ -f "$LOGDIR/$name.pid" ] && kill -0 "$(cat "$LOGDIR/$name.pid")" 2>/dev/null; then
        echo "[$name already running pid=$(cat "$LOGDIR/$name.pid")]"
        return
    fi
    nohup "$BIN/sfchain" -config "$BASE/prototype/configs/$config" > "$LOGDIR/$name.log" 2>&1 &
    echo $! > "$LOGDIR/$name.pid"
    echo "[$name started pid=$(cat "$LOGDIR/$name.pid") log=logs/$name.log]"
}

start_node management  management-node.yaml
start_node development development-node.yaml
start_node test-node   test-node.yaml
start_node operations  operations-node.yaml

# ---------- 3. Start the endorsement service ----------
if [ -f "$LOGDIR/endorsement.pid" ] && kill -0 "$(cat "$LOGDIR/endorsement.pid")" 2>/dev/null; then
    echo "[Endorsement service already running pid=$(cat "$LOGDIR/endorsement.pid")]"
else
    nohup "$BIN/endorsement_service" -interval 5s > "$LOGDIR/endorsement.log" 2>&1 &
    echo $! > "$LOGDIR/endorsement.pid"
    echo "[Endorsement service started pid=$(cat "$LOGDIR/endorsement.pid") interval=5s log=logs/endorsement.log]"
fi

# ---------- 4. Wait for ports to become ready ----------
echo ""
echo "Waiting for node HTTP ports to become ready..."
for i in $(seq 1 15); do
    ok=1
    for port in 9080 9081 9082 9083; do
        curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/" || ok=0
    done
    [ "$ok" = "1" ] && break
    sleep 1
done

echo "=== Port status ==="
for port in 9080 9081 9082 9083; do
    if curl -s -o /dev/null --max-time 1 "http://127.0.0.1:$port/"; then
        echo "Port $port: listening "
    else
        echo "Port $port: not listening (see the node's log under logs/)"
    fi
done
echo ""
echo "All started. To stop: scripts/wsl/stop_all.sh"
