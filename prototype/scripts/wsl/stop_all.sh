#!/bin/bash
# SFChain one-click stop script (WSL2 Ubuntu environment)
BASE="$(cd "$(dirname "$0")/../../../.." && pwd)"
LOGDIR=$BASE/logs

for name in management development test-node operations endorsement; do
    if [ -f "$LOGDIR/$name.pid" ]; then
        pid=$(cat "$LOGDIR/$name.pid")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && echo "Stopped $name (pid=$pid)"
        else
            echo "$name (pid=$pid) is no longer running"
        fi
        rm -f "$LOGDIR/$name.pid"
    fi
done
echo "All nodes stopped (MySQL keeps running)"
