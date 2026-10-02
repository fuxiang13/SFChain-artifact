#!/bin/bash
# RQ3 fault-injection experiment runner
# Usage: bash rq3_run.sh <scenario> <round>
#   base|d100|d500|d1000|dual500|bs100|bs400|bs500|disc|refuse
set -u
SC="${1:?scenario}"; R="${2:?round}"
BASE="$(cd "$(dirname "$0")/../../.." && pwd)"; cd "$BASE"
BIN=$BASE/bin; LOGDIR=$BASE/logs
mkdir -p "$LOGDIR"
cd "$BASE/prototype"
MYSQL="mysql -uroot -pqwer@123 -N -s"
MGMT_ENV="SFCHAIN_PACK_TICK=100ms SFCHAIN_NET=http"

stop_all() {
  pkill -f 'bin/sfchain -config' 2>/dev/null
  pkill -f bin/rq3proxy 2>/dev/null
  sleep 2
}

freshdb() {
  for t in man_blocks_management man_blocks_development man_blocks_test man_blocks_operations \
           man_headers_management man_headers_development man_headers_test man_headers_operations \
           dev_blocks dev_headers_management dev_headers_development dev_headers_test dev_headers_operations \
           test_blocks test_headers_management test_headers_development test_headers_test test_headers_operations \
           ops_blocks ops_headers_management ops_headers_development ops_headers_test ops_headers_operations \
           man_blockchain_info dev_blockchain_info test_blockchain_info ops_blockchain_info; do
    $MYSQL -e "TRUNCATE sfchain.$t" 2>/dev/null
  done
  $MYSQL -e "UPDATE sfchain.transactions SET status='endorsed'" 2>/dev/null
  $MYSQL -e "TRUNCATE sfchain.processed_transactions" 2>/dev/null
}

mk_config() {  # $1 = "dev" | "test" | "dev test"
  cp configs/management-node.yaml configs/management-node-rq3.yaml
  case "$1" in *dev*)
    sed -i '/- id: "development-node-1"/,/address:/ s/\(address: "127\.0\.0\.1:\)9081"/\19091"/' configs/management-node-rq3.yaml ;;
  esac
  case "$1" in *test*)
    sed -i '/- id: "test-node-1"/,/address:/ s/\(address: "127\.0\.0\.1:\)9082"/\19092"/' configs/management-node-rq3.yaml ;;
  esac
}

case "$SC" in
  base)    BS=200; ROUTE="";          PMODE="delay"; PARG=0 ;;
  d100)    BS=200; ROUTE="dev";       PMODE="delay"; PARG=100 ;;
  d500)    BS=200; ROUTE="dev";       PMODE="delay"; PARG=500 ;;
  d1000)   BS=200; ROUTE="dev";       PMODE="delay"; PARG=1000 ;;
  dual500) BS=200; ROUTE="dev test";  PMODE="delay"; PARG=500 ;;
  bs100)   BS=100; ROUTE="dev";       PMODE="delay"; PARG=500 ;;
  bs400)   BS=400; ROUTE="dev";       PMODE="delay"; PARG=500 ;;
  bs500)   BS=500; ROUTE="dev";       PMODE="delay"; PARG=500 ;;
  disc)    BS=200; ROUTE="dev";       PMODE="blackhole"; PARG=0 ;;
  refuse)  BS=200; ROUTE="";          PMODE="delay"; PARG=0 ;;
  *) echo "unknown scenario $SC"; exit 1 ;;
esac

stop_all
freshdb
mk_config "$ROUTE"
sed -i "s/^  block_size: .*/  block_size: $BS/" configs/management-node-rq3.yaml

[ -n "$ROUTE" ] && nohup "$BIN/rq3proxy" 9091 9081 $PMODE $PARG > /tmp/rq3_9091_r$R.log 2>&1 &
case "$ROUTE" in *test*) nohup "$BIN/rq3proxy" 9092 9082 $PMODE $PARG > /tmp/rq3_9092_r$R.log 2>&1 & ;; esac
sleep 1

TAG="rq3_${SC}_r${R}"
REFUSE_ENV=""
[ "$SC" = refuse ] && REFUSE_ENV="SFCHAIN_REFUSE=1"
nohup env SFCHAIN_NET=http $REFUSE_ENV "$BIN/sfchain" -config configs/development-node.yaml > "$LOGDIR/development_${TAG}.log" 2>&1 &
nohup env SFCHAIN_NET=http "$BIN/sfchain" -config configs/test-node.yaml > "$LOGDIR/test-node_${TAG}.log" 2>&1 &
nohup env SFCHAIN_NET=http "$BIN/sfchain" -config configs/operations-node.yaml > "$LOGDIR/operations_${TAG}.log" 2>&1 &
sleep 3
nohup env $MGMT_ENV "$BIN/sfchain" -config configs/management-node-rq3.yaml > "$LOGDIR/management_${TAG}.log" 2>&1 &

T0=$(date +%s.%N)
if [ "$SC" = disc ]; then
  sleep 10
  echo resume > /tmp/rq3_proxy_9091.cmd
  echo "resume at $(date +%s.%N)"
fi

if [ "$SC" != refuse ]; then
  MLOG="$LOGDIR/management_${TAG}.log"
  for i in $(seq 1 150); do
    endor=$($MYSQL -e "SELECT COUNT(*) FROM sfchain.transactions WHERE status='endorsed'" 2>/dev/null); endor=${endor:-9999}
    seal_n=$(grep -c "block created successfully, txType" "$MLOG" 2>/dev/null); seal_n=${seal_n:-0}
    cons_n=$(grep -c "consensus completed, height=" "$MLOG" 2>/dev/null); cons_n=${cons_n:-0}
    if [ "$seal_n" -gt 0 ] && [ "$cons_n" -ge "$seal_n" ] && [ "$endor" = "0" ]; then
      echo "[done] seals=$seal_n cons=$cons_n (i=$i)"; break
    fi
    sleep 3
  done
  sleep 2
else
  sleep 40
fi

T1=$(date +%s.%N)
stop_all
seal_n=$(grep -c "block created successfully, txType" "$LOGDIR/management_${TAG}.log" 2>/dev/null); seal_n=${seal_n:-0}
cons_n=$(grep -c "consensus completed, height=" "$LOGDIR/management_${TAG}.log" 2>/dev/null); cons_n=${cons_n:-0}
cp /tmp/rq3_9091_r$R.log "$LOGDIR/proxy9091_${TAG}.log" 2>/dev/null
cp /tmp/rq3_9092_r$R.log "$LOGDIR/proxy9092_${TAG}.log" 2>/dev/null
echo "=== $SC r$R: seals=$seal_n cons=$cons_n elapsed=$(echo "$T1 $T0" | awk '{printf "%.1f", $1-$2}')s ==="