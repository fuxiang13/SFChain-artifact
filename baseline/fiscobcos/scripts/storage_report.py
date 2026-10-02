#!/usr/bin/env python3
"""
SFChain vs FISCO BCOS storage-efficiency comparison

Run this script after both sides have attested the same number of logs; it
reports per-node storage volume and the "bytes per 1k logs" metric.

Metric conventions:
  SFChain (MySQL):
    - Per-role block tables + block-header tables summed from information_schema (data_length+index_length)
    - management node = full block tables of all 4 chains (man_blocks_*)
    - development/test/operations nodes = own-chain full blocks + header tables of the other 3 chains
      (e.g. dev_blocks + dev_headers_{management,test,operations})
    - software_factory_logs / transactions are ingest entry tables, not chain storage, and are excluded
  FISCO BCOS (RocksDB):
    - docker exec du of /data/data on each node (blocks + receipts + state, incl. contract state storage)
    - /data/log holds runtime logs and is excluded

Usage (from the fiscobcos/ directory):
  python scripts/storage_report.py \
      --sfchain-mysql 127.0.0.1 3306 root qwer@123 sfchain \
      --fisco-nodes fisco-node0,fisco-node1,fisco-node2,fisco-node3 \
      --logs 20000 --label storage_round1

  Inspect only one side:
  python scripts/storage_report.py --sfchain-only --sfchain-mysql ...
  python scripts/storage_report.py --fisco-only --logs 20000

Dependencies: pymysql (SFChain side)
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

SFCHAIN_ROLES = {
    # role -> (full block table, other chains for which headers are held)
    "management": (None, []),  # management node: full blocks of all 4 chains
    "development": ("dev_blocks", ["management", "test", "operations"]),
    "test": ("test_blocks", ["management", "development", "operations"]),
    "operations": ("ops_blocks", ["management", "development", "test"]),
}


def sfchain_tables_bytes(cur, schema, tables):
    """sum(data_length+index_length) of tables"""
    total, rows = 0, 0
    for t in tables:
        cur.execute(
            "SELECT COALESCE(data_length,0)+COALESCE(index_length,0) "
            "FROM information_schema.TABLES "
            "WHERE table_schema=%s AND table_name=%s", (schema, t))
        r = cur.fetchone()
        if r and r[0]:
            total += int(r[0])
    return total


def sfchain_tx_count(cur, schema, tables):
    placeholders = ",".join(["%s"] * len(tables))
    cur.execute(
        "SELECT table_name FROM information_schema.COLUMNS "
        "WHERE table_schema=%s AND column_name='transactions' "
        f"AND table_name IN ({placeholders})",
        [schema, *tables],
    )
    payload_tables = {row[0] for row in cur.fetchall()}
    n = 0
    for t in tables:
        if t not in payload_tables:
            continue
        cur.execute(
            f"SELECT COALESCE(JSON_LENGTH(transactions),0) "
            f"FROM `{schema}`.`{t}`"
        )
        n += sum(x[0] or 0 for x in cur.fetchall())
    return int(n)


def collect_sfchain(mysql_args):
    import pymysql
    host, port, user, password, db = mysql_args
    conn = pymysql.connect(host=host, port=int(port), user=user, password=password,
                           database=db, charset="utf8mb4")
    out = {}
    try:
        with conn.cursor() as cur:
            for role, (full_table, header_chains) in SFCHAIN_ROLES.items():
                prefix = {"management": "man", "development": "dev",
                          "test": "test", "operations": "ops"}[role]
                if role == "management":
                    tables = [f"man_blocks_{c}" for c in
                              ["management", "development", "test", "operations"]]
                else:
                    tables = [full_table] + [f"{prefix}_headers_{c}" for c in header_chains]
                for table in tables:
                    cur.execute(f"ANALYZE TABLE `{db}`.`{table}`")
                    cur.fetchall()
                out[role] = {
                    "tables": tables,
                    "bytes": sfchain_tables_bytes(cur, db, tables),
                    "tx_count": sfchain_tx_count(cur, db, tables),
                }
    finally:
        conn.close()
    return out


def collect_fisco(nodes):
    out = {}
    for node in nodes:
        try:
            r = subprocess.run(
                ["docker", "exec", node, "du", "-sb", "/data/data"],
                capture_output=True, text=True, timeout=30, shell=False)
            if r.returncode == 0 and r.stdout.strip():
                out[node] = {"bytes": int(r.stdout.split()[0])}
            else:
                raise RuntimeError(
                    f"FISCO storage collection failed for {node}: "
                    f"{(r.stderr or 'du failed').strip()}"
                )
        except Exception as e:
            raise RuntimeError(f"FISCO storage collection failed for {node}: {e}") from e
    return out


def collect_fabric(peers, orderers, peer_data_path, orderer_data_path):
    """Collect persistent Fabric peer and orderer ledger footprints."""
    out = {"peers": {}, "orderers": {}}
    paths = {
        "peers": peer_data_path,
        "orderers": orderer_data_path,
    }
    for group, names in (("peers", peers), ("orderers", orderers)):
        for name in names:
            try:
                r = subprocess.run(
                    ["docker", "exec", name, "du", "-sb", paths[group]],
                    capture_output=True, text=True, timeout=30, shell=False)
                if r.returncode == 0 and r.stdout.strip():
                    out[group][name] = {"bytes": int(r.stdout.split()[0])}
                else:
                    raise RuntimeError(
                        f"Fabric storage collection failed for {name}: "
                        f"{(r.stderr or 'du failed').strip()}"
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Fabric storage collection failed for {name}: {e}"
                ) from e
    return out


def human(n):
    if n is None:
        return "-"
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def delta_bytes(current, baseline):
    """Return byte/count leaves as current minus baseline."""
    if isinstance(current, dict) and isinstance(baseline, dict):
        out = {}
        for key, value in current.items():
            if key not in baseline:
                continue
            if key == "bytes" or key.endswith("_bytes"):
                out[key] = int(value) - int(baseline[key])
            elif isinstance(value, dict):
                nested = delta_bytes(value, baseline[key])
                if nested:
                    out[key] = nested
        return out
    return {}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sfchain-mysql", nargs=5,
                    metavar=("HOST", "PORT", "USER", "PASSWORD", "DB"),
                    default=["127.0.0.1", "3306", "root", "qwer@123", "sfchain"])
    ap.add_argument("--sfchain-only", action="store_true")
    ap.add_argument("--fisco-only", action="store_true")
    ap.add_argument("--fabric-only", action="store_true")
    ap.add_argument("--fisco-nodes", default="fisco-node0,fisco-node1,fisco-node2,fisco-node3")
    ap.add_argument("--fabric-peers",
                    default="peer0.org1.example.com,peer0.org2.example.com,"
                            "peer0.org3.example.com,peer0.org4.example.com")
    ap.add_argument("--fabric-orderers",
                    default="orderer1.example.com,orderer2.example.com,"
                            "orderer3.example.com,orderer4.example.com")
    ap.add_argument("--fabric-peer-data-path",
                    default="/etc/hyperledger/production/ledgersData")
    ap.add_argument("--fabric-orderer-data-path",
                    default="/var/hyperledger/production/orderer")
    ap.add_argument("--logs", type=int, default=None,
                    help="total logs attested this round (used to compute bytes per 1k logs)")
    ap.add_argument("--label", default="storage")
    ap.add_argument("--out", default="results")
    ap.add_argument("--baseline", default=None,
                    help="JSON report from the matching fresh-ledger baseline")
    args = ap.parse_args()

    result = {"label": args.label, "logs": args.logs,
              "generated_at": datetime.now().isoformat(timespec="seconds")}
    per_k = lambda b: round(b / (args.logs / 1000)) if args.logs else None

    if not args.fisco_only and not args.fabric_only:
        sf = collect_sfchain(args.sfchain_mysql)
        result["sfchain"] = sf
        print("========== SFChain chain storage (MySQL block/header tables) ==========")
        for role, v in sf.items():
            print(f"  {role:12s} {human(v['bytes']):>10s}  ({v['tx_count']} txs)  "
                  f"{per_k(v['bytes']) if per_k(v['bytes']) is not None else '-'} B/1k-logs")
        total = sum(v["bytes"] for v in sf.values())
        result["sfchain_total_bytes"] = total
        print(f"  {'TOTAL':12s} {human(total):>10s}  {per_k(total) if per_k(total) is not None else '-'} B/1k-logs")

    if not args.sfchain_only and not args.fabric_only:
        nodes = [n.strip() for n in args.fisco_nodes.split(",") if n.strip()]
        fi = collect_fisco(nodes)
        result["fisco"] = fi
        print("========== FISCO BCOS chain storage (RocksDB /data/data) ==========")
        total = 0
        for node, v in fi.items():
            if "bytes" in v:
                total += v["bytes"]
                print(f"  {node:12s} {human(v['bytes']):>10s}  "
                      f"{per_k(v['bytes']) if per_k(v['bytes']) is not None else '-'} B/1k-logs")
            else:
                print(f"  {node:12s} failed: {v['error']}")
        result["fisco_total_bytes"] = total
        print(f"  {'TOTAL':12s} {human(total):>10s}  {per_k(total) if per_k(total) is not None else '-'} B/1k-logs")

    if not args.sfchain_only and not args.fisco_only:
        peers = [n.strip() for n in args.fabric_peers.split(",") if n.strip()]
        orderers = [n.strip() for n in args.fabric_orderers.split(",") if n.strip()]
        fabric = collect_fabric(
            peers,
            orderers,
            args.fabric_peer_data_path,
            args.fabric_orderer_data_path,
        )
        result["fabric"] = fabric
        fabric_total = 0
        for group in ("peers", "orderers"):
            for value in fabric[group].values():
                if "bytes" in value:
                    fabric_total += value["bytes"]
        result["fabric_total_bytes"] = fabric_total
        print("========== Fabric persistent ledger ==========")
        for group in ("peers", "orderers"):
            for name, value in fabric[group].items():
                if "bytes" in value:
                    print(f"  {name:28s} {human(value['bytes']):>10s}  "
                          f"{per_k(value['bytes']) if per_k(value['bytes']) is not None else '-'} B/1000 logs")
                else:
                    print(f"  {name:28s} failed: {value['error']}")
        print(f"  {'network total':28s} {human(fabric_total):>10s}  "
              f"{per_k(fabric_total) if per_k(fabric_total) is not None else '-'} B/1000 logs")

    if args.baseline:
        with open(args.baseline, encoding="utf-8") as handle:
            baseline = json.load(handle)
        result["baseline"] = args.baseline
        result["delta_bytes"] = {}
        for key in ("sfchain", "fisco", "fabric"):
            if key in result and key in baseline:
                result["delta_bytes"][key] = delta_bytes(
                    result[key], baseline[key]
                )
        for key in ("sfchain_total_bytes", "fisco_total_bytes", "fabric_total_bytes"):
            if key in result and key in baseline:
                result["delta_bytes"][key] = (
                    int(result[key]) - int(baseline[key])
                )

    os.makedirs(args.out, exist_ok=True)
    out_path = os.path.join(args.out, f"storage_{args.label}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Summary written to: {out_path}")


if __name__ == "__main__":
    main()
