#!/usr/bin/env python3
"""Freeze one hash-bound SFChain round BEFORE resetting its database."""
from __future__ import annotations
import argparse
import hashlib
import json
import re
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parents[2]
CONS = re.compile(r"height=(\d+), chain=(\w+), sigs=(\d+), hash=([a-f0-9]+), final_ns=(\d+)")
ANCHOR = re.compile(r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+).*block hash computed, txType=(\w+), height=(\d+), hash=([a-f0-9]+)")

def mysql(sql):
    p = subprocess.run(["docker", "exec", "sf-mysql", "mysql", "-uroot", "-pqwer@123", "--raw", "-N", "-s", "-e", sql], capture_output=True, text=True, encoding="utf-8", check=True)
    return p.stdout

def metrics(values):
    v = sorted(values)
    return {"avg_ms": statistics.fmean(v), **{f"p{p}_ms": v[int(p / 100 * (len(v) - 1))] for p in (50,90,95,99)}} if v else {}

def freeze(log_path, total):
    consensus, anchors = {}, {}
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if m := CONS.search(line):
            height, chain, signers, digest, ns = m.groups()
            if int(signers) != 4:
                raise ValueError(f"not four-role attestation: {line}")
            consensus[(chain,int(height))] = (int(ns)/1e6,digest)
        if m := ANCHOR.search(line):
            date, chain, height, digest = m.groups()
            ts = datetime.strptime(date,"%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()*1000
            anchors[(chain,int(height))] = (ts,digest)
    if not consensus:
        raise ValueError("no machine-readable attestation events")
    source = mysql("SELECT id,tx_id FROM sfchain.software_factory_logs WHERE id NOT LIKE 'anchor-dummy-%' ORDER BY id")
    rows = [line.split("\t") for line in source.splitlines()]
    expected = {r[1] for r in rows if len(r)==2 and r[1]!="NULL"}
    if len(rows)!=total or len(expected)!=total:
        raise ValueError(f"source incomplete/duplicate: rows={len(rows)}, txids={len(expected)}")
    index, snapshot = {}, []
    for chain in ("management","development","test","operations"):
        raw = mysql(f"SELECT block_height,block_hash,transactions FROM sfchain.man_blocks_{chain} WHERE transactions IS NOT NULL")
        snapshot.append(raw)
        for line in raw.splitlines():
            height, digest, payload = line.split("\t",2)
            key = (chain,int(height))
            event = anchors.get(key)
            if not event or not digest.startswith(event[1]):
                raise ValueError(f"database/log block mismatch at {key}")
            if key in consensus and not digest.startswith(consensus[key][1]):
                raise ValueError(f"database/log attestation mismatch at {key}")
            for tx in json.loads(payload):
                txid = tx["tx_id"]
                if txid not in expected:
                    continue
                if txid in index:
                    raise ValueError(f"duplicate business transaction: {txid}")
                endorsements = tx.get("endorsements") or []
                if len(endorsements) != 1 or not endorsements[0].get("timestamp"):
                    raise ValueError(f"missing responsible-user endorsement timestamp: {txid}")
                entry = float(endorsements[0]["timestamp"])
                index[txid] = (key,entry)
    if set(index)!=expected:
        raise ValueError(f"missing persisted business records: {len(expected-set(index))}")
    result, paired = {}, []
    first = min(entry for _,entry in index.values())
    for endpoint in ("attestation","anchored"):
        latencies, completions = [], []
        for key, entry in index.values():
            lookup = key if endpoint=="attestation" else (key[0],key[1]+1)
            event = (consensus if endpoint=="attestation" else anchors).get(lookup)
            if event is None:
                continue
            done = event[0]
            if entry<=0 or done<entry:
                raise ValueError(f"invalid timestamp at {key}")
            if endpoint=="anchored" and key in consensus:
                delta = done-consensus[key][0]
                if delta<0:
                    raise ValueError(f"successor predates attestation at {key}")
                paired.append(delta)
            latencies.append(done-entry)
            completions.append(done)
        window = (max(completions)-first)/1000 if completions else 0
        result[endpoint] = {"count":len(latencies), "missing":total-len(latencies), "failed":0, "valid":len(latencies)==total, "window_s":window, "throughput_tps":len(latencies)/window if window>0 else 0, **metrics(latencies)}
    result["paired_anchor_after_attestation"] = {"count":len(paired), **metrics(paired)}
    result["provenance"] = {"frozen_utc":datetime.now(timezone.utc).isoformat(), "log_sha256":hashlib.sha256(log_path.read_bytes()).hexdigest(), "source_ids_sha256":hashlib.sha256(source.encode()).hexdigest(), "blocks_sha256":hashlib.sha256("".join(snapshot).encode()).hexdigest(), "block_hashes_checked":True}
    return result

def main():
    p=argparse.ArgumentParser()
    p.add_argument("suffix")
    p.add_argument("--bs",type=int,default=400)
    p.add_argument("--total",type=int,default=20000)
    p.add_argument("--log",type=Path)
    p.add_argument("--out",type=Path,required=True)
    p.add_argument("--require",choices=("attestation","anchored"),default="anchored")
    a=p.parse_args()
    log=a.log or BASE/"logs"/f"management_e1g_bs{a.bs}_{a.suffix}.log"
    result={"schema_version":2,"platform":"sfchain","protocol":"pipeline-entry-v2","round":a.suffix,"entry":"responsible-user endorsement timestamp (endorsements[0].timestamp)","completion":{"attestation":"four-role aggregate construction at Management","anchored":"successor header construction at Management; persistence excluded"},**freeze(log,a.total)}
    a.out.parent.mkdir(parents=True,exist_ok=True)
    a.out.write_text(json.dumps(result,indent=2),encoding="utf-8")
    print(json.dumps(result,indent=2))
    if not result[a.require]["valid"]:
        raise SystemExit(f"INCOMPLETE {a.require}; retained result must not be reported as complete")

if __name__=="__main__":
    main()
