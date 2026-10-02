#!/usr/bin/env python3
"""Compute RQ2 block-build-to-attestation metrics from explicit SFChain logs."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import statistics

STAMP = r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+)"
START = re.compile(STAMP + r".*block generation - step 0\] starting to generate (\w+) chain block")
COUNT = re.compile(STAMP + r".*creating new block, txType=(\w+), nextHeight=(\d+), txCount=(\d+)")
SEAL = re.compile(STAMP + r".*block created successfully, txType=(\w+), height=(\d+)")
DONE = re.compile(STAMP + r".*consensus completed, height=(\d+), chain=(\w+)")


def analyze(path):
    pending, starts, counts, seals, ends = {}, {}, {}, set(), {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if m := START.search(line):
            pending.setdefault(m[2], datetime.strptime(m[1], "%Y/%m/%d %H:%M:%S.%f"))
        elif m := COUNT.search(line):
            counts[(m[2], int(m[3]))] = int(m[4])
        elif m := SEAL.search(line):
            key = (m[2], int(m[3]))
            if m[2] not in pending:
                raise ValueError(f"{path}: missing build-start event for {key}")
            starts[key] = pending.pop(m[2])
            seals.add(key)
        elif m := DONE.search(line):
            ends.setdefault((m[3], int(m[2])),
                            datetime.strptime(m[1], "%Y/%m/%d %H:%M:%S.%f"))
    if not seals or seals != set(ends) or not seals.issubset(counts):
        raise ValueError(f"{path}: incomplete build/count/attestation event set")
    keys = [k for k in seals if counts[k] > 0]
    if not keys:
        raise ValueError(f"{path}: no data blocks")
    lat = [(ends[k] - starts[k]).total_seconds() * 1000 for k in keys]
    if min(lat) < 0:
        raise ValueError(f"{path}: negative latency")
    window = (max(ends[k] for k in keys) - min(starts[k] for k in keys)).total_seconds()
    if window <= 0:
        raise ValueError(f"{path}: invalid window")
    total = sum(counts[k] for k in keys)
    return {"log": path.name, "blocks": len(keys), "transactions": total,
            "window_s": window, "throughput_tps": total / window,
            "mean_block_latency_ms": statistics.fmean(lat),
            "p50_block_latency_ms": statistics.median(lat)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("logs", type=Path, nargs="+")
    p.add_argument("--out", type=Path)
    args = p.parse_args()
    result = {"start": "block-build start", "end": "four-role attestation",
              "rounds": [analyze(path) for path in args.logs]}
    text = json.dumps(result, indent=2)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
