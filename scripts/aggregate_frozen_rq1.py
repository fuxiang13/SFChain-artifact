#!/usr/bin/env python3
"""Offline aggregation of frozen unified-RQ1 summaries.

This script never connects to a database or ledger.  Run each platform
analyzer immediately after a round, save one summary per round, and then
aggregate only those frozen summaries.
"""

from __future__ import annotations

import argparse
import json
import statistics
import math
from pathlib import Path

METRICS = ("throughput_tps", "avg_ms", "p50_ms", "p95_ms", "p99_ms")
PLATFORMS = ("sfchain", "fiscobcos", "fabric")


def read_summary(path: Path, platform: str, total: int, endpoint: str = "anchored") -> dict:
    obj = json.loads(path.read_text(encoding="utf-8-sig"))
    if obj.get("protocol") != "pipeline-entry-v2" or obj.get("schema_version") != 2:
        raise RuntimeError(f"{path}: incompatible protocol/schema")
    if platform == "sfchain" and isinstance(obj.get(endpoint), dict):
        obj = {
            "platform": "sfchain",
            "protocol": obj.get("protocol", "unified-pipeline-entry"),
            "entry": obj.get("entry", "responsible-user endorsement timestamp (endorsements[0].timestamp)"),
            "completion": obj["completion"][endpoint],
            **obj[endpoint],
        }
    if obj.get("platform") != platform:
        raise RuntimeError(f"{path}: expected platform={platform}")
    if int(obj.get("count", 0)) != total:
        raise RuntimeError(f"{path}: incomplete count={obj.get('count')}")
    if int(obj.get("failed", obj.get("fail", 0))) != 0:
        raise RuntimeError(f"{path}: failed={obj.get('failed', obj.get('fail'))}")
    if obj.get("valid") is not True or int(obj.get("missing", 0)) != 0:
        raise RuntimeError(f"{path}: invalid result")
    for metric in METRICS:
        if metric not in obj:
            raise RuntimeError(f"{path}: missing {metric}")
        obj[metric] = float(obj[metric])
        if not math.isfinite(obj[metric]) or obj[metric] < 0:
            raise RuntimeError(f"{path}: invalid metric {metric}")
    return obj


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign", type=Path)
    parser.add_argument("--total", type=int, default=20000)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--endpoint", choices=("anchored", "attestation"), default="anchored")
    args = parser.parse_args()

    directories = sorted(p for p in args.campaign.glob("round_*") if p.is_dir())
    manifest_path = args.campaign / "protocol.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        expected = {f"round_{i:02d}" for i in range(1, int(manifest["rounds"]) + 1)}
        if {p.name for p in directories} != expected or int(manifest["total"]) != args.total:
            raise SystemExit("Campaign rounds/count do not match the planned manifest")
    rounds = []
    for directory in directories:
        if not directory.is_dir():
            continue
        row = {"round": directory.name}
        for platform in PLATFORMS:
            row[platform] = read_summary(
                directory / platform / "result.json", platform, args.total, args.endpoint
            )
        rounds.append(row)
    if not rounds:
        raise SystemExit(f"no round_* directories found under {args.campaign}")

    medians = {
        platform: {
            metric: statistics.median(row[platform][metric] for row in rounds)
            for metric in METRICS
        }
        for platform in PLATFORMS
    }
    output = {
        "protocol": "pipeline-entry-v2",
        "sfchain_endpoint": args.endpoint,
        "total": args.total,
        "valid_rounds": len(rounds),
        "rounds": rounds,
        "median": medians,
        "range": {
            platform: {metric: [
                min(row[platform][metric] for row in rounds),
                max(row[platform][metric] for row in rounds)]
                for metric in METRICS} for platform in PLATFORMS
        },
        "aggregation": {
            "method": "median across frozen valid round summaries",
            "database_access": False,
            "raw_result_data_required": False,
        },
    }
    out = args.out or args.campaign / "aggregate.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(json.dumps({"out": str(out), "median": medians}, indent=2))


if __name__ == "__main__":
    main()
