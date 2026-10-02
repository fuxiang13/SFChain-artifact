#!/usr/bin/env python3
"""Freeze one FISCO unified-RQ1 CSV as a summary.

The CSV path is explicit by design: selecting the newest file is unsafe in a
multi-round campaign.  The output contains only summary statistics and can be
included in a local campaign directory; the artifact itself need not ship it.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import hashlib
from pathlib import Path


def percentile(values: list[int], p: float) -> float:
    return float(values[int(p / 100 * (len(values) - 1))])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--total", type=int, default=20000)
    args = parser.parse_args()

    with args.csv.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != args.total or len({r["log_id"] for r in rows}) != args.total:
        raise SystemExit("Wrong count or duplicate log IDs")
    for row in rows:
        if (row.get("error", "").strip() or int(row.get("status", -1)) != 0
                or int(row.get("submitted_ms", 0)) <= 0
                or int(row.get("receipt_ms", 0)) < int(row["submitted_ms"])):
            raise SystemExit(f"Failed/invalid record: {row['log_id']}")
    timed = rows
    latencies = sorted(
        int(row["receipt_ms"]) - int(row["submitted_ms"]) for row in timed
    )
    if len(latencies) != args.total:
        raise SystemExit(
            f"incomplete FISCO campaign: rows={len(rows)}, timed={len(latencies)}, "
            f"expected={args.total}"
        )
    first = min(int(row["submitted_ms"]) for row in timed)
    last = max(int(row["receipt_ms"]) for row in timed)
    window = (last - first) / 1000.0
    if window <= 0:
        raise SystemExit("Non-positive FISCO measurement window")
    result = {
        "platform": "fiscobcos",
        "schema_version": 2,
        "protocol": "pipeline-entry-v2",
        "entry": "transaction submission (submitted_ms)",
        "completion": "successful transaction receipt",
        "count": len(latencies),
        "failed": 0,
        "recovered_receipts": sum(row.get("recovered") == "true" for row in rows),
        "source_csv_name": args.csv.name,
        "source_sha256": hashlib.sha256(args.csv.read_bytes()).hexdigest(),
        "window_s": window,
        "throughput_tps": len(latencies) / window,
        "avg_ms": statistics.fmean(latencies),
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "valid": True,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
