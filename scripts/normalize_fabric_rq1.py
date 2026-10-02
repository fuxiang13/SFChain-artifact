#!/usr/bin/env python3
"""Freeze one Fabric open-loop result as a unified-RQ1 summary."""

from __future__ import annotations

import argparse
import json
import hashlib
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--total", type=int, default=20000)
    args = parser.parse_args()

    source = json.loads(args.input.read_text(encoding="utf-8-sig"))
    count = int(source.get("count", 0))
    failed = int(source.get("fail", source.get("failed", 0)))
    if count != args.total or failed != 0:
        raise SystemExit(
            f"incomplete Fabric campaign: count={count}, failed={failed}, "
            f"expected={args.total}"
        )
    result = {
        "platform": "fabric",
        "schema_version": 2,
        "protocol": "pipeline-entry-v2",
        "source_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(),
        "entry": "per-record worker submission (submitTs)",
        "completion": "successful commit event",
        "count": count,
        "failed": failed,
        "throughput_tps": float(source["throughput_tps"]),
        "avg_ms": float(source["avg_ms"]),
        "p50_ms": float(source["p50_ms"]),
        "p95_ms": float(source["p95_ms"]),
        "p99_ms": float(source["p99_ms"]),
        "valid": True,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
