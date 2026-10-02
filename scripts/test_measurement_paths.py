"""Regression tests for the paper's analysis boundaries and workspace layout."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from analyze_rq2 import analyze

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "sf_round", ROOT / "baseline/fiscobcos/scripts/analyze_e1g.py")
sf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sf)


class MeasurementPaths(unittest.TestCase):
    def test_workspace_preserves_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / "workspace"
            subprocess.run([sys.executable, "-B", str(ROOT / "scripts/materialize_workspace.py"),
                            str(dest)], check=True, capture_output=True)
            for path in ("prototype/go.mod", "deploy/sf-docker/docker-compose.yml",
                         "baseline/fabric/net/configtx.yaml",
                         "baseline/fiscobcos/loadgen/main.go", "docs/FILE_MAP.md"):
                self.assertTrue((dest / path).is_file(), path)
            self.assertFalse(list(dest.rglob("*.pyc")))

    def test_sfchain_uses_endorsement_timestamp_and_requires_it(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "round.log"
            log.write_text(
                "2026/01/01 00:00:01.000000 block hash computed, txType=management, height=1, hash=abc\n"
                "height=1, chain=management, sigs=4, hash=abc, final_ns=1767225601100000000\n"
                "2026/01/01 00:00:02.000000 block hash computed, txType=management, height=2, hash=def\n",
                encoding="utf-8")
            tx = {"tx_id": "tx1", "timestamp": 1767225600000,
                  "endorsements": [{"timestamp": 1767225600500}]}

            def query(sql):
                if "software_factory_logs" in sql:
                    return "log1\ttx1\n"
                if "man_blocks_management" in sql:
                    return "1\tabc\t" + json.dumps([tx]) + "\n"
                return ""

            with patch.object(sf, "mysql", side_effect=query):
                result = sf.freeze(log, 1)
                self.assertEqual(result["attestation"]["p50_ms"], 600)
                self.assertEqual(result["anchored"]["p50_ms"], 1500)
                tx["endorsements"] = []
                with self.assertRaises(ValueError):
                    sf.freeze(log, 1)

    def test_rq2_counts_transactions_not_blocks(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "block.log"
            log.write_text(
                "2026/01/01 00:00:00.000000 block generation - step 0] starting to generate management chain block\n"
                "2026/01/01 00:00:00.010000 creating new block, txType=management, nextHeight=1, txCount=128\n"
                "2026/01/01 00:00:00.020000 block created successfully, txType=management, height=1\n"
                "2026/01/01 00:00:01.000000 consensus completed, height=1, chain=management\n",
                encoding="utf-8")
            result = analyze(log)
            self.assertEqual(result["throughput_tps"], 128)
            self.assertEqual(result["mean_block_latency_ms"], 1000)

    def test_fisco_recovered_receipt_is_not_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            trace, out = Path(directory) / "trace.csv", Path(directory) / "result.json"
            trace.write_text(
                "log_id,status,error,submitted_ms,receipt_ms,recovered\n"
                "one,0,,1000,1100,false\n"
                "two,0,,1000,2100,true\n", encoding="utf-8")
            subprocess.run([sys.executable, "-B", str(ROOT / "scripts/normalize_fisco_rq1.py"),
                            "--csv", str(trace), "--out", str(out), "--total", "2"],
                           check=True, capture_output=True)
            result = json.loads(out.read_text("utf-8"))
            self.assertEqual(result["count"], 2)
            self.assertEqual(result["recovered_receipts"], 1)
            self.assertEqual(result["avg_ms"], 600)


if __name__ == "__main__":
    unittest.main()
