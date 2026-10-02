"""Regression checks for the no-silent-exclusion measurement contract."""
import json
from pathlib import Path
import tempfile
import unittest

from aggregate_frozen_rq1 import read_summary, METRICS


class FrozenSummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "result.json"
        self.row = {
            "platform": "fabric", "protocol": "pipeline-entry-v2", "schema_version": 2,
            "count": 20000, "failed": 0, "valid": True,
            **{key: 1 for key in METRICS},
        }

    def tearDown(self):
        self.tmp.cleanup()

    def write(self):
        self.path.write_text(json.dumps(self.row), encoding="utf-8")

    def test_complete(self):
        self.write()
        self.assertEqual(read_summary(self.path, "fabric", 20000)["count"], 20000)

    def test_reject_incomplete_failed_nonfinite_or_other_protocol(self):
        for key, value in (("count", 19999), ("failed", 1), ("valid", False),
                           ("p50_ms", float("nan")), ("protocol", "incompatible-test-schema")):
            with self.subTest(key=key):
                original = self.row[key]
                self.row[key] = value
                self.write()
                with self.assertRaises(RuntimeError):
                    read_summary(self.path, "fabric", 20000)
                self.row[key] = original

    def test_milestones_are_not_interchangeable(self):
        self.row["platform"] = "sfchain"
        self.row["completion"] = {"attestation": "aggregate", "anchored": "successor"}
        self.row["attestation"] = {"count": 20000, "failed": 0, "valid": True,
                                   **{key: 1 for key in METRICS}}
        self.row["anchored"] = {**self.row["attestation"], "count": 19800, "valid": False}
        self.write()
        self.assertTrue(read_summary(self.path, "sfchain", 20000, "attestation")["valid"])
        with self.assertRaises(RuntimeError):
            read_summary(self.path, "sfchain", 20000, "anchored")


if __name__ == "__main__":
    unittest.main()
