#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("verify-y1-chr22-signatures.py")
ROWS = [
    "hgsvc_hprc\tsummaries\t808853\t14634967967081205611",
    "aou\tsummaries\t1166762\t17948364209855283030",
    "hgsvc_hprc\talleles\t1046072\t14614298358322652621",
    "aou\talleles\t3152223\t6909096278152444077",
    "hgsvc_hprc\tcarriers\t38285467\t5740761881423515696",
    "hgsvc_hprc\tfrequencies\t21967512\t3800520885522330351",
    "aou\tfrequencies\t18913338\t10838463094380439429",
    "aou\tcarriers\t0\t0",
]
RUN_IDS = {"hgsvc_hprc": "run-h", "aou": "run-a"}
PROVENANCE_ROWS = [f"{row}\t{RUN_IDS[row.split(chr(9), 1)[0]]}" for row in ROWS]


def load_module():
    spec = importlib.util.spec_from_file_location("signature_validator", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SignatureValidatorTest(unittest.TestCase):
    def run_tool(self, text: str, *extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--input", "-", *extra],
            input=text,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_accepts_shuffled_rows_and_writes_provenance_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "acceptance.tsv"
            result = self.run_tool(
                "\n".join(reversed(PROVENANCE_ROWS)) + "\n", "--artifact", str(artifact)
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                artifact.read_text().splitlines(),
                [PROVENANCE_ROWS[i] for i in (0, 2, 5, 4, 1, 3, 6, 7)],
            )

    def test_legacy_evidence_remains_comparable_but_cannot_become_activation_artifact(self):
        self.assertEqual(self.run_tool("\n".join(ROWS) + "\n").returncode, 0)
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "acceptance.tsv"
            result = self.run_tool("\n".join(ROWS) + "\n", "--artifact", str(artifact))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires explicit cohort-to-run provenance", result.stderr)
            self.assertFalse(artifact.exists())

    def test_rejects_missing_aou_zero_carriers(self):
        result = self.run_tool("\n".join(ROWS[:-1]) + "\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing: aou/carriers", result.stderr)

    def test_rejects_extra_duplicate_and_mismatch(self):
        cases = {
            "extra": ROWS + ["aou\tunknown\t0\t0"],
            "duplicate": ROWS + [ROWS[0]],
            "mismatch": [*ROWS[:-1], "aou\tcarriers\t1\t0"],
        }
        for name, rows in cases.items():
            with self.subTest(name=name):
                result = self.run_tool("\n".join(rows) + "\n")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name if name != "mismatch" else "mismatch aou/carriers", result.stderr)

    def test_rejects_malformed_values_and_removes_stale_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "acceptance.tsv"
            artifact.write_text("stale\n")
            bad = [*ROWS[:-1], "aou\tcarriers\t-1\t0"]
            result = self.run_tool("\n".join(bad) + "\n", "--artifact", str(artifact))
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(artifact.exists())
            self.assertIn("unsigned decimal", result.stderr)

    def test_rejects_swapped_and_mixed_run_associations(self):
        swapped = [
            f"{row}\t{RUN_IDS['aou'] if row.startswith('hgsvc_hprc') else RUN_IDS['hgsvc_hprc']}"
            for row in ROWS
        ]
        mixed = PROVENANCE_ROWS.copy()
        mixed[1] = ROWS[1] + "\t" + RUN_IDS["hgsvc_hprc"]
        module = load_module()
        expected_runs = RUN_IDS
        with self.assertRaisesRegex(ValueError, "unexpected run/cohort association"):
            module.compare(module.parse_tsv("\n".join(swapped) + "\n", "swapped"), expected_runs)
        with self.assertRaisesRegex(ValueError, "unexpected run/cohort association"):
            module.compare(module.parse_tsv("\n".join(mixed) + "\n", "mixed"), expected_runs)

    def test_query_is_pair_bound_order_independent_definition(self):
        module = load_module()
        sql = module.signature_sql()
        self.assertEqual(sql.count("groupBitXor(cityHash64(toJSONString(tuple("), 12)
        self.assertNotIn("run_id IN", sql)
        self.assertIn("run_id = {hgsvc_run_id:String} AND cohort = 'hgsvc_hprc'", sql)
        self.assertIn("run_id = {aou_run_id:String} AND cohort = 'aou'", sql)
        self.assertIn("cohort != 'hgsvc_hprc'", sql)
        self.assertIn("cohort != 'aou'", sql)
        self.assertNotIn("ORDER BY", sql)
        self.assertTrue(sql.endswith("FORMAT TabSeparated"))

    def test_pilot_database_is_accepted_and_sent_separately(self):
        module = load_module()

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return None

            def read(self, _limit):
                return ("\n".join(PROVENANCE_ROWS) + "\n").encode()

        args = argparse.Namespace(
            endpoint="http://127.0.0.1:8123",
            allow_remote=False,
            database="gnomad_lr_y1_pilot",
            hgsvc_run_id="run-h",
            aou_run_id="run-a",
            username_env=None,
            password_env=None,
            timeout=1.0,
        )
        with mock.patch.object(module.urllib.request, "urlopen", return_value=Response()) as opened:
            text = module.query(args)
        self.assertEqual(text.splitlines(), PROVENANCE_ROWS)
        request = opened.call_args.args[0]
        self.assertEqual(urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["database"], ["gnomad_lr_y1_pilot"])
        self.assertNotIn("gnomad_lr_y1_pilot", request.data.decode())

    def test_endpoint_and_database_guards_fail_before_network(self):
        command = [
            sys.executable, str(SCRIPT), "--endpoint", "http://127.0.0.1:8123",
            "--database", "default", "--hgsvc-run-id", "r2-h", "--aou-run-id", "r2-a",
        ]
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("explicit gnomad_lr_y1", result.stderr)


if __name__ == "__main__":
    unittest.main()
