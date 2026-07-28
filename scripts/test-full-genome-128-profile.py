#!/usr/bin/env python3
"""Exact offline contract test for the dormant 128-worker private pool profile."""

from pathlib import Path
import re
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "genohype.full-genome-128.toml"


class FullGenome128ProfileTest(unittest.TestCase):
    def test_zero_to_128_private_scaling_contract(self) -> None:
        config = tomllib.loads(PROFILE.read_text(encoding="utf-8"))
        self.assertEqual(
            config["defaults"],
            {
                "project": "gnomadev",
                "zone": "us-east1-b",
                "network": "gnomad-v4-dev",
                "public_ip": False,
                "manage_firewall": False,
            },
        )
        pool_name = "lr-full-genome-128"
        self.assertEqual(set(config["pools"]), {pool_name})
        self.assertRegex(f"{pool_name}-coordinator", re.compile(r"^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$"))
        pool = config["pools"][pool_name]
        self.assertEqual(pool["starting_workers"], 0)
        self.assertEqual(pool["workers"], 128)
        self.assertTrue(pool["spot"])
        self.assertTrue(pool["with_coordinator"])
        self.assertEqual(pool["subnet"], "gnomad-lr-y1-full-prototype")
        self.assertEqual(
            pool["service_account"],
            "lr-y1-full-proto-worker@gnomadev.iam.gserviceaccount.com",
        )
        self.assertEqual(
            pool["coordinator_service_account"],
            "lr-y1-full-proto-coord@gnomadev.iam.gserviceaccount.com",
        )
        self.assertEqual(
            pool["pool_db_path"],
            "gs://gnomad-lr-data/pool-ops/full-genome-128/ops.db",
        )


if __name__ == "__main__":
    unittest.main()
