#!/usr/bin/env python3
"""Exact offline contract test for the dormant 128-worker private pool profile."""

from pathlib import Path
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
        self.assertEqual(set(config["pools"]), {"lr_full_genome_128"})
        pool = config["pools"]["lr_full_genome_128"]
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
