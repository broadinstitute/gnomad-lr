#!/usr/bin/env python3
import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("generate-y1-chr22-manifest.py")
spec = importlib.util.spec_from_file_location("manifest", SCRIPT)
manifest = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(manifest)


def source():
    objects = []
    for cohort in manifest.COHORTS:
        name = f"gnomAD_LR_Y1.{cohort}.chr22.vcf.gz"
        objects += [
            {"cohort": cohort, "name": name, "mirror_generation": "1", "size": 100, "md5_base64": "vcf"},
            {"cohort": cohort, "name": name + ".tbi", "mirror_generation": "2", "size": 10, "md5_base64": "tbi"},
        ]
    return {"release": "Y1", "chromosome": "chr22", "mirror_prefix": "gs://gnomad-lr-data/y1/sources", "objects": objects}


class ManifestTests(unittest.TestCase):
    def test_generation_is_deterministic_and_exact(self):
        first = manifest.generate(source(), "hgsvc_hprc", "run-1", "attempt-1", 1_000_000)
        second = manifest.generate(source(), "hgsvc_hprc", "run-1", "attempt-1", 1_000_000)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 51)
        self.assertEqual((first[0]["start"], first[0]["stop"]), (1, 1_000_000))
        self.assertEqual((first[-1]["start"], first[-1]["stop"]), (50_000_001, manifest.CHR22_LENGTH))

    def test_boundary_position_has_exactly_one_owner(self):
        tasks = manifest.generate(source(), "aou", "run-2", "attempt-1", 1_000_000)
        for position in (1, 1_000_000, 1_000_001, manifest.CHR22_LENGTH):
            owners = [task for task in tasks if task["start"] <= position <= task["stop"]]
            self.assertEqual(len(owners), 1)

    def test_fail_once_has_distinct_immutable_attempts(self):
        tasks = manifest.generate(
            source(), "hgsvc_hprc", "run-3", "attempt-1", 1_000_000,
            7, "attempt-2", "controlled-exercise-20260727",
        )
        self.assertNotEqual(tasks[7]["attempt_id"], tasks[7]["retry_attempt_id"])
        self.assertEqual(tasks[7]["controlled_fail_once"]["mode"], "after_first_staged_batch")
        manifest.verify(tasks)

    def test_gap_and_source_drift_fail_closed(self):
        tasks = manifest.generate(source(), "aou", "run-2", "attempt-1", 1_000_000)
        tasks[1]["start"] += 1
        with self.assertRaises(ValueError):
            manifest.verify(tasks)
        tasks = manifest.generate(source(), "aou", "run-2", "attempt-1", 1_000_000)
        tasks[1]["source_generation"] = "changed"
        with self.assertRaises(ValueError):
            manifest.verify(tasks)


if __name__ == "__main__":
    unittest.main()
