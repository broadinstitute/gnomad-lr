#!/usr/bin/env python3
import hashlib
import importlib.util
import json
import unittest
from pathlib import Path

HERE = Path(__file__).parent

def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module

generic = load("grch38_manifest", "generate-y1-grch38-contig-manifest.py")
chr22 = load("chr22_manifest", "generate-y1-chr22-manifest.py")
SOURCE = json.loads((HERE.parent / "sources/y1/primary-source-manifest.json").read_text())

class ManifestTest(unittest.TestCase):
    def test_committed_source_manifest_hashes(self):
        checksum_path = HERE.parent / "sources/y1/primary-source-manifests.sha256"
        entries = [line.split() for line in checksum_path.read_text().splitlines() if line]
        self.assertEqual(len(entries), len(generic.GRCH38_CONTIG_LENGTHS))
        self.assertEqual({path for _, path in entries}, {
            f"sources/y1/primary-source-{contig}.json" for contig in generic.GRCH38_CONTIG_LENGTHS
        })
        for expected, relative_path in entries:
            actual = hashlib.sha256((HERE.parent / relative_path).read_bytes()).hexdigest()
            self.assertEqual(actual, expected, relative_path)

    def test_chr22_output_is_backward_compatible(self):
        old = chr22.generate(SOURCE, "hgsvc_hprc", "run-1", "attempt-1", 1_000_000)
        new = generic.generate(SOURCE, "hgsvc_hprc", "chr22", "run-1", "attempt-1", 1_000_000)
        self.assertEqual(old, new)

    def test_each_committed_canonical_contig_is_gap_free_and_deterministic(self):
        for contig, length in generic.GRCH38_CONTIG_LENGTHS.items():
            source_path = HERE.parent / f"sources/y1/primary-source-{contig}.json"
            source = json.loads(source_path.read_text())
            self.assertEqual(source["mirror_prefix"], generic.MIRROR_PREFIX)
            for cohort in generic.COHORTS:
                tasks = generic.generate(source, cohort, contig, "run", "attempt", 10_000_000)
                repeated = generic.generate(source, cohort, contig, "run", "attempt", 10_000_000)
                self.assertEqual(generic.canonical_bytes(tasks), generic.canonical_bytes(repeated))
                self.assertEqual((tasks[0]["start"], tasks[-1]["stop"]), (1, length))
                expected = f"{generic.MIRROR_PREFIX}/{cohort}/vcfs/gnomAD_LR_Y1.{cohort}.{contig}.vcf.gz"
                self.assertEqual(tasks[0]["source_uri"], expected)
                generic.verify_source_identity(tasks, source, cohort, contig)

    def test_rejects_gap_and_cross_contig_source(self):
        tasks = generic.generate(SOURCE, "aou", "chr22", "run", "attempt", 10_000_000)
        tasks[1]["start"] += 1
        with self.assertRaises(ValueError): generic.verify(tasks, "chr22")
        with self.assertRaises(ValueError): generic.generate(SOURCE, "aou", "chr21", "run", "attempt", 1_000_000)

    def test_rejects_noncanonical_mirror_contract(self):
        source = json.loads(json.dumps(SOURCE))
        source["mirror_prefix"] = "gs://other/y1/sources"
        with self.assertRaisesRegex(ValueError, "Rust canonical"):
            generic.generate(source, "aou", "chr22", "run", "attempt", 1_000_000)

    def test_mt_requires_explicit_immutable_per_contig_contract(self):
        ordinary = json.loads(json.dumps(SOURCE).replace("chr22", "chrM"))
        with self.assertRaisesRegex(ValueError, "unavailable"):
            generic.generate(ordinary, "aou", "chrM", "run", "attempt", 1_000_000)

        contracted = ordinary | {
            "schema_version": 2,
            "contract_type": "y1_per_contig_immutable_source",
            "reference_genome": "GRCh38",
            "mt_enabled": True,
        }
        tasks = generic.generate(contracted, "aou", "chrM", "run", "attempt", 10_000)
        self.assertEqual((tasks[0]["start"], tasks[-1]["stop"]), (1, generic.MT_CONTIG_LENGTH))
        generic.verify(tasks, "chrM")

if __name__ == "__main__": unittest.main()
