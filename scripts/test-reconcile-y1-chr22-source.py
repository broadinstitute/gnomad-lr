#!/usr/bin/env python3
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = Path(__file__).with_name("reconcile-y1-chr22-source.py")
spec = importlib.util.spec_from_file_location("reconcile", SCRIPT)
reconcile = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(reconcile)


class ReconciliationTests(unittest.TestCase):
    def fixture(self, name, cohort):
        with (ROOT / "tests" / "fixtures" / "y1" / name).open() as stream:
            return reconcile.reconcile(stream, cohort)

    def test_hgsvc_genotypes_and_annotations_are_independent_facts(self):
        facts = self.fixture("hgsvc_hprc_trv_13_alt.vcf", "hgsvc_hprc")
        self.assertEqual(facts["source_records"], 1)
        self.assertEqual(facts["alt_alleles"], 13)
        self.assertEqual(facts["frequency_rows"], 273)
        self.assertEqual(facts["carrier_alt_copies"], 214)
        self.assertEqual(facts["called_alleles"], 584)
        self.assertEqual(len(facts["genotype_content_sha256"]), 64)

    def test_aou_has_no_genotypes_or_carriers(self):
        facts = self.fixture("aou_summary_only_ins.vcf", "aou")
        self.assertEqual(facts["source_records"], 1)
        self.assertEqual(facts["alt_alleles"], 1)
        self.assertEqual(facts["frequency_rows"], 6)
        self.assertEqual(facts["called_alleles"], 0)
        self.assertEqual(facts["carrier_alt_copies"], 0)


if __name__ == "__main__":
    unittest.main()
