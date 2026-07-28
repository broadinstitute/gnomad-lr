#!/usr/bin/env python3
import importlib.util
import io
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


generic = load("generic_reconcile", "reconcile-y1-contig-source.py")
legacy = load("legacy_reconcile", "reconcile-y1-chr22-source.py")


class PerContigReconciliationTests(unittest.TestCase):
    def fixture_text(self, name):
        return (ROOT / "tests" / "fixtures" / "y1" / name).read_text()

    def manifest(self, contig):
        return json.loads((ROOT / "sources" / "y1" / f"primary-source-{contig}.json").read_text())

    def test_chr22_reconciliation_is_backward_compatible(self):
        text = self.fixture_text("hgsvc_hprc_trv_13_alt.vcf")
        generic_facts = generic.reconcile(io.StringIO(text), "hgsvc_hprc", "chr22")
        self.assertEqual(generic_facts, legacy.reconcile(io.StringIO(text), "hgsvc_hprc"))
        output = generic.build_output(
            json.loads((ROOT / "sources" / "y1" / "primary-source-manifest.json").read_text()),
            io.StringIO(text), "hgsvc_hprc", "chr22", "run", "gs://evidence", "test",
        )
        self.assertEqual(output["contract_version"], 1)
        self.assertEqual(output["chrom"], "chr22")
        self.assertEqual(output["facts"], generic_facts)
        self.assertEqual(output["counts"], {
            "source_records": 1, "summaries": 1, "alleles": 13,
            "frequencies": 273, "carriers": 214, "rejects": 0,
        })

    def test_other_grch38_contig_uses_exact_declared_length(self):
        text = self.fixture_text("aou_summary_only_ins.vcf")
        text = text.replace("ID=chr22,length=50818468", "ID=chr1,length=248956422", 1)
        text = text.replace("\nchr22\t", "\nchr1\t")
        facts = generic.reconcile(io.StringIO(text), "aou", "chr1")
        self.assertEqual(facts["source_records"], 1)
        self.assertEqual(facts["carrier_alt_copies"], 0)
        self.assertEqual(facts["called_alleles"], 0)

        wrong_length = text.replace("ID=chr1,length=248956422", "ID=chr1,length=248956421", 1)
        with self.assertRaisesRegex(ValueError, "exactly GRCh38 chr1 length"):
            generic.reconcile(io.StringIO(wrong_length), "aou", "chr1")

    def test_cross_contig_manifest_and_vcf_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "must describe Y1 chr2"):
            generic.checked_source(self.manifest("chr1"), "aou", "chr2")

        text = self.fixture_text("aou_summary_only_ins.vcf")
        with self.assertRaisesRegex(ValueError, "exactly GRCh38 chr1 length"):
            generic.reconcile(io.StringIO(text), "aou", "chr1")

    def test_source_name_mismatch_is_rejected(self):
        manifest = self.manifest("chr1")
        manifest["objects"][0]["name"] = "gnomAD_LR_Y1.hgsvc_hprc.chr2.vcf.gz"
        with self.assertRaisesRegex(ValueError, "canonical chr1 VCF/TBI pair"):
            generic.checked_source(manifest, "hgsvc_hprc", "chr1")

    def test_all_primary_contigs_have_checked_grch38_identity(self):
        self.assertEqual(len(generic.GRCH38_CONTIG_LENGTHS), 24)
        for contig in generic.GRCH38_CONTIG_LENGTHS:
            for cohort in generic.COHORTS:
                source = generic.checked_source(self.manifest(contig), cohort, contig)
                self.assertIn(f".{contig}.vcf.gz", source["uri"])
                self.assertGreater(source["size"], 0)


if __name__ == "__main__":
    unittest.main()
