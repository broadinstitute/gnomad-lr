#!/usr/bin/env python3
import copy
import importlib.util
import unittest
from pathlib import Path

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location("converter", HERE / "convert-y1-full-genome-source-inventory.py")
converter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(converter)


def synthetic_inventory() -> dict:
    objects = []
    ordinal = 0
    for cohort in converter.COHORTS:
        for contig in converter.NON_MT_CONTIGS:
            ordinal += 1
            name = f"gnomAD_LR_Y1.{cohort}.{contig}.vcf.gz"
            source_root = f"gs://approved-source/{cohort}/vcfs"
            destination_root = f"{converter.MIRROR_ROOT}/{cohort}/vcfs"
            vcf = {"uri": f"{source_root}/{name}", "generation": str(1000 + ordinal),
                   "size_bytes": 100000 + ordinal, "md5_base64": f"vcf-md5-{ordinal}"}
            tbi = {"uri": f"{source_root}/{name}.tbi", "generation": str(2000 + ordinal),
                   "size_bytes": 1000 + ordinal, "md5_base64": f"tbi-md5-{ordinal}"}
            objects.append({
                "cohort": cohort, "chrom": contig, "index_adjacency": True,
                "contig_naming": {"consistent": True}, "vcf": vcf, "tbi": tbi,
                "proposed_mirror_uri": f"{destination_root}/{name}",
                "proposed_mirror_index_uri": f"{destination_root}/{name}.tbi",
                "mirror_pair_present": True, "mirror_vcf_present": True, "mirror_tbi_present": True,
                "mirror_identity": [
                    {"kind": "vcf", "uri": f"{destination_root}/{name}",
                     "generation": str(3000 + ordinal), "size_bytes": vcf["size_bytes"],
                     "md5_base64": vcf["md5_base64"], "matches_source_size_md5": True},
                    {"kind": "tbi", "uri": f"{destination_root}/{name}.tbi",
                     "generation": str(4000 + ordinal), "size_bytes": tbi["size_bytes"],
                     "md5_base64": tbi["md5_base64"], "matches_source_size_md5": True},
                ],
            })
    inventory = {
        "schema_version": 1, "release": "Y1", "reference_genome": "GRCh38",
        "chromosome_order": list(converter.NON_MT_CONTIGS),
        "mirror_root": converter.MIRROR_ROOT, "objects": objects,
        "mt": {"classification": "absent"},
    }
    inventory["canonical_payload_sha256_before_digest_field"] = converter.canonical_digest(inventory)
    return inventory


class InventoryConversionTest(unittest.TestCase):
    def setUp(self):
        self.inventory = synthetic_inventory()

    def test_all_non_mt_contigs_convert_with_exact_destination_identity(self):
        for contig in converter.NON_MT_CONTIGS:
            manifest = converter.convert(self.inventory, contig)
            self.assertEqual((manifest["chromosome"], len(manifest["objects"])), (contig, 4))
            self.assertFalse(manifest["mt_enabled"])
            for obj in manifest["objects"]:
                self.assertTrue(obj["mirror_generation"].isdigit())
                self.assertGreater(obj["size"], 0)
                self.assertTrue(obj["md5_base64"])

    def test_proposed_destination_is_not_existence_evidence(self):
        inventory = copy.deepcopy(self.inventory)
        inventory.pop("canonical_payload_sha256_before_digest_field")
        obj = inventory["objects"][0]
        obj["mirror_pair_present"] = obj["mirror_vcf_present"] = False
        with self.assertRaisesRegex(ValueError, "destination pair is not recorded as present"):
            converter.convert(inventory, "chr1")

    def test_destination_uri_and_identity_must_match(self):
        for mutation, message in (("uri", "does not match"), ("md5_base64", "does not match")):
            inventory = copy.deepcopy(self.inventory)
            inventory.pop("canonical_payload_sha256_before_digest_field")
            identity = inventory["objects"][0]["mirror_identity"][0]
            identity[mutation] += "-changed"
            with self.assertRaisesRegex(ValueError, message):
                converter.convert(inventory, "chr1")

    def test_mt_is_explicitly_unavailable(self):
        with self.assertRaisesRegex(ValueError, "explicit immutable MT source contract"):
            converter.convert(self.inventory, "chrM")

    def test_inventory_digest_drift_is_rejected(self):
        inventory = copy.deepcopy(self.inventory)
        inventory["objects"][0]["vcf"]["size_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "digest"):
            converter.validate_inventory(inventory)

    def test_noncanonical_mirror_root_is_rejected(self):
        inventory = copy.deepcopy(self.inventory)
        inventory.pop("canonical_payload_sha256_before_digest_field")
        inventory["mirror_root"] = "gs://other/y1/sources"
        with self.assertRaisesRegex(ValueError, "Rust canonical"):
            converter.validate_inventory(inventory)


if __name__ == "__main__":
    unittest.main()
