#!/usr/bin/env python3
"""Convert a checked full-genome inventory into one immutable per-contig manifest.

The inventory's proposed mirror URIs are never treated as evidence that an object
exists. A pair is convertible only after both destination objects have recorded
GCS generations and identities matching their immutable source objects.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

COHORTS = ("hgsvc_hprc", "aou")
NON_MT_CONTIGS = tuple([*(f"chr{i}" for i in range(1, 23)), "chrX", "chrY"])
MIRROR_ROOT = "gs://gnomad-lr-data/y1/sources"


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_inventory(inventory: dict[str, Any]) -> None:
    if (inventory.get("schema_version"), inventory.get("release"), inventory.get("reference_genome")) != (1, "Y1", "GRCh38"):
        raise ValueError("unsupported full-genome source inventory contract")
    if inventory.get("chromosome_order") != list(NON_MT_CONTIGS):
        raise ValueError("inventory must enumerate exactly chr1-22,X,Y in canonical order")
    if inventory.get("mirror_root") != MIRROR_ROOT:
        raise ValueError("inventory mirror_root differs from the Rust canonical Y1 mirror contract")
    objects = inventory.get("objects")
    if not isinstance(objects, list) or len(objects) != len(NON_MT_CONTIGS) * len(COHORTS):
        raise ValueError("inventory must contain exactly two cohorts for every non-MT contig")
    keys = [(o.get("cohort"), o.get("chrom")) for o in objects if isinstance(o, dict)]
    expected = [(cohort, contig) for cohort in COHORTS for contig in NON_MT_CONTIGS]
    if sorted(keys) != sorted(expected) or len(set(keys)) != len(keys):
        raise ValueError("inventory cohort/contig coverage is incomplete or duplicated")
    mt = inventory.get("mt")
    if not isinstance(mt, dict) or mt.get("classification") not in {"absent", "available"}:
        raise ValueError("inventory must carry an explicit MT availability classification")
    claimed = inventory.get("canonical_payload_sha256_before_digest_field")
    if claimed is not None:
        payload = dict(inventory)
        del payload["canonical_payload_sha256_before_digest_field"]
        if claimed != canonical_digest(payload):
            raise ValueError("inventory canonical payload digest does not match")


def _checked_source_object(obj: dict[str, Any], kind: str, contig: str) -> dict[str, Any]:
    value = obj.get(kind)
    cohort = obj.get("cohort")
    name = f"gnomAD_LR_Y1.{cohort}.{contig}.vcf.gz" + (".tbi" if kind == "tbi" else "")
    if not isinstance(value, dict) or Path(str(value.get("uri", ""))).name != name:
        raise ValueError(f"{cohort} {contig} has an invalid source {kind} identity")
    if not str(value.get("generation", "")).isdigit() or int(value.get("size_bytes", 0)) <= 0 or not value.get("md5_base64"):
        raise ValueError(f"{obj.get('cohort')} {contig} source {kind} is not immutable")
    return value


def _checked_destination(obj: dict[str, Any], kind: str, source: dict[str, Any], contig: str) -> dict[str, Any]:
    if obj.get("mirror_pair_present") is not True or obj.get(f"mirror_{kind}_present") is not True:
        raise ValueError(f"{obj.get('cohort')} {contig} destination pair is not recorded as present")
    identities = obj.get("mirror_identity")
    matches = [x for x in identities if isinstance(x, dict) and x.get("kind") == kind] if isinstance(identities, list) else []
    if len(matches) != 1:
        raise ValueError(f"{obj.get('cohort')} {contig} has no unique destination {kind} identity")
    dest = matches[0]
    expected_uri = f"{MIRROR_ROOT}/{obj.get('cohort')}/vcfs/{Path(source['uri']).name}"
    if (dest.get("uri") != expected_uri
            or not str(dest.get("generation", "")).isdigit()
            or dest.get("matches_source_size_md5") is not True
            or int(dest.get("size_bytes", 0)) != int(source["size_bytes"])
            or dest.get("md5_base64") != source["md5_base64"]):
        raise ValueError(f"{obj.get('cohort')} {contig} destination {kind} does not match its immutable source")
    return dest


def convert(inventory: dict[str, Any], contig: str) -> dict[str, Any]:
    validate_inventory(inventory)
    allowed = set(NON_MT_CONTIGS)
    if contig == "chrM":
        mt = inventory["mt"]
        if mt.get("classification") != "available" or mt.get("immutable_source_contract") is not True:
            raise ValueError("chrM is unavailable without an explicit immutable MT source contract")
        allowed.add("chrM")
    if contig not in allowed:
        raise ValueError("unsupported or unavailable GRCh38 contig")

    source_entries = inventory["mt"].get("objects", []) if contig == "chrM" else inventory["objects"]
    selected = [o for o in source_entries if o.get("chrom") == contig]
    if len(selected) != len(COHORTS) or {o.get("cohort") for o in selected} != set(COHORTS):
        raise ValueError(f"inventory does not contain both cohorts for {contig}")
    mirror_root = inventory["mirror_root"].rstrip("/")
    output_objects = []
    for obj in sorted(selected, key=lambda o: COHORTS.index(o["cohort"])):
        cohort = obj["cohort"]
        if obj.get("index_adjacency") is not True or obj.get("contig_naming", {}).get("consistent") is not True:
            raise ValueError(f"{cohort} {contig} failed source adjacency/contig checks")
        vcf = _checked_source_object(obj, "vcf", contig)
        tbi = _checked_source_object(obj, "tbi", contig)
        vcf_dest = _checked_destination(obj, "vcf", vcf, contig)
        tbi_dest = _checked_destination(obj, "tbi", tbi, contig)
        name = Path(vcf["uri"]).name
        expected_uri = f"{mirror_root}/{cohort}/vcfs/{name}"
        if obj.get("proposed_mirror_uri") != expected_uri or obj.get("proposed_mirror_index_uri") != expected_uri + ".tbi":
            raise ValueError(f"{cohort} {contig} destination URI is outside the mirror contract")
        output_objects.extend([
            {"cohort": cohort, "name": name, "source_generation": str(vcf["generation"]),
             "mirror_generation": str(vcf_dest["generation"]), "size": int(vcf_dest["size_bytes"]),
             "md5_base64": vcf_dest["md5_base64"]},
            {"cohort": cohort, "name": name + ".tbi", "source_generation": str(tbi["generation"]),
             "mirror_generation": str(tbi_dest["generation"]), "size": int(tbi_dest["size_bytes"]),
             "md5_base64": tbi_dest["md5_base64"]},
        ])
    return {
        "schema_version": 2,
        "contract_type": "y1_per_contig_immutable_source",
        "release": "Y1",
        "reference_genome": "GRCh38",
        "chromosome": contig,
        "mirror_prefix": mirror_root,
        "full_genome_inventory_sha256": canonical_digest(inventory),
        "mt_enabled": contig == "chrM",
        "objects": output_objects,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--contig", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text())
    expected = convert(inventory, args.contig)
    if args.check:
        if json.loads(args.output.read_text()) != expected:
            raise ValueError("per-contig manifest differs from checked inventory conversion")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(expected, indent=2) + "\n")
    print(json.dumps({"contig": args.contig, "manifest": str(args.output), "objects": len(expected["objects"])}, sort_keys=True))


if __name__ == "__main__":
    main()
