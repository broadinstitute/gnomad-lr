#!/usr/bin/env python3
"""Verify the non-deployed Y1 primary-motif registry and storage foundation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "sources/y1/primary-repeat-registry.json"
SCHEMA = ROOT / "sources/y1/primary-repeat-registry.schema.json"
DDL_DIR = ROOT / "sql/y1/primary_motif"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
MOTIF = re.compile(r"^[ACGT]+$")


def canonical_digest(value: dict) -> str:
    value = dict(value)
    value.pop("content_sha256", None)
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def validate_registry(value: dict) -> None:
    expected_top = {
        "schema_version", "contract", "release", "reference_genome",
        "source_inventory_sha256", "approval_state", "design_authorization_receipt",
        "entries", "content_sha256",
    }
    if set(value) != expected_top:
        raise ValueError("registry top-level fields do not match the v1 contract")
    if (value["schema_version"], value["contract"], value["release"], value["reference_genome"]) != (
        1, "Y1_PRIMARY_REPEAT_REGISTRY_V1", "Y1", "GRCh38"
    ):
        raise ValueError("unsupported registry identity")
    if value["approval_state"] not in {"CANDIDATE_PENDING_SCIENCE", "REVIEWED"}:
        raise ValueError("invalid registry approval state")
    if not value["design_authorization_receipt"]:
        raise ValueError("missing operator-approved design receipt")
    for field in ("source_inventory_sha256", "content_sha256"):
        if not SHA256.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    if canonical_digest(value) != value["content_sha256"]:
        raise ValueError("registry canonical digest mismatch")

    expected_entry_fields = {
        "registry_entry_id", "catalog_id", "canonical_locus_id", "source_variant_id",
        "chrom", "source_position", "ordered_components", "component_index", "motif",
        "selection_basis", "biological_role", "approval_state", "reviewer",
        "approval_receipt", "catalog_digest",
    }
    ids: set[str] = set()
    source_pairs: set[tuple[str, str]] = set()
    by_catalog: dict[str, dict] = {}
    for entry in value["entries"]:
        if set(entry) != expected_entry_fields:
            raise ValueError("registry entry fields do not match the v1 contract")
        if entry["registry_entry_id"] in ids:
            raise ValueError("duplicate registry entry ID")
        ids.add(entry["registry_entry_id"])
        pair = (entry["canonical_locus_id"], entry["source_variant_id"])
        if pair in source_pairs:
            raise ValueError("duplicate canonical/source identity")
        source_pairs.add(pair)
        if not entry["ordered_components"]:
            raise ValueError("entry has no ordered components")
        for component in entry["ordered_components"]:
            if set(component) != {"start0", "end0", "motif"}:
                raise ValueError("component fields do not match the v1 contract")
            if component["start0"] >= component["end0"] or not MOTIF.fullmatch(component["motif"]):
                raise ValueError("invalid component")
        index = entry["component_index"]
        if not isinstance(index, int) or index < 0 or index >= len(entry["ordered_components"]):
            raise ValueError("component index is out of bounds")
        if entry["motif"] != entry["ordered_components"][index]["motif"]:
            raise ValueError("selected motif does not preserve exact stored component orientation")
        if entry["source_position"] != min(c["start0"] for c in entry["ordered_components"]):
            raise ValueError("source position does not satisfy the envelope-left-padding rule")
        if entry["approval_state"] != value["approval_state"]:
            raise ValueError("entry approval state differs from registry")
        if entry["approval_state"] == "CANDIDATE_PENDING_SCIENCE":
            if entry["reviewer"] is not None or entry["approval_receipt"] is not None:
                raise ValueError("candidate entry falsely claims science approval")
        elif (
            not isinstance(entry["reviewer"], str)
            or not entry["reviewer"].strip()
            or not isinstance(entry["approval_receipt"], str)
            or not entry["approval_receipt"].strip()
            or entry["catalog_digest"] is None
        ):
            raise ValueError("reviewed entry lacks reviewer, approval receipt, or catalog digest")
        if entry["catalog_digest"] is not None and not SHA256.fullmatch(entry["catalog_digest"]):
            raise ValueError("invalid catalog digest")
        by_catalog[entry["catalog_id"]] = entry

    expected = {
        "HTT": ("chr4-3074876-TRV-164", "CAG", 0, "coding polyglutamine repeat", 6),
        "ATXN1": ("chr6-16327633-TRV-90", "TGC", 0, "stored-orientation disease-associated repeat", 1),
        "RFC1": ("chr4-39348424-TRV-55", "AAAAG", 0, "benign reference motif", 1),
    }
    if set(by_catalog) != set(expected):
        raise ValueError("candidate fixture set must be exactly HTT, ATXN1, and RFC1")
    for catalog, (source, motif, index, role, component_count) in expected.items():
        entry = by_catalog[catalog]
        actual = (
            entry["source_variant_id"], entry["motif"], entry["component_index"],
            entry["biological_role"], len(entry["ordered_components"]),
        )
        if actual != (source, motif, index, role, component_count):
            raise ValueError(f"{catalog} candidate identity fixture drifted")

    for chrom in {entry["chrom"] for entry in value["entries"]}:
        manifest = json.loads((ROOT / f"sources/y1/primary-source-{chrom}.json").read_text())
        if manifest["full_genome_inventory_sha256"] != value["source_inventory_sha256"]:
            raise ValueError(f"{chrom} source inventory receipt differs from registry")
        for cohort in ("hgsvc_hprc", "aou"):
            objects = [obj for obj in manifest["objects"] if obj["cohort"] == cohort]
            if len(objects) != 2 or {obj["name"].endswith(".tbi") for obj in objects} != {False, True}:
                raise ValueError(f"{chrom} {cohort} lacks one immutable VCF/TBI pair")
            for obj in objects:
                if not obj["mirror_generation"] or not obj["size"] or not obj["md5_base64"]:
                    raise ValueError(f"{chrom} {cohort} immutable identity is incomplete")


def validate_storage() -> None:
    expected = {
        "lr_y1_primary_motif_runs.sql",
        "lr_y1_primary_motif_loci.sql",
        "lr_y1_primary_motif_allele_bins.sql",
    }
    if {path.name for path in DDL_DIR.glob("*.sql")} != expected:
        raise ValueError("primary-motif DDL inventory mismatch")
    combined = "\n".join((DDL_DIR / name).read_text() for name in sorted(expected))
    contract_source = combined + "\n" + (ROOT / "src/y1/primary_motif.rs").read_text()
    for required in (
        "product_run_id", "primary_run_id", "source_variant_id", "registry_digest",
        "registry_approval_state", "WHOLE_RECORD", "anchor_rule", "source_generation",
        "source_index_generation", "source_record_sha256", "stratum_an", "stratum_ref_copies",
        "reference_copies", "alternate_copies", "receipt_sha256", "serialized_bytes",
    ):
        if required not in contract_source:
            raise ValueError(f"primary-motif storage contract lacks {required}")
    schema_only = "\n".join(
        line for line in combined.splitlines() if not line.lstrip().startswith("--")
    ).lower()
    for forbidden in ("sample_id", "person_id", "raw_carrier", "genotype_pair"):
        if forbidden in schema_only:
            raise ValueError(f"aggregate-only product contains forbidden field {forbidden}")
    runs = (DDL_DIR / "lr_y1_primary_motif_runs.sql").read_text()
    if "ReplacingMergeTree(revision)" not in runs or "state LowCardinality(String)" not in runs:
        raise ValueError("product run ledger lacks revisioned lifecycle state")
    for name in expected - {"lr_y1_primary_motif_runs.sql"}:
        ddl = (DDL_DIR / name).read_text()
        if "product_run_id" not in ddl or "PARTITION BY" not in ddl:
            raise ValueError(f"{name} is not product-run partitioned")


def main() -> None:
    registry = json.loads(REGISTRY.read_text())
    json.loads(SCHEMA.read_text())
    validate_registry(registry)
    validate_storage()
    print(
        "Primary-motif foundation verified: 3 candidate_pending_science fixtures, "
        "generation receipts, aggregate-only DDL, no production approval"
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"primary-motif foundation verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
