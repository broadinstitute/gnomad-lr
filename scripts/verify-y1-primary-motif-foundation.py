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
GENOTYPE_EXPECTATIONS = ROOT / "tests/fixtures/y1/primary_motif_genotype_source_expectations.json"
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


def validate_genotype_expectations(value: dict) -> None:
    if set(value) != {"contract", "provenance", "loci"}:
        raise ValueError("genotype source-expectation fields drifted")
    if value["contract"] != "Y1_PRIMARY_MOTIF_GENOTYPE_SOURCE_EXPECTATIONS_V1":
        raise ValueError("unsupported genotype source-expectation contract")
    expected = {
        "HTT": ("chr4-3074876-TRV-164", "CAG", 72, 584, 292, 0, 58, "a336373f32c9bf0716b097f959bbb7bcf85d85e01ec333e3451a5eecd47f34af"),
        "ATXN1": ("chr6-16327633-TRV-90", "TGC", 76, 584, 292, 0, 54, "6aa57bbabd660055bdecd4414a5f5cd5ac498460c781f56efb8d3eb8b51e4a4c"),
        "RFC1": ("chr4-39348424-TRV-55", "AAAAG", 200, 582, 291, 1, 170, "20d6b33700bc84f067ac77a43a9bb15435e198ea5c36a98e2ca464f1fc9cb101"),
    }
    observed = {}
    for locus in value["loci"]:
        required = {
            "catalog_id", "source_variant_id", "motif", "source_alt_count", "info_an",
            "called_diploid_people", "partial_diploid_people", "no_call_people",
            "non_diploid_people", "cell_people", "observed_alt_margins_match_info",
            "source_uri", "source_generation", "source_index_generation",
            "prototype_artifact_sha256", "source_cells_sha256", "source_cell_count", "cells",
        }
        if set(locus) != required:
            raise ValueError("genotype locus expectation fields drifted")
        if locus["partial_diploid_people"] != 0 or locus["non_diploid_people"] != 0:
            raise ValueError("admitted source expectation unexpectedly changed GT call classes")
        if locus["cell_people"] != locus["called_diploid_people"]:
            raise ValueError("source genotype cells do not reconcile to called people")
        if (
            not locus["source_uri"].startswith("gs://gnomad-lr-data/y1/sources/hgsvc_hprc/")
            or not str(locus["source_generation"]).isdigit()
            or not str(locus["source_index_generation"]).isdigit()
            or not SHA256.fullmatch(locus["prototype_artifact_sha256"])
            or not SHA256.fullmatch(locus["source_cells_sha256"])
        ):
            raise ValueError("source genotype prototype identity receipt is incomplete")
        canonical_cells = json.dumps(
            locus["cells"], sort_keys=True, separators=(",", ":")
        ).encode()
        if hashlib.sha256(canonical_cells).hexdigest() != locus["source_cells_sha256"]:
            raise ValueError("source genotype cell digest mismatch")
        if len(locus["cells"]) != locus["source_cell_count"] or sum(locus["cells"].values()) != locus["cell_people"]:
            raise ValueError("source genotype cell fixture is incomplete")
        if locus["called_diploid_people"] + locus["no_call_people"] != 292:
            raise ValueError("source genotype call classes do not partition the 292-person roster")
        if locus["info_an"] != 2 * locus["called_diploid_people"]:
            raise ValueError("source genotype INFO/AN does not reconcile to complete diploid calls")
        if locus["observed_alt_margins_match_info"] is not True:
            raise ValueError("source genotype ALT margins are not exact")
        observed[locus["catalog_id"]] = (
            locus["source_variant_id"], locus["motif"], locus["source_alt_count"],
            locus["info_an"], locus["called_diploid_people"], locus["no_call_people"],
            locus["source_cell_count"], locus["source_cells_sha256"],
        )
    if observed != expected:
        raise ValueError("HTT/ATXN1/RFC1 source genotype expectations drifted")


EXPECTED_DDL = {
    "lr_y1_primary_motif_runs.sql",
    "lr_y1_primary_motif_loci.sql",
    "lr_y1_primary_motif_allele_bins.sql",
    "lr_y1_primary_motif_genotype_pairs.sql",
    "lr_y1_primary_motif_genotype_margins.sql",
}


def validate_storage_contract(ddls: dict[str, str], rust_source: str) -> None:
    if set(ddls) != EXPECTED_DDL:
        raise ValueError("primary-motif DDL inventory mismatch")
    required_by_table = {
        "lr_y1_primary_motif_runs.sql": {
            "product_run_id", "primary_run_id", "registry_digest", "registry_approval_state",
            "max_genotype_pairs_per_stratum", "max_genotype_cells_per_stratum",
            "max_serialized_aggregate_bytes", "bounds_status", "metadata_run_id",
            "accepted_metadata_receipt_sha256", "metadata_manifest_sha256",
            "header_roster_sha256", "header_mapping_sha256", "genotype_pair_rows",
            "genotype_margin_rows", "called_diploid_people", "partial_diploid_people",
            "no_call_people", "non_diploid_people", "receipt_sha256", "serialized_bytes",
        },
        "lr_y1_primary_motif_loci.sql": {
            "product_run_id", "primary_run_id", "source_variant_id", "registry_digest",
            "source_generation", "source_index_generation", "source_record_sha256",
            "genotype_content_sha256", "genotype_status", "genotype_reason_code",
            "called_diploid_people", "partial_diploid_people", "no_call_people",
            "non_diploid_people", "genotype_observed_an", "metadata_run_id",
            "accepted_metadata_receipt_sha256", "metadata_manifest_sha256",
            "header_roster_sha256", "header_mapping_sha256", "bounds_status",
            "genotype_receipt_sha256", "serialized_bytes",
        },
        "lr_y1_primary_motif_allele_bins.sql": {
            "product_run_id", "primary_run_id", "source_variant_id", "registry_digest",
            "stratum_an", "stratum_ref_copies", "reference_copies", "alternate_copies",
            "stratum_receipt_sha256",
        },
        "lr_y1_primary_motif_genotype_pairs.sql": {
            "product_run_id", "primary_run_id", "source_variant_id", "registry_digest",
            "shorter_allele_index", "longer_allele_index", "shorter_exact_units",
            "longer_exact_units", "people", "phased_people", "unphased_people",
            "pair_receipt_sha256",
        },
        "lr_y1_primary_motif_genotype_margins.sql": {
            "product_run_id", "primary_run_id", "source_variant_id", "registry_digest",
            "allele_index", "expected_copies", "paired_copies",
            "excluded_from_pairs_copies", "margin_receipt_sha256",
        },
    }
    for name, required_fields in required_by_table.items():
        missing = sorted(field for field in required_fields if field not in ddls[name])
        if missing:
            raise ValueError(f"{name} lacks required fields {missing}")
    for required in (
        "WHOLE_RECORD_EXACT_PRIMARY_MOTIF_UNITS_V1",
        "AGGREGATE_ONLY_SOURCE_NO_GT_PAIRING",
        "MAX_GENOTYPE_PAIRS_PER_STRATUM",
        "MAX_SERIALIZED_AGGREGATE_BYTES",
    ):
        if required not in rust_source:
            raise ValueError(f"primary-motif Rust contract lacks {required}")
    combined = "\n".join(ddls[name] for name in sorted(ddls))
    schema_only = "\n".join(
        line for line in combined.splitlines() if not line.lstrip().startswith("--")
    ).lower()
    for forbidden in ("sample_id", "person_id", "participant_id", "raw_gt", "raw_carrier"):
        if forbidden in schema_only:
            raise ValueError(f"aggregate-only product contains forbidden field {forbidden}")
    runs = ddls["lr_y1_primary_motif_runs.sql"]
    if "ReplacingMergeTree(revision)" not in runs or "state LowCardinality(String)" not in runs:
        raise ValueError("product run ledger lacks revisioned lifecycle state")
    for name in EXPECTED_DDL - {"lr_y1_primary_motif_runs.sql"}:
        if "product_run_id" not in ddls[name] or "PARTITION BY" not in ddls[name]:
            raise ValueError(f"{name} is not product-run partitioned")


def validate_storage() -> None:
    paths = {path.name: path.read_text() for path in DDL_DIR.glob("*.sql")}
    validate_storage_contract(paths, (ROOT / "src/y1/primary_motif.rs").read_text())


def main() -> None:
    registry = json.loads(REGISTRY.read_text())
    json.loads(SCHEMA.read_text())
    validate_registry(registry)
    validate_genotype_expectations(json.loads(GENOTYPE_EXPECTATIONS.read_text()))
    validate_storage()
    print(
        "Primary-motif foundation verified: 3 candidate_pending_science fixtures, "
        "generation and metadata receipts, anonymous exact genotype margins, "
        "aggregate-only DDL, no production approval"
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"primary-motif foundation verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
