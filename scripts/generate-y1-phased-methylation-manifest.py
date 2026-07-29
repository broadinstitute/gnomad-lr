#!/usr/bin/env python3
"""Generate the repository-owned Y1 phased methylation v2 manifest.

The checked narrow discovery TSV establishes only sample membership and the four
phased paths. It is deliberately insufficient to authorize a read. A normalized
Terra entity snapshot can be supplied later, but must pin every object and a
separately discovered combined BED index before the generated contract becomes
load-ready. The Terra wide entity-table download is never an accepted input.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "sources" / "y1"
DISCOVERY_SHA256 = "a0ea03cb7af9a0bf39ca6831bb93c02acfcc71606ee3427dbf9abded28e2bca4"
DISCOVERY_FIELDS = ("sample_id", "hap1_bed", "hap1_tbi", "hap2_bed", "hap2_tbi")
TERRA_FIELDS = ("cpg_combined_bed", "cpg_hap1_bed", "cpg_hap1_bed_idx", "cpg_hap2_bed", "cpg_hap2_bed_idx")
WORKSPACE = {"namespace": "talk-LR-gnomADLR_supplement", "name": "gnomAD_LR"}
ENTITY_TYPE = "LR_sample"
SKIP_SAMPLE = "HG00272"
OBJECT_SLOTS = {
    "combined_bed": "cpg_combined_bed",
    "combined_bed_index": "independently_verified_combined_bed_index",
    "hap1_bed": "cpg_hap1_bed",
    "hap1_bed_index": "cpg_hap1_bed_idx",
    "hap2_bed": "cpg_hap2_bed",
    "hap2_bed_index": "cpg_hap2_bed_idx",
}
BLOCKERS = [
    "frozen Terra LR_sample entity snapshot, capture time, and snapshot SHA-256 are unavailable",
    "cpg_combined_bed paths and generation-pinned combined BED indexes are unavailable",
    "object generations, byte sizes, checksums, creation times, and generation-bound read URIs are unavailable for the 231 phased source pairs",
]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_hash(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return sha256_bytes(encoded)


def read_discovery(path: Path) -> dict[str, dict[str, str]]:
    lowered = path.name.lower()
    if "lr_sample" in lowered or "entity" in lowered or "wide" in lowered:
        raise SystemExit("the Terra wide entity-table TSV is forbidden; use the frozen narrow discovery manifest")
    raw = path.read_bytes()
    actual_hash = sha256_bytes(raw)
    if actual_hash != DISCOVERY_SHA256:
        raise SystemExit(f"discovery manifest SHA-256 drift: expected {DISCOVERY_SHA256}, got {actual_hash}")
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if tuple(reader.fieldnames or ()) != DISCOVERY_FIELDS:
            raise SystemExit(f"discovery manifest fields must be exactly {DISCOVERY_FIELDS}")
        rows = list(reader)
    if len(rows) != 231:
        raise SystemExit(f"discovery manifest must contain exactly 231 rows, got {len(rows)}")
    sample_ids = [row["sample_id"] for row in rows]
    if sample_ids != sorted(sample_ids) or len(sample_ids) != len(set(sample_ids)):
        raise SystemExit("discovery sample IDs must be unique and sorted")
    if SKIP_SAMPLE in sample_ids:
        raise SystemExit(f"{SKIP_SAMPLE} must not be in the phased discovery inventory")
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        sample_id = row["sample_id"]
        if any(not row[field] for field in DISCOVERY_FIELDS):
            raise SystemExit(f"{sample_id}: discovery row contains an empty field")
        for field in DISCOVERY_FIELDS[1:]:
            uri = row[field]
            suffix = {
                "hap1_bed": ".hap1.bed.gz",
                "hap1_tbi": ".hap1.bed.gz.tbi",
                "hap2_bed": ".hap2.bed.gz",
                "hap2_tbi": ".hap2.bed.gz.tbi",
            }[field]
            if not uri.startswith("gs://") or not uri.endswith(f"/{sample_id}{suffix}"):
                raise SystemExit(f"{sample_id}: invalid {field} discovery URI")
        if row["hap1_tbi"] != row["hap1_bed"] + ".tbi" or row["hap2_tbi"] != row["hap2_bed"] + ".tbi":
            raise SystemExit(f"{sample_id}: phased index path is not adjacent to its BED")
        result[sample_id] = row
    return result


def read_roster(path: Path) -> list[str]:
    roster = path.read_text().splitlines()
    if len(roster) != 292 or len(roster) != len(set(roster)) or any(not item for item in roster):
        raise SystemExit("roster must contain exactly 292 unique nonblank sample IDs")
    if roster != sorted(roster):
        raise SystemExit("roster must be sorted")
    if SKIP_SAMPLE not in roster:
        raise SystemExit(f"roster does not contain required source-marked skip {SKIP_SAMPLE}")
    return roster


def validate_history(path: Path, discovery_ids: set[str]) -> dict[str, Any]:
    historical = json.loads(path.read_text())
    recorded_hash = historical.get("content_sha256")
    payload = dict(historical)
    payload.pop("content_sha256", None)
    if canonical_hash(payload) != recorded_hash:
        raise SystemExit("historical methylation manifest canonical hash mismatch")
    historical_ids = {entry["sample_id"] for entry in historical["samples"]}
    if len(historical_ids) != 232 or historical_ids - discovery_ids != {SKIP_SAMPLE} or discovery_ids - historical_ids:
        raise SystemExit("historical/new methylation membership difference must be exactly {HG00272}")
    return {
        "path": path.name,
        "schema_version": historical["schema_version"],
        "content_sha256": recorded_hash,
        "sample_count": len(historical_ids),
        "status": "superseded_history_only_not_a_v2_load_source",
    }


def immutable_object(value: Any, label: str, expected_uri: str | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemExit(f"{label}: normalized Terra snapshot object is missing")
    required = {"uri", "generation", "byte_size", "checksum", "created_at", "immutable_read_uri"}
    if set(value) != required:
        raise SystemExit(f"{label}: immutable object fields must be exactly {sorted(required)}")
    if expected_uri is not None and value["uri"] != expected_uri:
        raise SystemExit(f"{label}: object URI differs from the frozen discovery tuple")
    if not isinstance(value["generation"], str) or not value["generation"].isdigit():
        raise SystemExit(f"{label}: generation must be a nonempty decimal string")
    if not isinstance(value["byte_size"], int) or value["byte_size"] <= 0:
        raise SystemExit(f"{label}: byte_size must be positive")
    checksum = value["checksum"]
    if (
        not isinstance(checksum, dict)
        or set(checksum) != {"algorithm", "value"}
        or not isinstance(checksum["algorithm"], str)
        or not checksum["algorithm"]
        or checksum["algorithm"] == "none"
        or not isinstance(checksum["value"], str)
        or not checksum["value"]
    ):
        raise SystemExit(f"{label}: checksum must be complete")
    if not isinstance(value["created_at"], str) or not value["created_at"]:
        raise SystemExit(f"{label}: created_at must be nonempty")
    if not isinstance(value["immutable_read_uri"], str) or not value["immutable_read_uri"] or value["immutable_read_uri"] == value["uri"]:
        raise SystemExit(f"{label}: immutable_read_uri must bind the read to immutable object identity")
    return dict(value)


def read_snapshot(path: Path, roster: list[str], discovery: dict[str, dict[str, str]]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if path.suffix.lower() in {".tsv", ".csv"}:
        raise SystemExit("the Terra wide entity-table download is forbidden; supply the normalized frozen JSON snapshot")
    raw = path.read_bytes()
    snapshot = json.loads(raw)
    required = {"schema_version", "workspace", "entity_type", "captured_at", "entities"}
    if set(snapshot) != required or snapshot["schema_version"] != 1:
        raise SystemExit(f"normalized Terra snapshot fields must be exactly {sorted(required)} with schema_version=1")
    if snapshot["workspace"] != WORKSPACE or snapshot["entity_type"] != ENTITY_TYPE or not snapshot["captured_at"]:
        raise SystemExit("normalized Terra snapshot workspace/entity/capture identity is invalid")
    entities = snapshot["entities"]
    if not isinstance(entities, list) or [item.get("sample_id") for item in entities] != roster:
        raise SystemExit("normalized Terra snapshot must contain the exact sorted 292-sample roster")
    resolved: dict[str, dict[str, Any]] = {}
    for entity in entities:
        if set(entity) != {"sample_id", "attributes", "verified_sidecars"}:
            raise SystemExit("normalized Terra entity fields must be sample_id, attributes, and verified_sidecars")
        sample_id = entity["sample_id"]
        attrs = entity["attributes"]
        sidecars = entity["verified_sidecars"]
        if set(attrs) != set(TERRA_FIELDS) or set(sidecars) != {"cpg_combined_bed_idx"}:
            raise SystemExit(f"{sample_id}: normalized snapshot source fields are incomplete")
        if sample_id not in discovery:
            if any(value is not None for value in attrs.values()) or sidecars["cpg_combined_bed_idx"] is not None:
                raise SystemExit(f"{sample_id}: no-output/skip entries must authorize no objects")
            continue
        row = discovery[sample_id]
        objects = {
            "combined_bed": immutable_object(attrs["cpg_combined_bed"], f"{sample_id} combined BED"),
            "combined_bed_index": immutable_object(sidecars["cpg_combined_bed_idx"], f"{sample_id} combined BED index"),
            "hap1_bed": immutable_object(attrs["cpg_hap1_bed"], f"{sample_id} hap1 BED", row["hap1_bed"]),
            "hap1_bed_index": immutable_object(attrs["cpg_hap1_bed_idx"], f"{sample_id} hap1 BED index", row["hap1_tbi"]),
            "hap2_bed": immutable_object(attrs["cpg_hap2_bed"], f"{sample_id} hap2 BED", row["hap2_bed"]),
            "hap2_bed_index": immutable_object(attrs["cpg_hap2_bed_idx"], f"{sample_id} hap2 BED index", row["hap2_tbi"]),
        }
        if objects["combined_bed_index"]["uri"] != objects["combined_bed"]["uri"] + ".tbi":
            raise SystemExit(f"{sample_id}: verified combined index is not adjacent to combined BED")
        resolved[sample_id] = objects
    return {
        "workspace": WORKSPACE,
        "entity_type": ENTITY_TYPE,
        "source_fields": list(TERRA_FIELDS),
        "captured_at": snapshot["captured_at"],
        "entity_snapshot_sha256": sha256_bytes(raw),
        "status": "frozen_normalized_snapshot_complete",
    }, resolved


def discovered_objects(row: dict[str, str]) -> dict[str, str | None]:
    return {
        "combined_bed": None,
        "combined_bed_index": None,
        "hap1_bed": row["hap1_bed"],
        "hap1_bed_index": row["hap1_tbi"],
        "hap2_bed": row["hap2_bed"],
        "hap2_bed_index": row["hap2_tbi"],
    }


def generate(discovery_path: Path, roster_path: Path, historical_path: Path, terra_snapshot: Path | None) -> dict[str, Any]:
    discovery = read_discovery(discovery_path)
    roster = read_roster(roster_path)
    discovery_ids = set(discovery)
    roster_ids = set(roster)
    if not discovery_ids <= roster_ids:
        raise SystemExit(f"unexpected discovery IDs: {sorted(discovery_ids - roster_ids)}")
    no_output = sorted(roster_ids - discovery_ids - {SKIP_SAMPLE})
    if len(no_output) != 60 or 292 != len(discovery) + len(no_output) + 1:
        raise SystemExit("roster classification must be exactly 292 = 231 source_present + 60 no_methylation_output + 1 source_marked_skip")
    history = validate_history(historical_path, discovery_ids)

    if terra_snapshot is None:
        terra = {
            "workspace": WORKSPACE,
            "entity_type": ENTITY_TYPE,
            "source_fields": list(TERRA_FIELDS),
            "captured_at": None,
            "entity_snapshot_sha256": None,
            "status": "blocked_missing_frozen_entity_snapshot",
        }
        immutable: dict[str, dict[str, Any]] = {}
        readiness = {
            "status": "blocked_missing_immutable_source_metadata",
            "load_authorized": False,
            "blockers": BLOCKERS,
        }
    else:
        terra, immutable = read_snapshot(terra_snapshot, roster, discovery)
        if set(immutable) != discovery_ids:
            raise SystemExit("normalized Terra snapshot did not resolve all 231 source-present samples")
        readiness = {
            "status": "blocked_pending_runtime_immutable_identity_verification",
            "load_authorized": False,
            "blockers": [
                "runtime generation/size/checksum revalidation and generation-bound GCS reads are not implemented"
            ],
        }

    entries = []
    for sample_id in roster:
        if sample_id == SKIP_SAMPLE:
            status = "source_marked_skip"
            reason = "Terra source inventory explicitly marks HG00272 skip; no new total or phased object is authorized"
            discovered: dict[str, str | None] = {}
        elif sample_id in discovery:
            status = "source_present"
            reason = "complete frozen discovery hap1/hap2 pair; immutable Terra/object metadata is required before loading"
            discovered = discovered_objects(discovery[sample_id])
        else:
            status = "no_methylation_output"
            reason = "sample is in the 292-sample roster but absent from the complete phased source inventory"
            discovered = {}
        resolved = immutable.get(sample_id, {})
        runtime_authorized = readiness["load_authorized"]
        objects = {}
        for slot, source_field in OBJECT_SLOTS.items():
            objects[slot] = {
                "source_field": source_field,
                "discovery_uri": discovered.get(slot),
                "immutable_identity": resolved.get(slot),
                "load_authorized": runtime_authorized and slot in resolved,
            }
        entries.append({
            "entry_id": f"hgsvc_hprc:{sample_id}",
            "sample_id": sample_id,
            "inventory_status": status,
            "reason": reason,
            "coordinate_convention": "GRCh38 BED 0-based half-open [start0,end0); canonical position=start0+1",
            "source_schema": "exact nine columns: chrom,start0,end0,mod_score,type,coverage,estimated_modified_count,estimated_unmodified_count,discretized_mod_score",
            "objects": objects,
            "authorized_object_count": len(resolved) if runtime_authorized else 0,
        })

    payload: dict[str, Any] = {
        "schema_version": 2,
        "manifest_id": "hgsvc-hprc-y1-phased-methylation-v2",
        "release": "y1",
        "cohort": "hgsvc_hprc",
        "reference_genome": "GRCh38",
        "modalities": ["per_sample_methylation_total", "per_haplotype_methylation"],
        "source_version": "terra-talk-LR-gnomADLR_supplement-gnomAD_LR-LR_sample-snapshot-pending",
        "supersedes": history,
        "discovery_manifest": {
            "path": discovery_path.name,
            "sha256": DISCOVERY_SHA256,
            "record_count": len(discovery),
            "fields": list(DISCOVERY_FIELDS),
            "wide_entity_table_used": False,
        },
        "terra_entity_snapshot": terra,
        "roster": {
            "path": roster_path.name,
            "expected_samples": len(roster),
            "classification_counts": {
                "source_present": len(discovery),
                "no_methylation_output": len(no_output),
                "source_marked_skip": 1,
            },
            "equation": "292=231+60+1",
        },
        "phase_orientation": {
            "status": "unconfirmed",
            "browser_primary_vcf_source_identity": None,
            "browser_primary_vcf_run_identity": None,
            "approval_receipt": None,
            "contract": "source_haplotype is a source BED label and is not vcf_strand",
        },
        "allowed_serving_mode": {
            "per_sample_methylation_total": "blocked_pending_immutable_source_load_and_acceptance",
            "per_haplotype_methylation": "blocked_join_pending_orientation_confirmation",
        },
        "load_readiness": readiness,
        "samples": entries,
    }
    payload["content_sha256"] = canonical_hash(payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--discovery-manifest", type=Path, default=SOURCES / "haplotype-methylation-source-manifest.tsv")
    parser.add_argument("--roster", type=Path, default=SOURCES / "hgsvc_hprc_y1_chr22.roster.txt")
    parser.add_argument("--historical-manifest", type=Path, default=SOURCES / "methylation-sample-manifest.json")
    parser.add_argument("--terra-snapshot", type=Path)
    parser.add_argument("--output", type=Path, default=SOURCES / "methylation-phased-source-manifest.json")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    payload = generate(args.discovery_manifest, args.roster, args.historical_manifest, args.terra_snapshot)
    encoded = json.dumps(payload, indent=2) + "\n"
    if args.check:
        if not args.output.exists() or args.output.read_text() != encoded:
            raise SystemExit(f"generated v2 manifest differs from {args.output}")
        print(f"verified {args.output}: {payload['content_sha256']}")
    else:
        args.output.write_text(encoded)
        print(f"wrote {args.output}: {payload['content_sha256']}")


if __name__ == "__main__":
    main()
