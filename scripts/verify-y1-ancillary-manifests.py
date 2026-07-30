#!/usr/bin/env python3
"""Fail-closed validation for checked Y1 ancillary source manifests."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "sources" / "y1"
DISCOVERY_SHA256 = "a0ea03cb7af9a0bf39ca6831bb93c02acfcc71606ee3427dbf9abded28e2bca4"
OBJECT_SLOTS = {
    "combined_bed",
    "combined_bed_index",
    "hap1_bed",
    "hap1_bed_index",
    "hap2_bed",
    "hap2_bed_index",
}


def load(name: str) -> dict:
    with (SOURCES / name).open() as handle:
        return json.load(handle)


def canonical_hash(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def checked_payload(name: str) -> tuple[dict, str]:
    value = load(name)
    recorded_hash = value.pop("content_sha256")
    assert canonical_hash(value) == recorded_hash, f"{name}: canonical content hash mismatch"
    value["content_sha256"] = recorded_hash
    return value, recorded_hash


def require_fields(value: dict, fields: set[str], label: str) -> None:
    missing = fields - value.keys()
    assert not missing, f"{label}: missing fields {sorted(missing)}"


ancillary = load("ancillary-source-manifest.json")
require_fields(
    ancillary,
    {"schema_version", "manifest_id", "release", "reference_genome", "sources"},
    "ancillary manifest",
)
assert ancillary["release"] == "y1"
assert ancillary["reference_genome"] == "GRCh38"

required_source_fields = {
    "id", "release", "cohort", "reference_genome", "acquired_verified_on",
    "modality", "classification", "source_version", "uri",
    "immutable_uri", "generation", "byte_size", "checksum", "sidecars",
    "coordinate_convention", "schema_contract", "granularity", "expected_contigs",
    "expected_sample_count", "expected_sample_ids", "license_scientific_provenance",
    "allowed_serving_mode",
}
ids: set[str] = set()
modalities: set[str] = set()
for source in ancillary["sources"]:
    require_fields(source, required_source_fields, source.get("id", "source"))
    assert source["id"] not in ids, f"duplicate source id {source['id']}"
    ids.add(source["id"])
    modalities.update(source["modality"].split(","))
    if source["allowed_serving_mode"] == "accepted_y1":
        assert source["classification"] in {
            "hgsvc_hprc_y1_authoritative", "aou_y1_authoritative",
            "shared_reference_cohort_independent",
        }
        assert source["immutable_uri"] not in {"none", source.get("mutable_convenience_uri")}
        assert source["generation"] != "none"
        assert source["byte_size"] > 0
        assert source["checksum"]["algorithm"] != "none"

assert {
    "sequencing_coverage",
    "per_sample_methylation",
    "per_sample_methylation_total",
    "per_haplotype_methylation",
    "str_allele_frequency_histograms",
    "genes_and_transcripts",
    "recombination_rate",
    "sample_tracks",
} <= modalities, f"ancillary modality inventory incomplete: {sorted(modalities)}"
for source_id in {
    "shared-grch38-gene-annotations-unresolved",
    "shared-grch38-recombination-map-unresolved",
}:
    source = next(item for item in ancillary["sources"] if item["id"] == source_id)
    assert source["classification"] == "unresolved_and_blocked_from_serving"
    assert source["allowed_serving_mode"].startswith("blocked_")
    assert source["immutable_uri"] == "none"
    assert source["checksum"] == {"algorithm": "none", "value": "none"}

# The 232-source mirror remains byte-for-contract history; it is never rewritten
# to masquerade as the new Terra LR_sample inventory.
historical, historical_hash = checked_payload("methylation-sample-manifest.json")
assert historical_hash == "958a33fd4a093a89aa704234aa44610039cd2a18b1c31b62e32d992bd34f5ef4"
assert historical["schema_version"] == 1
historical_ids = [entry["sample_id"] for entry in historical["samples"]]
assert len(historical_ids) == len(set(historical_ids)) == 232
historical_pointer = next(source for source in ancillary["sources"] if source["id"] == "hgsvc-hprc-methylation-assay-subset-candidate")
assert historical_pointer["uri"] == "methylation-sample-manifest.json"
assert historical_pointer["classification"] == "superseded_historical_manifest"
assert historical_pointer["allowed_serving_mode"] == "superseded_history_only"
assert historical_pointer["checksum"]["value"] == historical_hash

# Verify the narrow discovery receipt itself, not a mutable or wide Terra TSV.
discovery_path = SOURCES / "haplotype-methylation-source-manifest.tsv"
discovery_raw = discovery_path.read_bytes()
assert hashlib.sha256(discovery_raw).hexdigest() == DISCOVERY_SHA256
with discovery_path.open(newline="") as handle:
    discovery_rows = list(csv.DictReader(handle, delimiter="\t"))
assert len(discovery_rows) == 231
assert list(discovery_rows[0]) == ["sample_id", "hap1_bed", "hap1_tbi", "hap2_bed", "hap2_tbi"]
discovery_ids = [row["sample_id"] for row in discovery_rows]
assert discovery_ids == sorted(discovery_ids)
assert len(discovery_ids) == len(set(discovery_ids))
assert "HG00272" not in discovery_ids
assert all(all(row.values()) for row in discovery_rows)

v2, v2_hash = checked_payload("methylation-phased-source-manifest.json")
assert v2["schema_version"] == 2
assert v2["release"] == "y1" and v2["cohort"] == "hgsvc_hprc" and v2["reference_genome"] == "GRCh38"
assert v2["modalities"] == ["per_sample_methylation_total", "per_haplotype_methylation"]
assert v2["discovery_manifest"]["sha256"] == DISCOVERY_SHA256
assert v2["discovery_manifest"]["record_count"] == 231
assert v2["discovery_manifest"]["wide_entity_table_used"] is False
assert v2["supersedes"] == {
    "path": "methylation-sample-manifest.json",
    "schema_version": 1,
    "content_sha256": historical_hash,
    "sample_count": 232,
    "status": "superseded_history_only_not_a_v2_load_source",
}
assert set(historical_ids) - set(discovery_ids) == {"HG00272"}
assert not set(discovery_ids) - set(historical_ids)

roster = (SOURCES / "hgsvc_hprc_y1_chr22.roster.txt").read_text().splitlines()
assert len(roster) == 292 and len(roster) == len(set(roster)) and roster == sorted(roster)
samples = v2["samples"]
assert [entry["sample_id"] for entry in samples] == roster
assert len({entry["entry_id"] for entry in samples}) == 292
by_status: dict[str, list[dict]] = {}
for entry in samples:
    by_status.setdefault(entry["inventory_status"], []).append(entry)
assert {status: len(entries) for status, entries in by_status.items()} == {
    "source_present": 231,
    "no_methylation_output": 60,
    "source_marked_skip": 1,
}
assert v2["roster"]["classification_counts"] == {
    "source_present": 231,
    "no_methylation_output": 60,
    "source_marked_skip": 1,
}
assert v2["roster"]["equation"] == "292=231+60+1"

row_by_sample = {row["sample_id"]: row for row in discovery_rows}
for entry in samples:
    assert set(entry["objects"]) == OBJECT_SLOTS
    assert "vcf_strand" not in json.dumps(entry)
    if entry["inventory_status"] == "source_present":
        row = row_by_sample[entry["sample_id"]]
        expected = {
            "hap1_bed": row["hap1_bed"],
            "hap1_bed_index": row["hap1_tbi"],
            "hap2_bed": row["hap2_bed"],
            "hap2_bed_index": row["hap2_tbi"],
        }
        for slot, uri in expected.items():
            assert entry["objects"][slot]["discovery_uri"] == uri
        assert entry["objects"]["combined_bed"]["discovery_uri"] is None
        assert entry["objects"]["combined_bed_index"]["discovery_uri"] is None
    else:
        assert all(obj["discovery_uri"] is None for obj in entry["objects"].values())

# Runtime immutable reads are ready, while overall loading remains blocked on
# the explicitly separate atomic attempt-ledger/finalizer milestone.
assert v2["load_readiness"] == {
    "status": "blocked_pending_atomic_attempt_ledger",
    "load_authorized": False,
    "immutable_source_reads_ready": True,
    "blockers": [
        "atomic methylation attempt/lease ledger and direct-canonical finalizer are not implemented"
    ],
}
assert v2["terra_entity_snapshot"]["entity_snapshot_sha256"] == "1c3314f2f1ea2e99374a31b8e858d5851021e3913e216574fd2ac83656879485"
assert v2["gcs_object_metadata_ledger"]["sha256"] == "9250ef5a4df19d03621c6db6f06d7065f12ca6727baf2513b086d54bba18908c"
assert "pending" not in v2["source_version"]
for entry in by_status["source_present"]:
    assert entry["authorized_object_count"] == 0
    for obj in entry["objects"].values():
        immutable = obj["immutable_identity"]
        assert obj["load_authorized"] is False
        require_fields(
            immutable,
            {"uri", "generation", "byte_size", "checksum", "created_at", "updated_at", "immutable_read_uri"},
            entry["entry_id"],
        )
        assert immutable["generation"].isdigit() and immutable["byte_size"] > 0
        assert immutable["checksum"]["algorithm"] == "md5_base64" and immutable["checksum"]["value"]
        assert immutable["created_at"] and immutable["updated_at"]
        assert immutable["immutable_read_uri"] == f"{immutable['uri']}?generation={immutable['generation']}"

skip = by_status["source_marked_skip"][0]
assert skip["sample_id"] == "HG00272" and skip["authorized_object_count"] == 0
assert all(obj["immutable_identity"] is None for obj in skip["objects"].values())
assert all(entry["authorized_object_count"] == 0 for entry in by_status["no_methylation_output"])
assert v2["phase_orientation"] == {
    "status": "unconfirmed",
    "browser_primary_vcf_source_identity": None,
    "browser_primary_vcf_run_identity": None,
    "approval_receipt": None,
    "contract": "source_haplotype is a source BED label and is not vcf_strand",
}
assert v2["allowed_serving_mode"]["per_haplotype_methylation"] == "blocked_join_pending_orientation_confirmation"

serialized = json.dumps(v2)
assert "LR_sample.tsv" not in serialized and "entities.tsv" not in serialized
v2_pointer = next(source for source in ancillary["sources"] if source["id"] == "hgsvc-hprc-phased-methylation-v2-blocked")
assert v2_pointer["uri"] == "methylation-phased-source-manifest.json"
assert v2_pointer["checksum"] == {"algorithm": "canonical_json_sha256", "value": v2_hash}
assert v2_pointer["byte_size"] == (SOURCES / v2_pointer["uri"]).stat().st_size
assert v2_pointer["expected_sample_count"] == 292
assert v2_pointer["allowed_serving_mode"].startswith("blocked_")
assert all(source["allowed_serving_mode"] != "accepted_y1" for source in ancillary["sources"]), (
    "inventory unexpectedly authorizes Y1 ancillary serving before acceptance"
)

print("Y1 ancillary manifests verified: v2 is frozen 292=231+60+1, immutable reads ready, and loading blocked on atomic ledger")
