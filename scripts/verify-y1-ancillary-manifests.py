#!/usr/bin/env python3
"""Fail-closed validation for checked Y1 ancillary source manifests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "sources" / "y1"


def load(name: str) -> dict:
    with (SOURCES / name).open() as f:
        return json.load(f)


def canonical_hash(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


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

methylation = load("methylation-sample-manifest.json")
recorded_hash = methylation.pop("content_sha256")
assert canonical_hash(methylation) == recorded_hash, "methylation canonical content hash mismatch"
methylation["content_sha256"] = recorded_hash
assert methylation["release"] == "y1"
assert methylation["cohort"] == "hgsvc_hprc"
assert methylation["reference_genome"] == "GRCh38"
assert methylation["allowed_serving_mode"].startswith("blocked_")

roster = (SOURCES / "hgsvc_hprc_y1_chr22.roster.txt").read_text().splitlines()
assert len(roster) == 292 and len(roster) == len(set(roster)), "invalid 292-sample roster"
samples = methylation["samples"]
sample_ids = [entry["sample_id"] for entry in samples]
assert sample_ids == sorted(sample_ids), "methylation samples are not sorted"
assert len(sample_ids) == len(set(sample_ids)), "duplicate methylation sample assignment"
assert len(samples) == methylation["roster"]["assay_inventory_samples"] == 232
assert not methylation["roster"]["unexpected_source_samples"]
assert sorted(set(roster) - set(sample_ids)) == methylation["roster"]["missing_roster_samples"]
assert len(methylation["roster"]["missing_roster_samples"]) == 60

for entry in samples:
    require_fields(entry, {"sample_id", "availability", "expected_reason", "coordinate_convention", "bed", "index"}, entry["sample_id"])
    assert entry["availability"] == "present"
    for kind in ("bed", "index"):
        obj = entry[kind]
        require_fields(obj, {"uri", "generation", "byte_size", "checksum", "created_at"}, f"{entry['sample_id']} {kind}")
        assert obj["uri"].startswith("gs://gnomad-lr-data/")
        assert obj["generation"].isdigit()
        assert obj["byte_size"] > 0
        assert obj["checksum"]["algorithm"] == "md5_base64"
        assert obj["checksum"]["value"]
    assert entry["index"]["uri"] == entry["bed"]["uri"] + ".tbi"

candidate = next(source for source in ancillary["sources"] if source["id"] == "hgsvc-hprc-methylation-assay-subset-candidate")
assert candidate["checksum"]["value"] == recorded_hash
assert candidate["expected_sample_count"] == len(samples)
assert all(source["allowed_serving_mode"] != "accepted_y1" for source in ancillary["sources"]), (
    "inventory unexpectedly authorizes Y1 ancillary serving before acceptance"
)

print(f"Y1 ancillary manifests verified: {len(samples)} methylation pairs, 60 explicit roster gaps, no serving inputs authorized")
