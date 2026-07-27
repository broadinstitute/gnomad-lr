#!/usr/bin/env python3
"""Generate a deterministic, fail-closed Y1 methylation sample manifest.

Input inventory is the JSON emitted by:
  gcloud storage ls --json 'gs://gnomad-lr-data/ancillary/methylation/methylation/*'
The generated manifest pins every generation/checksum; the mutable listing is
acquisition input, never a publication input.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath

SUFFIX = ".model.pbmm2.combined.bed.gz"


def canonical_hash(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-json", type=Path, required=True)
    parser.add_argument("--roster", type=Path, required=True)
    parser.add_argument("--verified-on", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    objects = json.loads(args.inventory_json.read_text())
    roster = args.roster.read_text().splitlines()
    if len(roster) != 292 or len(roster) != len(set(roster)) or any(not sample for sample in roster):
        raise SystemExit("roster must contain exactly 292 unique nonblank sample IDs")

    pairs: dict[str, dict[str, dict]] = {}
    for obj in objects:
        metadata = obj.get("metadata", {})
        name = PurePosixPath(metadata.get("name", "")).name
        if name.endswith(SUFFIX + ".tbi"):
            sample_id, kind = name[: -len(SUFFIX + ".tbi")], "index"
        elif name.endswith(SUFFIX):
            sample_id, kind = name[: -len(SUFFIX)], "bed"
        else:
            raise SystemExit(f"unexpected methylation object name: {name!r}")
        if kind in pairs.setdefault(sample_id, {}):
            raise SystemExit(f"duplicate/conflicting {kind} assignment for {sample_id}")
        required = {"bucket", "name", "generation", "size", "md5Hash", "timeCreated"}
        if required - metadata.keys():
            raise SystemExit(f"{name}: incomplete immutable object metadata")
        pairs[sample_id][kind] = {
            "uri": f"gs://{metadata['bucket']}/{metadata['name']}",
            "generation": metadata["generation"],
            "byte_size": int(metadata["size"]),
            "checksum": {"algorithm": "md5_base64", "value": metadata["md5Hash"]},
            "created_at": metadata["timeCreated"],
        }

    entries = []
    for sample_id in sorted(pairs):
        pair = pairs[sample_id]
        if set(pair) != {"bed", "index"}:
            raise SystemExit(f"{sample_id}: absent BED or index: {sorted(pair)}")
        if pair["index"]["uri"] != pair["bed"]["uri"] + ".tbi":
            raise SystemExit(f"{sample_id}: index is not adjacent to BED")
        entries.append({
            "sample_id": sample_id,
            "availability": "present",
            "expected_reason": "present in the pinned pb-cpg-tools assay inventory; scientific assay-selection criterion remains unresolved",
            "coordinate_convention": "GRCh38 BED 0-based half-open [start0,end0); browser position is start0 + 1",
            "bed": pair["bed"],
            "index": pair["index"],
        })

    sample_ids = {entry["sample_id"] for entry in entries}
    roster_ids = set(roster)
    payload = {
        "schema_version": 1,
        "manifest_id": "hgsvc-hprc-y1-methylation-candidate-2026-07-27",
        "release": "y1",
        "cohort": "hgsvc_hprc",
        "reference_genome": "GRCh38",
        "modality": "per_sample_methylation",
        "source_version": "pb-cpg-tools-model-pbmm2-combined-mirror-2026-05-31",
        "coordinate_convention": {
            "source": "BED 0-based half-open [pos1,pos2)",
            "canonical_browser": "1-based closed position = pos1 + 1; source interval retained",
        },
        "roster": {
            "uri": args.roster.name,
            "expected_samples": len(roster),
            "assay_inventory_samples": len(entries),
            "missing_roster_samples": sorted(roster_ids - sample_ids),
            "unexpected_source_samples": sorted(sample_ids - roster_ids),
            "coverage_interpretation": "documented assay subset candidate; absent samples are unavailable, never zero; serving blocked until scientific assay-selection provenance is supplied",
        },
        "license_scientific_provenance": "gnomAD data terms apply. Files are a generation-pinned repository mirror of pb-cpg-tools outputs; upstream assay inventory and sample-selection provenance have not yet been supplied.",
        "acquired_verified_on": args.verified_on,
        "allowed_serving_mode": "blocked_pending_scientific_provenance_and_acceptance",
        "samples": entries,
    }
    payload["content_sha256"] = canonical_hash(payload)
    args.output.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {len(entries)} pairs; missing={len(roster_ids - sample_ids)} unexpected={len(sample_ids - roster_ids)} hash={payload['content_sha256']}")


if __name__ == "__main__":
    main()
