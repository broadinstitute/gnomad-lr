#!/usr/bin/env python3
"""Focused tests for the fail-closed phased methylation v2 generator."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate-y1-phased-methylation-manifest.py"
SOURCES = ROOT / "sources" / "y1"


def run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", str(SCRIPT), *args],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


checked = run("--check")
assert checked.returncode == 0, checked.stdout

manifest = json.loads((SOURCES / "methylation-phased-source-manifest.json").read_text())
assert len(manifest["samples"]) == 292
counts = manifest["roster"]["classification_counts"]
assert counts == {"source_present": 231, "no_methylation_output": 60, "source_marked_skip": 1}
assert 292 == counts["source_present"] + counts["no_methylation_output"] + counts["source_marked_skip"]
assert manifest["load_readiness"]["load_authorized"] is False
assert manifest["phase_orientation"]["status"] == "unconfirmed"
assert manifest["allowed_serving_mode"]["per_haplotype_methylation"] == "blocked_join_pending_orientation_confirmation"

present = [entry for entry in manifest["samples"] if entry["inventory_status"] == "source_present"]
assert len(present) == 231
for entry in present:
    for slot in ("hap1_bed", "hap1_bed_index", "hap2_bed", "hap2_bed_index"):
        assert entry["objects"][slot]["discovery_uri"]
    assert entry["authorized_object_count"] == 0
skip = next(entry for entry in manifest["samples"] if entry["sample_id"] == "HG00272")
assert skip["inventory_status"] == "source_marked_skip"
assert skip["authorized_object_count"] == 0
assert all(value["discovery_uri"] is None for value in skip["objects"].values())

with tempfile.TemporaryDirectory() as tmp:
    tmp_path = Path(tmp)
    discovery = (SOURCES / "haplotype-methylation-source-manifest.tsv").read_text()

    drifted = tmp_path / "narrow-discovery.tsv"
    drifted.write_text(discovery.replace("HG00097.hap1.bed.gz", "HG00097.hap1.bed.gx", 1))
    result = run("--discovery-manifest", str(drifted), "--output", str(tmp_path / "out.json"))
    assert result.returncode != 0 and "SHA-256 drift" in result.stdout, result.stdout

    forbidden = tmp_path / "LR_sample.tsv"
    forbidden.write_text(discovery)
    result = run("--discovery-manifest", str(forbidden), "--output", str(tmp_path / "out.json"))
    assert result.returncode != 0 and "wide entity-table TSV is forbidden" in result.stdout, result.stdout

    fake_snapshot = tmp_path / "LR_sample.json"
    fake_snapshot.write_text("{}")
    result = run("--terra-snapshot", str(fake_snapshot), "--output", str(tmp_path / "out.json"))
    assert result.returncode != 0 and "normalized Terra snapshot fields" in result.stdout, result.stdout

    def immutable_object(uri: str) -> dict:
        return {
            "uri": uri,
            "generation": "1",
            "byte_size": 10,
            "checksum": {"algorithm": "md5_base64", "value": "fixture"},
            "created_at": "2026-07-29T00:00:00Z",
            "immutable_read_uri": f"{uri}?generation=1",
        }

    entities = []
    for entry in manifest["samples"]:
        sample_id = entry["sample_id"]
        if entry["inventory_status"] == "source_present":
            discovered = entry["objects"]
            combined_uri = f"gs://test-only/{sample_id}.combined.bed.gz"
            attributes = {
                "cpg_combined_bed": immutable_object(combined_uri),
                "cpg_hap1_bed": immutable_object(discovered["hap1_bed"]["discovery_uri"]),
                "cpg_hap1_bed_idx": immutable_object(discovered["hap1_bed_index"]["discovery_uri"]),
                "cpg_hap2_bed": immutable_object(discovered["hap2_bed"]["discovery_uri"]),
                "cpg_hap2_bed_idx": immutable_object(discovered["hap2_bed_index"]["discovery_uri"]),
            }
            sidecars = {"cpg_combined_bed_idx": immutable_object(combined_uri + ".tbi")}
        else:
            attributes = {
                "cpg_combined_bed": None,
                "cpg_hap1_bed": None,
                "cpg_hap1_bed_idx": None,
                "cpg_hap2_bed": None,
                "cpg_hap2_bed_idx": None,
            }
            sidecars = {"cpg_combined_bed_idx": None}
        entities.append({"sample_id": sample_id, "attributes": attributes, "verified_sidecars": sidecars})
    snapshot = {
        "schema_version": 1,
        "workspace": {"namespace": "talk-LR-gnomADLR_supplement", "name": "gnomAD_LR"},
        "entity_type": "LR_sample",
        "captured_at": "2026-07-29T00:00:00Z",
        "entities": entities,
    }
    normalized_snapshot = tmp_path / "normalized-snapshot.json"
    normalized_snapshot.write_text(json.dumps(snapshot))
    normalized_output = tmp_path / "normalized-output.json"
    result = run("--terra-snapshot", str(normalized_snapshot), "--output", str(normalized_output))
    assert result.returncode == 0, result.stdout
    normalized = json.loads(normalized_output.read_text())
    assert normalized["load_readiness"] == {
        "status": "blocked_pending_runtime_immutable_identity_verification",
        "load_authorized": False,
        "blockers": [
            "runtime generation/size/checksum revalidation and generation-bound GCS reads are not implemented"
        ],
    }
    normalized_present = next(entry for entry in normalized["samples"] if entry["inventory_status"] == "source_present")
    assert normalized_present["authorized_object_count"] == 0
    assert all(not obj["load_authorized"] for obj in normalized_present["objects"].values())
    assert all(obj["immutable_identity"] for obj in normalized_present["objects"].values())

    first_present_entity = next(
        entity for entity in snapshot["entities"] if entity["attributes"]["cpg_combined_bed"] is not None
    )
    first_present_entity["attributes"]["cpg_combined_bed"]["checksum"]["algorithm"] = ""
    normalized_snapshot.write_text(json.dumps(snapshot))
    result = run("--terra-snapshot", str(normalized_snapshot), "--output", str(normalized_output))
    assert result.returncode != 0 and "checksum must be complete" in result.stdout, result.stdout

print("phased methylation v2 generator tests passed")
