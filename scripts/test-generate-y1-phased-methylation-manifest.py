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
assert manifest["load_readiness"] == {
    "status": "blocked_pending_atomic_attempt_ledger",
    "load_authorized": False,
    "immutable_source_reads_ready": True,
    "blockers": [
        "atomic methylation attempt/lease ledger and direct-canonical finalizer are not implemented"
    ],
}
assert "pending" not in manifest["source_version"]
assert manifest["terra_entity_snapshot"]["entity_snapshot_sha256"] == "1c3314f2f1ea2e99374a31b8e858d5851021e3913e216574fd2ac83656879485"
assert manifest["gcs_object_metadata_ledger"]["sha256"] == "9250ef5a4df19d03621c6db6f06d7065f12ca6727baf2513b086d54bba18908c"
assert manifest["phase_orientation"]["status"] == "unconfirmed"
assert manifest["allowed_serving_mode"]["per_haplotype_methylation"] == "blocked_join_pending_orientation_confirmation"

present = [entry for entry in manifest["samples"] if entry["inventory_status"] == "source_present"]
assert len(present) == 231
for entry in present:
    for slot in ("hap1_bed", "hap1_bed_index", "hap2_bed", "hap2_bed_index"):
        assert entry["objects"][slot]["discovery_uri"]
    assert entry["authorized_object_count"] == 0
    assert all(obj["immutable_identity"] for obj in entry["objects"].values())
    assert all(obj["immutable_identity"]["updated_at"] for obj in entry["objects"].values())
    assert all(not obj["load_authorized"] for obj in entry["objects"].values())
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

    accepted_snapshot = json.loads((SOURCES / "terra-lr-sample-normalized-snapshot.json").read_text())
    first_present_entity = next(
        entity
        for entity in accepted_snapshot["entities"]
        if entity["attributes"]["cpg_combined_bed"] is not None
    )
    first_present_entity["attributes"]["cpg_combined_bed"]["checksum"]["algorithm"] = ""
    malformed_snapshot = tmp_path / "malformed-normalized-snapshot.json"
    malformed_snapshot.write_text(json.dumps(accepted_snapshot))
    result = run("--terra-snapshot", str(malformed_snapshot), "--output", str(tmp_path / "out.json"))
    assert result.returncode != 0 and "checksum must be complete" in result.stdout, result.stdout

    ledger = (SOURCES / "methylation-phased-gcs-object-metadata.tsv").read_text()
    stale_ledger = tmp_path / "stale-metadata.tsv"
    stale_ledger.write_text(ledger.replace("\t1777748016475903\t", "\t1777748016475904\t", 1))
    result = run("--gcs-metadata-ledger", str(stale_ledger), "--output", str(tmp_path / "out.json"))
    assert result.returncode != 0 and "metadata ledger SHA-256 drift" in result.stdout, result.stdout

print("phased methylation v2 generator tests passed")
