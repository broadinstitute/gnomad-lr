#!/usr/bin/env python3
"""Generate and verify deterministic Genohype Y1 chr22 task manifests.

The output is the JSON array consumed by `genohype pool submit custom --manifest`.
Records are owned by their one-based POS, so adjacent inclusive tasks are gap-free
without overlap. The loader independently enforces the same POS ownership rule.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

CHR22_LENGTH = 50_818_468
COHORTS = ("hgsvc_hprc", "aou")


def checked_source(source_manifest: dict[str, Any], cohort: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if source_manifest.get("release") != "Y1" or source_manifest.get("chromosome") != "chr22":
        raise ValueError("source manifest must describe Y1 chr22")
    objects = [obj for obj in source_manifest.get("objects", []) if obj.get("cohort") == cohort]
    vcfs = [obj for obj in objects if str(obj.get("name", "")).endswith(".vcf.gz")]
    indexes = [obj for obj in objects if str(obj.get("name", "")).endswith(".vcf.gz.tbi")]
    if len(vcfs) != 1 or len(indexes) != 1 or indexes[0]["name"] != vcfs[0]["name"] + ".tbi":
        raise ValueError(f"cohort {cohort} must have exactly one adjacent VCF/TBI pair")
    for obj in (vcfs[0], indexes[0]):
        if not obj.get("mirror_generation") or not obj.get("md5_base64") or int(obj.get("size", 0)) <= 0:
            raise ValueError(f"incomplete immutable identity for {obj.get('name')}")
    return vcfs[0], indexes[0]


def intervals(size: int) -> list[tuple[int, int]]:
    if size <= 0:
        raise ValueError("interval size must be positive")
    return [(start, min(start + size - 1, CHR22_LENGTH)) for start in range(1, CHR22_LENGTH + 1, size)]


def generate(
    source_manifest: dict[str, Any],
    cohort: str,
    run_id: str,
    attempt: str,
    size: int,
    fail_once_ordinal: int | None = None,
    retry_attempt: str | None = None,
    evidence_token: str | None = None,
) -> list[dict[str, Any]]:
    if cohort not in COHORTS or not run_id or not attempt:
        raise ValueError("cohort, run ID, and attempt label are required")
    fail_once_values = (fail_once_ordinal, retry_attempt, evidence_token)
    if any(value is not None for value in fail_once_values) and not all(value is not None for value in fail_once_values):
        raise ValueError("fail-once ordinal, retry attempt prefix, and evidence token are required together")
    vcf, index = checked_source(source_manifest, cohort)
    prefix = source_manifest.get("mirror_prefix")
    if not isinstance(prefix, str) or not prefix.startswith("gs://"):
        raise ValueError("source manifest mirror_prefix must be a gs:// URI")
    source_uri = f"{prefix}/{cohort}/vcfs/{vcf['name']}"
    tasks = []
    for ordinal, (start, stop) in enumerate(intervals(size)):
        task_id = f"y1-{cohort.replace('_', '-')}-chr22-{start}-{stop}"
        tasks.append({
            "coordinator_task_id": f"custom_{ordinal}",
            "label": f"{cohort} Y1 chr22:{start}-{stop}",
            "run_id": run_id,
            "task_id": task_id,
            "attempt_id": f"{attempt}-{ordinal:04d}",
            "release": "y1",
            "cohort": cohort,
            "reference_genome": "GRCh38",
            "chrom": "chr22",
            "start": start,
            "stop": stop,
            "source_uri": source_uri,
            "source_generation": str(vcf["mirror_generation"]),
            "source_checksum_algorithm": "md5_base64",
            "source_checksum": vcf["md5_base64"],
            "source_size_bytes": int(vcf["size"]),
            "source_index_uri": source_uri + ".tbi",
            "source_index_generation": str(index["mirror_generation"]),
            "source_index_checksum_algorithm": "md5_base64",
            "source_index_checksum": index["md5_base64"],
        })
    if fail_once_ordinal is not None:
        if fail_once_ordinal < 0 or fail_once_ordinal >= len(tasks):
            raise ValueError("fail-once ordinal is outside the generated manifest")
        task = tasks[fail_once_ordinal]
        task["retry_attempt_id"] = f"{retry_attempt}-{fail_once_ordinal:04d}"
        task["controlled_fail_once"] = {
            "mode": "after_first_staged_batch",
            "evidence_token": evidence_token,
        }
    verify(tasks)
    return tasks


def verify(tasks: list[dict[str, Any]]) -> None:
    if not tasks:
        raise ValueError("manifest must contain tasks")
    invariant = ("run_id", "release", "cohort", "reference_genome", "chrom", "source_uri", "source_generation", "source_checksum", "source_index_uri", "source_index_generation", "source_index_checksum")
    first = tasks[0]
    previous_stop = 0
    task_ids: set[str] = set()
    attempt_ids: set[str] = set()
    for ordinal, task in enumerate(tasks):
        if task.get("coordinator_task_id") != f"custom_{ordinal}":
            raise ValueError(f"task {ordinal} has a non-deterministic coordinator ID")
        if any(task.get(key) != first.get(key) for key in invariant):
            raise ValueError(f"task {ordinal} changes a run/source invariant")
        start, stop = int(task.get("start", 0)), int(task.get("stop", 0))
        if start != previous_stop + 1 or stop < start or stop > CHR22_LENGTH:
            raise ValueError(f"task {ordinal} is overlapping, gapped, or out of bounds")
        previous_stop = stop
        if task.get("task_id") in task_ids or task.get("attempt_id") in attempt_ids:
            raise ValueError(f"task {ordinal} duplicates a task or attempt ID")
        task_ids.add(task["task_id"])
        attempt_ids.add(task["attempt_id"])
        injection, retry_id = task.get("controlled_fail_once"), task.get("retry_attempt_id")
        if (injection is None) != (retry_id is None):
            raise ValueError(f"task {ordinal} has an incomplete fail-once contract")
        if injection is not None:
            if injection != {"mode": "after_first_staged_batch", "evidence_token": injection.get("evidence_token")} or not injection.get("evidence_token"):
                raise ValueError(f"task {ordinal} has an invalid fail-once contract")
            if retry_id == task["attempt_id"] or retry_id in attempt_ids:
                raise ValueError(f"task {ordinal} has a duplicate retry attempt ID")
            attempt_ids.add(retry_id)
    if previous_stop != CHR22_LENGTH or first.get("release") != "y1" or first.get("reference_genome") != "GRCh38" or first.get("chrom") != "chr22":
        raise ValueError("manifest does not exactly cover GRCh38 chr22")


def verify_source_identity(tasks: list[dict[str, Any]], source_manifest: dict[str, Any], cohort: str) -> None:
    vcf, index = checked_source(source_manifest, cohort)
    prefix = source_manifest["mirror_prefix"]
    source_uri = f"{prefix}/{cohort}/vcfs/{vcf['name']}"
    expected = {
        "cohort": cohort,
        "source_uri": source_uri,
        "source_generation": str(vcf["mirror_generation"]),
        "source_checksum": vcf["md5_base64"],
        "source_size_bytes": int(vcf["size"]),
        "source_index_uri": source_uri + ".tbi",
        "source_index_generation": str(index["mirror_generation"]),
        "source_index_checksum": index["md5_base64"],
    }
    if any(tasks[0].get(key) != value for key, value in expected.items()):
        raise ValueError("manifest identity differs from the checked source inventory")


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--cohort", required=True, choices=COHORTS)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt", required=True, help="immutable attempt prefix, e.g. attempt-1-03b8338")
    parser.add_argument("--interval-size", type=int, default=1_000_000)
    parser.add_argument("--fail-once-task-ordinal", type=int)
    parser.add_argument("--retry-attempt", help="immutable retry attempt prefix")
    parser.add_argument("--failure-evidence-token")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--check", action="store_true", help="verify an existing output instead of generating it")
    args = parser.parse_args()

    source = json.loads(args.source_manifest.read_text())
    if args.check:
        tasks = json.loads(args.output.read_text())
        verify(tasks)
        verify_source_identity(tasks, source, args.cohort)
    else:
        tasks = generate(
            source,
            args.cohort,
            args.run_id,
            args.attempt,
            args.interval_size,
            args.fail_once_task_ordinal,
            args.retry_attempt,
            args.failure_evidence_token,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(json.dumps(tasks, indent=2).encode() + b"\n")
    digest = hashlib.sha256(canonical_bytes(tasks)).hexdigest()
    print(json.dumps({"manifest": str(args.output), "tasks": len(tasks), "first": tasks[0]["start"], "last": tasks[-1]["stop"], "canonical_sha256": digest}, sort_keys=True))


if __name__ == "__main__":
    main()
