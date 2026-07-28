#!/usr/bin/env python3
"""Generate/verify one immutable Y1 task manifest for a canonical GRCh38 contig.

This is the full-genome counterpart to generate-y1-chr22-manifest.py.  A job
manifest intentionally covers exactly one contig so retries and finalization can
remain independently fenced per contig.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

GRCH38_CONTIG_LENGTHS = {
    **{f"chr{i}": n for i, n in enumerate([
        248956422, 242193529, 198295559, 190214555, 181538259, 170805979,
        159345973, 145138636, 138394717, 133797422, 135086622, 133275309,
        114364328, 107043718, 101991189, 90338345, 83257441, 80373285,
        58617616, 64444167, 46709983, 50818468,
    ], 1)},
    "chrX": 156040895,
    "chrY": 57227415,
}
MT_CONTIG_LENGTH = 16569
COHORTS = ("hgsvc_hprc", "aou")
MIRROR_PREFIX = "gs://gnomad-lr-data/y1/sources"


def checked_contig_length(source: dict[str, Any], contig: str) -> int:
    if contig in GRCH38_CONTIG_LENGTHS:
        return GRCH38_CONTIG_LENGTHS[contig]
    if (contig == "chrM"
            and source.get("contract_type") == "y1_per_contig_immutable_source"
            and source.get("schema_version") == 2
            and source.get("reference_genome") == "GRCh38"
            and source.get("chromosome") == "chrM"
            and source.get("mt_enabled") is True):
        return MT_CONTIG_LENGTH
    raise ValueError("unsupported or unavailable GRCh38 contig")


def checked_source(source: dict[str, Any], cohort: str, contig: str):
    if source.get("release") != "Y1" or source.get("chromosome") != contig:
        raise ValueError(f"source manifest must describe Y1 {contig}")
    if source.get("schema_version") == 2 and (source.get("contract_type") != "y1_per_contig_immutable_source" or source.get("reference_genome") != "GRCh38"):
        raise ValueError("invalid per-contig immutable source contract")
    objects = [o for o in source.get("objects", []) if o.get("cohort") == cohort]
    expected_name = f"gnomAD_LR_Y1.{cohort}.{contig}.vcf.gz"
    vcfs = [o for o in objects if o.get("name") == expected_name]
    indexes = [o for o in objects if o.get("name") == expected_name + ".tbi"]
    if len(objects) != 2 or len(vcfs) != 1 or len(indexes) != 1:
        raise ValueError(f"cohort {cohort} must have exactly the canonical adjacent {contig} VCF/TBI pair")
    for obj in (vcfs[0], indexes[0]):
        if not obj.get("mirror_generation") or not obj.get("md5_base64") or int(obj.get("size", 0)) <= 0:
            raise ValueError(f"incomplete immutable identity for {obj.get('name')}")
    return vcfs[0], indexes[0]


def generate(source: dict[str, Any], cohort: str, contig: str, run_id: str,
             attempt: str, interval_size: int) -> list[dict[str, Any]]:
    if cohort not in COHORTS:
        raise ValueError("unsupported cohort")
    length = checked_contig_length(source, contig)
    if not run_id or not attempt or interval_size <= 0:
        raise ValueError("run ID, attempt prefix, and positive interval size are required")
    vcf, index = checked_source(source, cohort, contig)
    prefix = source.get("mirror_prefix")
    if prefix != MIRROR_PREFIX:
        raise ValueError("source manifest mirror_prefix differs from the Rust canonical Y1 mirror contract")
    source_uri = f"{prefix}/{cohort}/vcfs/{vcf['name']}"
    tasks = []
    for ordinal, start in enumerate(range(1, length + 1, interval_size)):
        stop = min(start + interval_size - 1, length)
        task_id = f"y1-{cohort.replace('_', '-')}-{contig}-{start}-{stop}"
        tasks.append({
            "coordinator_task_id": f"custom_{ordinal}",
            "label": f"{cohort} Y1 {contig}:{start}-{stop}",
            "run_id": run_id, "task_id": task_id,
            "attempt_id": f"{attempt}-{ordinal:04d}",
            "release": "y1", "cohort": cohort, "reference_genome": "GRCh38",
            "chrom": contig, "start": start, "stop": stop,
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
    verify(tasks, contig)
    return tasks


def verify(tasks: list[dict[str, Any]], contig: str) -> None:
    if not tasks:
        raise ValueError("manifest tasks are required")
    first, previous_stop, task_ids, attempt_ids = tasks[0], 0, set(), set()
    if contig in GRCH38_CONTIG_LENGTHS:
        length = GRCH38_CONTIG_LENGTHS[contig]
    elif contig == "chrM" and first.get("chrom") == "chrM":
        length = MT_CONTIG_LENGTH
    else:
        raise ValueError("manifest uses an unsupported GRCh38 contig")
    invariant = ("run_id", "release", "cohort", "reference_genome", "chrom",
                 "source_uri", "source_generation", "source_checksum",
                 "source_index_uri", "source_index_generation", "source_index_checksum")
    for ordinal, task in enumerate(tasks):
        if task.get("coordinator_task_id") != f"custom_{ordinal}":
            raise ValueError(f"task {ordinal} has a non-deterministic coordinator ID")
        if any(task.get(k) != first.get(k) for k in invariant):
            raise ValueError(f"task {ordinal} changes a run/source invariant")
        start, stop = int(task.get("start", 0)), int(task.get("stop", 0))
        if start != previous_stop + 1 or stop < start or stop > length:
            raise ValueError(f"task {ordinal} is overlapping, gapped, or out of bounds")
        if task.get("task_id") in task_ids or task.get("attempt_id") in attempt_ids:
            raise ValueError(f"task {ordinal} duplicates a task or attempt ID")
        task_ids.add(task["task_id"]); attempt_ids.add(task["attempt_id"]); previous_stop = stop
        injection, retry_id = task.get("controlled_fail_once"), task.get("retry_attempt_id")
        if (injection is None) != (retry_id is None):
            raise ValueError(f"task {ordinal} has an incomplete controlled-retry contract")
        if injection is not None:
            if injection.get("mode") != "after_first_staged_batch" or not injection.get("evidence_token"):
                raise ValueError(f"task {ordinal} has an invalid controlled-retry contract")
            if retry_id == task["attempt_id"] or retry_id in attempt_ids:
                raise ValueError(f"task {ordinal} duplicates a retry attempt ID")
            attempt_ids.add(retry_id)
    if (first.get("release"), first.get("reference_genome"), first.get("chrom")) != ("y1", "GRCh38", contig) or previous_stop != length:
        raise ValueError(f"manifest does not exactly cover GRCh38 {contig}")


def verify_source_identity(tasks: list[dict[str, Any]], source: dict[str, Any], cohort: str, contig: str) -> None:
    vcf, index = checked_source(source, cohort, contig)
    source_uri = f"{source['mirror_prefix']}/{cohort}/vcfs/{vcf['name']}"
    expected = {
        "cohort": cohort, "source_uri": source_uri,
        "source_generation": str(vcf["mirror_generation"]),
        "source_checksum": vcf["md5_base64"], "source_size_bytes": int(vcf["size"]),
        "source_index_uri": source_uri + ".tbi",
        "source_index_generation": str(index["mirror_generation"]),
        "source_index_checksum": index["md5_base64"],
    }
    if any(tasks[0].get(key) != value for key, value in expected.items()):
        raise ValueError("manifest identity differs from the checked source inventory")


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source-manifest", required=True, type=Path)
    p.add_argument("--cohort", required=True, choices=COHORTS)
    p.add_argument("--contig", required=True, help="chr1-22,X,Y; chrM only with an explicit immutable MT contract")
    p.add_argument("--run-id", required=True); p.add_argument("--attempt", required=True)
    p.add_argument("--interval-size", type=int, default=1_000_000)
    p.add_argument("--output", required=True, type=Path); p.add_argument("--check", action="store_true")
    a = p.parse_args(); source = json.loads(a.source_manifest.read_text())
    tasks = json.loads(a.output.read_text()) if a.check else generate(source, a.cohort, a.contig, a.run_id, a.attempt, a.interval_size)
    verify(tasks, a.contig)
    verify_source_identity(tasks, source, a.cohort, a.contig)
    if not a.check:
        a.output.parent.mkdir(parents=True, exist_ok=True); a.output.write_text(json.dumps(tasks, indent=2) + "\n")
    print(json.dumps({"manifest": str(a.output), "contig": a.contig, "tasks": len(tasks), "canonical_sha256": hashlib.sha256(canonical_bytes(tasks)).hexdigest()}, sort_keys=True))

if __name__ == "__main__": main()
