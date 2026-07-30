#!/usr/bin/env python3
"""Verify the accepted phased mirror ledger and generate its fixed chr22 tasks."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "sources" / "y1" / "methylation-phased-mirror-ledger.json"
OUTPUT = ROOT / "manifests" / "y1" / "phased-methylation-mirror-chr22-canary.json"
RAW_SHA256 = "7f4e15a93920c842b11fc24ed3ee96aebefcc42549e001431164c2631e54b78b"
CONTENT_SHA256 = "97355c54eef458b56f31a318c740dddaff7261a0d76b1d83be5078b4efb13241"
SOURCE_MANIFEST_SHA256 = "f585cbc2b806dcb52944af2ecabe634338a41323f89e3938336235c7729e8743"
COPY_MANIFEST_SHA256 = "9ba362a055f74652c3852ce46e0389b2219acca48b054cc627839105bce4b2cc"
PREFIX = "gs://gnomad-lr-data/sources/y1/phased-methylation-v2/full-object-mirror/"
CONTRACT_ID = "mirror-only-chr22-source-phased-canary-v1"
RUN_ID = "y1-phased-mirror-chr22-canary-v1"
CHR22_STOP = 50_818_468
SLOTS = ("hap1_bed", "hap1_bed_index", "hap2_bed", "hap2_bed_index")
TOP_FIELDS = {
    "accepted_at", "byte_count", "content_sha256", "copy_manifest_canonical_sha256",
    "copy_semantics", "destination_prefix", "load_authorization_blockers", "load_authorized",
    "mirror_accepted", "object_count", "objects", "reconciliation", "sample_count",
    "schema_version", "source_manifest_content_sha256", "source_manifest_id", "status",
}


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def checked_md5(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} MD5 must be base64 text")
    try:
        decoded = base64.b64decode(value, validate=True)
    except Exception as error:
        raise ValueError(f"{label} MD5 is not valid base64") from error
    if len(decoded) != 16:
        raise ValueError(f"{label} MD5 must decode to 16 bytes")
    return value


def validate_object(value: Any, label: str, mirror: bool) -> dict[str, Any]:
    required = {"byte_size", "generation", "immutable_read_uri", "md5_base64", "uri"}
    if mirror:
        required |= {"crc32c_base64", "created_at"}
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError(f"{label} fields differ from the accepted schema")
    if not isinstance(value["byte_size"], int) or value["byte_size"] <= 0:
        raise ValueError(f"{label} byte_size must be positive")
    if not isinstance(value["generation"], str) or not value["generation"].isdigit() or value["generation"].startswith("0"):
        raise ValueError(f"{label} generation must be canonical positive decimal text")
    if not isinstance(value["uri"], str) or not value["uri"].startswith("gs://") or "?" in value["uri"]:
        raise ValueError(f"{label} URI must be a bare gs:// identity")
    if value["immutable_read_uri"] != f"{value['uri']}?generation={value['generation']}":
        raise ValueError(f"{label} immutable URI does not bind the exact generation")
    checked_md5(value["md5_base64"], label)
    if mirror and (not value["uri"].startswith(PREFIX) or not value["created_at"]):
        raise ValueError(f"{label} is outside the accepted mirror prefix or lacks creation identity")
    return value


def validate_ledger_bytes(
    raw: bytes, *, require_raw_identity: bool = True, require_content_identity: bool = True
) -> dict[str, Any]:
    if require_raw_identity and hashlib.sha256(raw).hexdigest() != RAW_SHA256:
        raise ValueError("mirror ledger raw SHA-256 drift")
    ledger = json.loads(raw)
    if not isinstance(ledger, dict) or set(ledger) != TOP_FIELDS:
        raise ValueError("mirror ledger top-level schema differs")
    payload = dict(ledger)
    recorded = payload.pop("content_sha256")
    actual_content = canonical_hash(payload)
    if recorded != actual_content or (require_content_identity and recorded != CONTENT_SHA256):
        raise ValueError("mirror ledger declared canonical content SHA-256 drift")
    exact = {
        "schema_version": 1,
        "status": "accepted_pool_readable_mirror",
        "source_manifest_id": "hgsvc-hprc-y1-phased-methylation-v2",
        "source_manifest_content_sha256": SOURCE_MANIFEST_SHA256,
        "copy_manifest_canonical_sha256": COPY_MANIFEST_SHA256,
        "destination_prefix": PREFIX,
        "mirror_accepted": True,
        "load_authorized": False,
        "object_count": 924,
        "sample_count": 231,
        "byte_count": 127_463_220_748,
    }
    for field, expected in exact.items():
        if ledger[field] != expected:
            raise ValueError(f"mirror ledger {field} differs from the accepted identity")
    if ledger["copy_semantics"] != {
        "delete": False, "destination_precondition": "does_not_exist", "overwrite": False,
        "public_access": False, "source": "exact original generation",
    }:
        raise ValueError("mirror ledger copy semantics differ")
    if ledger["reconciliation"] != {
        "duplicates": 0, "extra": 0, "identity_mismatches": 0, "missing": 0,
        "size_md5_equal_original": True, "unique_destination_generations": 924,
    }:
        raise ValueError("mirror ledger reconciliation is not exact and mismatch-free")
    objects = ledger["objects"]
    if not isinstance(objects, list) or len(objects) != 924:
        raise ValueError("mirror ledger must contain exactly 924 objects")
    seen: set[tuple[str, str]] = set()
    generations: set[str] = set()
    samples: set[str] = set()
    byte_count = 0
    previous: tuple[str, int] | None = None
    for ordinal, item in enumerate(objects):
        if not isinstance(item, dict) or set(item) != {"sample_id", "slot", "original", "mirror"}:
            raise ValueError(f"object {ordinal} fields differ from the accepted schema")
        sample = item["sample_id"]
        slot = item["slot"]
        if not isinstance(sample, str) or not sample or slot not in SLOTS:
            raise ValueError(f"object {ordinal} has invalid sample/slot identity")
        key = (sample, slot)
        if key in seen:
            raise ValueError(f"duplicate sample/slot identity {key}")
        seen.add(key); samples.add(sample)
        ordering = (sample, SLOTS.index(slot))
        if previous is not None and ordering <= previous:
            raise ValueError("mirror ledger objects must be ordered by sample and slot")
        previous = ordering
        original = validate_object(item["original"], f"{sample}/{slot}/original", False)
        mirror = validate_object(item["mirror"], f"{sample}/{slot}/mirror", True)
        if original["byte_size"] != mirror["byte_size"] or original["md5_base64"] != mirror["md5_base64"]:
            raise ValueError(f"{sample}/{slot} original/mirror size or MD5 mismatch")
        if mirror["generation"] in generations:
            raise ValueError("mirror destination generations must be unique")
        generations.add(mirror["generation"]); byte_count += mirror["byte_size"]
    if len(samples) != 231 or len(seen) != 924 or len(generations) != 924 or byte_count != 127_463_220_748:
        raise ValueError("mirror ledger counts or byte total differ")
    for sample in samples:
        if {(sample, slot) for slot in SLOTS} - seen:
            raise ValueError(f"{sample} does not have exactly the four phased slots")
    return ledger


def task_object(item: dict[str, Any]) -> dict[str, Any]:
    mirror = item["mirror"]
    return {
        "slot": item["slot"], "uri": mirror["uri"], "generation": mirror["generation"],
        "byte_size": mirror["byte_size"], "md5_base64": mirror["md5_base64"],
        "immutable_read_uri": mirror["immutable_read_uri"],
    }


def generate(ledger: dict[str, Any]) -> list[dict[str, Any]]:
    by_sample = {(item["sample_id"], item["slot"]): item for item in ledger["objects"]}
    tasks = []
    for sample in sorted({item["sample_id"] for item in ledger["objects"]}):
        for haplotype in ("hap1", "hap2"):
            ordinal = len(tasks)
            bed = by_sample[(sample, f"{haplotype}_bed")]
            tbi = by_sample[(sample, f"{haplotype}_bed_index")]
            tasks.append({
                "schema_version": 1,
                "contract_id": CONTRACT_ID,
                "coordinator_task_id": f"custom_{ordinal}",
                "label": f"{sample} {haplotype} chr22",
                "run_id": RUN_ID,
                "task_id": f"{sample}:{haplotype}:chr22",
                "attempt_prefix": f"{RUN_ID}:{sample}:{haplotype}",
                "ledger_content_sha256": CONTENT_SHA256,
                "ledger_raw_sha256": RAW_SHA256,
                "sample": sample,
                "source_haplotype": haplotype,
                "chrom": "chr22",
                "start": 1,
                "stop": CHR22_STOP,
                "bed": task_object(bed),
                "tbi": task_object(tbi),
                "joinable_to_vcf": False,
                "orientation_status": "UNCONFIRMED",
            })
    if len(tasks) != 462:
        raise ValueError("generator did not produce exactly 462 tasks")
    return tasks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, default=LEDGER)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    ledger = validate_ledger_bytes(args.ledger.read_bytes())
    tasks = generate(ledger)
    encoded = json.dumps(tasks, indent=2) + "\n"
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    if args.check:
        if not args.output.exists() or args.output.read_text() != encoded:
            raise SystemExit(f"generated canary tasks differ from {args.output}")
        print(f"verified {len(tasks)} tasks: sha256 {digest}")
    else:
        args.output.write_text(encoded)
        print(f"wrote {len(tasks)} tasks: sha256 {digest}")


if __name__ == "__main__":
    main()
