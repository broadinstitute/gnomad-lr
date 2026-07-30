#!/usr/bin/env python3
"""Focused fail-closed tests for the accepted mirror ledger and 462 tasks."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import subprocess
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate-y1-phased-mirror-canary.py"
spec = importlib.util.spec_from_file_location("mirror_canary", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(module)

raw = module.LEDGER.read_bytes()
ledger = module.validate_ledger_bytes(raw)
tasks = module.generate(ledger)
assert len(tasks) == 462
assert [(task["sample"], task["source_haplotype"]) for task in tasks] == sorted(
    (task["sample"], task["source_haplotype"]) for task in tasks
)
assert [task["coordinator_task_id"] for task in tasks] == [f"custom_{i}" for i in range(462)]
assert all(task["chrom"] == "chr22" and task["start"] == 1 and task["stop"] == 50_818_468 for task in tasks)
assert all(not task["joinable_to_vcf"] and task["orientation_status"] == "UNCONFIRMED" for task in tasks)
assert all(task["bed"]["uri"].startswith(module.PREFIX) and task["tbi"]["uri"].startswith(module.PREFIX) for task in tasks)
assert not any("original" in json.dumps(task) or "fc-" in json.dumps(task) for task in tasks)

checked = subprocess.run(
    ["python3", str(SCRIPT), "--check"], cwd=ROOT, text=True,
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
)
assert checked.returncode == 0, checked.stdout


def rejected(mutator, expected: str) -> None:
    changed = copy.deepcopy(ledger)
    mutator(changed)
    changed.pop("content_sha256", None)
    changed["content_sha256"] = module.canonical_hash(changed)
    try:
        module.validate_ledger_bytes(
            (json.dumps(changed) + "\n").encode(),
            require_raw_identity=False,
            require_content_identity=False,
        )
    except ValueError as error:
        assert expected in str(error), (expected, str(error))
    else:
        raise AssertionError(f"accepted substituted ledger expected to fail with {expected}")


rejected(lambda value: value.__setitem__("object_count", 923), "object_count")
rejected(lambda value: value.__setitem__("destination_prefix", "gs://mutable/"), "destination_prefix")
rejected(lambda value: value["reconciliation"].__setitem__("identity_mismatches", 1), "reconciliation")
rejected(lambda value: value["objects"][0]["mirror"].__setitem__("generation", "1785438228047876"), "immutable URI")
rejected(lambda value: value["objects"][0]["mirror"].__setitem__("byte_size", value["objects"][0]["mirror"]["byte_size"] + 1), "size or MD5 mismatch")
rejected(lambda value: value["objects"][0]["mirror"].__setitem__("md5_base64", "AAAAAAAAAAAAAAAAAAAAAA=="), "size or MD5 mismatch")
rejected(lambda value: value["objects"].__setitem__(1, copy.deepcopy(value["objects"][0])), "duplicate sample/slot")

with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / "ledger.json"
    path.write_bytes(raw + b" ")
    result = subprocess.run(
        ["python3", str(SCRIPT), "--ledger", str(path), "--output", str(Path(directory) / "out.json")],
        cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
    )
    assert result.returncode != 0 and "raw SHA-256 drift" in result.stdout, result.stdout

manifest_bytes = module.OUTPUT.read_bytes()
manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
run_manifest = json.loads(
    (ROOT / "manifests" / "y1" / "phased-methylation-mirror-canary-run.json").read_text()
)
assert run_manifest["load_authorized"] is False
assert run_manifest["starting_workers"] == 0 and run_manifest["canary_workers"] == 1
assert run_manifest["coordinator_batch_size"] == 1
assert run_manifest["task_count"] == 462 and run_manifest["task_manifest_sha256"] == manifest_sha256
with (ROOT / "genohype.phased-canary.toml").open("rb") as handle:
    profile = tomllib.load(handle)["pools"]["lr-phased-chr22-canary"]
assert profile["starting_workers"] == 0 and profile["workers"] == 1
assert profile["worker_binary"] == "target/release/gnomad-lr-worker"
print(f"phased mirror canary tests passed: 462 tasks, sha256 {manifest_sha256}")
