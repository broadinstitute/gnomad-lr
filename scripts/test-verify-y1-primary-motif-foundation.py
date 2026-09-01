#!/usr/bin/env python3
"""Focused corruption tests for the primary-motif foundation verifier."""

import copy
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/verify-y1-primary-motif-foundation.py"
spec = importlib.util.spec_from_file_location("primary_motif_verifier", SCRIPT)
module = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(module)

registry = json.loads((ROOT / "sources/y1/primary-repeat-registry.json").read_text())
module.validate_registry(registry)
module.validate_storage()


def rejected(change, message: str, *, refresh_digest: bool = True) -> None:
    value = copy.deepcopy(registry)
    change(value)
    if refresh_digest:
        value["content_sha256"] = module.canonical_digest(value)
    try:
        module.validate_registry(value)
    except ValueError:
        return
    raise AssertionError(message)


rejected(
    lambda value: value.__setitem__("content_sha256", "0" * 64),
    "stale digest accepted",
    refresh_digest=False,
)
rejected(lambda value: value["entries"][1].__setitem__("motif", "CAG"), "ATXN1 rotation accepted")
rejected(lambda value: value["entries"][2].__setitem__("biological_role", "pathogenic motif"), "RFC1 role drift accepted")
rejected(lambda value: value["entries"][0].__setitem__("source_position", 3074877), "shifted source position accepted")
rejected(lambda value: value["entries"][0].__setitem__("reviewer", "unnamed"), "candidate approval claim accepted")
rejected(lambda value: value["entries"][0].__setitem__("component_index", 6), "out-of-range component accepted")


def whitespace_reviewed(value: dict) -> None:
    value["approval_state"] = "REVIEWED"
    for entry in value["entries"]:
        entry["approval_state"] = "REVIEWED"
        entry["reviewer"] = "   "
        entry["approval_receipt"] = "receipt"
        entry["catalog_digest"] = "0" * 64


rejected(whitespace_reviewed, "whitespace-only reviewed identity accepted")

print("Primary-motif verifier corruption tests passed")
