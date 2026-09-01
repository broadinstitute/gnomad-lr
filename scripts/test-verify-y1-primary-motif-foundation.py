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
genotype_expectations = json.loads(module.GENOTYPE_EXPECTATIONS.read_text())
module.validate_genotype_expectations(genotype_expectations)
module.validate_storage()
product = module.PRODUCT_LIFECYCLE.read_text()
producer = module.PRODUCT_PRODUCER.read_text()
cli = (ROOT / "src/cli.rs").read_text()
main_rs = (ROOT / "src/main.rs").read_text()
frozen_storage = (ROOT / "src/y1/storage.rs").read_text()
module.validate_product_lifecycle(product, producer, cli, main_rs, frozen_storage)
try:
    module.validate_product_lifecycle(
        product.replace("FORMAT RowBinary", "FORMAT JSONEachRow"),
        producer,
        cli,
        main_rs,
        frozen_storage,
    )
except ValueError:
    pass
else:
    raise AssertionError("product lifecycle without RowBinary hashing accepted")

ddls = {path.name: path.read_text() for path in module.DDL_DIR.glob("*.sql")}
missing_run_mapping = copy.deepcopy(ddls)
missing_run_mapping["lr_y1_primary_motif_runs.sql"] = missing_run_mapping[
    "lr_y1_primary_motif_runs.sql"
].replace("    header_mapping_sha256 Nullable(FixedString(64)),\n", "")
try:
    module.validate_storage_contract(
        missing_run_mapping, (ROOT / "src/y1/primary_motif.rs").read_text()
    )
except ValueError:
    pass
else:
    raise AssertionError("run DDL missing header mapping receipt accepted")


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

corrupt_genotypes = copy.deepcopy(genotype_expectations)
corrupt_genotypes["loci"][2]["no_call_people"] = 0
try:
    module.validate_genotype_expectations(corrupt_genotypes)
except ValueError:
    pass
else:
    raise AssertionError("RFC1 no-call fixture corruption accepted")

print("Primary-motif verifier corruption tests passed")
