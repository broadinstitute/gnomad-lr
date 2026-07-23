#!/usr/bin/env python3
"""Fail when duplicated immutable dependency/image pins drift apart."""

from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parent.parent

with (ROOT / "Cargo.toml").open("rb") as handle:
    cargo = tomllib.load(handle)
core_rev = cargo["dependencies"]["genohype-core"]["rev"]
pool_rev = cargo["dependencies"]["genohype-pool"]["rev"]
makefile = (ROOT / "Makefile").read_text()
make_match = re.search(r"^GENOHYPE_REV := ([0-9a-f]{40})$", makefile, re.MULTILINE)
assert make_match, "Makefile GENOHYPE_REV is missing or not a full SHA"
make_rev = make_match.group(1)
assert core_rev == pool_rev == make_rev, (
    f"Genohype pins differ: core={core_rev}, pool={pool_rev}, Makefile={make_rev}"
)

with (ROOT / "Cargo.lock").open("rb") as handle:
    lock = tomllib.load(handle)
locked_genohype = set()
for package in lock["package"]:
    if package["name"] in {"genohype-core", "genohype-pool"}:
        locked_genohype.add(package["name"])
        assert make_rev in package.get("source", ""), (
            f"Cargo.lock {package['name']} does not use {make_rev}"
        )
assert locked_genohype == {"genohype-core", "genohype-pool"}, (
    f"Cargo.lock Genohype packages are incomplete: {sorted(locked_genohype)}"
)

image_pattern = re.compile(
    r"clickhouse/clickhouse-server:[^\s@]+@sha256:[0-9a-f]{64}"
)
compose = image_pattern.findall((ROOT / "development/clickhouse.compose.yml").read_text())
workflow = image_pattern.findall((ROOT / ".github/workflows/ci.yml").read_text())
assert len(compose) == 1 and len(workflow) == 1, "ClickHouse image pin is missing"
assert compose[0] == workflow[0], (
    f"ClickHouse pins differ: compose={compose[0]}, CI={workflow[0]}"
)

print(f"Pins verified: Genohype {make_rev[:8]}, {compose[0]}")
