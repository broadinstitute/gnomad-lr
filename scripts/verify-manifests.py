#!/usr/bin/env python3
"""Validate the checked-in legacy smoke safety boundary."""

from pathlib import Path
import tomllib

ROOT = Path(__file__).resolve().parent.parent


def read_toml(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


smoke = read_toml(ROOT / "development" / "smoke.toml")
assert smoke["smoke"]["profile"] == "legacy_v1_plumbing"
assert smoke["smoke"]["database"].startswith("gnomad_lr_smoke_")
assert "gnomAD_LR_Y1." not in smoke["inputs"]["vcf"]
assert "/gnomAD_LR_vcfs/" not in smoke["inputs"]["vcf"], (
    "the legacy smoke must not point at the Y1 source tree"
)

print("Manifest verified: legacy smoke is isolated from Y1 sources")
