#!/usr/bin/env python3
"""Backward-compatible chr22 entry point for the generic Y1 reconciler."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
from typing import Any, TextIO

_GENERIC = Path(__file__).with_name("reconcile-y1-contig-source.py")
_spec = importlib.util.spec_from_file_location("reconcile_y1_contig_source", _GENERIC)
_generic = importlib.util.module_from_spec(_spec)
assert _spec.loader
_spec.loader.exec_module(_generic)

# Keep the existing import-level API usable by focused tests and operators.
CHR22_LENGTH = _generic.GRCH38_CONTIG_LENGTHS["chr22"]
ANNOTATION_KEYS = _generic.ANNOTATION_KEYS
open_vcf = _generic.open_vcf
parse_info = _generic.parse_info
values = _generic.values
alt_has_annotation = _generic.alt_has_annotation


def checked_source(source_manifest: dict[str, Any], cohort: str) -> dict[str, Any]:
    return _generic.checked_source(source_manifest, cohort, "chr22")


def reconcile(stream: TextIO, cohort: str) -> dict[str, Any]:
    return _generic.reconcile(stream, cohort, "chr22")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--cohort", required=True, choices=_generic.COHORTS)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evidence-uri", required=True)
    parser.add_argument("--producer", required=True, help="independent program/version or operator identity")
    parser.add_argument("--vcf", help="checked local mirror override; identity still comes from source manifest")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    manifest = json.loads(args.source_manifest.read_text())
    source = checked_source(manifest, args.cohort)
    with open_vcf(args.vcf or source["uri"]) as stream:
        output = _generic.build_output(manifest, stream, args.cohort, "chr22",
                                       args.run_id, args.evidence_uri, args.producer)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "counts": output["counts"],
                      "facts": output["facts"]}, sort_keys=True))


if __name__ == "__main__":
    main()
