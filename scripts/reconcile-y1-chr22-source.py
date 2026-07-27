#!/usr/bin/env python3
"""Independently derive full-chr22 source/genotype/annotation acceptance inputs.

This deliberately parses raw VCF text without importing or invoking the Rust loader.
Its versioned JSON is the only input accepted by `finalize-y1-chr22`.
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import hashlib
import io
import json
import subprocess
from pathlib import Path
from typing import Any, Iterator, TextIO

CHR22_LENGTH = 50_818_468
ANNOTATION_KEYS = (
    "dbSNP_ID", "cadd_phred", "phylop", "vep",
    "gnomAD_V4_match_ID", "gnomAD_V4_match_type", "gnomAD_V4_match_source",
)


def checked_source(source_manifest: dict[str, Any], cohort: str) -> dict[str, Any]:
    if source_manifest.get("release") != "Y1" or source_manifest.get("chromosome") != "chr22":
        raise ValueError("source manifest must describe Y1 chr22")
    vcfs = [obj for obj in source_manifest.get("objects", [])
            if obj.get("cohort") == cohort and str(obj.get("name", "")).endswith(".vcf.gz")]
    if len(vcfs) != 1:
        raise ValueError(f"cohort {cohort} must have exactly one VCF")
    source = vcfs[0]
    for field in ("mirror_generation", "md5_base64", "size"):
        if not source.get(field):
            raise ValueError(f"source identity is missing {field}")
    prefix = source_manifest.get("mirror_prefix")
    if not isinstance(prefix, str) or not prefix.startswith("gs://"):
        raise ValueError("source manifest has no checked mirror prefix")
    return {**source, "uri": f"{prefix}/{cohort}/vcfs/{source['name']}"}


@contextlib.contextmanager
def open_vcf(uri: str) -> Iterator[TextIO]:
    if uri.startswith("gs://"):
        process = subprocess.Popen(
            ["gcloud", "storage", "cat", uri], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        assert process.stdout and process.stderr
        stream = io.TextIOWrapper(gzip.GzipFile(fileobj=process.stdout), encoding="utf-8")
        try:
            yield stream
        finally:
            stream.close()
            stderr = process.stderr.read().decode(errors="replace")
            status = process.wait()
            if status:
                raise RuntimeError(f"gcloud storage cat failed ({status}): {stderr}")
    else:
        with gzip.open(uri, "rt", encoding="utf-8") as stream:
            yield stream


def parse_info(raw: str) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for entry in raw.split(";"):
        key, separator, value = entry.partition("=")
        if not key or key in result:
            raise ValueError(f"invalid or duplicate INFO entry {entry!r}")
        result[key] = value if separator else None
    return result


def values(raw: str | None) -> list[str]:
    return [] if raw in (None, "", ".") else raw.split(",")


def alt_has_annotation(info: dict[str, str | None], alt_index: int, alt_count: int) -> bool:
    for key in ANNOTATION_KEYS:
        candidates = values(info.get(key))
        if not candidates:
            continue
        candidate = candidates[alt_index] if len(candidates) == alt_count else candidates[0]
        if candidate not in ("", "."):
            return True
    return False


def reconcile(stream: TextIO, cohort: str) -> dict[str, Any]:
    info_ids: set[str] = set()
    samples: list[str] = []
    facts = {
        "source_records": 0, "alt_alleles": 0, "frequency_rows": 0,
        "genotype_calls": 0, "called_alleles": 0, "carrier_alt_copies": 0,
        "fully_missing_genotypes": 0, "partially_called_genotypes": 0,
        "annotated_alt_alleles": 0,
    }
    source_hash, genotype_hash, annotation_hash = hashlib.sha256(), hashlib.sha256(), hashlib.sha256()
    divisions: set[str] | None = None

    for line_number, line in enumerate(stream, 1):
        if line.startswith("##INFO=<ID="):
            info_ids.add(line.split("=", 2)[2].split(",", 1)[0])
            continue
        if line.startswith("#CHROM"):
            columns = line.rstrip("\n").split("\t")
            samples = columns[9:]
            divisions = {key[3:] for key in info_ids if key.startswith("AC_") and key != "AC_grpmax"
                         and f"AN_{key[3:]}" in info_ids and f"AF_{key[3:]}" in info_ids}
            continue
        if line.startswith("#"):
            continue
        if divisions is None:
            raise ValueError("VCF records appeared before #CHROM")
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 8:
            raise ValueError(f"line {line_number}: fewer than 8 VCF columns")
        chrom, position = parts[0], int(parts[1])
        if chrom != "chr22" or not 1 <= position <= CHR22_LENGTH:
            raise ValueError(f"line {line_number}: record outside GRCh38 chr22")
        alts = parts[4].split(",")
        if not alts or any(not alt or alt == "." for alt in alts):
            raise ValueError(f"line {line_number}: invalid ALT")
        info = parse_info(parts[7])
        ac = [int(value) for value in values(info.get("AC"))]
        an_values = values(info.get("AN"))
        if len(ac) != len(alts) or len(an_values) != 1:
            raise ValueError(f"line {line_number}: AC/AN cardinality mismatch")
        expected_an = int(an_values[0])

        facts["source_records"] += 1
        facts["alt_alleles"] += len(alts)
        facts["frequency_rows"] += len(alts) * (1 + len(divisions))
        source_hash.update("\t".join(parts[:8]).encode() + b"\n")
        annotation_values = []
        for alt_index in range(len(alts)):
            annotated = alt_has_annotation(info, alt_index, len(alts))
            facts["annotated_alt_alleles"] += int(annotated)
            annotation_values.append("1" if annotated else "0")
        annotation_hash.update(f"{chrom}\t{position}\t{parts[2]}\t{','.join(annotation_values)}\n".encode())

        if cohort == "aou":
            if len(parts) != 8 or samples:
                raise ValueError(f"line {line_number}: AoU unexpectedly contains genotypes")
            continue
        if len(parts) != 9 + len(samples):
            raise ValueError(f"line {line_number}: HGSVC/HPRC sample count mismatch")
        format_keys = parts[8].split(":")
        if "GT" not in format_keys:
            raise ValueError(f"line {line_number}: FORMAT has no GT")
        gt_index = format_keys.index("GT")
        observed_ac = [0] * len(alts)
        observed_an = 0
        for sample, sample_value in zip(samples, parts[9:]):
            fields = sample_value.split(":")
            gt = fields[gt_index] if gt_index < len(fields) else "."
            alleles = gt.replace("|", "/").split("/")
            called = [int(allele) for allele in alleles if allele != "."]
            facts["genotype_calls"] += 1
            facts["called_alleles"] += len(called)
            if not called:
                facts["fully_missing_genotypes"] += 1
            elif len(called) != len(alleles):
                facts["partially_called_genotypes"] += 1
            for allele in called:
                if allele < 0 or allele > len(alts):
                    raise ValueError(f"line {line_number}: GT ALT index out of range")
                observed_an += 1
                if allele:
                    observed_ac[allele - 1] += 1
                    facts["carrier_alt_copies"] += 1
            genotype_hash.update(f"{chrom}\t{position}\t{parts[2]}\t{sample}\t{gt}\n".encode())
        if observed_an != expected_an or observed_ac != ac:
            raise ValueError(f"line {line_number}: genotype AC/AN does not reconcile to INFO")

    if facts["source_records"] == 0:
        raise ValueError("no chr22 source records found")
    facts.update({
        "source_content_sha256": source_hash.hexdigest(),
        "genotype_content_sha256": genotype_hash.hexdigest(),
        "annotation_content_sha256": annotation_hash.hexdigest(),
    })
    return facts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--cohort", required=True, choices=("hgsvc_hprc", "aou"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evidence-uri", required=True)
    parser.add_argument("--producer", required=True, help="independent program/version or operator identity")
    parser.add_argument("--vcf", help="checked local mirror override; identity still comes from source manifest")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    source = checked_source(json.loads(args.source_manifest.read_text()), args.cohort)
    with open_vcf(args.vcf or source["uri"]) as stream:
        facts = reconcile(stream, args.cohort)
    output = {
        "contract_version": 1,
        "run_id": args.run_id,
        "cohort": args.cohort,
        "chrom": "chr22",
        "evidence_uri": args.evidence_uri,
        "producer": args.producer,
        "source_generation": str(source["mirror_generation"]),
        "source_checksum": source["md5_base64"],
        "counts": {
            "source_records": facts["source_records"],
            "summaries": facts["source_records"],
            "alleles": facts["alt_alleles"],
            "frequencies": facts["frequency_rows"],
            "carriers": facts["carrier_alt_copies"],
            "rejects": 0,
        },
        "facts": facts,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "counts": output["counts"], "facts": facts}, sort_keys=True))


if __name__ == "__main__":
    main()
