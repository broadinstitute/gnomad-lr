#!/usr/bin/env python3
"""Generate one-shot source-haplotype methylation tasks from the pinned v2 source.

By default this reads only the first 64 KiB of every generation-pinned TBI with
`gcloud storage cat --range=0-65535` and emits tasks only for canonical contigs
actually named by that index. For offline/reproducible generation, pass a JSON
--contig-inventory mapping `sample_id:hap1|hap2` to a list of TBI contig names.
"""

import argparse
import base64
import gzip
import hashlib
import json
import struct
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "sources/y1/methylation-phased-source-manifest.json"
CONTIGS = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY"]
LENGTHS = {
    "chr1": 248956422, "chr2": 242193529, "chr3": 198295559, "chr4": 190214555,
    "chr5": 181538259, "chr6": 170805979, "chr7": 159345973, "chr8": 145138636,
    "chr9": 138394717, "chr10": 133797422, "chr11": 135086622, "chr12": 133275309,
    "chr13": 114364328, "chr14": 107043718, "chr15": 101991189, "chr16": 90338345,
    "chr17": 83257441, "chr18": 80373285, "chr19": 58617616, "chr20": 64444167,
    "chr21": 46709983, "chr22": 50818468, "chrX": 156040895, "chrY": 57227415,
}


def checked_manifest(path: Path):
    value = json.loads(path.read_text())
    recorded = value.pop("content_sha256")
    actual = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if recorded != actual or value.get("manifest_id") != "hgsvc-hprc-y1-phased-methylation-v2":
        raise SystemExit("source manifest canonical hash/identity mismatch")
    return value


def identity(entry, slot):
    descriptor = entry["objects"][slot]
    item = descriptor["immutable_identity"]
    if not item or descriptor["load_authorized"] is not False:
        raise ValueError(f"{entry['sample_id']}:{slot} is not the pinned blocked v2 identity")
    checksum = item["checksum"]
    if checksum["algorithm"] != "md5_base64" or len(base64.b64decode(checksum["value"])) != 16:
        raise ValueError(f"{entry['sample_id']}:{slot} has invalid MD5 identity")
    if item["immutable_read_uri"] != f"{item['uri']}?generation={item['generation']}":
        raise ValueError(f"{entry['sample_id']}:{slot} is not generation pinned")
    return item


def tbi_names_from_prefix(data: bytes):
    if len(data) < 18 or data[:2] != b"\x1f\x8b" or data[12:14] != b"BC":
        raise ValueError("TBI is not BGZF")
    block_size = struct.unpack_from("<H", data, 16)[0] + 1
    decoded = gzip.decompress(data[:block_size])
    if decoded[:4] != b"TBI\x01" or len(decoded) < 36:
        raise ValueError("invalid TBI header")
    n_ref = struct.unpack_from("<i", decoded, 4)[0]
    names_len = struct.unpack_from("<i", decoded, 32)[0]
    names = decoded[36:36 + names_len]
    if len(names) != names_len:
        raise ValueError("TBI names do not fit in first BGZF block")
    parsed = [name.decode() for name in names.rstrip(b"\0").split(b"\0") if name]
    if len(parsed) != n_ref:
        raise ValueError("TBI reference-name count mismatch")
    return parsed


def inspect_tbi(item):
    pinned = f"{item['uri']}#{item['generation']}"
    result = subprocess.run(
        ["gcloud", "storage", "cat", "--range=0-65535", pinned],
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return tbi_names_from_prefix(result.stdout)


def validate_clickhouse_url(value):
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise SystemExit("--clickhouse-url must be credential-free http(s) with a host")
    databases = parse_qs(parsed.query).get("database", [])
    if len(databases) != 1 or databases[0] in ("", "default"):
        raise SystemExit("--clickhouse-url must select one non-default database")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--clickhouse-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--contig-inventory", type=Path)
    args = parser.parse_args()
    validate_clickhouse_url(args.clickhouse_url)
    source = checked_manifest(SOURCE)
    inventory = json.loads(args.contig_inventory.read_text()) if args.contig_inventory else None
    tasks = []
    source_samples = [entry for entry in source["samples"] if entry["inventory_status"] == "source_present"]
    if len(source_samples) != 231:
        raise SystemExit(f"expected 231 pinned source-present samples, found {len(source_samples)}")

    for entry in sorted(source_samples, key=lambda value: value["sample_id"]):
        sample = entry["sample_id"]
        for haplotype in (1, 2):
            name = f"hap{haplotype}"
            bed = identity(entry, f"{name}_bed")
            index = identity(entry, f"{name}_bed_index")
            available = inventory[f"{sample}:{name}"] if inventory is not None else inspect_tbi(index)
            for chrom in CONTIGS:
                if chrom not in available:
                    continue
                ordinal = len(tasks)
                tasks.append({
                    "coordinator_task_id": f"custom_{ordinal}",
                    "bed_path": bed["uri"], "bed_generation": bed["generation"],
                    "bed_byte_size": bed["byte_size"], "bed_md5_base64": bed["checksum"]["value"],
                    "bed_index_path": index["uri"], "bed_index_generation": index["generation"],
                    "bed_index_byte_size": index["byte_size"], "bed_index_md5_base64": index["checksum"]["value"],
                    "sample_id": sample, "chrom": chrom, "start": 1, "stop": LENGTHS[chrom],
                    "source_haplotype": haplotype, "clickhouse_url": args.clickhouse_url,
                })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(tasks, indent=2) + "\n")
    print(json.dumps({
        "source_samples": 231, "haplotypes_per_sample": 2,
        "canonical_contigs": 24, "upper_bound": 231 * 2 * 24,
        "expected_task_count_from_tbi_inventory": len(tasks), "output": str(args.output),
    }, indent=2), file=sys.stderr)


if __name__ == "__main__":
    main()
