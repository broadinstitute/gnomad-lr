#!/usr/bin/env python3
"""Offline focused checks for the direct methylation presentation manifest."""
import json
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
source = json.loads((ROOT / "sources/y1/methylation-phased-source-manifest.json").read_text())
samples = sorted(entry["sample_id"] for entry in source["samples"] if entry["inventory_status"] == "source_present")
contigs = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY"]
ddl = (ROOT / "sql/y1/lr_y1_methylation_source_haplotype_presentation.sql").read_text()
for field in ["stable_key FixedString(64)", "chrom LowCardinality(String)", "pos1 UInt32", "pos2 UInt32", "sample_id LowCardinality(String)", "source_haplotype UInt8", "methylation Float32", "coverage UInt32"]:
    assert field in ddl
assert "source_haplotype IN (1, 2)" in ddl
assert len(samples) == 231
with tempfile.TemporaryDirectory() as directory:
    directory = Path(directory)
    inventory = {f"{sample}:hap{hap}": contigs for sample in samples for hap in (1, 2)}
    inventory_path = directory / "inventory.json"
    output = directory / "manifest.json"
    inventory_path.write_text(json.dumps(inventory))
    subprocess.run([
        "python3", str(ROOT / "scripts/generate-y1-direct-methylation-presentation-manifest.py"),
        "--clickhouse-url", "http://clickhouse:8123/?database=fresh_presentation",
        "--contig-inventory", str(inventory_path), "--output", str(output),
    ], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    tasks = json.loads(output.read_text())
    assert len(tasks) == 231 * 2 * 24 == 11088
    assert [task["coordinator_task_id"] for task in tasks] == [f"custom_{i}" for i in range(len(tasks))]
    assert {task["source_haplotype"] for task in tasks} == {1, 2}
    assert all(task["start"] == 1 and task["clickhouse_url"].endswith("fresh_presentation") for task in tasks)
    assert all(task["bed_path"].startswith("gs://") and task["bed_generation"].isdigit() for task in tasks)
print("direct methylation presentation manifest: 11,088-task full canonical fixture passed")
