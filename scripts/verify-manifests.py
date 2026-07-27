#!/usr/bin/env python3
"""Validate legacy isolation and the repository-owned Y1 schema inventory."""

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

y1_tables = {
    "lr_y1_load_runs",
    "lr_y1_task_attempts",
    "lr_y1_active_partitions",
    "lr_y1_metadata_runs",
    "lr_y1_active_metadata",
    "lr_y1_sample_metadata_staging",
    "lr_y1_metadata_audit_staging",
    "lr_y1_sample_metadata",
    "lr_y1_metadata_audit",
    "lr_y1_ancillary_runs",
    "lr_y1_ancillary_task_attempts",
    "lr_y1_active_ancillary",
    "lr_y1_coverage_staging",
    "lr_y1_coverage",
    "lr_y1_methylation_staging",
    "lr_y1_methylation",
    "lr_y1_methylation_summary",
    "lr_y1_str_histograms_staging",
    "lr_y1_str_histograms",
    "lr_y1_rejects_staging",
    "lr_y1_summaries_staging",
    "lr_y1_alleles_staging",
    "lr_y1_frequencies_staging",
    "lr_y1_carriers_staging",
    "lr_y1_summaries",
    "lr_y1_alleles",
    "lr_y1_frequencies",
    "lr_y1_carriers",
}
y1_sql_dir = ROOT / "sql" / "y1"
actual_y1_files = {path.stem for path in y1_sql_dir.glob("*.sql")}
assert actual_y1_files == y1_tables, (
    f"Y1 DDL inventory mismatch: missing={sorted(y1_tables - actual_y1_files)}, "
    f"unexpected={sorted(actual_y1_files - y1_tables)}"
)
for table in sorted(y1_tables):
    ddl = (y1_sql_dir / f"{table}.sql").read_text()
    assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl
    assert "CREATE TABLE IF NOT EXISTS lr_variants" not in ddl
    assert "CREATE TABLE IF NOT EXISTS lr_haplotypes" not in ddl

for table in ["lr_y1_summaries", "lr_y1_alleles", "lr_y1_frequencies", "lr_y1_carriers"]:
    ddl = (y1_sql_dir / f"{table}.sql").read_text()
    assert "PARTITION BY (release, cohort, reference_genome, chrom, run_id)" in ddl

for table in [
    "lr_y1_coverage_staging", "lr_y1_coverage",
    "lr_y1_methylation_staging", "lr_y1_methylation",
    "lr_y1_methylation_summary", "lr_y1_str_histograms_staging",
    "lr_y1_str_histograms",
]:
    ddl = (y1_sql_dir / f"{table}.sql").read_text()
    for identity in ["ancillary_run_id", "release", "cohort", "reference_genome", "modality", "source_version"]:
        assert identity in ddl, f"{table} lacks ancillary identity {identity}"
    assert "PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)" in ddl

active_ancillary = (y1_sql_dir / "lr_y1_active_ancillary.sql").read_text()
assert "ORDER BY (release, cohort, reference_genome, modality)" in active_ancillary

exec((ROOT / "scripts" / "verify-y1-ancillary-manifests.py").read_text(), {"__name__": "__main__", "__file__": str(ROOT / "scripts" / "verify-y1-ancillary-manifests.py")})
print("Manifests verified: legacy smoke is isolated and Y1 DDL is versioned separately")
