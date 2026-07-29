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
    "lr_y1_methylation_phased_staging",
    "lr_y1_methylation_phased",
    "lr_y1_methylation_availability",
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
    "lr_y1_methylation_phased_staging", "lr_y1_methylation_phased",
    "lr_y1_methylation_availability", "lr_y1_methylation_summary", "lr_y1_str_histograms_staging",
    "lr_y1_str_histograms",
]:
    ddl = (y1_sql_dir / f"{table}.sql").read_text()
    for identity in ["ancillary_run_id", "release", "cohort", "reference_genome", "modality", "source_version"]:
        assert identity in ddl, f"{table} lacks ancillary identity {identity}"
    assert "PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)" in ddl

active_ancillary = (y1_sql_dir / "lr_y1_active_ancillary.sql").read_text()
assert "ORDER BY (release, cohort, reference_genome, modality)" in active_ancillary

storage = (ROOT / "src" / "y1" / "storage.rs").read_text()
assert "pub const Y1_SCHEMA_VERSION: u16 = 4;" in storage
for table in [
    "lr_y1_methylation_phased_staging",
    "lr_y1_methylation_phased",
    "lr_y1_methylation_availability",
]:
    assert f'include_str!("../../sql/y1/{table}.sql")' in storage

source_measures = {
    "methylation Float32",
    "coverage UInt32",
    "estimated_modified_count UInt32",
    "estimated_unmodified_count UInt32",
    "discretized_methylation Float32",
}
for table in ["lr_y1_methylation_staging", "lr_y1_methylation"]:
    ddl = (y1_sql_dir / f"{table}.sql").read_text()
    assert all(measure in ddl for measure in source_measures)
    assert "combined" in ddl.lower() or "Total" in ddl
for nullable_migration in [
    '("estimated_modified_count", "Nullable(UInt32)")',
    '("estimated_unmodified_count", "Nullable(UInt32)")',
    '("discretized_methylation", "Nullable(Float32)")',
]:
    assert nullable_migration in storage, "existing v3 rows must not receive synthetic source-measure zeroes"

for table in ["lr_y1_methylation_phased_staging", "lr_y1_methylation_phased"]:
    ddl = (y1_sql_dir / f"{table}.sql").read_text()
    assert "source_haplotype UInt8" in ddl
    assert "vcf_strand" not in ddl
    assert all(measure in ddl for measure in source_measures)
    assert "sample_id, source_haplotype" in ddl

availability = (y1_sql_dir / "lr_y1_methylation_availability.sql").read_text()
for column in [
    "inventory_status", "load_status", "source_rows", "canonical_rows", "reason",
    "orientation_status", "queryable_raw", "joinable_to_vcf", "source_manifest_hash",
]:
    assert column in availability
assert "source_haplotype Nullable(UInt8)" in availability

attempts = (y1_sql_dir / "lr_y1_ancillary_task_attempts.sql").read_text()
for column in [
    "lease_id", "sample_id", "data_layer", "source_haplotype", "manifest_entry_id",
    "source_object_slot", "source_generation", "source_size_bytes", "source_checksum",
    "key_hash", "content_hash",
]:
    assert column in attempts

exec((ROOT / "scripts" / "verify-y1-ancillary-manifests.py").read_text(), {"__name__": "__main__", "__file__": str(ROOT / "scripts" / "verify-y1-ancillary-manifests.py")})
print("Manifests verified: legacy smoke is isolated and Y1 schema v4 phased methylation remains unjoined")
