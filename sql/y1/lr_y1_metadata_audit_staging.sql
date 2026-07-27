CREATE TABLE IF NOT EXISTS lr_y1_metadata_audit_staging (
    metadata_run_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    sample_id String,
    roster_index UInt16,
    event_type LowCardinality(String),
    primary_present UInt8,
    primary_subpopulation Nullable(String),
    primary_superpopulation Nullable(String),
    supplemental_ethnicity Nullable(String),
    selected_superpopulation Nullable(String),
    selected_source Nullable(String),
    details String,
    source_manifest_id String,
    source_manifest_sha256 FixedString(64)
) ENGINE = MergeTree
PARTITION BY (release, cohort, reference_genome)
ORDER BY (metadata_run_id, sample_id, event_type);
