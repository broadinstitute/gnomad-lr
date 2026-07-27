CREATE TABLE IF NOT EXISTS lr_y1_active_metadata (
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    revision UInt64,
    metadata_run_id String,
    previous_metadata_run_id String,
    activated_at_ms UInt64,
    activated_by String
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (release, cohort, reference_genome);
