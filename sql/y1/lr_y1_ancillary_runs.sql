CREATE TABLE IF NOT EXISTS lr_y1_ancillary_runs (
    ancillary_run_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    modality LowCardinality(String),
    source_version String,
    source_manifest_hash FixedString(64),
    scope LowCardinality(String),
    state LowCardinality(String),
    source_rows UInt64,
    canonical_rows UInt64,
    reject_rows UInt64,
    content_hash FixedString(64),
    peak_rss_bytes UInt64,
    created_at DateTime64(3, 'UTC'),
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (release, cohort, reference_genome, modality, ancillary_run_id);
