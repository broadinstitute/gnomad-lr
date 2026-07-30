-- Durable in-place freeze/acceptance receipts for ancillary raw candidates.
-- No row in this table is an active pointer or joined-serving authorization.
CREATE TABLE IF NOT EXISTS lr_y1_ancillary_runs (
    ancillary_run_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    modality LowCardinality(String),
    data_layer LowCardinality(String),
    chrom LowCardinality(String),
    source_version String,
    source_manifest_id String,
    source_manifest_hash FixedString(64),
    scope LowCardinality(String),
    state LowCardinality(String),
    expected_tasks UInt32,
    source_rows UInt64,
    canonical_rows UInt64,
    reject_rows UInt64,
    key_hash FixedString(64),
    content_hash FixedString(64),
    worker_principal String,
    peak_rss_bytes UInt64,
    frozen_at_ms UInt64,
    report_json String,
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (release, cohort, reference_genome, modality, data_layer, chrom, ancillary_run_id);
