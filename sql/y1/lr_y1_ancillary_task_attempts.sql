CREATE TABLE IF NOT EXISTS lr_y1_ancillary_task_attempts (
    ancillary_run_id String,
    modality LowCardinality(String),
    chrom LowCardinality(String),
    task_id String,
    attempt_id String,
    interval_start UInt32,
    interval_end UInt32,
    state LowCardinality(String),
    source_rows UInt64,
    staged_rows UInt64,
    reject_rows UInt64,
    content_hash FixedString(64),
    error Nullable(String),
    created_at DateTime64(3, 'UTC'),
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (ancillary_run_id, modality, chrom, task_id, attempt_id);
