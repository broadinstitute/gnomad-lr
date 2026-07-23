CREATE TABLE IF NOT EXISTS lr_y1_task_attempts (
    run_id String,
    task_id String,
    attempt_id String,
    revision UInt64,
    state LowCardinality(String),
    chrom LowCardinality(String),
    interval_start UInt32,
    interval_end UInt32,
    source_records UInt64,
    summary_rows UInt64,
    allele_rows UInt64,
    frequency_rows UInt64,
    carrier_rows UInt64,
    rejected_records UInt64,
    report_json String,
    started_at_ms UInt64,
    updated_at_ms UInt64,
    error String
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (run_id, task_id, attempt_id);
