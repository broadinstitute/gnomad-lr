CREATE TABLE IF NOT EXISTS lr_y1_rejects_staging (
    run_id String,
    task_id String,
    attempt_id String,
    record_number Nullable(UInt64),
    source_variant_id Nullable(String),
    reject_code LowCardinality(String),
    message String
) ENGINE = MergeTree()
PARTITION BY run_id
ORDER BY (run_id, task_id, attempt_id, reject_code);
