-- Append-only start and terminal receipts; no serving activation is implied.
CREATE TABLE lr_str_source_histogram_receipts_v1
(
    contract String,
    cohort String,
    run_id String,
    task_id String,
    source_uri String,
    source_generation String,
    source_size_bytes UInt64,
    source_md5_base64 String,
    database String,
    status String,
    completeness String,
    diagnostic String,
    gcs_metadata_verified Bool,
    eof_observed Bool,
    complete_body_identity_verified Bool,
    bytes_read UInt64,
    computed_md5_base64 String,
    data_rows_examined UInt64,
    validated_rows UInt64,
    zero_called_rows UInt64,
    rows_insert_attempted UInt64,
    rows_insert_acknowledged UInt64,
    partial_writes_possible Bool
)
ENGINE = MergeTree
ORDER BY (cohort, run_id, task_id, status);
