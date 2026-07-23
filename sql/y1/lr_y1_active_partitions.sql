CREATE TABLE IF NOT EXISTS lr_y1_active_partitions (
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    chrom LowCardinality(String),
    revision UInt64,
    run_id String,
    previous_run_id String,
    activated_at_ms UInt64,
    activated_by String
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (release, cohort, reference_genome, chrom);
