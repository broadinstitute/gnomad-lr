-- source_start0/source_end0 preserve BED coordinates; position is start0 + 1.
CREATE TABLE IF NOT EXISTS lr_y1_methylation_staging (
    ancillary_run_id String, attempt_id String,
    release LowCardinality(String), cohort LowCardinality(String),
    reference_genome LowCardinality(String), modality LowCardinality(String),
    source_version String, chrom LowCardinality(String),
    source_start0 UInt32, source_end0 UInt32, position UInt32,
    sample_id LowCardinality(String), methylation Float32, coverage UInt16
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)
ORDER BY (ancillary_run_id, attempt_id, chrom, position, sample_id);
