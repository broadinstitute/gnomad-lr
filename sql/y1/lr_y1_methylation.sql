CREATE TABLE IF NOT EXISTS lr_y1_methylation (
    ancillary_run_id String,
    release LowCardinality(String), cohort LowCardinality(String),
    reference_genome LowCardinality(String), modality LowCardinality(String),
    source_version String, chrom LowCardinality(String),
    source_start0 UInt32, source_end0 UInt32, position UInt32,
    sample_id LowCardinality(String), methylation Float32, coverage UInt16
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)
ORDER BY (ancillary_run_id, chrom, position, sample_id);
