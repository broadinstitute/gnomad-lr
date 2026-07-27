-- Populated only from one accepted canonical detail run. Missing positions and
-- absent assay samples have no row; callers must not coalesce absence to zero.
CREATE TABLE IF NOT EXISTS lr_y1_methylation_summary (
    ancillary_run_id String,
    release LowCardinality(String), cohort LowCardinality(String),
    reference_genome LowCardinality(String), modality LowCardinality(String),
    source_version String, chrom LowCardinality(String),
    source_start0 UInt32, source_end0 UInt32, position UInt32,
    mean_methylation Float64, mean_coverage Float64, num_samples UInt32,
    std_methylation Float64, min_methylation Float32, max_methylation Float32
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)
ORDER BY (ancillary_run_id, chrom, position);
