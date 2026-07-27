CREATE TABLE IF NOT EXISTS lr_y1_coverage (
    ancillary_run_id String,
    release LowCardinality(String), cohort LowCardinality(String),
    reference_genome LowCardinality(String), modality LowCardinality(String),
    source_version String, chrom LowCardinality(String), position UInt32,
    mean Float32, median Float32,
    over_1 Float32, over_5 Float32, over_10 Float32, over_15 Float32,
    over_20 Float32, over_25 Float32, over_30 Float32, over_50 Float32, over_100 Float32
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)
ORDER BY (ancillary_run_id, chrom, position);
