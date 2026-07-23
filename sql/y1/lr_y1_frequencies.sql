CREATE TABLE IF NOT EXISTS lr_y1_frequencies (
    run_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    chrom LowCardinality(String),
    position UInt32,
    source_variant_id String,
    alt_index UInt16,
    division LowCardinality(String),
    ac Nullable(UInt32),
    an Nullable(UInt32),
    af Nullable(Float64),
    values_available UInt8
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, run_id)
ORDER BY (release, cohort, reference_genome, chrom, position, source_variant_id, alt_index, division, run_id);
