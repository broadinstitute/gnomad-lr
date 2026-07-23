CREATE TABLE IF NOT EXISTS lr_y1_summaries_staging (
    run_id String,
    task_id String,
    attempt_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    chrom LowCardinality(String),
    position UInt32,
    source_variant_id String,
    ref_allele String,
    alts Array(String),
    allele_type Nullable(String),
    qual Nullable(Float64),
    filters Array(String),
    ac Array(UInt32),
    an UInt32,
    af Array(Float64),
    allele_lengths Array(Int32),
    length_provenance Array(String),
    source_allele_length Nullable(Int32),
    source_svlen Array(Int32),
    source_svlen_present UInt8,
    frequencies_json String,
    source_info_json String
) ENGINE = MergeTree()
PARTITION BY run_id
ORDER BY (run_id, task_id, attempt_id, chrom, position, source_variant_id);
