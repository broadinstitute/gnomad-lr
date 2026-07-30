CREATE TABLE IF NOT EXISTS lr_y1_carriers (
    run_id String,
    task_id String,
    attempt_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    chrom LowCardinality(String),
    position UInt32,
    source_variant_id String,
    alt_index UInt16,
    alt String,
    sample_id LowCardinality(String),
    genotype_position UInt16,
    gt_alleles Array(Nullable(UInt16)),
    gt_phased UInt8,
    genotype_fields_json String,
    position_fields_json String
) ENGINE = MergeTree()
PARTITION BY run_id
ORDER BY (run_id, task_id, attempt_id, chrom, position, source_variant_id, alt_index, sample_id, genotype_position);
