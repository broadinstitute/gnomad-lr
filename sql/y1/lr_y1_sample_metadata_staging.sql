CREATE TABLE IF NOT EXISTS lr_y1_sample_metadata_staging (
    metadata_run_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    sample_id String,
    roster_index UInt16,
    subpopulation LowCardinality(String),
    superpopulation LowCardinality(String),
    population_descriptor String,
    sex LowCardinality(String),
    collection LowCardinality(String),
    primary_metadata_present UInt8,
    ancestry_source LowCardinality(String),
    source_manifest_id String,
    source_manifest_sha256 FixedString(64)
) ENGINE = MergeTree
PARTITION BY (release, cohort, reference_genome)
ORDER BY (metadata_run_id, release, cohort, reference_genome, sample_id);
