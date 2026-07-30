-- Reserved compatibility table. Operational v5 phased attempts write directly
-- to lr_y1_methylation_phased; this exact-shape table is not a publication hop.
CREATE TABLE IF NOT EXISTS lr_y1_methylation_phased_staging (
    ancillary_run_id String, task_id String, attempt_id String, lease_id String,
    release LowCardinality(String), cohort LowCardinality(String),
    reference_genome LowCardinality(String), modality LowCardinality(String),
    source_version String, source_manifest_hash FixedString(64),
    manifest_entry_id String, chrom LowCardinality(String),
    source_start0 UInt32, source_end0 UInt32, position UInt32,
    sample_id LowCardinality(String), source_haplotype UInt8,
    methylation Float32, coverage UInt32,
    estimated_modified_count UInt32, estimated_unmodified_count UInt32,
    discretized_methylation Float32
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)
ORDER BY (ancillary_run_id, task_id, attempt_id, lease_id, chrom, position, sample_id, source_haplotype);
