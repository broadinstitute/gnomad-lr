-- Materialized from the immutable 292-sample roster and accepted per-contig
-- task receipts, never inferred from a numeric detail row or one queried locus.
CREATE TABLE IF NOT EXISTS lr_y1_methylation_availability (
    ancillary_run_id String,
    release LowCardinality(String), cohort LowCardinality(String),
    reference_genome LowCardinality(String), modality LowCardinality(String),
    source_version String, source_manifest_hash FixedString(64),
    chrom LowCardinality(String), sample_id LowCardinality(String),
    data_layer LowCardinality(String), source_haplotype Nullable(UInt8),
    inventory_status LowCardinality(String), load_status LowCardinality(String),
    source_rows UInt64, canonical_rows UInt64, reason String,
    orientation_status String, queryable_raw Bool, joinable_to_vcf Bool
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)
ORDER BY (ancillary_run_id, chrom, sample_id, data_layer, source_haplotype)
SETTINGS allow_nullable_key = 1;
