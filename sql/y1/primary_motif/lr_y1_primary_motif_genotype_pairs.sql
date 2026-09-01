-- Anonymous internal verifier rows. Allele indices are source identities needed
-- to prove margins; no contributor identifier or raw GT is persisted.
CREATE TABLE IF NOT EXISTS lr_y1_primary_motif_genotype_pairs (
    product_run_id String,
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    chrom LowCardinality(String),
    primary_run_id String,
    source_variant_id String,
    canonical_locus_id String,
    registry_digest FixedString(64),
    metric LowCardinality(String),
    division LowCardinality(String),
    ancestry LowCardinality(Nullable(String)),
    sex LowCardinality(Nullable(String)),
    shorter_allele_index UInt16,
    longer_allele_index UInt16,
    shorter_exact_units UInt32,
    longer_exact_units UInt32,
    people UInt32,
    phased_people UInt32,
    unphased_people UInt32,
    pair_receipt_sha256 FixedString(64)
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, product_run_id)
ORDER BY (product_run_id, chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), shorter_exact_units, longer_exact_units, shorter_allele_index, longer_allele_index);
