-- Exact anonymous REF/ALT margin proof. Index zero is REF. Excluded copies are
-- called alleles from partial or non-diploid GTs and never become diploid cells.
CREATE TABLE IF NOT EXISTS lr_y1_primary_motif_genotype_margins (
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
    allele_index UInt16,
    expected_copies UInt32,
    paired_copies UInt32,
    excluded_from_pairs_copies UInt32,
    margin_receipt_sha256 FixedString(64)
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, product_run_id)
ORDER BY (product_run_id, chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), allele_index);
