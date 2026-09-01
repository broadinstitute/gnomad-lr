-- Bounded aggregate allele-copy bins only. Exact ALT/source sequences and
-- contributor identities are deliberately absent from this serving shape.
CREATE TABLE IF NOT EXISTS lr_y1_primary_motif_allele_bins (
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
    exact_units UInt32,
    allele_copies UInt32,
    reference_copies UInt32,
    alternate_copies UInt32,
    stratum_an UInt32,
    stratum_alt_ac UInt64,
    stratum_ref_copies UInt32,
    stratum_receipt_sha256 FixedString(64)
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, product_run_id)
ORDER BY (product_run_id, chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), exact_units);
