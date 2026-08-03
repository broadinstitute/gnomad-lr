-- One-shot presentation-only source hap1/hap2 methylation.
-- This table has no accepted ledger, active pointer, VCF orientation, or serving contract.
CREATE TABLE IF NOT EXISTS lr_y1_methylation_source_haplotype_presentation (
    stable_key FixedString(64),
    chrom LowCardinality(String),
    pos1 UInt32,
    pos2 UInt32,
    sample_id LowCardinality(String),
    source_haplotype UInt8,
    methylation Float32,
    coverage UInt32,
    CONSTRAINT source_haplotype_is_1_or_2 CHECK source_haplotype IN (1, 2),
    CONSTRAINT one_base_bed_interval CHECK pos2 = pos1 + 1,
    CONSTRAINT methylation_percentage CHECK isFinite(methylation) AND methylation >= 0 AND methylation <= 100
) ENGINE = MergeTree()
PARTITION BY chrom
ORDER BY (chrom, pos1, sample_id, source_haplotype, stable_key);
