-- Apply explicitly to a NEW gnomad_lr_y1_scratch_histogram_<cohort>_<run_id>
-- database. Not part of init_tables/init-y1; never ALTER the legacy table.
-- Intentionally no IF NOT EXISTS: an existing table is not a fresh candidate.
CREATE TABLE lr_str_source_histograms_v1
(
    contract LowCardinality(String),
    cohort LowCardinality(String),
    run_id String,
    task_id String,
    source_uri String,
    source_generation String,
    source_size_bytes UInt64,
    source_md5_base64 String,
    row_ordinal UInt64,
    locus_id String,
    motif String,
    chrom LowCardinality(String),
    locus_start UInt32,
    locus_end UInt32,
    source_interval String,
    context_chrom LowCardinality(String),
    context_start UInt32,
    context_end UInt32,
    source_vc Nullable(String),
    num_called_alleles UInt32,
    unique_allele_lengths UInt32,
    source_header Array(String),
    source_fields Map(String, String)
)
ENGINE = MergeTree
ORDER BY (cohort, run_id, chrom, locus_start, locus_end, motif, source_interval, row_ordinal);
