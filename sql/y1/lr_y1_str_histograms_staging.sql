CREATE TABLE IF NOT EXISTS lr_y1_str_histograms_staging (
    ancillary_run_id String, attempt_id String,
    release LowCardinality(String), cohort LowCardinality(String),
    reference_genome LowCardinality(String), modality LowCardinality(String),
    source_version String, chrom LowCardinality(String),
    source_start UInt32, source_end UInt32, motif String,
    allele_size_histogram String, biallelic_histogram String,
    min_repeats Float32, mode_repeats Float32, mean_repeats Float32,
    stdev_repeats Float32, median_repeats Float32, p99_repeats Float32,
    max_repeats Float32, unique_allele_lengths UInt32, num_called_alleles UInt32,
    populations Map(String, String)
) ENGINE = MergeTree()
PARTITION BY (release, cohort, reference_genome, chrom, ancillary_run_id)
ORDER BY (ancillary_run_id, attempt_id, chrom, source_start, motif);
