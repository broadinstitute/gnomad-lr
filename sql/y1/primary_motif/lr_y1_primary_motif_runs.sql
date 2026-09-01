-- Optional Phase 7 product ledger. This DDL is intentionally not part of the
-- frozen Y1 v5 initializer; a future product initializer/finalizer must attest it
-- independently and accept only REVIEWED registry receipts.
CREATE TABLE IF NOT EXISTS lr_y1_primary_motif_runs (
    product_run_id String,
    revision UInt64,
    state LowCardinality(String),
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    chrom LowCardinality(String),
    primary_database String,
    primary_run_id String,
    registry_digest FixedString(64),
    registry_approval_state LowCardinality(String),
    metric LowCardinality(String),
    algorithm_version String,
    algorithm_sha256 FixedString(64),
    executable_revision String,
    executable_sha256 FixedString(64),
    anchor_rule LowCardinality(String),
    source_inventory_sha256 FixedString(64),
    max_alt_identities UInt32,
    max_represented_sequence_bytes UInt64,
    max_producer_bins UInt32,
    locus_rows UInt64,
    bin_rows UInt64,
    serialized_bytes UInt64,
    locus_content_sha256 FixedString(64),
    bin_content_sha256 FixedString(64),
    receipt_sha256 FixedString(64),
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    operator_identity String,
    message String
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (release, cohort, reference_genome, chrom, product_run_id);
