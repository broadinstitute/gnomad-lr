-- One pointer per modality. Queries must resolve all identity columns and the
-- latest revision; absence means unavailable, never an implicit legacy fallback.
CREATE TABLE IF NOT EXISTS lr_y1_active_ancillary (
    release LowCardinality(String),
    cohort LowCardinality(String),
    reference_genome LowCardinality(String),
    modality LowCardinality(String),
    ancillary_run_id String,
    source_version String,
    activated_by String,
    activated_at DateTime64(3, 'UTC'),
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (release, cohort, reference_genome, modality);
