-- Durable receipt for fail-closed in-place schema preflights. A schema version
-- is recorded only after every CREATE/ALTER in init_schema succeeds.
CREATE TABLE IF NOT EXISTS lr_y1_schema_versions (
    schema_version UInt16,
    state LowCardinality(String),
    contract String,
    applied_at DateTime64(3, 'UTC'),
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY schema_version;
