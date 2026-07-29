-- Complete checked Y1 schema attestation only; never load authorization. D0
-- writes a receipt only after strict creation in a fresh isolated database.
CREATE TABLE IF NOT EXISTS lr_y1_schema_versions (
    schema_scope LowCardinality(String),
    schema_version UInt16,
    state LowCardinality(String),
    contract String,
    applied_at DateTime64(3, 'UTC'),
    revision UInt64
) ENGINE = ReplacingMergeTree(revision)
ORDER BY (schema_scope, schema_version);
