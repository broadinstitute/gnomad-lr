use super::interval::PoolY1AttemptReport;
use super::model::*;
use super::target::ClickHouseTarget;
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const Y1_SCHEMA_VERSION: u16 = 5;

// This receipt attests the complete checked Y1 table set. It is never load
// authorization and does not relax any runtime source/write/finalization gate.
const Y1_SCHEMA_CONTRACT: &str =
    "y1_full_v5_single_primary_copy_schema_attestation_not_load_authorization";

const Y1_SCHEMA_TABLE_NAMES: &[&str] = &[
    "lr_y1_schema_versions",
    "lr_y1_load_runs",
    "lr_y1_task_attempts",
    "lr_y1_active_partitions",
    "lr_y1_metadata_runs",
    "lr_y1_active_metadata",
    "lr_y1_ancillary_runs",
    "lr_y1_ancillary_task_attempts",
    "lr_y1_active_ancillary",
    "lr_y1_coverage_staging",
    "lr_y1_coverage",
    "lr_y1_methylation_staging",
    "lr_y1_methylation",
    "lr_y1_methylation_phased_staging",
    "lr_y1_methylation_phased",
    "lr_y1_methylation_availability",
    "lr_y1_methylation_summary",
    "lr_y1_str_histograms_staging",
    "lr_y1_str_histograms",
    "lr_y1_sample_metadata_staging",
    "lr_y1_metadata_audit_staging",
    "lr_y1_sample_metadata",
    "lr_y1_metadata_audit",
    "lr_y1_rejects_staging",
    "lr_y1_summaries",
    "lr_y1_alleles",
    "lr_y1_frequencies",
    "lr_y1_carriers",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ColumnContract {
    name: &'static str,
    column_type: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct TableContract {
    name: &'static str,
    engine: &'static str,
    columns: &'static [ColumnContract],
    partition_key: &'static [&'static str],
    sorting_key: &'static [&'static str],
    must_be_empty_before_upgrade: bool,
}

macro_rules! columns {
    ($(($name:literal, $column_type:literal)),+ $(,)?) => {
        &[$(ColumnContract { name: $name, column_type: $column_type }),+]
    };
}

const EMPTY_KEY: &[&str] = &[];
const METHYLATION_PARTITION_KEY: &[&str] = &[
    "release",
    "cohort",
    "reference_genome",
    "chrom",
    "ancillary_run_id",
];

const METHYLATION_V4_TABLES: &[TableContract] = &[
    TableContract {
        name: "lr_y1_schema_versions",
        engine: "ReplacingMergeTree",
        columns: columns![
            ("schema_scope", "LowCardinality(String)"),
            ("schema_version", "UInt16"),
            ("state", "LowCardinality(String)"),
            ("contract", "String"),
            ("applied_at", "DateTime64(3, 'UTC')"),
            ("revision", "UInt64"),
        ],
        partition_key: EMPTY_KEY,
        sorting_key: &["schema_scope", "schema_version"],
        must_be_empty_before_upgrade: false,
    },
    TableContract {
        name: "lr_y1_ancillary_runs",
        engine: "ReplacingMergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("source_version", "String"),
            ("source_manifest_hash", "FixedString(64)"),
            ("scope", "LowCardinality(String)"),
            ("state", "LowCardinality(String)"),
            ("source_rows", "UInt64"),
            ("canonical_rows", "UInt64"),
            ("reject_rows", "UInt64"),
            ("content_hash", "FixedString(64)"),
            ("peak_rss_bytes", "UInt64"),
            ("created_at", "DateTime64(3, 'UTC')"),
            ("revision", "UInt64"),
        ],
        partition_key: EMPTY_KEY,
        sorting_key: &[
            "release",
            "cohort",
            "reference_genome",
            "modality",
            "ancillary_run_id",
        ],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_active_ancillary",
        engine: "ReplacingMergeTree",
        columns: columns![
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("ancillary_run_id", "String"),
            ("source_version", "String"),
            ("activated_by", "String"),
            ("activated_at", "DateTime64(3, 'UTC')"),
            ("revision", "UInt64"),
        ],
        partition_key: EMPTY_KEY,
        sorting_key: &["release", "cohort", "reference_genome", "modality"],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_ancillary_task_attempts",
        engine: "ReplacingMergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("modality", "LowCardinality(String)"),
            ("chrom", "LowCardinality(String)"),
            ("task_id", "String"),
            ("attempt_id", "String"),
            ("lease_id", "String"),
            ("sample_id", "LowCardinality(String)"),
            ("data_layer", "LowCardinality(String)"),
            ("source_haplotype", "Nullable(UInt8)"),
            ("manifest_entry_id", "String"),
            ("source_object_slot", "LowCardinality(String)"),
            ("source_uri", "String"),
            ("source_generation", "String"),
            ("source_size_bytes", "UInt64"),
            ("source_checksum_algorithm", "LowCardinality(String)"),
            ("source_checksum", "String"),
            ("interval_start", "UInt32"),
            ("interval_end", "UInt32"),
            ("state", "LowCardinality(String)"),
            ("source_rows", "UInt64"),
            ("staged_rows", "UInt64"),
            ("reject_rows", "UInt64"),
            ("key_hash", "FixedString(64)"),
            ("content_hash", "FixedString(64)"),
            ("error", "Nullable(String)"),
            ("created_at", "DateTime64(3, 'UTC')"),
            ("revision", "UInt64"),
        ],
        partition_key: EMPTY_KEY,
        sorting_key: &[
            "ancillary_run_id",
            "modality",
            "chrom",
            "task_id",
            "attempt_id",
        ],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_methylation_staging",
        engine: "MergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("attempt_id", "String"),
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("source_version", "String"),
            ("chrom", "LowCardinality(String)"),
            ("source_start0", "UInt32"),
            ("source_end0", "UInt32"),
            ("position", "UInt32"),
            ("sample_id", "LowCardinality(String)"),
            ("methylation", "Float32"),
            ("coverage", "UInt32"),
            ("estimated_modified_count", "UInt32"),
            ("estimated_unmodified_count", "UInt32"),
            ("discretized_methylation", "Float32"),
        ],
        partition_key: METHYLATION_PARTITION_KEY,
        sorting_key: &[
            "ancillary_run_id",
            "attempt_id",
            "chrom",
            "position",
            "sample_id",
        ],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_methylation",
        engine: "MergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("source_version", "String"),
            ("chrom", "LowCardinality(String)"),
            ("source_start0", "UInt32"),
            ("source_end0", "UInt32"),
            ("position", "UInt32"),
            ("sample_id", "LowCardinality(String)"),
            ("methylation", "Float32"),
            ("coverage", "UInt32"),
            ("estimated_modified_count", "UInt32"),
            ("estimated_unmodified_count", "UInt32"),
            ("discretized_methylation", "Float32"),
        ],
        partition_key: METHYLATION_PARTITION_KEY,
        sorting_key: &["ancillary_run_id", "chrom", "position", "sample_id"],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_methylation_phased_staging",
        engine: "MergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("attempt_id", "String"),
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("source_version", "String"),
            ("chrom", "LowCardinality(String)"),
            ("source_start0", "UInt32"),
            ("source_end0", "UInt32"),
            ("position", "UInt32"),
            ("sample_id", "LowCardinality(String)"),
            ("source_haplotype", "UInt8"),
            ("methylation", "Float32"),
            ("coverage", "UInt32"),
            ("estimated_modified_count", "UInt32"),
            ("estimated_unmodified_count", "UInt32"),
            ("discretized_methylation", "Float32"),
        ],
        partition_key: METHYLATION_PARTITION_KEY,
        sorting_key: &[
            "ancillary_run_id",
            "attempt_id",
            "chrom",
            "position",
            "sample_id",
            "source_haplotype",
        ],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_methylation_phased",
        engine: "MergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("source_version", "String"),
            ("chrom", "LowCardinality(String)"),
            ("source_start0", "UInt32"),
            ("source_end0", "UInt32"),
            ("position", "UInt32"),
            ("sample_id", "LowCardinality(String)"),
            ("source_haplotype", "UInt8"),
            ("methylation", "Float32"),
            ("coverage", "UInt32"),
            ("estimated_modified_count", "UInt32"),
            ("estimated_unmodified_count", "UInt32"),
            ("discretized_methylation", "Float32"),
        ],
        partition_key: METHYLATION_PARTITION_KEY,
        sorting_key: &[
            "ancillary_run_id",
            "chrom",
            "position",
            "sample_id",
            "source_haplotype",
        ],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_methylation_availability",
        engine: "MergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("source_version", "String"),
            ("source_manifest_hash", "FixedString(64)"),
            ("chrom", "LowCardinality(String)"),
            ("sample_id", "LowCardinality(String)"),
            ("data_layer", "LowCardinality(String)"),
            ("source_haplotype", "Nullable(UInt8)"),
            ("inventory_status", "LowCardinality(String)"),
            ("load_status", "LowCardinality(String)"),
            ("source_rows", "UInt64"),
            ("canonical_rows", "UInt64"),
            ("reason", "String"),
            ("orientation_status", "String"),
            ("queryable_raw", "Bool"),
            ("joinable_to_vcf", "Bool"),
        ],
        partition_key: METHYLATION_PARTITION_KEY,
        sorting_key: &[
            "ancillary_run_id",
            "chrom",
            "sample_id",
            "data_layer",
            "source_haplotype",
        ],
        must_be_empty_before_upgrade: true,
    },
    TableContract {
        name: "lr_y1_methylation_summary",
        engine: "MergeTree",
        columns: columns![
            ("ancillary_run_id", "String"),
            ("release", "LowCardinality(String)"),
            ("cohort", "LowCardinality(String)"),
            ("reference_genome", "LowCardinality(String)"),
            ("modality", "LowCardinality(String)"),
            ("source_version", "String"),
            ("chrom", "LowCardinality(String)"),
            ("source_start0", "UInt32"),
            ("source_end0", "UInt32"),
            ("position", "UInt32"),
            ("mean_methylation", "Float64"),
            ("mean_coverage", "Float64"),
            ("num_samples", "UInt32"),
            ("std_methylation", "Float64"),
            ("min_methylation", "Float32"),
            ("max_methylation", "Float32"),
        ],
        partition_key: METHYLATION_PARTITION_KEY,
        sorting_key: &["ancillary_run_id", "chrom", "position"],
        must_be_empty_before_upgrade: true,
    },
];

trait SchemaBackend {
    fn database(&self) -> &str;
    fn execute(&self, query: &str) -> anyhow::Result<()>;
    fn query_text(&self, query: &str, parameters: &[(&str, &str)]) -> anyhow::Result<String>;
}

impl SchemaBackend for ClickHouseTarget {
    fn database(&self) -> &str {
        ClickHouseTarget::database(self)
    }

    fn execute(&self, query: &str) -> anyhow::Result<()> {
        ClickHouseTarget::execute(self, query)
    }

    fn query_text(&self, query: &str, parameters: &[(&str, &str)]) -> anyhow::Result<String> {
        ClickHouseTarget::query_text(self, query, parameters)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaDisposition {
    AlreadyAttested,
    FreshIsolatedV5,
}

pub fn init_schema(target: &ClickHouseTarget) -> anyhow::Result<()> {
    init_schema_with_backend(target)
}

fn init_schema_with_backend<B: SchemaBackend>(backend: &B) -> anyhow::Result<()> {
    let disposition = preflight_y1_v5_initialization(backend)?;
    if disposition == SchemaDisposition::AlreadyAttested {
        // An attested schema is read-only to this initializer. In particular,
        // reruns never CREATE or ALTER historical/intermediate tables.
        return Ok(());
    }

    // The preflight proved that the explicitly versioned database had no
    // tables. Remove IF NOT EXISTS so a concurrent creator causes a hard error
    // rather than letting this initializer adopt or mutate its object.
    for name in Y1_SCHEMA_TABLE_NAMES {
        backend
            .execute(&fresh_create_statement(y1_schema_ddl(name))?)
            .with_context(|| format!("failed to initialize fresh Y1 table {name}"))?;
    }

    validate_exact_y1_table_set(&read_database_table_names(backend)?)?;
    validate_exact_y1_semantic_schema(backend)?;
    let post_ddl = read_methylation_schema_inventory(backend)?;
    validate_exact_methylation_v4_schema(&post_ddl)?;
    if disposition == SchemaDisposition::FreshIsolatedV5 {
        require_empty_attestation_tables(&post_ddl)?;
        backend
            .execute(&format!(
                "INSERT INTO lr_y1_schema_versions \
                 (schema_scope, schema_version, state, contract, applied_at, revision) VALUES \
                 ('y1_full', 5, 'applied', '{Y1_SCHEMA_CONTRACT}', now64(3), toUInt64(toUnixTimestamp64Milli(now64(3))))"
            ))
            .context("failed to record applied Y1 schema version 5")?;
    }

    // The full-schema receipt is accepted only together with a fresh live
    // semantic attestation. It is evidence of shape, never load authorization.
    validate_exact_y1_semantic_schema(backend)?;
    let verified = read_methylation_schema_inventory(backend)?;
    validate_exact_methylation_v4_schema(&verified)?;
    require_applied_schema_receipt(backend, &verified)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnSemantics {
    name: String,
    column_type: String,
    default_kind: String,
    default_expression: String,
    compression_codec: String,
    ttl_expression: String,
    is_in_partition_key: bool,
    is_in_sorting_key: bool,
    is_in_primary_key: bool,
    is_in_sampling_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableInventory {
    engine: String,
    columns: Vec<(String, String)>,
    column_semantics: Vec<ColumnSemantics>,
    partition_key: Vec<String>,
    sorting_key: Vec<String>,
    primary_key: Vec<String>,
    sampling_key: Vec<String>,
    create_table_query: String,
    rows: u64,
}

#[derive(Debug, Deserialize)]
struct CreateCatalogRow {
    name: String,
    create_table_query: String,
}

#[derive(Debug, Deserialize)]
struct TableCatalogRow {
    name: String,
    engine: String,
    partition_key: String,
    sorting_key: String,
    primary_key: String,
    sampling_key: String,
    create_table_query: String,
}

#[derive(Debug, Deserialize)]
struct ColumnCatalogRow {
    table: String,
    name: String,
    r#type: String,
    position: u64,
    default_kind: String,
    default_expression: String,
    compression_codec: String,
    is_in_partition_key: u8,
    is_in_sorting_key: u8,
    is_in_primary_key: u8,
    is_in_sampling_key: u8,
}

type SchemaInventory = BTreeMap<String, TableInventory>;

fn preflight_y1_v5_initialization<B: SchemaBackend>(
    backend: &B,
) -> anyhow::Result<SchemaDisposition> {
    // All reads in this preflight precede every DDL statement. Without a real
    // exclusive migration fence, D0 never adopts or ALTERs an existing object.
    let database_tables = read_database_table_names(backend)?;
    let inventory = read_methylation_schema_inventory(backend)?;
    match read_schema_receipt(backend, &inventory)? {
        Some((state, contract)) => {
            if state != "applied" || contract != Y1_SCHEMA_CONTRACT {
                bail!(
                    "refusing Y1 methylation schema receipt with unrecognized state/contract: state={state:?} contract={contract:?}"
                );
            }
            validate_exact_y1_table_set(&database_tables)?;
            validate_exact_y1_semantic_schema(backend)?;
            validate_exact_methylation_v4_schema(&inventory)?;
            Ok(SchemaDisposition::AlreadyAttested)
        }
        None => {
            if !database_tables.is_empty() {
                bail!(
                    "refusing in-place Y1 schema initialization without an exact full-v5 attestation; database contains tables [{}]. Use a new isolated versioned v5 database",
                    database_tables.join(", ")
                );
            }
            if !backend.database().split('_').any(|part| part == "v5") {
                bail!(
                    "fresh Y1 schema initialization requires an isolated database name containing the version token _v5_"
                );
            }
            Ok(SchemaDisposition::FreshIsolatedV5)
        }
    }
}

fn validate_exact_y1_table_set(actual: &[String]) -> anyhow::Result<()> {
    let mut expected = Y1_SCHEMA_TABLE_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    expected.sort();
    if actual != expected {
        bail!("Y1 initializer requires the exact checked 28-table set; no missing or extra table is accepted");
    }
    Ok(())
}

fn validate_exact_y1_semantic_schema<B: SchemaBackend>(backend: &B) -> anyhow::Result<()> {
    let table_names = Y1_SCHEMA_TABLE_NAMES
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT name, create_table_query FROM system.tables \
         WHERE database = {{database:String}} AND name IN ({table_names}) \
         ORDER BY name FORMAT JSONEachRow"
    );
    let body = backend.query_text(&query, &[("database", backend.database())])?;
    let mut actual = BTreeMap::new();
    for line in body.lines().filter(|line| !line.is_empty()) {
        let row: CreateCatalogRow = serde_json::from_str(line)
            .context("full Y1 create-table inventory returned malformed JSON")?;
        if !Y1_SCHEMA_TABLE_NAMES.contains(&row.name.as_str())
            || actual.insert(row.name, row.create_table_query).is_some()
        {
            bail!("full Y1 create-table inventory returned an unexpected table row");
        }
    }
    for name in Y1_SCHEMA_TABLE_NAMES {
        let create = actual
            .get(*name)
            .ok_or_else(|| anyhow::anyhow!("full Y1 schema is missing table {name}"))?;
        if normalize_create_statement(create, name) != expected_normalized_create_statement(name) {
            bail!("Y1 schema v5 table {name} does not match its exact normalized SHOW CREATE contract");
        }
    }
    Ok(())
}

fn fresh_create_statement(ddl: &str) -> anyhow::Result<String> {
    const IDEMPOTENT_CREATE: &str = "CREATE TABLE IF NOT EXISTS ";
    if ddl.matches(IDEMPOTENT_CREATE).count() != 1 {
        bail!("checked Y1 DDL must contain exactly one CREATE TABLE IF NOT EXISTS statement");
    }
    Ok(ddl.replacen(IDEMPOTENT_CREATE, "CREATE TABLE ", 1))
}

fn read_database_table_names<B: SchemaBackend>(backend: &B) -> anyhow::Result<Vec<String>> {
    let body = backend.query_text(
        "SELECT name FROM system.tables WHERE database = {database:String} ORDER BY name FORMAT TabSeparated",
        &[("database", backend.database())],
    )?;
    let mut names = Vec::new();
    for line in body.lines().filter(|line| !line.is_empty()) {
        if line.contains('\t')
            || !line
                .bytes()
                .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            bail!("system.tables database inventory returned a malformed table name");
        }
        names.push(line.to_string());
    }
    Ok(names)
}

fn read_methylation_schema_inventory<B: SchemaBackend>(
    backend: &B,
) -> anyhow::Result<SchemaInventory> {
    let table_names = METHYLATION_V4_TABLES
        .iter()
        .map(|table| format!("'{}'", table.name))
        .collect::<Vec<_>>()
        .join(", ");
    let tables_query = format!(
        "SELECT name, engine, partition_key, sorting_key, primary_key, sampling_key, create_table_query FROM system.tables \
         WHERE database = {{database:String}} AND name IN ({table_names}) \
         ORDER BY name FORMAT JSONEachRow"
    );
    let table_rows = backend.query_text(&tables_query, &[("database", backend.database())])?;
    let mut inventory = SchemaInventory::new();
    for line in table_rows.lines().filter(|line| !line.is_empty()) {
        let row: TableCatalogRow = serde_json::from_str(line)
            .context("system.tables schema inventory returned malformed JSON")?;
        if !METHYLATION_V4_TABLES
            .iter()
            .any(|table| table.name == row.name)
            || inventory.contains_key(&row.name)
        {
            bail!("system.tables schema inventory returned an unexpected table row");
        }
        inventory.insert(
            row.name,
            TableInventory {
                engine: row.engine,
                columns: Vec::new(),
                column_semantics: Vec::new(),
                partition_key: parse_key_expression(&row.partition_key),
                sorting_key: parse_key_expression(&row.sorting_key),
                primary_key: parse_key_expression(&row.primary_key),
                sampling_key: parse_key_expression(&row.sampling_key),
                create_table_query: row.create_table_query,
                rows: 0,
            },
        );
    }

    let columns_query = format!(
        "SELECT table, name, type, position, default_kind, default_expression, compression_codec, \
         is_in_partition_key, is_in_sorting_key, is_in_primary_key, is_in_sampling_key FROM system.columns \
         WHERE database = {{database:String}} AND table IN ({table_names}) \
         ORDER BY table, position FORMAT JSONEachRow"
    );
    let column_rows = backend.query_text(&columns_query, &[("database", backend.database())])?;
    let mut positions: BTreeMap<String, u64> = BTreeMap::new();
    for line in column_rows.lines().filter(|line| !line.is_empty()) {
        let row: ColumnCatalogRow = serde_json::from_str(line)
            .context("system.columns schema inventory returned malformed JSON")?;
        let table = inventory
            .get_mut(&row.table)
            .ok_or_else(|| anyhow::anyhow!("system.columns returned an absent table"))?;
        let expected_position = positions.entry(row.table).or_insert(1);
        if row.position != *expected_position {
            bail!("system.columns positions are duplicated or noncontiguous");
        }
        *expected_position += 1;
        table.columns.push((row.name.clone(), row.r#type.clone()));
        table.column_semantics.push(ColumnSemantics {
            name: row.name,
            column_type: row.r#type,
            default_kind: row.default_kind,
            default_expression: row.default_expression,
            compression_codec: row.compression_codec,
            // Column and table TTL clauses are compared in create_table_query;
            // ClickHouse 26.3 does not expose ttl_expression in system.columns.
            ttl_expression: String::new(),
            is_in_partition_key: catalog_flag(row.is_in_partition_key)?,
            is_in_sorting_key: catalog_flag(row.is_in_sorting_key)?,
            is_in_primary_key: catalog_flag(row.is_in_primary_key)?,
            is_in_sampling_key: catalog_flag(row.is_in_sampling_key)?,
        });
    }

    for (name, table) in &mut inventory {
        let population_filter = match name.as_str() {
            "lr_y1_ancillary_runs" | "lr_y1_active_ancillary" => {
                " WHERE positionCaseInsensitive(modality, 'methylation') > 0"
            }
            _ => "",
        };
        let body = backend.query_text(
            &format!("SELECT count() FROM {name}{population_filter} FORMAT TabSeparated"),
            &[],
        )?;
        table.rows = parse_single_count(&body, name)?;
    }
    Ok(inventory)
}

fn parse_key_expression(expression: &str) -> Vec<String> {
    let mut compact: String = expression
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    loop {
        if compact.starts_with("tuple(") && compact.ends_with(')') {
            compact = compact[6..compact.len() - 1].to_string();
        } else if compact.starts_with('(') && compact.ends_with(')') {
            compact = compact[1..compact.len() - 1].to_string();
        } else {
            break;
        }
    }
    if compact.is_empty() {
        Vec::new()
    } else {
        compact.split(',').map(str::to_string).collect()
    }
}

fn read_schema_receipt<B: SchemaBackend>(
    backend: &B,
    inventory: &SchemaInventory,
) -> anyhow::Result<Option<(String, String)>> {
    if !inventory.contains_key("lr_y1_schema_versions") {
        return Ok(None);
    }
    let body = backend.query_text(
        "SELECT state, contract FROM lr_y1_schema_versions FINAL WHERE schema_scope = 'y1_full' AND schema_version = 5 FORMAT TabSeparated",
        &[],
    )?;
    if body.trim().is_empty() {
        return Ok(None);
    }
    let rows: Vec<&str> = body.lines().filter(|line| !line.is_empty()).collect();
    if rows.len() != 1 {
        bail!("schema v5 receipt query returned multiple resolved rows");
    }
    let fields: Vec<&str> = rows[0].split('\t').collect();
    if fields.len() != 2 {
        bail!("schema v5 receipt query returned a malformed row");
    }
    Ok(Some((fields[0].to_string(), fields[1].to_string())))
}

fn require_applied_schema_receipt<B: SchemaBackend>(
    backend: &B,
    inventory: &SchemaInventory,
) -> anyhow::Result<()> {
    match read_schema_receipt(backend, inventory)? {
        Some((state, contract)) if state == "applied" && contract == Y1_SCHEMA_CONTRACT => Ok(()),
        _ => bail!("Y1 schema v5 lacks its exact full-schema attestation receipt"),
    }
}

fn require_empty_attestation_tables(inventory: &SchemaInventory) -> anyhow::Result<()> {
    for contract in METHYLATION_V4_TABLES
        .iter()
        .filter(|table| table.must_be_empty_before_upgrade)
    {
        if let Some(table) = inventory.get(contract.name) {
            if table.rows != 0 {
                bail!(
                    "refusing Y1 full-v5 schema attestation: fresh isolated table {} was populated during initialization",
                    contract.name
                );
            }
        }
    }
    Ok(())
}

fn validate_exact_methylation_v4_schema(inventory: &SchemaInventory) -> anyhow::Result<()> {
    for contract in METHYLATION_V4_TABLES {
        let table = inventory
            .get(contract.name)
            .ok_or_else(|| anyhow::anyhow!("Y1 schema v4 is missing table {}", contract.name))?;
        if !table_matches_contract(table, contract) {
            bail!(
                "Y1 methylation schema v4 table {} does not match its exact semantic catalog/SHOW CREATE contract",
                contract.name
            );
        }
    }
    Ok(())
}

fn table_matches_contract(table: &TableInventory, contract: &TableContract) -> bool {
    let expected_columns: Vec<(String, String)> = contract
        .columns
        .iter()
        .map(|column| (column.name.to_string(), column.column_type.to_string()))
        .collect();
    let expected_partition: Vec<String> = contract
        .partition_key
        .iter()
        .map(|value| (*value).to_string())
        .collect();
    let expected_sorting: Vec<String> = contract
        .sorting_key
        .iter()
        .map(|value| (*value).to_string())
        .collect();
    let column_semantics_match = table.column_semantics.iter().all(|column| {
        column.default_kind.is_empty()
            && column.default_expression.is_empty()
            && column.compression_codec.is_empty()
            && column.ttl_expression.is_empty()
            && column.is_in_partition_key == contract.partition_key.contains(&column.name.as_str())
            && column.is_in_sorting_key == contract.sorting_key.contains(&column.name.as_str())
            && column.is_in_primary_key == contract.sorting_key.contains(&column.name.as_str())
            && !column.is_in_sampling_key
    });
    table.engine == contract.engine
        && table.columns == expected_columns
        && table.column_semantics.len() == expected_columns.len()
        && column_semantics_match
        && table.partition_key == expected_partition
        && table.sorting_key == expected_sorting
        && table.primary_key == expected_sorting
        && table.sampling_key.is_empty()
        && normalize_create_statement(&table.create_table_query, contract.name)
            == expected_normalized_create_statement(contract.name)
}

fn catalog_flag(value: u8) -> anyhow::Result<bool> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => bail!("system catalog returned a non-boolean key-membership flag"),
    }
}

fn methylation_v4_ddl(table: &str) -> &'static str {
    y1_schema_ddl(table)
}

fn y1_schema_ddl(table: &str) -> &'static str {
    match table {
        "lr_y1_schema_versions" => include_str!("../../sql/y1/lr_y1_schema_versions.sql"),
        "lr_y1_load_runs" => include_str!("../../sql/y1/lr_y1_load_runs.sql"),
        "lr_y1_task_attempts" => include_str!("../../sql/y1/lr_y1_task_attempts.sql"),
        "lr_y1_active_partitions" => include_str!("../../sql/y1/lr_y1_active_partitions.sql"),
        "lr_y1_metadata_runs" => include_str!("../../sql/y1/lr_y1_metadata_runs.sql"),
        "lr_y1_active_metadata" => include_str!("../../sql/y1/lr_y1_active_metadata.sql"),
        "lr_y1_ancillary_runs" => include_str!("../../sql/y1/lr_y1_ancillary_runs.sql"),
        "lr_y1_ancillary_task_attempts" => {
            include_str!("../../sql/y1/lr_y1_ancillary_task_attempts.sql")
        }
        "lr_y1_active_ancillary" => include_str!("../../sql/y1/lr_y1_active_ancillary.sql"),
        "lr_y1_coverage_staging" => include_str!("../../sql/y1/lr_y1_coverage_staging.sql"),
        "lr_y1_coverage" => include_str!("../../sql/y1/lr_y1_coverage.sql"),
        "lr_y1_methylation_staging" => {
            include_str!("../../sql/y1/lr_y1_methylation_staging.sql")
        }
        "lr_y1_methylation" => include_str!("../../sql/y1/lr_y1_methylation.sql"),
        "lr_y1_methylation_phased_staging" => {
            include_str!("../../sql/y1/lr_y1_methylation_phased_staging.sql")
        }
        "lr_y1_methylation_phased" => {
            include_str!("../../sql/y1/lr_y1_methylation_phased.sql")
        }
        "lr_y1_methylation_availability" => {
            include_str!("../../sql/y1/lr_y1_methylation_availability.sql")
        }
        "lr_y1_methylation_summary" => {
            include_str!("../../sql/y1/lr_y1_methylation_summary.sql")
        }
        "lr_y1_str_histograms_staging" => {
            include_str!("../../sql/y1/lr_y1_str_histograms_staging.sql")
        }
        "lr_y1_str_histograms" => include_str!("../../sql/y1/lr_y1_str_histograms.sql"),
        "lr_y1_sample_metadata_staging" => {
            include_str!("../../sql/y1/lr_y1_sample_metadata_staging.sql")
        }
        "lr_y1_metadata_audit_staging" => {
            include_str!("../../sql/y1/lr_y1_metadata_audit_staging.sql")
        }
        "lr_y1_sample_metadata" => include_str!("../../sql/y1/lr_y1_sample_metadata.sql"),
        "lr_y1_metadata_audit" => include_str!("../../sql/y1/lr_y1_metadata_audit.sql"),
        "lr_y1_rejects_staging" => include_str!("../../sql/y1/lr_y1_rejects_staging.sql"),
        "lr_y1_summaries" => include_str!("../../sql/y1/lr_y1_summaries.sql"),
        "lr_y1_alleles" => include_str!("../../sql/y1/lr_y1_alleles.sql"),
        "lr_y1_frequencies" => include_str!("../../sql/y1/lr_y1_frequencies.sql"),
        "lr_y1_carriers" => include_str!("../../sql/y1/lr_y1_carriers.sql"),
        _ => panic!("Y1 table lacks checked DDL: {table}"),
    }
}

fn expected_normalized_create_statement(table: &str) -> String {
    let mut expected = normalize_create_statement(methylation_v4_ddl(table), table);
    // ClickHouse 26.3 renders this effective default into create_table_query.
    // Pinning it makes a different granularity fail attestation rather than
    // normalizing the semantic setting away.
    if expected.contains("SETTINGS") {
        expected.push_str(",index_granularity=8192");
    } else {
        expected.push_str("SETTINGSindex_granularity=8192");
    }
    expected
}

fn normalize_create_statement(query: &str, table: &str) -> String {
    let mut compact = String::new();
    let mut characters = query.chars().peekable();
    let mut quoted = false;
    while let Some(character) = characters.next() {
        if !quoted && character == '-' && characters.peek() == Some(&'-') {
            characters.next();
            for rest in characters.by_ref() {
                if rest == '\n' {
                    break;
                }
            }
            continue;
        }
        if character == '\'' {
            quoted = !quoted;
            compact.push(character);
        } else if quoted || (!character.is_whitespace() && character != '`') {
            compact.push(character);
        }
    }
    compact = compact
        .replace("CREATETABLEIFNOTEXISTS", "CREATETABLE")
        .replace("MergeTree()", "MergeTree");
    while compact.ends_with(';') {
        compact.pop();
    }

    // ClickHouse qualifies SHOW CREATE names with the selected database. UUIDs
    // and qualification identify the physical object, not table semantics.
    let create = "CREATETABLE";
    if compact.starts_with(create) {
        if let Some(table_start) = compact[create.len()..].find(table) {
            let table_start = create.len() + table_start;
            let suffix = compact[table_start + table.len()..].to_string();
            compact = format!("{create}{table}{suffix}");
        }
    }
    compact
}

fn parse_single_count(body: &str, label: &str) -> anyhow::Result<u64> {
    let value = body.trim();
    if value.is_empty() || value.contains('\t') || value.contains('\n') {
        bail!("{label} returned an invalid ClickHouse count row");
    }
    value
        .parse::<u64>()
        .with_context(|| format!("{label} returned a nonnumeric ClickHouse count"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadScope {
    Synthetic,
    Interval,
    FullChromosome,
}

impl LoadScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Synthetic => "synthetic",
            Self::Interval => "interval",
            Self::FullChromosome => "full_chromosome",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptContext {
    pub run_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub cohort: Cohort,
    pub chrom: String,
    pub interval_start: u32,
    pub interval_end: u32,
}

impl AttemptContext {
    pub fn validate(&self) -> anyhow::Result<()> {
        for (label, value) in [
            ("run_id", self.run_id.as_str()),
            ("task_id", self.task_id.as_str()),
            ("attempt_id", self.attempt_id.as_str()),
            ("chrom", self.chrom.as_str()),
        ] {
            if value.is_empty() {
                bail!("{label} must not be empty");
            }
        }
        if self.interval_start == 0 || self.interval_start > self.interval_end {
            bail!("attempt interval must be one-based and non-empty");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct StagedCounts {
    pub source_records: u64,
    pub summaries: u64,
    pub alleles: u64,
    pub frequencies: u64,
    pub carriers: u64,
    pub rejects: u64,
}

#[derive(Debug, Serialize)]
struct SummaryStageRow {
    run_id: String,
    task_id: String,
    attempt_id: String,
    release: String,
    cohort: String,
    reference_genome: String,
    chrom: String,
    position: u32,
    source_variant_id: String,
    ref_allele: String,
    alts: Vec<String>,
    allele_type: Option<String>,
    qual: Option<f64>,
    filters: Vec<String>,
    ac: Vec<u32>,
    an: u32,
    af: Vec<f64>,
    allele_lengths: Vec<i32>,
    length_provenance: Vec<String>,
    source_allele_length: Option<i32>,
    source_svlen: Vec<i32>,
    source_svlen_present: u8,
    frequencies_json: String,
    source_info_json: String,
}

#[derive(Debug, Serialize)]
struct AlleleStageRow {
    run_id: String,
    task_id: String,
    attempt_id: String,
    release: String,
    cohort: String,
    reference_genome: String,
    chrom: String,
    position: u32,
    reference_end: u32,
    xpos: u64,
    source_variant_id: String,
    alt_index: u16,
    ref_allele: String,
    alt: String,
    allele_type: Option<String>,
    qual: Option<f64>,
    filters: Vec<String>,
    ac: u32,
    an: u32,
    af: f64,
    allele_length: i32,
    length_provenance: String,
    rsids: Vec<String>,
    cadd_phred: Option<f64>,
    phylop: Option<f64>,
    major_consequence: Option<String>,
    short_read_match_id: Option<String>,
    short_read_match_type: Option<String>,
    short_read_match_source: Option<String>,
}

#[derive(Debug, Serialize)]
struct FrequencyStageRow {
    run_id: String,
    task_id: String,
    attempt_id: String,
    release: String,
    cohort: String,
    reference_genome: String,
    chrom: String,
    position: u32,
    source_variant_id: String,
    alt_index: u16,
    division: String,
    ac: Option<u32>,
    an: Option<u32>,
    af: Option<f64>,
    values_available: u8,
}

#[derive(Debug, Serialize)]
struct CarrierStageRow {
    run_id: String,
    task_id: String,
    attempt_id: String,
    release: String,
    cohort: String,
    reference_genome: String,
    chrom: String,
    position: u32,
    source_variant_id: String,
    alt_index: u16,
    alt: String,
    sample_id: String,
    genotype_position: u16,
    gt_alleles: Vec<Option<u16>>,
    gt_phased: u8,
    genotype_fields_json: String,
    position_fields_json: String,
}

#[derive(Debug, Serialize)]
struct RejectStageRow {
    run_id: String,
    task_id: String,
    attempt_id: String,
    record_number: Option<u64>,
    source_variant_id: Option<String>,
    reject_code: String,
    message: String,
}

const CONSEQUENCE_TERMS: &[&str] = &[
    "transcript_ablation",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",
    "initiator_codon_variant",
    "transcript_amplification",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",
    "splice_region_variant",
    "incomplete_terminal_codon_variant",
    "start_retained_variant",
    "stop_retained_variant",
    "synonymous_variant",
    "coding_sequence_variant",
    "mature_miRNA_variant",
    "5_prime_UTR_variant",
    "3_prime_UTR_variant",
    "non_coding_transcript_exon_variant",
    "non_coding_exon_variant",
    "intron_variant",
    "NMD_transcript_variant",
    "non_coding_transcript_variant",
    "nc_transcript_variant",
    "upstream_gene_variant",
    "downstream_gene_variant",
    "TFBS_ablation",
    "TFBS_amplification",
    "TF_binding_site_variant",
    "regulatory_region_ablation",
    "regulatory_region_amplification",
    "feature_elongation",
    "regulatory_region_variant",
    "feature_truncation",
    "intergenic_variant",
];

fn alt_info_value(
    info: &std::collections::BTreeMap<String, Option<String>>,
    key: &str,
    alt_index: usize,
) -> Option<String> {
    let value = info.get(key)?.as_deref()?;
    if value.is_empty() || value == "." {
        return None;
    }
    let values: Vec<&str> = value.split(',').collect();
    let selected = if values.len() == 1 {
        values[0]
    } else {
        *values.get(alt_index.checked_sub(1)?)?
    };
    (!selected.is_empty() && selected != ".").then(|| selected.to_string())
}

fn normalized_vep_allele(ref_allele: &str, alt: &str) -> String {
    if alt.starts_with('<') {
        return alt.to_string();
    }
    let ref_bytes = ref_allele.as_bytes();
    let alt_bytes = alt.as_bytes();
    let mut start = 0;
    while start < ref_bytes.len() && start < alt_bytes.len() && ref_bytes[start] == alt_bytes[start]
    {
        start += 1;
    }
    let mut ref_end = ref_bytes.len();
    let mut alt_end = alt_bytes.len();
    while ref_end > start && alt_end > start && ref_bytes[ref_end - 1] == alt_bytes[alt_end - 1] {
        ref_end -= 1;
        alt_end -= 1;
    }
    let value = &alt[start..alt_end];
    if value.is_empty() {
        "-".to_string()
    } else {
        value.to_string()
    }
}

fn major_consequence(
    info: &std::collections::BTreeMap<String, Option<String>>,
    ref_allele: &str,
    alt: &str,
    alt_index: usize,
) -> Option<String> {
    let vep = info.get("vep")?.as_deref()?;
    let entries: Vec<Vec<&str>> = vep
        .split(',')
        .map(|entry| entry.split('|').collect())
        .filter(|fields: &Vec<&str>| {
            fields.get(5) == Some(&"Transcript") && fields.get(22) == Some(&"1")
        })
        .collect();
    let normalized = normalized_vep_allele(ref_allele, alt);
    let mut selected: Vec<&Vec<&str>> = entries
        .iter()
        .filter(|fields| {
            fields.first() == Some(&alt) || fields.first() == Some(&normalized.as_str())
        })
        .collect();
    if selected.is_empty() {
        let mut distinct = Vec::new();
        for fields in &entries {
            if let Some(allele) = fields.first() {
                if !distinct.contains(allele) {
                    distinct.push(*allele);
                }
            }
        }
        if let Some(fallback) = distinct.get(alt_index.saturating_sub(1)) {
            selected = entries
                .iter()
                .filter(|fields| fields.first() == Some(fallback))
                .collect();
        }
    }
    selected
        .iter()
        .flat_map(|fields| fields.get(1).into_iter().flat_map(|value| value.split('&')))
        .filter(|term| !term.is_empty())
        .min_by_key(|term| {
            CONSEQUENCE_TERMS
                .iter()
                .position(|candidate| candidate == term)
                .unwrap_or(usize::MAX)
        })
        .map(str::to_string)
}

#[derive(Debug, Default)]
struct StageRows {
    summaries: Vec<SummaryStageRow>,
    alleles: Vec<AlleleStageRow>,
    frequencies: Vec<FrequencyStageRow>,
    carriers: Vec<CarrierStageRow>,
    rejects: Vec<RejectStageRow>,
}

impl StageRows {
    fn from_batch(context: &AttemptContext, batch: &TransformationBatch) -> anyhow::Result<Self> {
        context.validate()?;
        if batch.report.summary_rows != batch.summaries.len()
            || batch.report.carrier_rows != batch.carriers.len()
            || batch.report.rejected_records != batch.report.rejects.len()
            || batch.report.source_records
                != batch.report.summary_rows + batch.report.rejected_records
        {
            bail!("transformation report does not match transformed row vectors");
        }

        let mut rows = Self::default();
        for summary in &batch.summaries {
            validate_summary_context(context, summary)?;
            let source_info_json = serde_json::to_string(&summary.source_info)?;
            let reference_end = summary
                .position
                .checked_add(
                    u32::try_from(summary.ref_allele.len().saturating_sub(1))
                        .context("REF length exceeds UInt32")?,
                )
                .context("reference end exceeds UInt32")?;
            let xpos = compute_xpos(&summary.chrom, summary.position)?;

            rows.summaries.push(SummaryStageRow {
                run_id: context.run_id.clone(),
                task_id: context.task_id.clone(),
                attempt_id: context.attempt_id.clone(),
                release: summary.identity.release.as_str().to_string(),
                cohort: summary.identity.cohort.as_str().to_string(),
                reference_genome: summary.reference_genome.as_str().to_string(),
                chrom: summary.chrom.clone(),
                position: summary.position,
                source_variant_id: summary.identity.source_variant_id.clone(),
                ref_allele: summary.ref_allele.clone(),
                alts: summary.alts.clone(),
                allele_type: summary.allele_type.clone(),
                qual: summary.qual,
                filters: summary.filters.clone(),
                ac: summary.ac.clone(),
                an: summary.an,
                af: summary.af.clone(),
                allele_lengths: summary
                    .allele_lengths
                    .iter()
                    .map(|value| value.value)
                    .collect(),
                length_provenance: summary
                    .allele_lengths
                    .iter()
                    .map(|value| value.provenance.as_str().to_string())
                    .collect(),
                source_allele_length: summary.source_allele_length,
                source_svlen: summary.source_svlen.clone().unwrap_or_default(),
                source_svlen_present: summary.source_svlen.is_some() as u8,
                frequencies_json: serde_json::to_string(&summary.frequencies)?,
                source_info_json: source_info_json.clone(),
            });

            for (index, (((alt, ac), af), length)) in summary
                .alts
                .iter()
                .zip(&summary.ac)
                .zip(&summary.af)
                .zip(&summary.allele_lengths)
                .enumerate()
            {
                let alt_index = u16::try_from(index + 1).context("ALT index exceeds UInt16")?;
                rows.alleles.push(AlleleStageRow {
                    run_id: context.run_id.clone(),
                    task_id: context.task_id.clone(),
                    attempt_id: context.attempt_id.clone(),
                    release: summary.identity.release.as_str().to_string(),
                    cohort: summary.identity.cohort.as_str().to_string(),
                    reference_genome: summary.reference_genome.as_str().to_string(),
                    chrom: summary.chrom.clone(),
                    position: summary.position,
                    reference_end,
                    xpos,
                    source_variant_id: summary.identity.source_variant_id.clone(),
                    alt_index,
                    ref_allele: summary.ref_allele.clone(),
                    alt: alt.clone(),
                    allele_type: summary.allele_type.clone(),
                    qual: summary.qual,
                    filters: summary.filters.clone(),
                    ac: *ac,
                    an: summary.an,
                    af: *af,
                    allele_length: length.value,
                    length_provenance: length.provenance.as_str().to_string(),
                    rsids: alt_info_value(&summary.source_info, "dbSNP_ID", index + 1)
                        .map(|value| {
                            value
                                .split('&')
                                .filter(|id| !id.is_empty())
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default(),
                    cadd_phred: alt_info_value(&summary.source_info, "cadd_phred", index + 1)
                        .and_then(|value| value.parse().ok()),
                    phylop: alt_info_value(&summary.source_info, "phylop", index + 1)
                        .and_then(|value| value.parse().ok()),
                    major_consequence: major_consequence(
                        &summary.source_info,
                        &summary.ref_allele,
                        alt,
                        index + 1,
                    ),
                    short_read_match_id: alt_info_value(
                        &summary.source_info,
                        "gnomAD_V4_match_ID",
                        index + 1,
                    ),
                    short_read_match_type: alt_info_value(
                        &summary.source_info,
                        "gnomAD_V4_match_type",
                        index + 1,
                    ),
                    short_read_match_source: alt_info_value(
                        &summary.source_info,
                        "gnomAD_V4_match_source",
                        index + 1,
                    ),
                });
            }

            for frequency in &summary.frequencies {
                for index in 0..summary.alts.len() {
                    let ac = frequency
                        .ac
                        .as_ref()
                        .and_then(|values| values.get(index))
                        .copied();
                    let af = frequency
                        .af
                        .as_ref()
                        .and_then(|values| values.get(index))
                        .copied();
                    let an = frequency.an;
                    rows.frequencies.push(FrequencyStageRow {
                        run_id: context.run_id.clone(),
                        task_id: context.task_id.clone(),
                        attempt_id: context.attempt_id.clone(),
                        release: summary.identity.release.as_str().to_string(),
                        cohort: summary.identity.cohort.as_str().to_string(),
                        reference_genome: summary.reference_genome.as_str().to_string(),
                        chrom: summary.chrom.clone(),
                        position: summary.position,
                        source_variant_id: summary.identity.source_variant_id.clone(),
                        alt_index: u16::try_from(index + 1)
                            .context("frequency ALT index exceeds UInt16")?,
                        division: frequency.division.clone(),
                        ac,
                        an,
                        af,
                        values_available: (ac.is_some() && an.is_some() && af.is_some()) as u8,
                    });
                }
            }
        }

        for carrier in &batch.carriers {
            validate_carrier_context(context, carrier)?;
            rows.carriers.push(CarrierStageRow {
                run_id: context.run_id.clone(),
                task_id: context.task_id.clone(),
                attempt_id: context.attempt_id.clone(),
                release: carrier.identity.release.as_str().to_string(),
                cohort: carrier.identity.cohort.as_str().to_string(),
                reference_genome: carrier.reference_genome.as_str().to_string(),
                chrom: carrier.chrom.clone(),
                position: carrier.position,
                source_variant_id: carrier.identity.source_variant_id.clone(),
                alt_index: carrier.alt_index,
                alt: carrier.alt.clone(),
                sample_id: carrier.sample_id.clone(),
                genotype_position: carrier.genotype_position,
                gt_alleles: carrier.gt_alleles.clone(),
                gt_phased: carrier.gt_phased as u8,
                genotype_fields_json: serde_json::to_string(&carrier.genotype_fields)?,
                position_fields_json: serde_json::to_string(&carrier.position_fields)?,
            });
        }

        rows.rejects = batch
            .report
            .rejects
            .iter()
            .map(|reject| RejectStageRow {
                run_id: context.run_id.clone(),
                task_id: context.task_id.clone(),
                attempt_id: context.attempt_id.clone(),
                record_number: reject.record_number.map(|value| value as u64),
                source_variant_id: reject.source_variant_id.clone(),
                reject_code: reject.code.as_str().to_string(),
                message: reject.message.clone(),
            })
            .collect();
        Ok(rows)
    }

    fn counts(&self, source_records: usize) -> anyhow::Result<StagedCounts> {
        Ok(StagedCounts {
            source_records: u64::try_from(source_records)?,
            summaries: u64::try_from(self.summaries.len())?,
            alleles: u64::try_from(self.alleles.len())?,
            frequencies: u64::try_from(self.frequencies.len())?,
            carriers: u64::try_from(self.carriers.len())?,
            rejects: u64::try_from(self.rejects.len())?,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct InsertStats {
    pub rows: u64,
    /// Bytes in the acknowledged JSONEachRow request bodies (including newlines).
    pub bytes: u64,
    pub requests: u64,
}

pub fn stage_attempt(
    target: &ClickHouseTarget,
    context: &AttemptContext,
    batch: &TransformationBatch,
) -> anyhow::Result<StagedCounts> {
    stage_attempt_tracked(target, context, batch, &mut InsertStats::default())
}

pub fn stage_attempt_tracked(
    target: &ClickHouseTarget,
    context: &AttemptContext,
    batch: &TransformationBatch,
    inserted: &mut InsertStats,
) -> anyhow::Result<StagedCounts> {
    let rows = StageRows::from_batch(context, batch)?;
    let counts = rows.counts(batch.report.source_records)?;

    ensure_run_accepts_primary_writes(target, &context.run_id)?;
    insert_tracked(target, "lr_y1_summaries", &rows.summaries, inserted)?;
    insert_tracked(target, "lr_y1_alleles", &rows.alleles, inserted)?;
    insert_tracked(target, "lr_y1_frequencies", &rows.frequencies, inserted)?;
    insert_tracked(target, "lr_y1_carriers", &rows.carriers, inserted)?;
    insert_tracked(target, "lr_y1_rejects_staging", &rows.rejects, inserted)?;
    Ok(counts)
}

pub(crate) fn ensure_run_accepts_primary_writes(
    target: &ClickHouseTarget,
    run_id: &str,
) -> anyhow::Result<()> {
    let body = target.query_text(
        "SELECT state FROM lr_y1_load_runs WHERE run_id = {run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT TabSeparated",
        &[("run_id", run_id)],
    )?;
    validate_primary_write_state((!body.trim().is_empty()).then(|| body.trim()))
}

pub(crate) fn validate_primary_write_state(state: Option<&str>) -> anyhow::Result<()> {
    match state {
        None | Some("loading" | "validated") => Ok(()),
        Some(state) => {
            bail!("primary run is fenced in state {state:?}; late canonical writes are rejected")
        }
    }
}

pub(crate) fn delete_attempt_rows(
    target: &ClickHouseTarget,
    run_id: &str,
    task_id: &str,
    attempt_id: &str,
) -> anyhow::Result<()> {
    let parameters = [
        ("run_id", run_id),
        ("task_id", task_id),
        ("attempt_id", attempt_id),
    ];
    for table in [
        "lr_y1_summaries",
        "lr_y1_alleles",
        "lr_y1_frequencies",
        "lr_y1_carriers",
        "lr_y1_rejects_staging",
    ] {
        target.execute_with_params(
            &format!(
                "ALTER TABLE {table} DELETE WHERE run_id = {{run_id:String}} AND task_id = {{task_id:String}} AND attempt_id = {{attempt_id:String}} SETTINGS mutations_sync = 2"
            ),
            &parameters,
        )?;
    }
    Ok(())
}

fn insert_tracked<T: Serialize>(
    target: &ClickHouseTarget,
    table: &str,
    rows: &[T],
    inserted: &mut InsertStats,
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let bytes = rows.iter().try_fold(0u64, |total, row| {
        let row_bytes = u64::try_from(serde_json::to_vec(row)?.len())?;
        total
            .checked_add(row_bytes + 1)
            .context("insert byte count overflow")
    })?;
    target.insert_json_each_row(table, rows)?;
    inserted.rows = inserted
        .rows
        .checked_add(u64::try_from(rows.len())?)
        .context("insert row count overflow")?;
    inserted.bytes = inserted
        .bytes
        .checked_add(bytes)
        .context("insert byte count overflow")?;
    inserted.requests += 1;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptState {
    Running,
    Failed,
    Accepted,
}

impl AttemptState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Accepted => "accepted",
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct TaskAttemptLedgerRow {
    run_id: String,
    task_id: String,
    attempt_id: String,
    revision: u64,
    state: String,
    chrom: String,
    interval_start: u32,
    interval_end: u32,
    source_records: u64,
    summary_rows: u64,
    allele_rows: u64,
    frequency_rows: u64,
    carrier_rows: u64,
    rejected_records: u64,
    report_json: String,
    started_at_ms: u64,
    updated_at_ms: u64,
    error: String,
}

impl TaskAttemptLedgerRow {
    pub(super) fn new(
        context: &AttemptContext,
        revision: u64,
        state: AttemptState,
        counts: StagedCounts,
        report: &PoolY1AttemptReport,
        error: impl Into<String>,
    ) -> anyhow::Result<Self> {
        context.validate()?;
        if report.worker_principal.trim().is_empty() {
            bail!("Y1 attempt report requires an authenticated ClickHouse worker_principal");
        }
        if report.run_id != context.run_id
            || report.task_id != context.task_id
            || report.attempt_id != context.attempt_id
            || report.cohort != context.cohort
            || report.chrom != context.chrom
            || report.start != context.interval_start
            || report.stop != context.interval_end
            || report.state != state.as_str()
            || report.counts != counts
        {
            bail!("Y1 attempt report does not match its ledger context, state, or counts");
        }
        Ok(Self {
            run_id: context.run_id.clone(),
            task_id: context.task_id.clone(),
            attempt_id: context.attempt_id.clone(),
            revision,
            state: state.as_str().to_string(),
            chrom: context.chrom.clone(),
            interval_start: context.interval_start,
            interval_end: context.interval_end,
            source_records: counts.source_records,
            summary_rows: counts.summaries,
            allele_rows: counts.alleles,
            frequency_rows: counts.frequencies,
            carrier_rows: counts.carriers,
            rejected_records: counts.rejects,
            report_json: serde_json::to_string(report)?,
            started_at_ms: report.started_at_ms,
            updated_at_ms: report.finished_at_ms,
            error: error.into(),
        })
    }
}

pub(super) fn record_task_attempt(
    target: &ClickHouseTarget,
    row: &TaskAttemptLedgerRow,
) -> anyhow::Result<()> {
    target.insert_json_each_row("lr_y1_task_attempts", std::slice::from_ref(row))
}

#[derive(Debug, Serialize)]
pub struct LoadRunLedgerRow {
    pub run_id: String,
    pub revision: u64,
    pub state: String,
    pub load_scope: String,
    pub release: String,
    pub cohort: String,
    pub reference_genome: String,
    pub chrom: String,
    pub interval_start: u32,
    pub interval_end: u32,
    pub source_uri: String,
    pub source_generation: String,
    pub source_checksum_algorithm: String,
    pub source_checksum: String,
    pub source_index_uri: String,
    pub source_index_generation: String,
    pub source_index_checksum: String,
    pub schema_version: u16,
    pub loader_version: String,
    pub expected_tasks: u32,
    pub expected_source_records: u64,
    pub summary_rows: u64,
    pub allele_rows: u64,
    pub frequency_rows: u64,
    pub carrier_rows: u64,
    pub rejected_records: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub message: String,
}

pub fn record_load_run(target: &ClickHouseTarget, row: &LoadRunLedgerRow) -> anyhow::Result<()> {
    target.insert_json_each_row("lr_y1_load_runs", std::slice::from_ref(row))
}

fn validate_summary_context(
    context: &AttemptContext,
    summary: &SummaryRecord,
) -> anyhow::Result<()> {
    if summary.identity.release != Release::Y1
        || summary.identity.cohort != context.cohort
        || summary.reference_genome != ReferenceGenome::Grch38
        || summary.chrom != context.chrom
        || summary.position < context.interval_start
        || summary.position > context.interval_end
    {
        bail!(
            "summary {} is outside its declared attempt context",
            summary.identity.source_variant_id
        );
    }
    Ok(())
}

fn validate_carrier_context(
    context: &AttemptContext,
    carrier: &CarrierRecord,
) -> anyhow::Result<()> {
    if carrier.identity.release != Release::Y1
        || carrier.identity.cohort != context.cohort
        || carrier.reference_genome != ReferenceGenome::Grch38
        || carrier.chrom != context.chrom
        || carrier.position < context.interval_start
        || carrier.position > context.interval_end
    {
        bail!(
            "carrier {} is outside its declared attempt context",
            carrier.identity.source_variant_id
        );
    }
    Ok(())
}

fn compute_xpos(chrom: &str, position: u32) -> anyhow::Result<u64> {
    let raw = chrom.strip_prefix("chr").unwrap_or(chrom);
    let chromosome_number = match raw {
        "X" => 23,
        "Y" => 24,
        "M" | "MT" => 25,
        _ => raw
            .parse::<u64>()
            .with_context(|| format!("unsupported chromosome {chrom:?}"))?,
    };
    if !(1..=25).contains(&chromosome_number) {
        bail!("unsupported chromosome {chrom:?}");
    }
    Ok(chromosome_number * 1_000_000_000 + u64::from(position))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    const HGSVC_FIXTURE: &str = include_str!("../../tests/fixtures/y1/hgsvc_hprc_trv_13_alt.vcf");
    const AOU_FIXTURE: &str = include_str!("../../tests/fixtures/y1/aou_summary_only_ins.vcf");

    fn fixture_batch(fixture: &str, cohort: Cohort) -> TransformationBatch {
        let header = super::super::parser::Y1Header::parse(fixture, cohort).unwrap();
        let records: Vec<&str> = fixture
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect();
        super::super::parser::transform_records(&header, records)
    }

    #[derive(Default)]
    struct MockSchemaState {
        tables: SchemaInventory,
        other_tables: BTreeMap<String, String>,
        receipt: Option<(String, String)>,
        executed: Vec<String>,
    }

    struct MockSchemaBackend {
        state: RefCell<MockSchemaState>,
    }

    impl MockSchemaBackend {
        fn fresh() -> Self {
            Self {
                state: RefCell::new(MockSchemaState::default()),
            }
        }

        fn historical_v3() -> Self {
            let contract = METHYLATION_V4_TABLES
                .iter()
                .find(|table| table.name == "lr_y1_methylation")
                .unwrap();
            let mut table = table_inventory_from_contract(contract, 0);
            let coverage = table
                .columns
                .iter_mut()
                .find(|(name, _)| name == "coverage")
                .unwrap();
            coverage.1 = "UInt16".into();
            let coverage = table
                .column_semantics
                .iter_mut()
                .find(|column| column.name == "coverage")
                .unwrap();
            coverage.column_type = "UInt16".into();
            table.create_table_query =
                table
                    .create_table_query
                    .replacen("coverageUInt32", "coverageUInt16", 1);
            Self {
                state: RefCell::new(MockSchemaState {
                    tables: [(contract.name.to_string(), table)].into_iter().collect(),
                    ..MockSchemaState::default()
                }),
            }
        }

        fn exact_v4_with_receipt() -> Self {
            let mut tables = SchemaInventory::new();
            for contract in METHYLATION_V4_TABLES {
                tables.insert(
                    contract.name.to_string(),
                    table_inventory_from_contract(contract, 0),
                );
            }
            let scoped = METHYLATION_V4_TABLES
                .iter()
                .map(|table| table.name)
                .collect::<std::collections::BTreeSet<_>>();
            let other_tables = Y1_SCHEMA_TABLE_NAMES
                .iter()
                .filter(|name| !scoped.contains(**name))
                .map(|name| {
                    (
                        (*name).to_string(),
                        expected_normalized_create_statement(name),
                    )
                })
                .collect();
            Self {
                state: RefCell::new(MockSchemaState {
                    tables,
                    other_tables,
                    receipt: Some(("applied".into(), Y1_SCHEMA_CONTRACT.into())),
                    ..MockSchemaState::default()
                }),
            }
        }
    }

    impl SchemaBackend for MockSchemaBackend {
        fn database(&self) -> &str {
            "gnomad_lr_y1_scratch_v5_schema_mock"
        }

        fn execute(&self, query: &str) -> anyhow::Result<()> {
            let mut state = self.state.borrow_mut();
            state.executed.push(query.to_string());

            for name in Y1_SCHEMA_TABLE_NAMES {
                let expected = fresh_create_statement(y1_schema_ddl(name))?;
                if query == expected {
                    if state.tables.contains_key(*name) || state.other_tables.contains_key(*name) {
                        bail!("mock strict CREATE collided with existing table {name}");
                    }
                    if let Some(contract) = METHYLATION_V4_TABLES
                        .iter()
                        .find(|contract| contract.name == *name)
                    {
                        state.tables.insert(
                            (*name).to_string(),
                            table_inventory_from_contract(contract, 0),
                        );
                    } else {
                        state.other_tables.insert(
                            (*name).to_string(),
                            expected_normalized_create_statement(name),
                        );
                    }
                    return Ok(());
                }
            }

            let expected_receipt = format!(
                "INSERT INTO lr_y1_schema_versions \
                 (schema_scope, schema_version, state, contract, applied_at, revision) VALUES \
                 ('y1_full', 5, 'applied', '{Y1_SCHEMA_CONTRACT}', now64(3), toUInt64(toUnixTimestamp64Milli(now64(3))))"
            );
            if query == expected_receipt {
                state.receipt = Some(("applied".into(), Y1_SCHEMA_CONTRACT.into()));
                state
                    .tables
                    .get_mut("lr_y1_schema_versions")
                    .ok_or_else(|| anyhow::anyhow!("mock receipt table is absent"))?
                    .rows = 1;
                return Ok(());
            }
            bail!("unhandled mock schema statement: {query}")
        }

        fn query_text(&self, query: &str, _parameters: &[(&str, &str)]) -> anyhow::Result<String> {
            let state = self.state.borrow();
            if query == "SELECT name FROM system.tables WHERE database = {database:String} ORDER BY name FORMAT TabSeparated" {
                let mut names = state.tables.keys().cloned().collect::<Vec<_>>();
                names.extend(state.other_tables.keys().cloned());
                names.sort();
                return Ok(names.into_iter().map(|name| format!("{name}\n")).collect());
            }
            if query.starts_with("SELECT name, create_table_query FROM system.tables ")
                && query.ends_with("ORDER BY name FORMAT JSONEachRow")
            {
                let mut body = String::new();
                for (name, table) in &state.tables {
                    body.push_str(
                        &serde_json::json!({
                            "name": name,
                            "create_table_query": table.create_table_query,
                        })
                        .to_string(),
                    );
                    body.push('\n');
                }
                for (name, create_table_query) in &state.other_tables {
                    body.push_str(
                        &serde_json::json!({
                            "name": name,
                            "create_table_query": create_table_query,
                        })
                        .to_string(),
                    );
                    body.push('\n');
                }
                return Ok(body);
            }
            if query.starts_with("SELECT name, engine, partition_key, sorting_key, primary_key, sampling_key, create_table_query FROM system.tables ")
                && query.ends_with("ORDER BY name FORMAT JSONEachRow")
            {
                let mut body = String::new();
                for (name, table) in &state.tables {
                    body.push_str(&serde_json::json!({
                        "name": name,
                        "engine": table.engine,
                        "partition_key": table.partition_key.join(", "),
                        "sorting_key": table.sorting_key.join(", "),
                        "primary_key": table.primary_key.join(", "),
                        "sampling_key": table.sampling_key.join(", "),
                        "create_table_query": table.create_table_query,
                    }).to_string());
                    body.push('\n');
                }
                return Ok(body);
            }
            if query.starts_with("SELECT table, name, type, position, default_kind, default_expression, compression_codec, ")
                && query.ends_with("ORDER BY table, position FORMAT JSONEachRow")
            {
                let mut body = String::new();
                for (table_name, table) in &state.tables {
                    for (index, column) in table.column_semantics.iter().enumerate() {
                        body.push_str(&serde_json::json!({
                            "table": table_name,
                            "name": column.name,
                            "type": column.column_type,
                            "position": index + 1,
                            "default_kind": column.default_kind,
                            "default_expression": column.default_expression,
                            "compression_codec": column.compression_codec,
                            "is_in_partition_key": u8::from(column.is_in_partition_key),
                            "is_in_sorting_key": u8::from(column.is_in_sorting_key),
                            "is_in_primary_key": u8::from(column.is_in_primary_key),
                            "is_in_sampling_key": u8::from(column.is_in_sampling_key),
                        }).to_string());
                        body.push('\n');
                    }
                }
                return Ok(body);
            }
            if query == "SELECT state, contract FROM lr_y1_schema_versions FINAL WHERE schema_scope = 'y1_full' AND schema_version = 5 FORMAT TabSeparated" {
                return Ok(state
                    .receipt
                    .as_ref()
                    .map(|(state, contract)| format!("{state}\t{contract}\n"))
                    .unwrap_or_default());
            }
            if let Some(rest) = query.strip_prefix("SELECT count() FROM ") {
                let table_name = rest
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("mock count query lacks table"))?;
                let rows = state
                    .tables
                    .get(table_name)
                    .ok_or_else(|| anyhow::anyhow!("mock count query names absent table"))?
                    .rows;
                return Ok(format!("{rows}\n"));
            }
            bail!("unhandled mock schema query: {query}")
        }
    }

    fn table_inventory_from_contract(contract: &TableContract, rows: u64) -> TableInventory {
        let column_semantics = contract
            .columns
            .iter()
            .map(|column| ColumnSemantics {
                name: column.name.into(),
                column_type: column.column_type.into(),
                default_kind: String::new(),
                default_expression: String::new(),
                compression_codec: String::new(),
                ttl_expression: String::new(),
                is_in_partition_key: contract.partition_key.contains(&column.name),
                is_in_sorting_key: contract.sorting_key.contains(&column.name),
                is_in_primary_key: contract.sorting_key.contains(&column.name),
                is_in_sampling_key: false,
            })
            .collect();
        TableInventory {
            engine: contract.engine.into(),
            columns: contract
                .columns
                .iter()
                .map(|column| (column.name.into(), column.column_type.into()))
                .collect(),
            column_semantics,
            partition_key: contract
                .partition_key
                .iter()
                .map(|value| (*value).into())
                .collect(),
            sorting_key: contract
                .sorting_key
                .iter()
                .map(|value| (*value).into())
                .collect(),
            primary_key: contract
                .sorting_key
                .iter()
                .map(|value| (*value).into())
                .collect(),
            sampling_key: Vec::new(),
            create_table_query: expected_normalized_create_statement(contract.name),
            rows,
        }
    }

    #[test]
    fn schema_preflight_count_rows_fail_closed() {
        assert_eq!(parse_single_count("0\n", "count").unwrap(), 0);
        assert_eq!(parse_single_count("17", "count").unwrap(), 17);
        for invalid in ["", "1\t2\n", "1\n2\n", "not-a-count\n"] {
            assert!(parse_single_count(invalid, "count").is_err());
        }
    }

    #[test]
    fn schema_mock_fresh_v4_is_strictly_created_and_receipted_once() {
        let backend = MockSchemaBackend::fresh();
        init_schema_with_backend(&backend).unwrap();
        let state = backend.state.borrow();
        validate_exact_methylation_v4_schema(&state.tables).unwrap();
        assert_eq!(
            state.receipt,
            Some(("applied".into(), Y1_SCHEMA_CONTRACT.into()))
        );
        assert_eq!(state.executed.len(), Y1_SCHEMA_TABLE_NAMES.len() + 1);
        assert!(state
            .executed
            .iter()
            .all(|statement| !statement.contains("ALTER TABLE")));
        assert!(state
            .executed
            .iter()
            .filter(|statement| statement.contains("CREATE TABLE"))
            .all(|statement| !statement.contains("IF NOT EXISTS")));
    }

    #[test]
    fn schema_mock_exact_attested_retry_executes_nothing() {
        let backend = MockSchemaBackend::exact_v4_with_receipt();
        init_schema_with_backend(&backend).unwrap();
        assert!(backend.state.borrow().executed.is_empty());
    }

    #[test]
    fn schema_mock_rejects_v3_partial_and_unreceipted_states_before_ddl() {
        let historical = MockSchemaBackend::historical_v3();
        let error = init_schema_with_backend(&historical).unwrap_err();
        assert!(error.to_string().contains("refusing in-place"));
        assert!(historical.state.borrow().executed.is_empty());

        let partial = MockSchemaBackend::fresh();
        partial
            .state
            .borrow_mut()
            .other_tables
            .insert("lr_y1_load_runs".into(), "partial historical shape".into());
        let error = init_schema_with_backend(&partial).unwrap_err();
        assert!(error.to_string().contains("refusing in-place"));
        assert!(partial.state.borrow().executed.is_empty());

        let unreceipted = MockSchemaBackend::exact_v4_with_receipt();
        unreceipted.state.borrow_mut().receipt = None;
        let error = init_schema_with_backend(&unreceipted).unwrap_err();
        assert!(error.to_string().contains("refusing in-place"));
        assert!(unreceipted.state.borrow().executed.is_empty());
    }

    #[test]
    fn applied_v4_receipt_never_bypasses_full_semantic_attestation() {
        for mutation in [
            "columns",
            "types",
            "keys",
            "partition",
            "default",
            "codec",
            "setting",
            "sampling",
            "constraint",
        ] {
            let backend = MockSchemaBackend::exact_v4_with_receipt();
            {
                let mut state = backend.state.borrow_mut();
                let table = state.tables.get_mut("lr_y1_methylation_phased").unwrap();
                match mutation {
                    "columns" => {
                        table.columns.pop();
                        table.column_semantics.pop();
                    }
                    "types" => {
                        table.columns[14].1 = "Nullable(UInt32)".into();
                        table.column_semantics[14].column_type = "Nullable(UInt32)".into();
                    }
                    "keys" => {
                        table.sorting_key.swap(3, 4);
                    }
                    "partition" => {
                        table.partition_key.pop();
                    }
                    "default" => {
                        table.column_semantics[14].default_kind = "DEFAULT".into();
                        table.column_semantics[14].default_expression = "7".into();
                    }
                    "codec" => {
                        table.column_semantics[14].compression_codec = "ZSTD(1)".into();
                    }
                    "setting" => {
                        table.create_table_query = table.create_table_query.replace("8192", "4096");
                    }
                    "sampling" => {
                        table.sampling_key.push("position".into());
                    }
                    "constraint" => {
                        table
                            .create_table_query
                            .push_str("CONSTRAINTunexpectedCHECK1");
                    }
                    _ => unreachable!(),
                }
            }
            let error = init_schema_with_backend(&backend).unwrap_err();
            assert!(
                error.to_string().contains("SHOW CREATE"),
                "{mutation} mutation was not rejected: {error:#}"
            );
            assert!(backend.state.borrow().executed.is_empty());
        }
    }

    #[test]
    fn full_receipt_rejects_a_non_methylation_table_semantic_change() {
        let backend = MockSchemaBackend::exact_v4_with_receipt();
        backend
            .state
            .borrow_mut()
            .other_tables
            .get_mut("lr_y1_load_runs")
            .unwrap()
            .push_str("CONSTRAINTunexpectedCHECK1");
        let error = init_schema_with_backend(&backend).unwrap_err();
        assert!(error
            .to_string()
            .contains("exact normalized SHOW CREATE contract"));
        assert!(backend.state.borrow().executed.is_empty());
    }

    #[test]
    fn schema_mock_fails_on_every_unhandled_statement_or_query() {
        let backend = MockSchemaBackend::fresh();
        assert!(backend
            .execute("ALTER TABLE anything ADD COLUMN surprise UInt8")
            .is_err());
        assert!(backend.query_text("SELECT 1", &[]).is_err());
    }

    #[test]
    fn expands_canonical_records_into_browser_and_carrier_shapes() {
        let hgsvc_batch = fixture_batch(HGSVC_FIXTURE, Cohort::HgsvcHprc);
        let hgsvc_context = AttemptContext {
            run_id: "unit-hgsvc".to_string(),
            task_id: "chr22-20m".to_string(),
            attempt_id: "attempt-1".to_string(),
            cohort: Cohort::HgsvcHprc,
            chrom: "chr22".to_string(),
            interval_start: 20_000_000,
            interval_end: 20_010_000,
        };
        let rows = StageRows::from_batch(&hgsvc_context, &hgsvc_batch).unwrap();
        assert_eq!(
            rows.counts(hgsvc_batch.report.source_records).unwrap(),
            StagedCounts {
                source_records: 1,
                summaries: 1,
                alleles: 13,
                frequencies: 273,
                carriers: 214,
                rejects: 0,
            }
        );
        assert!(rows.carriers.iter().all(|row| row.gt_phased == 0));
        assert_eq!(rows.alleles.last().unwrap().alt_index, 13);

        let aou_batch = fixture_batch(AOU_FIXTURE, Cohort::Aou);
        let aou_context = AttemptContext {
            run_id: "unit-aou".to_string(),
            task_id: "chr22-20m".to_string(),
            attempt_id: "attempt-1".to_string(),
            cohort: Cohort::Aou,
            chrom: "chr22".to_string(),
            interval_start: 20_000_000,
            interval_end: 20_010_000,
        };
        let rows = StageRows::from_batch(&aou_context, &aou_batch).unwrap();
        assert_eq!(
            rows.counts(aou_batch.report.source_records).unwrap(),
            StagedCounts {
                source_records: 1,
                summaries: 1,
                alleles: 1,
                frequencies: 6,
                carriers: 0,
                rejects: 0,
            }
        );
        let divisions: std::collections::BTreeSet<_> = rows
            .frequencies
            .iter()
            .map(|row| row.division.as_str())
            .collect();
        assert_eq!(
            divisions,
            std::collections::BTreeSet::from(["all", "XX", "XY", "afr", "afr_XX", "afr_XY"])
        );
    }

    #[test]
    fn materializes_alt_specific_annotations() {
        let info = std::collections::BTreeMap::from([
            ("cadd_phred".to_string(), Some("1.25,9.5".to_string())),
            (
                "gnomAD_V4_match_ID".to_string(),
                Some("22-100-A-C,22-100-A-G".to_string()),
            ),
        ]);
        assert_eq!(
            alt_info_value(&info, "cadd_phred", 2).as_deref(),
            Some("9.5")
        );
        assert_eq!(
            alt_info_value(&info, "gnomAD_V4_match_ID", 1).as_deref(),
            Some("22-100-A-C")
        );
        assert_eq!(alt_info_value(&info, "phylop", 1), None);
    }

    #[test]
    fn ranks_pick_vep_consequences_for_the_selected_alt() {
        let info = std::collections::BTreeMap::from([(
            "vep".to_string(),
            Some([
                "T|intron_variant|MODIFIER|GENE||Transcript|||||||||||||||||1",
                "G|synonymous_variant&splice_region_variant|LOW|GENE||Transcript|||||||||||||||||1",
            ].join(",")),
        )]);
        assert_eq!(
            major_consequence(&info, "C", "G", 2).as_deref(),
            Some("splice_region_variant")
        );
    }
}
