//! Optional primary-motif product lifecycle.
//!
//! This module is intentionally separate from the frozen Y1 v5 primary schema. It
//! resolves immutable primary inputs, initializes only product tables, hashes
//! persisted aggregate rows as ordered RowBinary, and enforces append-only run
//! transitions. It never persists sample identifiers or raw genotypes.

use super::primary_motif::{
    validate_run_state_transition, PrimaryMotifRunState, PrimaryRepeatRegistry,
};
use super::{ClickHouseTarget, Cohort};
use crate::loader::immutable_gcs::ImmutableGcsObject;
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const PRODUCT_TABLES: &[&str] = &[
    "lr_y1_primary_motif_runs",
    "lr_y1_primary_motif_loci",
    "lr_y1_primary_motif_allele_bins",
    "lr_y1_primary_motif_genotype_pairs",
    "lr_y1_primary_motif_genotype_margins",
];

const PRODUCT_DDLS: &[(&str, &str)] = &[
    (
        "lr_y1_primary_motif_runs",
        include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_runs.sql"),
    ),
    (
        "lr_y1_primary_motif_loci",
        include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_loci.sql"),
    ),
    (
        "lr_y1_primary_motif_allele_bins",
        include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_allele_bins.sql"),
    ),
    (
        "lr_y1_primary_motif_genotype_pairs",
        include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_genotype_pairs.sql"),
    ),
    (
        "lr_y1_primary_motif_genotype_margins",
        include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_genotype_margins.sql"),
    ),
];

/// Create the optional product schema as an all-or-nothing unit and attest every
/// declared column. A partially present schema is never repaired implicitly.
pub fn init_primary_motif_schema(target: &ClickHouseTarget) -> anyhow::Result<()> {
    let present = present_product_tables(target)?;
    if !present.is_empty() && present.len() != PRODUCT_TABLES.len() {
        bail!("refusing to repair a partial primary-motif product schema: {present:?}");
    }
    if present.is_empty() {
        for (_, ddl) in PRODUCT_DDLS {
            target.execute(ddl)?;
        }
    }
    attest_primary_motif_schema(target)
}

pub fn attest_primary_motif_schema(target: &ClickHouseTarget) -> anyhow::Result<()> {
    let present = present_product_tables(target)?;
    let expected = PRODUCT_TABLES.iter().copied().collect::<BTreeSet<_>>();
    if present != expected {
        bail!("primary-motif product table inventory differs: {present:?}");
    }
    for (table, ddl) in PRODUCT_DDLS {
        let expected_columns = ddl_columns(ddl)?;
        let body = target.query_text(&format!("DESCRIBE TABLE {table} FORMAT JSONEachRow"), &[])?;
        let mut observed = Vec::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let row: DescribeColumn = serde_json::from_str(line)
                .with_context(|| format!("invalid DESCRIBE receipt for {table}"))?;
            observed.push((row.name, row.column_type));
        }
        if observed != expected_columns {
            bail!("primary-motif product column contract differs for {table}");
        }
        let shape_body = target.query_text(
            "SELECT engine_full, partition_key, sorting_key FROM system.tables WHERE database = {database:String} AND name = {table:String} FORMAT JSONEachRow",
            &[("database", target.database()), ("table", table)],
        )?;
        let shape: TableShape = exactly_one_json(&shape_body, "product table shape")?;
        let expected_shape = ddl_shape(ddl)?;
        if normalize_shape(&shape.engine_full) != normalize_shape(&expected_shape.engine_full)
            || normalize_shape(&shape.partition_key)
                != normalize_shape(&expected_shape.partition_key)
            || normalize_shape(&shape.sorting_key) != normalize_shape(&expected_shape.sorting_key)
        {
            bail!("primary-motif product engine/partition/order contract differs for {table}");
        }
    }
    Ok(())
}

fn present_product_tables(target: &ClickHouseTarget) -> anyhow::Result<BTreeSet<&'static str>> {
    let body = target.query_text(
        "SELECT name FROM system.tables WHERE database = {database:String} AND startsWith(name, 'lr_y1_primary_motif_') ORDER BY name FORMAT TabSeparated",
        &[("database", target.database())],
    )?;
    let mut found = BTreeSet::new();
    for name in body.lines() {
        let matched = PRODUCT_TABLES
            .iter()
            .copied()
            .find(|expected| *expected == name)
            .ok_or_else(|| anyhow::anyhow!("unexpected product table inventory row {name:?}"))?;
        found.insert(matched);
    }
    Ok(found)
}

#[derive(Deserialize)]
struct TableShape {
    engine_full: String,
    partition_key: String,
    sorting_key: String,
}

struct ExpectedTableShape {
    engine_full: String,
    partition_key: String,
    sorting_key: String,
}

#[derive(Deserialize)]
struct DescribeColumn {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
}

fn ddl_shape(ddl: &str) -> anyhow::Result<ExpectedTableShape> {
    let after_engine = ddl
        .split_once("ENGINE = ")
        .context("product DDL lacks ENGINE")?
        .1;
    let (engine_full, keys) = if let Some(value) = after_engine.split_once("\nPARTITION BY ") {
        (value.0.trim(), value.1)
    } else {
        after_engine
            .split_once("\nORDER BY ")
            .map(|(engine, order)| (engine.trim(), order))
            .context("product DDL lacks ORDER BY")?
    };
    let (partition_key, sorting_key) = if after_engine.contains("\nPARTITION BY ") {
        let (partition, order) = keys
            .split_once("\nORDER BY ")
            .context("partitioned product DDL lacks ORDER BY")?;
        (partition.trim(), order.trim_end_matches(';').trim())
    } else {
        ("", keys.trim_end_matches(';').trim())
    };
    Ok(ExpectedTableShape {
        engine_full: engine_full.to_string(),
        partition_key: partition_key.to_string(),
        sorting_key: sorting_key.to_string(),
    })
}

fn normalize_shape(value: &str) -> String {
    let mut compact = value
        .chars()
        .filter(|character| !character.is_whitespace() && *character != '`')
        .collect::<String>();
    loop {
        if compact.starts_with("tuple(") && compact.ends_with(')') {
            compact = compact[6..compact.len() - 1].to_string();
        } else if compact.starts_with('(') && compact.ends_with(')') {
            compact = compact[1..compact.len() - 1].to_string();
        } else {
            return compact;
        }
    }
}

fn ddl_columns(ddl: &str) -> anyhow::Result<Vec<(String, String)>> {
    let start = ddl.find('(').context("product DDL lacks column list")?;
    let end = ddl
        .rfind(") ENGINE")
        .context("product DDL lacks ENGINE boundary")?;
    ddl[start + 1..end]
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("--"))
        .map(|line| {
            let line = line.strip_suffix(',').unwrap_or(line);
            let (name, ty) = line
                .split_once(char::is_whitespace)
                .context("product DDL column lacks a type")?;
            Ok((name.to_string(), ty.trim().to_string()))
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContigSourceManifest {
    schema_version: Option<u16>,
    contract_type: Option<String>,
    release: String,
    reference_genome: Option<String>,
    chromosome: String,
    mirror_prefix: String,
    full_genome_inventory_sha256: Option<String>,
    mt_enabled: Option<bool>,
    objects: Vec<SourceObject>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceObject {
    cohort: String,
    name: String,
    source_generation: String,
    mirror_generation: String,
    size: u64,
    md5_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ImmutableProductSource {
    pub manifest_sha256: String,
    pub inventory_sha256: String,
    pub source_uri: String,
    pub source_generation: String,
    pub source_size_bytes: u64,
    pub source_md5_base64: String,
    pub source_index_uri: String,
    pub source_index_generation: String,
    pub source_index_size_bytes: u64,
    pub source_index_md5_base64: String,
}

impl ImmutableProductSource {
    pub fn vcf_object(&self) -> ImmutableGcsObject {
        immutable_object(
            &self.source_uri,
            &self.source_generation,
            self.source_size_bytes,
            &self.source_md5_base64,
        )
    }
    pub fn index_object(&self) -> ImmutableGcsObject {
        immutable_object(
            &self.source_index_uri,
            &self.source_index_generation,
            self.source_index_size_bytes,
            &self.source_index_md5_base64,
        )
    }
}

fn immutable_object(uri: &str, generation: &str, size: u64, md5: &str) -> ImmutableGcsObject {
    ImmutableGcsObject {
        uri: uri.to_string(),
        generation: generation.to_string(),
        byte_size: size,
        checksum_algorithm: "md5_base64".into(),
        checksum: md5.to_string(),
        immutable_read_uri: format!("{uri}?generation={generation}"),
    }
}

pub fn resolve_product_source_manifest(
    path: &Path,
    cohort: Cohort,
    chrom: &str,
    expected_inventory_sha256: &str,
) -> anyhow::Result<ImmutableProductSource> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read source manifest {}", path.display()))?;
    let manifest: ContigSourceManifest =
        serde_json::from_slice(&bytes).context("invalid per-contig primary source manifest")?;
    if manifest.schema_version != Some(2)
        || manifest.contract_type.as_deref() != Some("y1_per_contig_immutable_source")
        || manifest.release != "Y1"
        || manifest.reference_genome.as_deref() != Some("GRCh38")
        || manifest.chromosome != chrom
        || manifest.mt_enabled != Some(false)
        || manifest.full_genome_inventory_sha256.as_deref() != Some(expected_inventory_sha256)
    {
        bail!("per-contig source manifest identity differs from the product registry");
    }
    let cohort = cohort.as_str();
    let base = format!("gnomAD_LR_Y1.{cohort}.{chrom}.vcf.gz");
    let object = exactly_one_object(&manifest.objects, cohort, &base)?;
    let index = exactly_one_object(&manifest.objects, cohort, &format!("{base}.tbi"))?;
    let prefix = manifest.mirror_prefix.trim_end_matches('/');
    let source = ImmutableProductSource {
        manifest_sha256: format!("{:x}", Sha256::digest(&bytes)),
        inventory_sha256: expected_inventory_sha256.to_string(),
        source_uri: format!("{prefix}/{}", object.name),
        source_generation: object.mirror_generation.clone(),
        source_size_bytes: object.size,
        source_md5_base64: object.md5_base64.clone(),
        source_index_uri: format!("{prefix}/{}", index.name),
        source_index_generation: index.mirror_generation.clone(),
        source_index_size_bytes: index.size,
        source_index_md5_base64: index.md5_base64.clone(),
    };
    source.vcf_object().request()?;
    source.index_object().request()?;
    Ok(source)
}

fn exactly_one_object<'a>(
    objects: &'a [SourceObject],
    cohort: &str,
    name: &str,
) -> anyhow::Result<&'a SourceObject> {
    let matches = objects
        .iter()
        .filter(|row| row.cohort == cohort && row.name == name)
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        bail!(
            "source manifest has {} objects for {cohort}/{name}; expected one",
            matches.len()
        );
    }
    if matches[0].source_generation.parse::<u64>().is_err() {
        bail!("source provenance generation is malformed");
    }
    Ok(matches[0])
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalPrimaryLocus {
    pub source_variant_id: String,
    pub position: u32,
    pub task_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedPrimaryProductInput {
    pub primary_database: String,
    pub primary_run_id: String,
    pub cohort: String,
    pub chrom: String,
    pub source: ImmutableProductSource,
    pub loci: Vec<CanonicalPrimaryLocus>,
}

#[derive(Deserialize)]
struct PrimaryRunRow {
    run_id: String,
    source_uri: String,
    source_generation: String,
    source_checksum_algorithm: String,
    source_checksum: String,
    source_index_uri: String,
    source_index_generation: String,
    source_index_checksum: String,
}

#[derive(Deserialize)]
struct CanonicalLocusRow {
    source_variant_id: String,
    position: u32,
    task_id: String,
    attempt_id: String,
    rows: u64,
    attempt_state: String,
}

/// Resolve exactly one accepted-frozen primary run and exactly one canonical
/// summary/accepted attempt for every registered locus on the selected contig.
pub fn resolve_accepted_primary_input(
    target: &ClickHouseTarget,
    registry: &PrimaryRepeatRegistry,
    source_manifest: &Path,
    cohort: Cohort,
    chrom: &str,
) -> anyhow::Result<ResolvedPrimaryProductInput> {
    registry.validate()?;
    let entries = registry
        .entries
        .iter()
        .filter(|entry| entry.chrom == chrom)
        .collect::<Vec<_>>();
    if entries.is_empty() {
        bail!("registry has no loci on selected chromosome");
    }
    let source = resolve_product_source_manifest(
        source_manifest,
        cohort,
        chrom,
        &registry.source_inventory_sha256,
    )?;
    let body = target.query_text(
        "SELECT run_id, source_uri, source_generation, source_checksum_algorithm, source_checksum, source_index_uri, source_index_generation, source_index_checksum FROM lr_y1_load_runs FINAL WHERE state = 'accepted_frozen' AND release = 'y1' AND cohort = {cohort:String} AND reference_genome = 'GRCh38' AND chrom = {chrom:String} FORMAT JSONEachRow",
        &[("cohort", cohort.as_str()), ("chrom", chrom)],
    )?;
    let run: PrimaryRunRow = exactly_one_json(&body, "accepted-frozen primary run")?;
    if run.source_uri != source.source_uri
        || run.source_generation != source.source_generation
        || run.source_checksum_algorithm != "md5_base64"
        || run.source_checksum != source.source_md5_base64
        || run.source_index_uri != source.source_index_uri
        || run.source_index_generation != source.source_index_generation
        || run.source_index_checksum != source.source_index_md5_base64
    {
        bail!("accepted primary run immutable source identity differs from checked manifest");
    }
    let mut loci = Vec::with_capacity(entries.len());
    for entry in entries {
        let position = entry.source_position.to_string();
        let body = target.query_text(
            "SELECT s.source_variant_id, s.position, s.task_id, s.attempt_id, count() AS rows, any(a.state) AS attempt_state FROM lr_y1_summaries AS s INNER JOIN lr_y1_task_attempts FINAL AS a ON a.run_id = s.run_id AND a.task_id = s.task_id AND a.attempt_id = s.attempt_id WHERE s.run_id = {run_id:String} AND s.chrom = {chrom:String} AND s.position = {position:UInt32} AND s.source_variant_id = {source_variant_id:String} GROUP BY s.source_variant_id, s.position, s.task_id, s.attempt_id FORMAT JSONEachRow",
            &[("run_id", &run.run_id), ("chrom", chrom), ("position", &position), ("source_variant_id", &entry.source_variant_id)],
        )?;
        let row: CanonicalLocusRow = exactly_one_json(&body, "canonical registered source record")?;
        if row.rows != 1 || row.attempt_state != "accepted" {
            bail!("registered canonical source record is duplicated or lacks one accepted attempt");
        }
        loci.push(CanonicalPrimaryLocus {
            source_variant_id: row.source_variant_id,
            position: row.position,
            task_id: row.task_id,
            attempt_id: row.attempt_id,
        });
    }
    loci.sort_by_key(|row| (row.position, row.source_variant_id.clone()));
    Ok(ResolvedPrimaryProductInput {
        primary_database: target.database().to_string(),
        primary_run_id: run.run_id,
        cohort: cohort.as_str().to_string(),
        chrom: chrom.to_string(),
        source,
        loci,
    })
}

fn exactly_one_json<T: for<'de> Deserialize<'de>>(body: &str, label: &str) -> anyhow::Result<T> {
    let rows = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    if rows.len() != 1 {
        bail!("resolved {} {label} rows; expected exactly one", rows.len());
    }
    serde_json::from_str(rows[0]).with_context(|| format!("invalid {label} receipt"))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProductPhysicalSnapshot {
    pub product_run_id: String,
    pub locus_rows: u64,
    pub bin_rows: u64,
    pub genotype_pair_rows: u64,
    pub genotype_margin_rows: u64,
    pub locus_content_sha256: String,
    pub bin_content_sha256: String,
    pub genotype_pair_content_sha256: String,
    pub genotype_margin_content_sha256: String,
}

pub fn snapshot_product_rows(
    target: &ClickHouseTarget,
    product_run_id: &str,
) -> anyhow::Result<ProductPhysicalSnapshot> {
    if product_run_id.trim().is_empty() {
        bail!("product run ID must not be empty");
    }
    let specs = [
        ("lr_y1_primary_motif_loci", "chrom, source_position, source_variant_id"),
        ("lr_y1_primary_motif_allele_bins", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), exact_units"),
        ("lr_y1_primary_motif_genotype_pairs", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), shorter_exact_units, longer_exact_units, shorter_allele_index, longer_allele_index"),
        ("lr_y1_primary_motif_genotype_margins", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), allele_index"),
    ];
    let mut counts = Vec::new();
    let mut hashes = Vec::new();
    for (table, order) in specs {
        let count = target.query_text(&format!("SELECT count() FROM {table} WHERE product_run_id = {{product_run_id:String}} FORMAT TabSeparated"), &[("product_run_id", product_run_id)])?;
        counts.push(
            count
                .trim()
                .parse::<u64>()
                .context("invalid physical product row count")?,
        );
        let domain = format!("Y1_PRIMARY_MOTIF_ROWBINARY_V1\0{table}\0{product_run_id}");
        hashes.push(target.query_sha256(
            &format!("SELECT * FROM {table} WHERE product_run_id = {{product_run_id:String}} ORDER BY {order} FORMAT RowBinary"),
            &[("product_run_id", product_run_id)], domain.as_bytes())?);
    }
    Ok(ProductPhysicalSnapshot {
        product_run_id: product_run_id.to_string(),
        locus_rows: counts[0],
        bin_rows: counts[1],
        genotype_pair_rows: counts[2],
        genotype_margin_rows: counts[3],
        locus_content_sha256: hashes[0].clone(),
        bin_content_sha256: hashes[1].clone(),
        genotype_pair_content_sha256: hashes[2].clone(),
        genotype_margin_content_sha256: hashes[3].clone(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct IndependentProductReceipt {
    pub contract: String,
    pub product_run_id: String,
    pub primary_run_id: String,
    pub release: String,
    pub cohort: String,
    pub reference_genome: String,
    pub chrom: String,
    pub source_inventory_sha256: String,
    pub registry_digest: String,
    pub registry_approval_state: String,
    pub registered_locus_ids: Vec<String>,
    pub complete_strata: bool,
    pub no_truncation: bool,
    pub exact_ac_an_and_genotype_margins: bool,
    pub metadata_run_id: Option<String>,
    pub metadata_receipt_sha256: Option<String>,
    pub metadata_manifest_sha256: Option<String>,
    pub physical: ProductPhysicalSnapshot,
    pub receipt_sha256: String,
}

impl IndependentProductReceipt {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.contract != "Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1"
            || self.release != "y1"
            || !matches!(self.cohort.as_str(), "hgsvc_hprc" | "aou")
            || self.reference_genome != "GRCh38"
            || !self.chrom.starts_with("chr")
            || !self.complete_strata
            || !self.no_truncation
            || !self.exact_ac_an_and_genotype_margins
            || self.product_run_id != self.physical.product_run_id
        {
            bail!("independent primary-motif reconciliation is incomplete or truncated");
        }
        require_sha256(
            &self.source_inventory_sha256,
            "independent source inventory",
        )?;
        require_sha256(&self.registry_digest, "independent registry")?;
        for digest in [
            &self.physical.locus_content_sha256,
            &self.physical.bin_content_sha256,
            &self.physical.genotype_pair_content_sha256,
            &self.physical.genotype_margin_content_sha256,
        ] {
            require_sha256(digest, "independent RowBinary content")?;
        }
        if self.registered_locus_ids.is_empty()
            || self
                .registered_locus_ids
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != self.registered_locus_ids.len()
        {
            bail!("independent reconciliation locus inventory is empty or duplicated");
        }
        let expected = independent_receipt_digest(self)?;
        if expected != self.receipt_sha256 {
            bail!("independent reconciliation receipt digest differs");
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct FinalizableRunRow {
    state: String,
    cohort: String,
    chrom: String,
    primary_run_id: String,
    registry_digest: String,
    registry_approval_state: String,
    source_inventory_sha256: String,
    metadata_run_id: Option<String>,
    accepted_metadata_receipt_sha256: Option<String>,
    metadata_manifest_sha256: Option<String>,
    locus_rows: u64,
    bin_rows: u64,
    genotype_pair_rows: u64,
    genotype_margin_rows: u64,
    locus_content_sha256: String,
    bin_content_sha256: String,
    genotype_pair_content_sha256: Option<String>,
    genotype_margin_content_sha256: Option<String>,
    bounds_status: String,
}

/// Bind finalization evidence to the latest independently-verified ledger
/// revision rather than trusting caller-supplied run identity or counts.
pub fn attest_produced_run(
    target: &ClickHouseTarget,
    product_run_id: &str,
    primary_run_id: &str,
    registry: &PrimaryRepeatRegistry,
    independent: &IndependentProductReceipt,
    physical: &ProductPhysicalSnapshot,
) -> anyhow::Result<()> {
    attest_product_run_evidence(
        target,
        product_run_id,
        primary_run_id,
        registry,
        independent,
        physical,
        "produced",
    )
}

pub fn attest_finalizable_run(
    target: &ClickHouseTarget,
    product_run_id: &str,
    primary_run_id: &str,
    registry: &PrimaryRepeatRegistry,
    independent: &IndependentProductReceipt,
    physical: &ProductPhysicalSnapshot,
) -> anyhow::Result<()> {
    attest_product_run_evidence(
        target,
        product_run_id,
        primary_run_id,
        registry,
        independent,
        physical,
        "independently_verified",
    )
}

fn attest_product_run_evidence(
    target: &ClickHouseTarget,
    product_run_id: &str,
    primary_run_id: &str,
    registry: &PrimaryRepeatRegistry,
    independent: &IndependentProductReceipt,
    physical: &ProductPhysicalSnapshot,
    expected_state: &str,
) -> anyhow::Result<()> {
    let body = target.query_text(
        "SELECT state, cohort, chrom, primary_run_id, registry_digest, registry_approval_state, source_inventory_sha256, metadata_run_id, accepted_metadata_receipt_sha256, metadata_manifest_sha256, locus_rows, bin_rows, genotype_pair_rows, genotype_margin_rows, locus_content_sha256, bin_content_sha256, genotype_pair_content_sha256, genotype_margin_content_sha256, bounds_status FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT JSONEachRow",
        &[("product_run_id", product_run_id)],
    )?;
    let run: FinalizableRunRow = exactly_one_json(&body, "finalizable primary-motif run")?;
    if run.state != expected_state
        || run.cohort != independent.cohort
        || run.chrom != independent.chrom
        || run.primary_run_id != primary_run_id
        || run.registry_digest != registry.content_sha256
        || run.registry_approval_state != "REVIEWED"
        || run.source_inventory_sha256 != registry.source_inventory_sha256
        || run.bounds_status != "complete_no_truncation"
        || run.locus_rows != physical.locus_rows
        || run.bin_rows != physical.bin_rows
        || run.genotype_pair_rows != physical.genotype_pair_rows
        || run.genotype_margin_rows != physical.genotype_margin_rows
        || run.locus_content_sha256 != physical.locus_content_sha256
        || run.bin_content_sha256 != physical.bin_content_sha256
        || run.genotype_pair_content_sha256.as_deref()
            != (physical.genotype_pair_rows > 0)
                .then_some(physical.genotype_pair_content_sha256.as_str())
        || run.genotype_margin_content_sha256.as_deref()
            != (physical.genotype_margin_rows > 0)
                .then_some(physical.genotype_margin_content_sha256.as_str())
        || run.metadata_run_id != independent.metadata_run_id
        || run.accepted_metadata_receipt_sha256 != independent.metadata_receipt_sha256
        || run.metadata_manifest_sha256 != independent.metadata_manifest_sha256
    {
        bail!("latest product ledger revision differs from finalization identities, bounds, counts, or RowBinary hashes");
    }
    match (
        &independent.metadata_run_id,
        &independent.metadata_receipt_sha256,
        &independent.metadata_manifest_sha256,
    ) {
        (Some(metadata_run_id), Some(receipt), Some(manifest)) => {
            let body = target.query_text(
                "SELECT state, source_manifest_sha256, report_sha256, output_rows FROM lr_y1_metadata_runs FINAL WHERE metadata_run_id = {metadata_run_id:String} FORMAT JSONEachRow",
                &[("metadata_run_id", metadata_run_id)],
            )?;
            let metadata: AcceptedMetadataRow = exactly_one_json(&body, "accepted metadata run")?;
            if metadata.state != "accepted"
                || metadata.source_manifest_sha256 != *manifest
                || metadata.report_sha256 != *receipt
                || metadata.output_rows != 292
            {
                bail!("accepted metadata ledger receipt differs from product evidence");
            }
        }
        (None, None, None) => {}
        _ => bail!("metadata finalization identity is partially populated"),
    }
    Ok(())
}

#[derive(Deserialize)]
struct AcceptedMetadataRow {
    state: String,
    source_manifest_sha256: String,
    report_sha256: String,
    output_rows: u16,
}

pub fn validate_finalizer_gates(
    registry: &PrimaryRepeatRegistry,
    cohort: Cohort,
    primary_run_id: &str,
    physical: &ProductPhysicalSnapshot,
    independent: &IndependentProductReceipt,
) -> anyhow::Result<()> {
    registry.require_production_approval()?;
    independent.validate()?;
    let expected_loci = registry
        .entries
        .iter()
        .filter(|entry| entry.chrom == independent.chrom)
        .map(|entry| entry.registry_entry_id.clone())
        .collect::<BTreeSet<_>>();
    let observed_loci = independent
        .registered_locus_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if expected_loci != observed_loci
        || independent.primary_run_id != primary_run_id
        || independent.cohort != cohort.as_str()
        || independent.source_inventory_sha256 != registry.source_inventory_sha256
        || independent.registry_digest != registry.content_sha256
        || independent.registry_approval_state != "REVIEWED"
        || &independent.physical != physical
    {
        bail!("finalizer registry, primary-run, locus, or physical/content evidence differs");
    }
    if physical.locus_rows != expected_loci.len() as u64 || physical.bin_rows == 0 {
        bail!("finalizer physical locus/bin counts are incomplete");
    }
    match cohort {
        Cohort::HgsvcHprc => {
            if independent
                .metadata_run_id
                .as_deref()
                .is_none_or(str::is_empty)
                || independent
                    .metadata_manifest_sha256
                    .as_deref()
                    .is_none_or(|value| require_sha256(value, "metadata manifest").is_err())
                || independent
                    .metadata_receipt_sha256
                    .as_deref()
                    .is_none_or(|value| require_sha256(value, "accepted metadata receipt").is_err())
                || physical.genotype_pair_rows == 0
                || physical.genotype_margin_rows == 0
            {
                bail!("HGSVC/HPRC finalization lacks metadata or complete genotype evidence");
            }
        }
        Cohort::Aou => {
            if independent.metadata_run_id.is_some()
                || independent.metadata_receipt_sha256.is_some()
                || independent.metadata_manifest_sha256.is_some()
                || physical.genotype_pair_rows != 0
                || physical.genotype_margin_rows != 0
            {
                bail!("AoU finalization must not contain metadata-bound genotype pairing");
            }
        }
    }
    Ok(())
}

/// Append a complete new run-ledger revision. Existing revisions and product
/// rows are never updated or deleted.
pub fn append_product_run_transition(
    target: &ClickHouseTarget,
    product_run_id: &str,
    to: PrimaryMotifRunState,
    registry: &PrimaryRepeatRegistry,
    operator_identity: &str,
    message: &str,
) -> anyhow::Result<()> {
    if operator_identity.trim().is_empty()
        || message.trim().is_empty()
        || [operator_identity, message].iter().any(|value| {
            value
                .chars()
                .any(|character| matches!(character, '\t' | '\n' | '\r'))
        })
    {
        bail!("transition requires nonempty single-line operator identity and message");
    }
    let body = target.query_text(
        "SELECT * FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT JSONEachRow",
        &[("product_run_id", product_run_id)],
    )?;
    let mut row: Value = exactly_one_json(&body, "latest primary-motif product run")?;
    let object = row
        .as_object_mut()
        .context("product run ledger row is not an object")?;
    let from: PrimaryMotifRunState = serde_json::from_value(
        object
            .get("state")
            .cloned()
            .context("product run lacks state")?,
    )?;
    let bound = object
        .get("registry_digest")
        .and_then(Value::as_str)
        .context("product run lacks registry digest")?;
    validate_run_state_transition(from, to, registry, bound)?;
    let old_revision = object
        .get("revision")
        .and_then(Value::as_u64)
        .context("product run revision is not UInt64")?;
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let now_ms = u64::try_from(elapsed.as_millis())?;
    let now_ns = u64::try_from(elapsed.as_nanos())?;
    let revision = old_revision
        .checked_add(1)
        .context("product run revision overflow")?
        .max(now_ns);
    object.insert("revision".into(), Value::from(revision));
    object.insert("state".into(), serde_json::to_value(to)?);
    object.insert("updated_at".into(), Value::from(now_ms as f64 / 1000.0));
    object.insert("operator_identity".into(), Value::from(operator_identity));
    object.insert("message".into(), Value::from(message));
    target.insert_json_each_row("lr_y1_primary_motif_runs", &[row])?;

    let revision_text = revision.to_string();
    let duplicates = target.query_text(
        "SELECT count() FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} AND revision = {revision:UInt64} FORMAT TabSeparated",
        &[("product_run_id", product_run_id), ("revision", &revision_text)],
    )?;
    if duplicates.trim() != "1" {
        bail!("product run transition revision collided; manual ledger inspection is required");
    }
    let latest = target.query_text(
        "SELECT revision, state, operator_identity, message FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT TabSeparated",
        &[("product_run_id", product_run_id)],
    )?;
    let state = serde_json::to_value(to)?;
    let expected = format!(
        "{revision}\t{}\t{operator_identity}\t{message}",
        state.as_str().unwrap()
    );
    if latest.trim_end() != expected {
        bail!("a concurrent product transition superseded the appended revision");
    }
    Ok(())
}

pub fn independent_receipt_digest(receipt: &IndependentProductReceipt) -> anyhow::Result<String> {
    let mut value = serde_json::to_value(receipt)?;
    value
        .as_object_mut()
        .context("receipt is not an object")?
        .remove("receipt_sha256");
    let bytes = serde_json::to_vec(&value)?;
    let mut hasher = Sha256::new();
    hasher.update(b"Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1\0");
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn require_sha256(value: &str, label: &str) -> anyhow::Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} is not lowercase SHA-256");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::y1::primary_motif::RegistryApprovalState;

    #[test]
    fn checked_manifest_resolves_exact_generation_bound_vcf_and_tbi() {
        let registry = PrimaryRepeatRegistry::from_slice(include_bytes!(
            "../../sources/y1/primary-repeat-registry.json"
        ))
        .unwrap();
        let source = resolve_product_source_manifest(
            Path::new("sources/y1/primary-source-chr4.json"),
            Cohort::HgsvcHprc,
            "chr4",
            &registry.source_inventory_sha256,
        )
        .unwrap();
        assert!(source
            .source_uri
            .ends_with("gnomAD_LR_Y1.hgsvc_hprc.chr4.vcf.gz"));
        assert_eq!(source.source_generation, "1785248047778434");
        assert_eq!(source.source_index_generation, "1785248051777254");
        source.vcf_object().request().unwrap();
        source.index_object().request().unwrap();
    }

    fn reviewed_registry() -> PrimaryRepeatRegistry {
        let mut value: Value = serde_json::from_slice(include_bytes!(
            "../../sources/y1/primary-repeat-registry.json"
        ))
        .unwrap();
        value["approval_state"] = Value::from("REVIEWED");
        for entry in value["entries"].as_array_mut().unwrap() {
            entry["approval_state"] = Value::from("REVIEWED");
            entry["reviewer"] = Value::from("synthetic-test-reviewer");
            entry["approval_receipt"] = Value::from("synthetic-fixture-only");
            entry["catalog_digest"] = Value::from("a".repeat(64));
        }
        value.as_object_mut().unwrap().remove("content_sha256");
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&value).unwrap()));
        value["content_sha256"] = Value::from(digest);
        PrimaryRepeatRegistry::from_slice(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    #[test]
    fn finalizer_accepts_complete_reviewed_synthetic_evidence_and_rejects_corruption() {
        let registry = reviewed_registry();
        let physical = ProductPhysicalSnapshot {
            product_run_id: "product".into(),
            locus_rows: 2,
            bin_rows: 9,
            genotype_pair_rows: 12,
            genotype_margin_rows: 7,
            locus_content_sha256: "1".repeat(64),
            bin_content_sha256: "2".repeat(64),
            genotype_pair_content_sha256: "3".repeat(64),
            genotype_margin_content_sha256: "4".repeat(64),
        };
        let mut receipt = IndependentProductReceipt {
            contract: "Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1".into(),
            product_run_id: "product".into(),
            primary_run_id: "primary".into(),
            release: "y1".into(),
            cohort: "hgsvc_hprc".into(),
            reference_genome: "GRCh38".into(),
            chrom: "chr4".into(),
            source_inventory_sha256: registry.source_inventory_sha256.clone(),
            registry_digest: registry.content_sha256.clone(),
            registry_approval_state: "REVIEWED".into(),
            registered_locus_ids: registry
                .entries
                .iter()
                .filter(|entry| entry.chrom == "chr4")
                .map(|entry| entry.registry_entry_id.clone())
                .collect(),
            complete_strata: true,
            no_truncation: true,
            exact_ac_an_and_genotype_margins: true,
            metadata_run_id: Some("metadata".into()),
            metadata_receipt_sha256: Some("5".repeat(64)),
            metadata_manifest_sha256: Some("6".repeat(64)),
            physical: physical.clone(),
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = independent_receipt_digest(&receipt).unwrap();
        validate_finalizer_gates(&registry, Cohort::HgsvcHprc, "primary", &physical, &receipt)
            .unwrap();

        let mut truncated = receipt.clone();
        truncated.no_truncation = false;
        truncated.receipt_sha256 = independent_receipt_digest(&truncated).unwrap();
        assert!(validate_finalizer_gates(
            &registry,
            Cohort::HgsvcHprc,
            "primary",
            &physical,
            &truncated,
        )
        .is_err());

        let mut changed = physical.clone();
        changed.bin_content_sha256 = "9".repeat(64);
        assert!(validate_finalizer_gates(
            &registry,
            Cohort::HgsvcHprc,
            "primary",
            &changed,
            &receipt,
        )
        .is_err());
    }

    #[test]
    fn finalizer_is_fail_closed_for_candidate_missing_locus_hash_and_aou_pairing() {
        let candidate = PrimaryRepeatRegistry::from_slice(include_bytes!(
            "../../sources/y1/primary-repeat-registry.json"
        ))
        .unwrap();
        let physical = ProductPhysicalSnapshot {
            product_run_id: "product".into(),
            locus_rows: 3,
            bin_rows: 4,
            genotype_pair_rows: 0,
            genotype_margin_rows: 0,
            locus_content_sha256: "1".repeat(64),
            bin_content_sha256: "2".repeat(64),
            genotype_pair_content_sha256: "3".repeat(64),
            genotype_margin_content_sha256: "4".repeat(64),
        };
        let mut receipt = IndependentProductReceipt {
            contract: "Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1".into(),
            product_run_id: "product".into(),
            primary_run_id: "primary".into(),
            release: "y1".into(),
            cohort: "aou".into(),
            reference_genome: "GRCh38".into(),
            chrom: "chr4".into(),
            source_inventory_sha256: candidate.source_inventory_sha256.clone(),
            registry_digest: candidate.content_sha256.clone(),
            registry_approval_state: "REVIEWED".into(),
            registered_locus_ids: candidate
                .entries
                .iter()
                .filter(|entry| entry.chrom == "chr4")
                .map(|e| e.registry_entry_id.clone())
                .collect(),
            complete_strata: true,
            no_truncation: true,
            exact_ac_an_and_genotype_margins: true,
            metadata_run_id: None,
            metadata_receipt_sha256: None,
            metadata_manifest_sha256: None,
            physical: physical.clone(),
            receipt_sha256: String::new(),
        };
        receipt.receipt_sha256 = independent_receipt_digest(&receipt).unwrap();
        assert_eq!(
            candidate.approval_state,
            RegistryApprovalState::CandidatePendingScience
        );
        assert!(
            validate_finalizer_gates(&candidate, Cohort::Aou, "primary", &physical, &receipt)
                .is_err()
        );
        let mut paired = physical.clone();
        paired.genotype_pair_rows = 1;
        let mut reviewed = candidate.clone();
        reviewed.approval_state = RegistryApprovalState::Reviewed;
        // Deliberately not forgeable into a valid reviewed registry: changing approval
        // state invalidates the content digest before any later gate is reached.
        assert!(
            validate_finalizer_gates(&reviewed, Cohort::Aou, "primary", &paired, &receipt).is_err()
        );
    }

    #[test]
    fn ddl_parser_attests_every_declared_product_column() {
        for (_, ddl) in PRODUCT_DDLS {
            let columns = ddl_columns(ddl).unwrap();
            assert!(columns.len() >= 18);
            assert_eq!(columns[0].0, "product_run_id");
            assert!(!columns
                .iter()
                .any(|(name, _)| matches!(name.as_str(), "sample_id" | "raw_gt" | "person_id")));
            let shape = ddl_shape(ddl).unwrap();
            assert!(shape.engine_full.contains("MergeTree"));
            assert!(shape.sorting_key.contains("product_run_id"));
        }
    }
}
