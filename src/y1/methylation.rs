//! Typed, fail-closed Y1 methylation foundation.
//!
//! This module does not adapt the legacy methylation command or pool action. A
//! task names a checked manifest entry, never a path or sample ID. Preparation
//! resolves generation-bound read identities from the repository v2 manifest
//! and is restricted to a fenced scratch [`ClickHouseTarget`]. General loading
//! and finalization remain blocked. The sole exception is one exact, code-pinned,
//! single-owner smoke into a fresh disposable schema-v5 database; it cannot
//! publish, activate, join, retry, or select a caller-provided source.

use super::contig::grch38_contig_length;
use super::{attest_fresh_y1_schema, ClickHouseTarget, TargetKind, Y1_SCHEMA_VERSION};
use crate::loader::immutable_gcs::{HttpGcsBackend, ImmutableGcsObject};
use crate::loader::strict_bed_reader::{StrictBedLines, StrictBedStream, ValidatedBedRecord};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

const CANONICAL_METHYLATION_MANIFEST_PATH: &str =
    "sources/y1/methylation-phased-source-manifest.json";
const CANONICAL_METHYLATION_MANIFEST_ID: &str = "hgsvc-hprc-y1-phased-methylation-v2";
const CANONICAL_METHYLATION_MANIFEST_SHA256: &str =
    "f585cbc2b806dcb52944af2ecabe634338a41323f89e3938336235c7729e8743";
const CANONICAL_METHYLATION_AUTHORIZATION_STATUS: &str = "blocked_pending_atomic_attempt_ledger";
pub const PHASED_METHYLATION_SMOKE_DATABASE_PREFIX: &str =
    "gnomad_lr_y1_scratch_phased_methylation_smoke_v5_";
pub const PHASED_METHYLATION_EVALUATION_DATABASE: &str =
    "gnomad_lr_y1_scratch_phased_methylation_evaluation_v5_hg00097_chr22_47040000_47050000_v1";
const PHASED_METHYLATION_SMOKE_AUTHORIZATION_ID: &str =
    "hg00097-hap1-chr22-20000000-20010000-single-owner-v1";
const PHASED_METHYLATION_EVALUATION_AUTHORIZATION_ID: &str =
    "hg00097-source-hap1-hap2-chr22-47040000-47050000-retained-evaluation-v1";
const PHASED_METHYLATION_SMOKE_ENTRY_ID: &str = "hgsvc_hprc:HG00097";
const PHASED_METHYLATION_SMOKE_SAMPLE_ID: &str = "HG00097";
const PHASED_METHYLATION_SMOKE_CHROM: &str = "chr22";
const PHASED_METHYLATION_SMOKE_START: u32 = 20_000_000;
const PHASED_METHYLATION_SMOKE_STOP: u32 = 20_010_000;
const PHASED_METHYLATION_EVALUATION_START: u32 = 47_040_000;
const PHASED_METHYLATION_EVALUATION_STOP: u32 = 47_050_000;
const PHASED_METHYLATION_SMOKE_TABLE: &str = "lr_y1_methylation_phased_staging";
const PHASED_METHYLATION_SMOKE_PRINCIPAL: &str = "gnomad_lr_y1_worker";
const SMOKE_KEY_HASH_DOMAIN: &[u8] = b"phased-methylation-smoke-key-v1";
const SMOKE_CONTENT_HASH_DOMAIN: &[u8] = b"phased-methylation-smoke-content-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MethylationDataLayer {
    SampleTotal,
    SourcePhased,
}

impl MethylationDataLayer {
    pub fn modality(self) -> &'static str {
        match self {
            Self::SampleTotal => "per_sample_methylation_total",
            Self::SourcePhased => "per_haplotype_methylation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceHaplotype {
    Hap1,
    Hap2,
}

impl SourceHaplotype {
    pub fn value(self) -> u8 {
        match self {
            Self::Hap1 => 1,
            Self::Hap2 => 2,
        }
    }

    fn object_slots(self) -> (&'static str, &'static str) {
        match self {
            Self::Hap1 => ("hap1_bed", "hap1_bed_index"),
            Self::Hap2 => ("hap2_bed", "hap2_bed_index"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum MethylationSourceType {
    Total,
    #[serde(rename = "hap1")]
    Hap1,
    #[serde(rename = "hap2")]
    Hap2,
}

impl MethylationSourceType {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "Total" => Ok(Self::Total),
            "hap1" => Ok(Self::Hap1),
            "hap2" => Ok(Self::Hap2),
            _ => bail!("methylation type must be exactly Total, hap1, or hap2"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MethylationRecord {
    pub chrom: String,
    pub source_start0: u32,
    pub source_end0: u32,
    pub position: u32,
    pub methylation: f32,
    pub source_type: MethylationSourceType,
    pub coverage: u32,
    pub estimated_modified_count: u32,
    pub estimated_unmodified_count: u32,
    pub discretized_methylation: f32,
}

/// Parse one exact nine-column pb-cpg-tools row without defaults or coercion.
///
/// This source-shape parser deliberately does not apply query chromosome/type
/// membership. The strict indexed reader calls it before filtering valid chunk
/// spill; selected rows then apply the expected-object checks below.
pub(crate) fn parse_methylation_source_record(line: &str) -> anyhow::Result<MethylationRecord> {
    let fields: Vec<&str> = line.split('\t').collect();
    if fields.len() != 9 {
        bail!(
            "methylation row must contain exactly nine tab-delimited columns, got {}",
            fields.len()
        );
    }
    let source_type = MethylationSourceType::parse(fields[4])?;

    let parse_u32 = |column: &str, value: &str| -> anyhow::Result<u32> {
        value
            .parse::<u32>()
            .with_context(|| format!("methylation {column} is not a UInt32: {value:?}"))
    };
    let parse_score = |column: &str, value: &str| -> anyhow::Result<f32> {
        let score = value
            .parse::<f32>()
            .with_context(|| format!("methylation {column} is not a Float32: {value:?}"))?;
        if !score.is_finite() || !(0.0..=100.0).contains(&score) {
            bail!("methylation {column} must be finite and in [0,100]");
        }
        Ok(score)
    };

    let source_start0 = parse_u32("start0", fields[1])?;
    let source_end0 = parse_u32("end0", fields[2])?;
    let expected_end0 = source_start0
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("methylation start0 overflows the one-base interval"))?;
    if source_end0 != expected_end0 {
        bail!("methylation source interval must satisfy end0=start0+1");
    }
    let position = expected_end0;
    let methylation = parse_score("mod_score", fields[3])?;
    let coverage = parse_u32("coverage", fields[5])?;
    let estimated_modified_count = parse_u32("estimated_modified_count", fields[6])?;
    let estimated_unmodified_count = parse_u32("estimated_unmodified_count", fields[7])?;
    let count_sum = estimated_modified_count
        .checked_add(estimated_unmodified_count)
        .ok_or_else(|| anyhow::anyhow!("methylation source counts overflow UInt32"))?;
    if count_sum != coverage {
        bail!("estimated modified + unmodified counts must equal coverage");
    }
    let discretized_methylation = parse_score("discretized_mod_score", fields[8])?;

    Ok(MethylationRecord {
        chrom: fields[0].to_string(),
        source_start0,
        source_end0,
        position,
        methylation,
        source_type,
        coverage,
        estimated_modified_count,
        estimated_unmodified_count,
        discretized_methylation,
    })
}

/// Validate source shape and source-object type without applying query membership.
///
/// Strict indexed readers must use this before their chromosome/range spill filter.
/// Only retained rows may then call [`parse_methylation_record`] to enforce query
/// membership. Keeping this callback shared prevents loader-specific validation order.
pub(crate) fn validate_methylation_source_record(
    line: &str,
    expected_type: MethylationSourceType,
) -> anyhow::Result<MethylationRecord> {
    let record = parse_methylation_source_record(line)?;
    if record.source_type != expected_type {
        bail!(
            "methylation source type mismatch: expected {:?}, got {:?}",
            expected_type,
            record.source_type
        );
    }
    Ok(record)
}

pub(crate) fn methylation_source_coordinates(
    line: &str,
    expected_type: MethylationSourceType,
) -> anyhow::Result<ValidatedBedRecord> {
    let record = validate_methylation_source_record(line, expected_type)?;
    Ok(ValidatedBedRecord {
        chrom: record.chrom,
        start0: record.source_start0,
        end0: record.source_end0,
    })
}

pub fn parse_methylation_record(
    line: &str,
    expected_chrom: &str,
    expected_type: MethylationSourceType,
) -> anyhow::Result<MethylationRecord> {
    let record = validate_methylation_source_record(line, expected_type)?;
    if record.chrom != expected_chrom {
        bail!(
            "methylation chromosome mismatch: expected {expected_chrom}, got {}",
            record.chrom
        );
    }
    Ok(record)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Y1MethylationTaskSpec {
    pub coordinator_task_id: String,
    pub label: String,
    pub ancillary_run_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_id: String,
    pub release: String,
    pub cohort: String,
    pub reference_genome: String,
    pub source_manifest_id: String,
    pub source_manifest_hash: String,
    pub manifest_entry_id: String,
    pub data_layer: MethylationDataLayer,
    pub source_haplotype: Option<SourceHaplotype>,
    pub chrom: String,
    pub start: u32,
    pub stop: u32,
}

impl Y1MethylationTaskSpec {
    pub fn validate(&self, descriptor_id: &str) -> anyhow::Result<()> {
        if self.coordinator_task_id != descriptor_id {
            bail!("descriptor ID must exactly match coordinator_task_id");
        }
        for (name, value) in [
            ("label", &self.label),
            ("ancillary_run_id", &self.ancillary_run_id),
            ("task_id", &self.task_id),
            ("attempt_id", &self.attempt_id),
            ("lease_id", &self.lease_id),
            ("source_manifest_id", &self.source_manifest_id),
            ("source_manifest_hash", &self.source_manifest_hash),
            ("manifest_entry_id", &self.manifest_entry_id),
        ] {
            if value.trim().is_empty() || value.chars().any(char::is_control) {
                bail!("{name} must be a nonempty control-free value");
            }
        }
        if self.release != "y1" || self.cohort != "hgsvc_hprc" || self.reference_genome != "GRCh38"
        {
            bail!("Y1 methylation tasks are restricted to y1/hgsvc_hprc/GRCh38");
        }
        if self.source_manifest_hash.len() != 64
            || !self
                .source_manifest_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("source_manifest_hash must be a 64-character hexadecimal SHA-256");
        }
        match (self.data_layer, self.source_haplotype) {
            (MethylationDataLayer::SampleTotal, None) => {}
            (MethylationDataLayer::SourcePhased, Some(_)) => {}
            (MethylationDataLayer::SampleTotal, Some(_)) => {
                bail!("sample_total tasks must not set source_haplotype")
            }
            (MethylationDataLayer::SourcePhased, None) => {
                bail!("source_phased tasks require source_haplotype")
            }
        }
        let contig_length = grch38_contig_length(&self.chrom)?;
        if self.start == 0 || self.start > self.stop || self.stop > contig_length {
            bail!(
                "task interval must be nonempty, one-based inclusive, and within the GRCh38 contig"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ImmutableObjectIdentity {
    uri: String,
    generation: String,
    byte_size: u64,
    checksum_algorithm: String,
    checksum: String,
    created_at: String,
    updated_at: String,
    immutable_read_uri: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreparedMethylationAttempt {
    ancillary_run_id: String,
    task_id: String,
    attempt_id: String,
    lease_id: String,
    source_manifest_hash: String,
    manifest_entry_id: String,
    sample_id: String,
    data_layer: MethylationDataLayer,
    source_haplotype: Option<SourceHaplotype>,
    expected_type: MethylationSourceType,
    chrom: String,
    start: u32,
    stop: u32,
    source: ImmutableObjectIdentity,
    index: ImmutableObjectIdentity,
}

fn verified_canonical_manifest(bytes: &[u8]) -> anyhow::Result<(Value, String)> {
    let mut manifest: Value =
        serde_json::from_slice(bytes).context("embedded methylation manifest is invalid JSON")?;
    let recorded_hash = manifest
        .get("content_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("methylation manifest lacks content_sha256"))?
        .to_string();
    manifest
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("methylation manifest must be a JSON object"))?
        .remove("content_sha256");
    let actual_hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&manifest)?));
    if recorded_hash != actual_hash || recorded_hash != CANONICAL_METHYLATION_MANIFEST_SHA256 {
        bail!(
            "methylation manifest canonical hash does not match the pinned repository trust root"
        );
    }
    if manifest.get("schema_version").and_then(Value::as_u64) != Some(2)
        || manifest.get("manifest_id").and_then(Value::as_str)
            != Some(CANONICAL_METHYLATION_MANIFEST_ID)
    {
        bail!("repository methylation trust root is not the pinned v2 manifest");
    }
    Ok((manifest, recorded_hash))
}

/// Resolve a typed general-load task against the checked immutable manifest.
/// This performs no read and no ClickHouse mutation. The general path remains
/// blocked on the separate atomic attempt-ledger/finalizer milestone.
pub fn prepare_methylation_attempt(
    target: &ClickHouseTarget,
    task: &Y1MethylationTaskSpec,
    descriptor_id: &str,
    manifest_path: &Path,
) -> anyhow::Result<PreparedMethylationAttempt> {
    task.validate(descriptor_id)?;
    if target.kind() != TargetKind::Scratch {
        bail!("Y1 methylation interval attempts may target only a fenced scratch database");
    }

    // This experimental planning API accepts no caller-selected trust root. The
    // exact blocked repository manifest is embedded in the binary; the path is
    // retained only as an explicit canonical-contract assertion.
    if manifest_path != Path::new(CANONICAL_METHYLATION_MANIFEST_PATH) {
        bail!(
            "methylation source manifest override is forbidden; expected exact repository path {CANONICAL_METHYLATION_MANIFEST_PATH}"
        );
    }
    let (manifest, recorded_hash) = verified_canonical_manifest(include_bytes!(
        "../../sources/y1/methylation-phased-source-manifest.json"
    ))?;
    if task.source_manifest_hash != CANONICAL_METHYLATION_MANIFEST_SHA256
        || task.source_manifest_id != CANONICAL_METHYLATION_MANIFEST_ID
    {
        bail!("typed task does not resolve to the pinned repository v2 methylation manifest");
    }

    let readiness = manifest
        .get("load_readiness")
        .ok_or_else(|| anyhow::anyhow!("methylation manifest lacks load_readiness"))?;
    if readiness.get("status").and_then(Value::as_str)
        != Some(CANONICAL_METHYLATION_AUTHORIZATION_STATUS)
        || readiness.get("load_authorized").and_then(Value::as_bool) != Some(true)
    {
        let blockers = readiness
            .get("blockers")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .unwrap_or_else(|| "unspecified manifest readiness blocker".to_string());
        bail!("methylation v2 source is not load-ready: {blockers}");
    }
    // Shape validation of repository metadata is not runtime object validation;
    // open_prepared_methylation_records performs exact-generation GCS checks.
    // Overall loading remains independently blocked on attempt/lease ownership.
    if !runtime_atomic_methylation_ledger_enabled() {
        bail!("Y1 methylation loading is disabled until the separate atomic attempt/lease ledger milestone is implemented");
    }

    let entry = manifest
        .get("samples")
        .and_then(Value::as_array)
        .and_then(|entries| {
            entries.iter().find(|entry| {
                entry.get("entry_id").and_then(Value::as_str) == Some(&task.manifest_entry_id)
            })
        })
        .ok_or_else(|| anyhow::anyhow!("manifest_entry_id is absent from the v2 manifest"))?;
    if entry.get("inventory_status").and_then(Value::as_str) != Some("source_present") {
        bail!("typed tasks may not load no-output or source-marked-skip roster entries");
    }
    let sample_id = entry
        .get("sample_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("manifest entry lacks sample_id"))?;
    let (source_slot, index_slot, expected_type) = match (task.data_layer, task.source_haplotype) {
        (MethylationDataLayer::SampleTotal, None) => (
            "combined_bed",
            "combined_bed_index",
            MethylationSourceType::Total,
        ),
        (MethylationDataLayer::SourcePhased, Some(source_haplotype)) => {
            let (source, index) = source_haplotype.object_slots();
            let expected_type = match source_haplotype {
                SourceHaplotype::Hap1 => MethylationSourceType::Hap1,
                SourceHaplotype::Hap2 => MethylationSourceType::Hap2,
            };
            (source, index, expected_type)
        }
        _ => unreachable!("task layer/haplotype shape was validated"),
    };
    let objects = entry
        .get("objects")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("manifest entry lacks objects"))?;
    let source = resolve_immutable_object(objects.get(source_slot), source_slot)?;
    let index = resolve_immutable_object(objects.get(index_slot), index_slot)?;

    Ok(PreparedMethylationAttempt {
        ancillary_run_id: task.ancillary_run_id.clone(),
        task_id: task.task_id.clone(),
        attempt_id: task.attempt_id.clone(),
        lease_id: task.lease_id.clone(),
        source_manifest_hash: recorded_hash,
        manifest_entry_id: task.manifest_entry_id.clone(),
        sample_id: sample_id.to_string(),
        data_layer: task.data_layer,
        source_haplotype: task.source_haplotype,
        expected_type,
        chrom: task.chrom.clone(),
        start: task.start,
        stop: task.stop,
        source,
        index,
    })
}

fn runtime_atomic_methylation_ledger_enabled() -> bool {
    false
}

fn resolve_immutable_object(
    value: Option<&Value>,
    slot: &str,
) -> anyhow::Result<ImmutableObjectIdentity> {
    resolve_immutable_object_with_authorization(value, slot, true)
}

fn resolve_smoke_immutable_object(
    value: Option<&Value>,
    slot: &str,
) -> anyhow::Result<ImmutableObjectIdentity> {
    // The general load flag must remain false. This exact code-level exception
    // authorizes only the one bounded smoke contract named by constants above.
    resolve_immutable_object_with_authorization(value, slot, false)
}

fn resolve_immutable_object_with_authorization(
    value: Option<&Value>,
    slot: &str,
    expected_load_authorized: bool,
) -> anyhow::Result<ImmutableObjectIdentity> {
    let descriptor =
        value.ok_or_else(|| anyhow::anyhow!("manifest entry lacks object slot {slot}"))?;
    if descriptor.get("load_authorized").and_then(Value::as_bool) != Some(expected_load_authorized)
    {
        bail!("manifest object slot {slot} has an unexpected general load authorization state");
    }
    let identity = descriptor
        .get("immutable_identity")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("manifest object slot {slot} lacks immutable identity"))?;
    let string = |name: &str| -> anyhow::Result<String> {
        identity
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("manifest object slot {slot} lacks {name}"))
    };
    let checksum = identity
        .get("checksum")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("manifest object slot {slot} lacks checksum"))?;
    let checksum_string = |name: &str| -> anyhow::Result<String> {
        checksum
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && *value != "none")
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("manifest object slot {slot} lacks checksum {name}"))
    };
    let uri = string("uri")?;
    let immutable_read_uri = string("immutable_read_uri")?;
    if immutable_read_uri == uri {
        bail!("manifest object slot {slot} does not bind reads to immutable identity");
    }
    let generation = string("generation")?;
    if !generation.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("manifest object slot {slot} has a nondecimal generation");
    }
    let byte_size = identity
        .get("byte_size")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow::anyhow!("manifest object slot {slot} lacks positive byte_size"))?;
    Ok(ImmutableObjectIdentity {
        uri,
        generation,
        byte_size,
        checksum_algorithm: checksum_string("algorithm")?,
        checksum: checksum_string("value")?,
        created_at: string("created_at")?,
        updated_at: string("updated_at")?,
        immutable_read_uri,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct PreparedPhasedMethylationSmoke {
    authorization_id: String,
    source_manifest_id: String,
    source_manifest_hash: String,
    manifest_entry_id: String,
    sample_id: String,
    data_layer: MethylationDataLayer,
    source_haplotype: SourceHaplotype,
    source_version: String,
    chrom: String,
    start: u32,
    stop: u32,
    source_object_slot: String,
    index_object_slot: String,
    source: ImmutableObjectIdentity,
    index: ImmutableObjectIdentity,
}

#[derive(Debug, Clone, Serialize)]
struct PhasedMethylationSmokeRow {
    ancillary_run_id: String,
    attempt_id: String,
    release: String,
    cohort: String,
    reference_genome: String,
    modality: String,
    source_version: String,
    chrom: String,
    source_start0: u32,
    source_end0: u32,
    position: u32,
    sample_id: String,
    source_haplotype: u8,
    methylation: f32,
    coverage: u32,
    estimated_modified_count: u32,
    estimated_unmodified_count: u32,
    discretized_methylation: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SmokeReadback {
    row_count: u64,
    key_sha256: String,
    content_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PhasedMethylationSmokeReceipt {
    schema_version: u16,
    capability: &'static str,
    status: &'static str,
    database: String,
    authenticated_principal: String,
    backend_revision: &'static str,
    worker_build_identity: &'static str,
    source: PreparedPhasedMethylationSmoke,
    ancillary_run_id: String,
    attempt_id: String,
    table_written: &'static str,
    row_count: u64,
    reject_count: u64,
    key_sha256: String,
    content_sha256: String,
    synchronous_inserts: bool,
    fresh_exact_schema_v5_attested_before_insert: bool,
    serving_state_written: bool,
    summaries_written: bool,
    availability_written: bool,
    joined_tables_written: bool,
    active_pointers_written: bool,
}

#[derive(Debug, Clone, Serialize)]
struct PhasedMethylationEvaluationVerification {
    source_hap1: SmokeReadback,
    source_hap2: SmokeReadback,
    combined: SmokeReadback,
}

#[derive(Debug, Clone, Serialize)]
pub struct PhasedMethylationEvaluationReceipt {
    schema_version: u16,
    capability: &'static str,
    status: &'static str,
    database: &'static str,
    authenticated_principal: String,
    backend_revision: &'static str,
    worker_build_identity: &'static str,
    sources: Vec<PreparedPhasedMethylationSmoke>,
    ancillary_run_id: String,
    attempt_id: String,
    table_written: &'static str,
    verification: PhasedMethylationEvaluationVerification,
    reject_count: u64,
    synchronous_inserts: bool,
    both_sources_fully_parsed_before_insert: bool,
    fresh_exact_schema_v5_attested_before_insert: bool,
    retained_for_evaluation: bool,
    joinable_to_vcf: bool,
    orientation_status: &'static str,
    serving_state_written: bool,
    summaries_written: bool,
    availability_written: bool,
    joined_tables_written: bool,
    active_pointers_written: bool,
}

fn validate_evaluation_database_name(database: &str) -> anyhow::Result<()> {
    if database != PHASED_METHYLATION_EVALUATION_DATABASE {
        bail!("phased-methylation evaluation requires the exact fixed evaluation database");
    }
    Ok(())
}

fn validate_smoke_database_name(database: &str) -> anyhow::Result<()> {
    let suffix = database
        .strip_prefix(PHASED_METHYLATION_SMOKE_DATABASE_PREFIX)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "phased-methylation smoke database must start with {PHASED_METHYLATION_SMOKE_DATABASE_PREFIX}"
            )
        })?;
    if suffix.len() < 12
        || suffix.len() > 80
        || !suffix
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_lowercase() || byte.is_ascii_digit())
        || suffix.starts_with('_')
        || suffix.ends_with('_')
    {
        bail!("phased-methylation smoke database requires a 12-80 character lowercase alphanumeric/underscore unique suffix");
    }
    Ok(())
}

fn prepare_fixed_phased_methylation_source(
    target: &ClickHouseTarget,
    authorization_id: &str,
    source_haplotype: SourceHaplotype,
    start: u32,
    stop: u32,
) -> anyhow::Result<PreparedPhasedMethylationSmoke> {
    if target.kind() != TargetKind::Scratch {
        bail!("fixed phased-methylation load is restricted to scratch targets");
    }
    let (manifest, source_manifest_hash) = verified_canonical_manifest(include_bytes!(
        "../../sources/y1/methylation-phased-source-manifest.json"
    ))?;
    let readiness = manifest
        .get("load_readiness")
        .ok_or_else(|| anyhow::anyhow!("methylation manifest lacks load_readiness"))?;
    if readiness.get("status").and_then(Value::as_str)
        != Some(CANONICAL_METHYLATION_AUTHORIZATION_STATUS)
        || readiness.get("load_authorized").and_then(Value::as_bool) != Some(false)
    {
        bail!("fixed single-owner load requires the general phased-methylation load path to remain blocked");
    }
    for (field, expected) in [
        ("release", "y1"),
        ("cohort", "hgsvc_hprc"),
        ("reference_genome", "GRCh38"),
    ] {
        if manifest.get(field).and_then(Value::as_str) != Some(expected) {
            bail!("repository methylation manifest substituted {field}");
        }
    }
    let source_version = manifest
        .get("source_version")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("repository methylation manifest lacks source_version"))?;
    let entry = manifest
        .get("samples")
        .and_then(Value::as_array)
        .and_then(|entries| {
            entries.iter().find(|entry| {
                entry.get("entry_id").and_then(Value::as_str)
                    == Some(PHASED_METHYLATION_SMOKE_ENTRY_ID)
            })
        })
        .ok_or_else(|| {
            anyhow::anyhow!("fixed HG00097 entry is absent from the repository manifest")
        })?;
    if entry.get("sample_id").and_then(Value::as_str) != Some(PHASED_METHYLATION_SMOKE_SAMPLE_ID)
        || entry.get("inventory_status").and_then(Value::as_str) != Some("source_present")
    {
        bail!("fixed phased-methylation entry substituted sample or inventory identity");
    }
    let objects = entry
        .get("objects")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("fixed phased-methylation entry lacks objects"))?;
    let (source_slot, index_slot) = source_haplotype.object_slots();
    let source = resolve_smoke_immutable_object(objects.get(source_slot), source_slot)?;
    let index = resolve_smoke_immutable_object(objects.get(index_slot), index_slot)?;
    crate::loader::immutable_gcs::validate_source_index_pair(
        &source.as_gcs_object(),
        &index.as_gcs_object(),
    )?;

    Ok(PreparedPhasedMethylationSmoke {
        authorization_id: authorization_id.to_string(),
        source_manifest_id: CANONICAL_METHYLATION_MANIFEST_ID.to_string(),
        source_manifest_hash,
        manifest_entry_id: PHASED_METHYLATION_SMOKE_ENTRY_ID.to_string(),
        sample_id: PHASED_METHYLATION_SMOKE_SAMPLE_ID.to_string(),
        data_layer: MethylationDataLayer::SourcePhased,
        source_haplotype,
        source_version: source_version.to_string(),
        chrom: PHASED_METHYLATION_SMOKE_CHROM.to_string(),
        start,
        stop,
        source_object_slot: source_slot.to_string(),
        index_object_slot: index_slot.to_string(),
        source,
        index,
    })
}

fn prepare_phased_methylation_smoke(
    target: &ClickHouseTarget,
) -> anyhow::Result<PreparedPhasedMethylationSmoke> {
    validate_smoke_database_name(target.database())?;
    prepare_fixed_phased_methylation_source(
        target,
        PHASED_METHYLATION_SMOKE_AUTHORIZATION_ID,
        SourceHaplotype::Hap1,
        PHASED_METHYLATION_SMOKE_START,
        PHASED_METHYLATION_SMOKE_STOP,
    )
}

fn open_phased_methylation_smoke_records(
    prepared: &PreparedPhasedMethylationSmoke,
) -> anyhow::Result<MethylationRecordStream> {
    let expected_source_type = match prepared.source_haplotype {
        SourceHaplotype::Hap1 => MethylationSourceType::Hap1,
        SourceHaplotype::Hap2 => MethylationSourceType::Hap2,
    };
    open_methylation_record_stream(
        &prepared.source,
        &prepared.index,
        &prepared.chrom,
        prepared.start,
        prepared.stop,
        expected_source_type,
    )
}

fn smoke_rows(
    prepared: &PreparedPhasedMethylationSmoke,
    database: &str,
    records: Vec<MethylationRecord>,
) -> anyhow::Result<Vec<PhasedMethylationSmokeRow>> {
    fixed_phased_methylation_rows(prepared, database, "single-owner-smoke", records)
}

fn fixed_phased_methylation_rows(
    prepared: &PreparedPhasedMethylationSmoke,
    database: &str,
    run_kind: &str,
    records: Vec<MethylationRecord>,
) -> anyhow::Result<Vec<PhasedMethylationSmokeRow>> {
    if records.is_empty() {
        bail!("fixed phased-methylation interval returned zero records");
    }
    let expected_source_type = match prepared.source_haplotype {
        SourceHaplotype::Hap1 => MethylationSourceType::Hap1,
        SourceHaplotype::Hap2 => MethylationSourceType::Hap2,
    };
    for record in &records {
        if record.chrom != prepared.chrom
            || record.position < prepared.start
            || record.position > prepared.stop
            || record.source_type != expected_source_type
        {
            bail!("fixed phased-methylation record substituted its interval or source type");
        }
    }
    let ancillary_run_id = format!("{run_kind}:{database}");
    let attempt_id = "single-owner".to_string();
    let mut rows = records
        .into_iter()
        .map(|record| PhasedMethylationSmokeRow {
            ancillary_run_id: ancillary_run_id.clone(),
            attempt_id: attempt_id.clone(),
            release: "y1".to_string(),
            cohort: "hgsvc_hprc".to_string(),
            reference_genome: "GRCh38".to_string(),
            modality: MethylationDataLayer::SourcePhased.modality().to_string(),
            source_version: prepared.source_version.clone(),
            chrom: record.chrom,
            source_start0: record.source_start0,
            source_end0: record.source_end0,
            position: record.position,
            sample_id: prepared.sample_id.clone(),
            source_haplotype: prepared.source_haplotype.value(),
            methylation: record.methylation,
            coverage: record.coverage,
            estimated_modified_count: record.estimated_modified_count,
            estimated_unmodified_count: record.estimated_unmodified_count,
            discretized_methylation: record.discretized_methylation,
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| smoke_row_key(left).cmp(&smoke_row_key(right)));
    for pair in rows.windows(2) {
        if smoke_row_key(&pair[0]) == smoke_row_key(&pair[1]) {
            bail!("authorized phased-methylation smoke source contains a duplicate canonical key");
        }
    }
    Ok(rows)
}

fn smoke_row_key(row: &PhasedMethylationSmokeRow) -> (&str, &str, &str, u32, &str, u8, u32, u32) {
    (
        &row.ancillary_run_id,
        &row.attempt_id,
        &row.chrom,
        row.position,
        &row.sample_id,
        row.source_haplotype,
        row.source_start0,
        row.source_end0,
    )
}

trait SmokeInsertReadback {
    fn insert(&self, rows: &[PhasedMethylationSmokeRow]) -> anyhow::Result<()>;
    fn readback(&self) -> anyhow::Result<SmokeReadback>;
}

struct ClickHouseSmokeInsertReadback<'a>(&'a ClickHouseTarget);

impl SmokeInsertReadback for ClickHouseSmokeInsertReadback<'_> {
    fn insert(&self, rows: &[PhasedMethylationSmokeRow]) -> anyhow::Result<()> {
        self.0
            .insert_json_each_row(PHASED_METHYLATION_SMOKE_TABLE, rows)
    }

    fn readback(&self) -> anyhow::Result<SmokeReadback> {
        clickhouse_smoke_readback(self.0, "")
    }
}

fn clickhouse_smoke_readback(
    target: &ClickHouseTarget,
    static_where_clause: &str,
) -> anyhow::Result<SmokeReadback> {
    let count_query = format!(
        "SELECT count() FROM {PHASED_METHYLATION_SMOKE_TABLE} {static_where_clause} FORMAT TabSeparated"
    );
    let count = target.query_text(&count_query, &[])?;
    let row_count = parse_exact_count(&count)?;
    let order = "ancillary_run_id, attempt_id, chrom, position, sample_id, source_haplotype, source_start0, source_end0";
    let key_query = format!(
        "SELECT ancillary_run_id, attempt_id, chrom, position, sample_id, source_haplotype, source_start0, source_end0 FROM {PHASED_METHYLATION_SMOKE_TABLE} {static_where_clause} ORDER BY {order} FORMAT RowBinary"
    );
    let content_query = format!(
        "SELECT ancillary_run_id, attempt_id, release, cohort, reference_genome, modality, source_version, chrom, source_start0, source_end0, position, sample_id, source_haplotype, methylation, coverage, estimated_modified_count, estimated_unmodified_count, discretized_methylation FROM {PHASED_METHYLATION_SMOKE_TABLE} {static_where_clause} ORDER BY {order} FORMAT RowBinary"
    );
    Ok(SmokeReadback {
        row_count,
        key_sha256: target.query_sha256(&key_query, &[], SMOKE_KEY_HASH_DOMAIN)?,
        content_sha256: target.query_sha256(&content_query, &[], SMOKE_CONTENT_HASH_DOMAIN)?,
    })
}

fn parse_exact_count(value: &str) -> anyhow::Result<u64> {
    let trimmed = value.trim_end_matches('\n');
    if trimmed.is_empty()
        || trimmed.contains(['\t', '\n', '\r'])
        || !trimmed.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("phased-methylation smoke readback count is malformed");
    }
    Ok(trimmed.parse()?)
}

fn insert_and_verify_smoke<B: SmokeInsertReadback>(
    backend: &B,
    rows: &[PhasedMethylationSmokeRow],
) -> anyhow::Result<SmokeReadback> {
    let expected = expected_smoke_readback(rows)?;
    backend.insert(rows)?;
    let actual = backend.readback()?;
    if actual != expected {
        bail!(
            "phased-methylation smoke readback mismatch: expected {:?}, got {:?}",
            expected,
            actual
        );
    }
    Ok(actual)
}

fn expected_smoke_readback(rows: &[PhasedMethylationSmokeRow]) -> anyhow::Result<SmokeReadback> {
    let mut key_bytes = Vec::new();
    let mut content_bytes = Vec::new();
    for row in rows {
        encode_smoke_key(row, &mut key_bytes)?;
        encode_smoke_content(row, &mut content_bytes)?;
    }
    Ok(SmokeReadback {
        row_count: u64::try_from(rows.len())?,
        key_sha256: canonical_smoke_sha256(SMOKE_KEY_HASH_DOMAIN, &key_bytes),
        content_sha256: canonical_smoke_sha256(SMOKE_CONTENT_HASH_DOMAIN, &content_bytes),
    })
}

fn canonical_smoke_sha256(domain: &[u8], bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"gnomad-lr-y1-canonical-content-v1\0");
    digest.update(domain);
    digest.update([0]);
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn encode_smoke_key(row: &PhasedMethylationSmokeRow, output: &mut Vec<u8>) -> anyhow::Result<()> {
    encode_rowbinary_string(&row.ancillary_run_id, output)?;
    encode_rowbinary_string(&row.attempt_id, output)?;
    encode_rowbinary_string(&row.chrom, output)?;
    output.extend_from_slice(&row.position.to_le_bytes());
    encode_rowbinary_string(&row.sample_id, output)?;
    output.push(row.source_haplotype);
    output.extend_from_slice(&row.source_start0.to_le_bytes());
    output.extend_from_slice(&row.source_end0.to_le_bytes());
    Ok(())
}

fn encode_smoke_content(
    row: &PhasedMethylationSmokeRow,
    output: &mut Vec<u8>,
) -> anyhow::Result<()> {
    for value in [
        &row.ancillary_run_id,
        &row.attempt_id,
        &row.release,
        &row.cohort,
        &row.reference_genome,
        &row.modality,
        &row.source_version,
        &row.chrom,
    ] {
        encode_rowbinary_string(value, output)?;
    }
    output.extend_from_slice(&row.source_start0.to_le_bytes());
    output.extend_from_slice(&row.source_end0.to_le_bytes());
    output.extend_from_slice(&row.position.to_le_bytes());
    encode_rowbinary_string(&row.sample_id, output)?;
    output.push(row.source_haplotype);
    output.extend_from_slice(&row.methylation.to_bits().to_le_bytes());
    output.extend_from_slice(&row.coverage.to_le_bytes());
    output.extend_from_slice(&row.estimated_modified_count.to_le_bytes());
    output.extend_from_slice(&row.estimated_unmodified_count.to_le_bytes());
    output.extend_from_slice(&row.discretized_methylation.to_bits().to_le_bytes());
    Ok(())
}

fn encode_rowbinary_string(value: &str, output: &mut Vec<u8>) -> anyhow::Result<()> {
    let mut length = u64::try_from(value.len())?;
    loop {
        let mut byte = (length & 0x7f) as u8;
        length >>= 7;
        if length != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if length == 0 {
            break;
        }
    }
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn validate_smoke_release_identity(
    backend_revision: &str,
    worker_build_identity: &str,
) -> anyhow::Result<()> {
    if backend_revision.len() != 40
        || !backend_revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        bail!("phased-methylation smoke requires a full lowercase 40-hex backend revision");
    }
    let expected = format!("gnomad-lr/{backend_revision}/host-release/features-clickhouse");
    if worker_build_identity != expected {
        bail!(
            "phased-methylation smoke requires the clean revision-bound host release identity {expected:?}; got {worker_build_identity:?}"
        );
    }
    Ok(())
}

/// Execute the only repository-authorized phased-methylation smoke. No caller
/// can select a manifest, source URI, sample, haplotype, layer, interval,
/// authentication mode, credential variable names, or expected principal.
pub fn run_phased_methylation_smoke(
    target: &ClickHouseTarget,
) -> anyhow::Result<PhasedMethylationSmokeReceipt> {
    validate_smoke_release_identity(
        crate::pool::BACKEND_REVISION,
        crate::pool::WORKER_BUILD_IDENTITY,
    )?;
    let prepared = prepare_phased_methylation_smoke(target)?;
    let authenticated_principal = target.attest_current_user(PHASED_METHYLATION_SMOKE_PRINCIPAL)?;
    target.attest_synchronous_inserts()?;
    attest_fresh_y1_schema(target)?;

    // Buffer the bounded interval completely so malformed source records cannot
    // cause a prefix insert. One synchronous request is then verified by an
    // exact count and two independent ordered RowBinary digests.
    let records =
        open_phased_methylation_smoke_records(&prepared)?.collect::<anyhow::Result<Vec<_>>>()?;
    let rows = smoke_rows(&prepared, target.database(), records)?;
    let readback = insert_and_verify_smoke(&ClickHouseSmokeInsertReadback(target), &rows)?;

    Ok(PhasedMethylationSmokeReceipt {
        schema_version: Y1_SCHEMA_VERSION,
        capability: "single_owner_phased_methylation_smoke",
        status: "verified_scratch_only",
        database: target.database().to_string(),
        authenticated_principal,
        backend_revision: crate::pool::BACKEND_REVISION,
        worker_build_identity: crate::pool::WORKER_BUILD_IDENTITY,
        source: prepared,
        ancillary_run_id: rows[0].ancillary_run_id.clone(),
        attempt_id: rows[0].attempt_id.clone(),
        table_written: PHASED_METHYLATION_SMOKE_TABLE,
        row_count: readback.row_count,
        reject_count: 0,
        key_sha256: readback.key_sha256,
        content_sha256: readback.content_sha256,
        synchronous_inserts: true,
        fresh_exact_schema_v5_attested_before_insert: true,
        serving_state_written: false,
        summaries_written: false,
        availability_written: false,
        joined_tables_written: false,
        active_pointers_written: false,
    })
}

/// Load the one retained visual-evaluation contract. Source hap1/hap2 labels
/// remain raw source identities and are explicitly not mapped to VCF strands.
pub fn run_phased_methylation_evaluation(
    target: &ClickHouseTarget,
) -> anyhow::Result<PhasedMethylationEvaluationReceipt> {
    validate_smoke_release_identity(
        crate::pool::BACKEND_REVISION,
        crate::pool::WORKER_BUILD_IDENTITY,
    )?;
    validate_evaluation_database_name(target.database())?;
    let authenticated_principal = target.attest_current_user(PHASED_METHYLATION_SMOKE_PRINCIPAL)?;
    target.attest_synchronous_inserts()?;
    attest_fresh_y1_schema(target)?;

    let hap1 = prepare_fixed_phased_methylation_source(
        target,
        PHASED_METHYLATION_EVALUATION_AUTHORIZATION_ID,
        SourceHaplotype::Hap1,
        PHASED_METHYLATION_EVALUATION_START,
        PHASED_METHYLATION_EVALUATION_STOP,
    )?;
    let hap2 = prepare_fixed_phased_methylation_source(
        target,
        PHASED_METHYLATION_EVALUATION_AUTHORIZATION_ID,
        SourceHaplotype::Hap2,
        PHASED_METHYLATION_EVALUATION_START,
        PHASED_METHYLATION_EVALUATION_STOP,
    )?;

    // Parse and validate both immutable sources completely before the sole
    // synchronous insert, preventing a malformed second source from leaving a
    // verified-looking first-source prefix.
    let hap1_records =
        open_phased_methylation_smoke_records(&hap1)?.collect::<anyhow::Result<Vec<_>>>()?;
    let hap2_records =
        open_phased_methylation_smoke_records(&hap2)?.collect::<anyhow::Result<Vec<_>>>()?;
    let hap1_rows = fixed_phased_methylation_rows(
        &hap1,
        target.database(),
        "single-owner-evaluation",
        hap1_records,
    )?;
    let hap2_rows = fixed_phased_methylation_rows(
        &hap2,
        target.database(),
        "single-owner-evaluation",
        hap2_records,
    )?;
    let expected_hap1 = expected_smoke_readback(&hap1_rows)?;
    let expected_hap2 = expected_smoke_readback(&hap2_rows)?;
    let mut rows = hap1_rows;
    rows.extend(hap2_rows);
    rows.sort_by(|left, right| smoke_row_key(left).cmp(&smoke_row_key(right)));
    for pair in rows.windows(2) {
        if smoke_row_key(&pair[0]) == smoke_row_key(&pair[1]) {
            bail!("fixed phased-methylation evaluation contains a duplicate canonical key");
        }
    }
    let expected_combined = expected_smoke_readback(&rows)?;
    ClickHouseSmokeInsertReadback(target).insert(&rows)?;

    let verification = PhasedMethylationEvaluationVerification {
        source_hap1: clickhouse_smoke_readback(target, "WHERE source_haplotype = 1")?,
        source_hap2: clickhouse_smoke_readback(target, "WHERE source_haplotype = 2")?,
        combined: clickhouse_smoke_readback(target, "")?,
    };
    if verification.source_hap1 != expected_hap1
        || verification.source_hap2 != expected_hap2
        || verification.combined != expected_combined
        || verification.combined.row_count
            != verification.source_hap1.row_count + verification.source_hap2.row_count
    {
        bail!("phased-methylation evaluation readback count/hash mismatch");
    }

    Ok(PhasedMethylationEvaluationReceipt {
        schema_version: Y1_SCHEMA_VERSION,
        capability: "retained_source_phased_methylation_evaluation",
        status: "verified_retained_evaluation_only",
        database: PHASED_METHYLATION_EVALUATION_DATABASE,
        authenticated_principal,
        backend_revision: crate::pool::BACKEND_REVISION,
        worker_build_identity: crate::pool::WORKER_BUILD_IDENTITY,
        sources: vec![hap1, hap2],
        ancillary_run_id: rows[0].ancillary_run_id.clone(),
        attempt_id: rows[0].attempt_id.clone(),
        table_written: PHASED_METHYLATION_SMOKE_TABLE,
        verification,
        reject_count: 0,
        synchronous_inserts: true,
        both_sources_fully_parsed_before_insert: true,
        fresh_exact_schema_v5_attested_before_insert: true,
        retained_for_evaluation: true,
        joinable_to_vcf: false,
        orientation_status: "UNCONFIRMED",
        serving_state_written: false,
        summaries_written: false,
        availability_written: false,
        joined_tables_written: false,
        active_pointers_written: false,
    })
}

impl ImmutableObjectIdentity {
    fn as_gcs_object(&self) -> ImmutableGcsObject {
        ImmutableGcsObject {
            uri: self.uri.clone(),
            generation: self.generation.clone(),
            byte_size: self.byte_size,
            checksum_algorithm: self.checksum_algorithm.clone(),
            checksum: self.checksum.clone(),
            immutable_read_uri: self.immutable_read_uri.clone(),
        }
    }
}

/// Open the generation-bound prepared source and parse strict records lazily.
pub fn open_prepared_methylation_records(
    prepared: &PreparedMethylationAttempt,
) -> anyhow::Result<MethylationRecordStream> {
    open_methylation_record_stream(
        &prepared.source,
        &prepared.index,
        &prepared.chrom,
        prepared.start,
        prepared.stop,
        prepared.expected_type,
    )
}

fn open_methylation_record_stream(
    source: &ImmutableObjectIdentity,
    index: &ImmutableObjectIdentity,
    chrom: &str,
    start: u32,
    stop: u32,
    expected_type: MethylationSourceType,
) -> anyhow::Result<MethylationRecordStream> {
    let backend =
        Arc::new(HttpGcsBackend::new().context(
            "failed to initialize read-only GCS backend for immutable methylation source",
        )?);
    let lines = StrictBedStream::open_immutable_region(
        backend,
        &source.as_gcs_object(),
        &index.as_gcs_object(),
        chrom,
        start,
        stop,
        move |line: &str| methylation_source_coordinates(line, expected_type),
    )?
    .records();
    Ok(MethylationRecordStream {
        lines,
        expected_chrom: chrom.to_string(),
        expected_type,
        failed: false,
    })
}

pub struct MethylationRecordStream {
    lines: StrictBedLines,
    expected_chrom: String,
    expected_type: MethylationSourceType,
    failed: bool,
}

impl Iterator for MethylationRecordStream {
    type Item = anyhow::Result<MethylationRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.lines.next()? {
            Ok(line) => {
                match parse_methylation_record(&line, &self.expected_chrom, self.expected_type) {
                    Ok(record) => Some(Ok(record)),
                    Err(error) => {
                        self.failed = true;
                        Some(Err(error))
                    }
                }
            }
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Y1MethylationFinalizationSpec {
    pub ancillary_run_id: String,
    pub release: String,
    pub cohort: String,
    pub reference_genome: String,
    pub source_manifest_hash: String,
    pub data_layer: MethylationDataLayer,
    pub chrom: String,
    pub expected_tasks: u32,
    pub activate: bool,
}

/// Exact current task owner recorded by an authoritative expected-task set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MethylationTaskOwnerIdentity {
    pub ancillary_run_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_id: String,
    pub data_layer: MethylationDataLayer,
    pub sample_id: String,
    pub source_haplotype: Option<SourceHaplotype>,
    pub chrom: String,
    pub start: u32,
    pub stop: u32,
    pub source_manifest_hash: String,
    pub manifest_entry_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MethylationLedgerState {
    Running,
    Failed,
    Accepted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MethylationLeaseOwnership {
    Current,
    Expired,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MethylationResolvedAttempt {
    pub identity: MethylationTaskOwnerIdentity,
    pub state: MethylationLedgerState,
    pub ownership: MethylationLeaseOwnership,
}

/// A provider may create this only after one atomic read has resolved both the
/// expected task owners and the latest attempt states. No such provider exists
/// in D0; this type documents the non-bypassable finalization boundary.
#[derive(Debug, Clone)]
pub struct AuthoritativeMethylationLedgerSnapshot {
    atomically_resolved: bool,
    expected_task_owners: Vec<MethylationTaskOwnerIdentity>,
    resolved_attempts: Vec<MethylationResolvedAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MethylationFinalizationPlan {
    pub staging_table: &'static str,
    pub canonical_table: &'static str,
    pub accepted_attempts: Vec<MethylationTaskOwnerIdentity>,
    pub derive_total_summary: bool,
    pub requires_unique_canonical_keys: bool,
    pub materialize_availability_from_roster: bool,
    pub joined_serving_allowed: bool,
}

/// Runtime finalization is deliberately unavailable until an atomic ownership
/// ledger can produce [`AuthoritativeMethylationLedgerSnapshot`]. Caller-
/// supplied accepted-attempt IDs are not part of this API.
pub fn plan_methylation_finalization(
    target: &ClickHouseTarget,
    spec: &Y1MethylationFinalizationSpec,
) -> anyhow::Result<MethylationFinalizationPlan> {
    validate_finalization_spec(target, spec)?;
    bail!("Y1 methylation finalization is disabled: no atomic authoritative expected-task/attempt/lease ledger integration is implemented")
}

fn validate_finalization_spec(
    target: &ClickHouseTarget,
    spec: &Y1MethylationFinalizationSpec,
) -> anyhow::Result<()> {
    if target.kind() != TargetKind::Scratch {
        bail!("Y1 methylation finalization may target only a fenced scratch database");
    }
    if spec.release != "y1" || spec.cohort != "hgsvc_hprc" || spec.reference_genome != "GRCh38" {
        bail!("Y1 methylation finalization is restricted to y1/hgsvc_hprc/GRCh38");
    }
    if spec.ancillary_run_id.trim().is_empty()
        || spec.source_manifest_hash.len() != 64
        || !spec
            .source_manifest_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("finalization requires immutable run and manifest identities");
    }
    grch38_contig_length(&spec.chrom)?;
    if spec.expected_tasks == 0 {
        bail!("finalization requires a nonempty authoritative expected task set");
    }
    if spec.activate {
        bail!("D0 methylation finalization cannot activate any ancillary pointer");
    }
    Ok(())
}

pub fn plan_methylation_finalization_from_snapshot(
    target: &ClickHouseTarget,
    spec: &Y1MethylationFinalizationSpec,
    snapshot: &AuthoritativeMethylationLedgerSnapshot,
) -> anyhow::Result<MethylationFinalizationPlan> {
    validate_finalization_spec(target, spec)?;
    let accepted_attempts = reconcile_authoritative_attempts(spec, snapshot)?;
    Ok(match spec.data_layer {
        MethylationDataLayer::SampleTotal => MethylationFinalizationPlan {
            staging_table: "lr_y1_methylation_staging",
            canonical_table: "lr_y1_methylation",
            accepted_attempts,
            derive_total_summary: true,
            requires_unique_canonical_keys: true,
            materialize_availability_from_roster: true,
            joined_serving_allowed: false,
        },
        MethylationDataLayer::SourcePhased => MethylationFinalizationPlan {
            staging_table: "lr_y1_methylation_phased_staging",
            canonical_table: "lr_y1_methylation_phased",
            accepted_attempts,
            derive_total_summary: false,
            requires_unique_canonical_keys: true,
            materialize_availability_from_roster: true,
            joined_serving_allowed: false,
        },
    })
}

fn reconcile_authoritative_attempts(
    spec: &Y1MethylationFinalizationSpec,
    snapshot: &AuthoritativeMethylationLedgerSnapshot,
) -> anyhow::Result<Vec<MethylationTaskOwnerIdentity>> {
    if !snapshot.atomically_resolved {
        bail!("methylation ownership snapshot was not atomically resolved");
    }
    if snapshot.expected_task_owners.len() != spec.expected_tasks as usize {
        bail!("authoritative expected task count does not match finalization spec");
    }

    let mut expected = BTreeMap::new();
    for identity in &snapshot.expected_task_owners {
        validate_owner_identity(spec, identity)?;
        if expected
            .insert(identity.task_id.as_str(), identity)
            .is_some()
        {
            bail!("authoritative expected task set contains duplicate task IDs");
        }
    }

    let mut accepted = BTreeMap::new();
    let mut resolved_identities = BTreeSet::new();
    for attempt in &snapshot.resolved_attempts {
        validate_owner_identity(spec, &attempt.identity)?;
        let expected_identity =
            expected
                .get(attempt.identity.task_id.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("ledger contains a task outside the expected task set")
                })?;
        if *expected_identity != &attempt.identity {
            bail!("resolved attempt substitutes stale or non-authoritative task/attempt/lease identity");
        }
        let identity_key = (
            attempt.identity.task_id.as_str(),
            attempt.identity.attempt_id.as_str(),
            attempt.identity.lease_id.as_str(),
        );
        if !resolved_identities.insert(identity_key) {
            bail!("ledger contains a duplicate resolved attempt identity");
        }
        if attempt.state != MethylationLedgerState::Accepted
            || attempt.ownership != MethylationLeaseOwnership::Current
        {
            bail!(
                "finalization requires one accepted attempt with current lease ownership per task"
            );
        }
        if accepted
            .insert(attempt.identity.task_id.as_str(), attempt.identity.clone())
            .is_some()
        {
            bail!("ledger contains multiple accepted current owners for one task");
        }
    }
    if accepted.len() != expected.len() || expected.keys().ne(accepted.keys()) {
        bail!("ledger does not resolve exactly one accepted current owner per expected task");
    }
    Ok(accepted.into_values().collect())
}

fn validate_owner_identity(
    spec: &Y1MethylationFinalizationSpec,
    identity: &MethylationTaskOwnerIdentity,
) -> anyhow::Result<()> {
    for (name, value) in [
        ("ancillary_run_id", identity.ancillary_run_id.as_str()),
        ("task_id", identity.task_id.as_str()),
        ("attempt_id", identity.attempt_id.as_str()),
        ("lease_id", identity.lease_id.as_str()),
        ("sample_id", identity.sample_id.as_str()),
        ("manifest_entry_id", identity.manifest_entry_id.as_str()),
    ] {
        if value.trim().is_empty() || value.chars().any(char::is_control) {
            bail!("authoritative owner {name} must be nonempty and control-free");
        }
    }
    if identity.ancillary_run_id != spec.ancillary_run_id
        || identity.data_layer != spec.data_layer
        || identity.chrom != spec.chrom
        || identity.source_manifest_hash != spec.source_manifest_hash
    {
        bail!("authoritative owner has cross-run/layer/contig/manifest identity");
    }
    match (identity.data_layer, identity.source_haplotype) {
        (MethylationDataLayer::SampleTotal, None)
        | (MethylationDataLayer::SourcePhased, Some(_)) => {}
        _ => bail!("authoritative owner has invalid layer/source-haplotype identity"),
    }
    let contig_length = grch38_contig_length(&identity.chrom)?;
    if identity.start == 0 || identity.start > identity.stop || identity.stop > contig_length {
        bail!("authoritative owner interval is outside its GRCh38 contig");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::y1::AuthSource;

    const TOTAL: &str = include_str!("../../tests/fixtures/y1/methylation_total.bed");
    const HAP1: &str = include_str!("../../tests/fixtures/y1/methylation_hap1.bed");
    const HAP2: &str = include_str!("../../tests/fixtures/y1/methylation_hap2.bed");

    fn scratch_target() -> ClickHouseTarget {
        ClickHouseTarget::new(
            "http://127.0.0.1:8123",
            "gnomad_lr_y1_full_prototype_scratch_methylation_d0",
            TargetKind::Scratch,
            AuthSource::None,
            false,
            false,
        )
        .unwrap()
    }

    fn task() -> Y1MethylationTaskSpec {
        Y1MethylationTaskSpec {
            coordinator_task_id: "descriptor-1".into(),
            label: "source phased chr22".into(),
            ancillary_run_id: "run-1".into(),
            task_id: "task-1".into(),
            attempt_id: "attempt-1".into(),
            lease_id: "lease-1".into(),
            release: "y1".into(),
            cohort: "hgsvc_hprc".into(),
            reference_genome: "GRCh38".into(),
            source_manifest_id: "hgsvc-hprc-y1-phased-methylation-v2".into(),
            source_manifest_hash:
                "f585cbc2b806dcb52944af2ecabe634338a41323f89e3938336235c7729e8743".into(),
            manifest_entry_id: "hgsvc_hprc:HG00097".into(),
            data_layer: MethylationDataLayer::SourcePhased,
            source_haplotype: Some(SourceHaplotype::Hap1),
            chrom: "chr22".into(),
            start: 1,
            stop: 10_000,
        }
    }

    #[test]
    fn exact_total_hap1_hap2_fixtures_preserve_all_nine_source_fields() {
        let total =
            parse_methylation_record(TOTAL.trim_end(), "chr22", MethylationSourceType::Total)
                .unwrap();
        assert_eq!(total.source_start0, 99);
        assert_eq!(total.source_end0, 100);
        assert_eq!(total.position, 100);
        assert_eq!(total.methylation, 82.5);
        assert_eq!(total.source_type, MethylationSourceType::Total);
        assert_eq!(total.coverage, 4);
        assert_eq!(total.estimated_modified_count, 3);
        assert_eq!(total.estimated_unmodified_count, 1);
        assert_eq!(total.discretized_methylation, 75.0);

        let hap1 = parse_methylation_record(HAP1.trim_end(), "chr22", MethylationSourceType::Hap1)
            .unwrap();
        assert_eq!(hap1.source_type, MethylationSourceType::Hap1);
        assert_eq!(hap1.methylation, 10.25);
        assert_eq!(hap1.discretized_methylation, 33.3);

        let hap2 = parse_methylation_record(HAP2.trim_end(), "chr22", MethylationSourceType::Hap2)
            .unwrap();
        assert_eq!(hap2.source_type, MethylationSourceType::Hap2);
        assert_eq!(hap2.methylation, 95.0);
        assert_eq!(hap2.discretized_methylation, 100.0);
    }

    #[test]
    fn parser_rejects_schema_type_score_coordinate_and_count_errors() {
        let failures = [
            "chr22\t99\t100\t80\tTotal\t2\t1\t1",
            "chr22\t99\t101\t80\tTotal\t2\t1\t1\t50",
            "chr22\t99\t100\tNaN\tTotal\t2\t1\t1\t50",
            "chr22\t99\t100\t101\tTotal\t2\t1\t1\t50",
            "chr22\t99\t100\t80\tTotal\t2\t1\t0\t50",
            "chr22\t99\t100\t80\tTotal\t2\t1\t1\tinf",
        ];
        for line in failures {
            assert!(
                parse_methylation_record(line, "chr22", MethylationSourceType::Total).is_err(),
                "accepted {line}"
            );
        }
        assert!(
            parse_methylation_record(TOTAL.trim_end(), "chr22", MethylationSourceType::Hap1)
                .is_err()
        );
        assert!(
            parse_methylation_record(TOTAL.trim_end(), "chr21", MethylationSourceType::Total)
                .is_err()
        );
    }

    #[test]
    fn typed_task_contains_no_free_form_source_and_requires_layer_haplotype_shape() {
        let mut value = serde_json::to_value(task()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("source_uri".into(), Value::String("gs://mutable".into()));
        assert!(serde_json::from_value::<Y1MethylationTaskSpec>(value).is_err());

        let mut invalid = task();
        invalid.source_haplotype = None;
        assert!(invalid.validate("descriptor-1").is_err());
        let mut total = task();
        total.data_layer = MethylationDataLayer::SampleTotal;
        assert!(total.validate("descriptor-1").is_err());
        total.source_haplotype = None;
        assert!(total.validate("descriptor-1").is_ok());
    }

    #[test]
    fn checked_manifest_fails_closed_with_exact_immutable_metadata_blockers() {
        let error = prepare_methylation_attempt(
            &scratch_target(),
            &task(),
            "descriptor-1",
            Path::new("sources/y1/methylation-phased-source-manifest.json"),
        )
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("not load-ready"));
        assert!(message.contains("atomic methylation attempt/lease ledger"));
    }

    #[test]
    fn rehashed_load_authorized_manifest_is_rejected_as_a_noncanonical_override() {
        let source_path = Path::new("sources/y1/methylation-phased-source-manifest.json");
        let mut manifest: Value =
            serde_json::from_slice(&std::fs::read(source_path).unwrap()).unwrap();
        manifest.as_object_mut().unwrap().remove("content_sha256");
        manifest["load_readiness"] = serde_json::json!({
            "status": "load_ready",
            "load_authorized": true,
            "blockers": [],
        });
        let hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&manifest).unwrap())
        );
        manifest["content_sha256"] = Value::String(hash.clone());
        let path = std::env::temp_dir().join(format!(
            "gnomad-lr-methylation-runtime-gate-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let mut forged_task = task();
        forged_task.source_manifest_hash = hash;
        let error =
            prepare_methylation_attempt(&scratch_target(), &forged_task, "descriptor-1", &path)
                .unwrap_err();
        std::fs::remove_file(path).unwrap();
        assert!(error
            .to_string()
            .contains("source manifest override is forbidden"));
    }

    fn finalization_spec(data_layer: MethylationDataLayer) -> Y1MethylationFinalizationSpec {
        Y1MethylationFinalizationSpec {
            ancillary_run_id: "run-1".into(),
            release: "y1".into(),
            cohort: "hgsvc_hprc".into(),
            reference_genome: "GRCh38".into(),
            source_manifest_hash: "a".repeat(64),
            data_layer,
            chrom: "chr22".into(),
            expected_tasks: 1,
            activate: false,
        }
    }

    fn owner(spec: &Y1MethylationFinalizationSpec) -> MethylationTaskOwnerIdentity {
        MethylationTaskOwnerIdentity {
            ancillary_run_id: spec.ancillary_run_id.clone(),
            task_id: "task-1".into(),
            attempt_id: "attempt-2".into(),
            lease_id: "lease-2".into(),
            data_layer: spec.data_layer,
            sample_id: "HG00097".into(),
            source_haplotype: (spec.data_layer == MethylationDataLayer::SourcePhased)
                .then_some(SourceHaplotype::Hap1),
            chrom: spec.chrom.clone(),
            start: 1,
            stop: 10_000,
            source_manifest_hash: spec.source_manifest_hash.clone(),
            manifest_entry_id: "hgsvc_hprc:HG00097".into(),
        }
    }

    fn accepted_snapshot(
        identity: MethylationTaskOwnerIdentity,
    ) -> AuthoritativeMethylationLedgerSnapshot {
        AuthoritativeMethylationLedgerSnapshot {
            atomically_resolved: true,
            expected_task_owners: vec![identity.clone()],
            resolved_attempts: vec![MethylationResolvedAttempt {
                identity,
                state: MethylationLedgerState::Accepted,
                ownership: MethylationLeaseOwnership::Current,
            }],
        }
    }

    #[test]
    fn runtime_finalization_is_blocked_without_atomic_ledger_integration() {
        let spec = finalization_spec(MethylationDataLayer::SampleTotal);
        let error = plan_methylation_finalization(&scratch_target(), &spec).unwrap_err();
        assert!(error.to_string().contains("atomic authoritative"));
    }

    #[test]
    fn authoritative_snapshot_keeps_total_summary_separate_and_phased_unjoined() {
        let total_spec = finalization_spec(MethylationDataLayer::SampleTotal);
        let total = plan_methylation_finalization_from_snapshot(
            &scratch_target(),
            &total_spec,
            &accepted_snapshot(owner(&total_spec)),
        )
        .unwrap();
        assert!(total.derive_total_summary);
        assert!(!total.joined_serving_allowed);
        assert_eq!(total.accepted_attempts[0].attempt_id, "attempt-2");

        let phased_spec = finalization_spec(MethylationDataLayer::SourcePhased);
        let phased = plan_methylation_finalization_from_snapshot(
            &scratch_target(),
            &phased_spec,
            &accepted_snapshot(owner(&phased_spec)),
        )
        .unwrap();
        assert!(!phased.derive_total_summary);
        assert!(!phased.joined_serving_allowed);
        assert_eq!(phased.canonical_table, "lr_y1_methylation_phased");
    }

    #[test]
    fn authoritative_reconciliation_rejects_stale_expired_cross_identity_and_duplicates() {
        let spec = finalization_spec(MethylationDataLayer::SourcePhased);
        let current = owner(&spec);

        let mut stale = accepted_snapshot(current.clone());
        stale.resolved_attempts[0].identity.attempt_id = "attempt-1".into();
        stale.resolved_attempts[0].identity.lease_id = "lease-1".into();
        assert!(reconcile_authoritative_attempts(&spec, &stale).is_err());

        let mut expired = accepted_snapshot(current.clone());
        expired.resolved_attempts[0].ownership = MethylationLeaseOwnership::Expired;
        assert!(reconcile_authoritative_attempts(&spec, &expired).is_err());

        let mut duplicate = accepted_snapshot(current.clone());
        duplicate
            .resolved_attempts
            .push(duplicate.resolved_attempts[0].clone());
        assert!(reconcile_authoritative_attempts(&spec, &duplicate).is_err());

        let mut non_atomic = accepted_snapshot(current.clone());
        non_atomic.atomically_resolved = false;
        assert!(reconcile_authoritative_attempts(&spec, &non_atomic).is_err());

        let mut substitutions = Vec::new();
        let mut cross_run = current.clone();
        cross_run.ancillary_run_id = "run-2".into();
        substitutions.push(cross_run);
        let mut cross_task = current.clone();
        cross_task.task_id = "task-2".into();
        substitutions.push(cross_task);
        let mut cross_lease = current.clone();
        cross_lease.lease_id = "lease-3".into();
        substitutions.push(cross_lease);
        let mut cross_layer = current.clone();
        cross_layer.data_layer = MethylationDataLayer::SampleTotal;
        cross_layer.source_haplotype = None;
        substitutions.push(cross_layer);
        let mut cross_sample = current.clone();
        cross_sample.sample_id = "HG00099".into();
        substitutions.push(cross_sample);
        let mut cross_haplotype = current.clone();
        cross_haplotype.source_haplotype = Some(SourceHaplotype::Hap2);
        substitutions.push(cross_haplotype);
        let mut cross_contig = current.clone();
        cross_contig.chrom = "chr21".into();
        substitutions.push(cross_contig);
        let mut cross_interval = current.clone();
        cross_interval.start = 2;
        substitutions.push(cross_interval);
        let mut cross_manifest_entry = current.clone();
        cross_manifest_entry.manifest_entry_id = "hgsvc_hprc:HG00099".into();
        substitutions.push(cross_manifest_entry);
        let mut cross_manifest_hash = current.clone();
        cross_manifest_hash.source_manifest_hash = "b".repeat(64);
        substitutions.push(cross_manifest_hash);

        for substitution in substitutions {
            let mut snapshot = accepted_snapshot(current.clone());
            snapshot.resolved_attempts[0].identity = substitution;
            assert!(reconcile_authoritative_attempts(&spec, &snapshot).is_err());
        }
    }

    fn smoke_target() -> ClickHouseTarget {
        ClickHouseTarget::new(
            "http://127.0.0.1:8123",
            "gnomad_lr_y1_scratch_phased_methylation_smoke_v5_unit_0123456789ab",
            TargetKind::Scratch,
            AuthSource::None,
            false,
            false,
        )
        .unwrap()
    }

    fn evaluation_target() -> ClickHouseTarget {
        ClickHouseTarget::new(
            "http://127.0.0.1:8123",
            PHASED_METHYLATION_EVALUATION_DATABASE,
            TargetKind::Scratch,
            AuthSource::None,
            false,
            false,
        )
        .unwrap()
    }

    fn smoke_fixture_rows() -> Vec<PhasedMethylationSmokeRow> {
        let prepared = prepare_phased_methylation_smoke(&smoke_target()).unwrap();
        smoke_rows(
            &prepared,
            smoke_target().database(),
            vec![MethylationRecord {
                chrom: "chr22".into(),
                source_start0: 19_999_999,
                source_end0: 20_000_000,
                position: 20_000_000,
                methylation: 10.25,
                source_type: MethylationSourceType::Hap1,
                coverage: 4,
                estimated_modified_count: 1,
                estimated_unmodified_count: 3,
                discretized_methylation: 25.0,
            }],
        )
        .unwrap()
    }

    struct FakeSmokeStorage {
        insert_failure: bool,
        inserted_rows: std::cell::Cell<usize>,
        readback_calls: std::cell::Cell<usize>,
        readback: SmokeReadback,
    }

    impl SmokeInsertReadback for FakeSmokeStorage {
        fn insert(&self, rows: &[PhasedMethylationSmokeRow]) -> anyhow::Result<()> {
            if self.insert_failure {
                self.inserted_rows.set(rows.len().min(1));
                bail!("injected partial insert failure");
            }
            self.inserted_rows.set(rows.len());
            Ok(())
        }

        fn readback(&self) -> anyhow::Result<SmokeReadback> {
            self.readback_calls.set(self.readback_calls.get() + 1);
            Ok(self.readback.clone())
        }
    }

    #[test]
    fn smoke_release_identity_rejects_unversioned_dirty_and_nonrelease_builds() {
        let revision = "5".repeat(40);
        let release = format!("gnomad-lr/{revision}/host-release/features-clickhouse");
        assert!(validate_smoke_release_identity(&revision, &release).is_ok());

        for (candidate_revision, candidate_identity) in [
            (
                "unversioned-development-build".to_string(),
                "gnomad-lr/0.1.0/development-build".to_string(),
            ),
            (
                revision.clone(),
                format!("gnomad-lr/{revision}-dirty/host-release/features-clickhouse"),
            ),
            (
                revision.clone(),
                format!("gnomad-lr/{revision}/host-test/features-clickhouse"),
            ),
            (
                "A".repeat(40),
                format!(
                    "gnomad-lr/{}/host-release/features-clickhouse",
                    "A".repeat(40)
                ),
            ),
        ] {
            assert!(
                validate_smoke_release_identity(&candidate_revision, &candidate_identity).is_err(),
                "accepted revision={candidate_revision:?} identity={candidate_identity:?}"
            );
        }
    }

    #[test]
    fn exact_smoke_authorization_keeps_general_loading_blocked() {
        let prepared = prepare_phased_methylation_smoke(&smoke_target()).unwrap();
        assert_eq!(prepared.sample_id, "HG00097");
        assert_eq!(prepared.source_haplotype, SourceHaplotype::Hap1);
        assert_eq!(prepared.chrom, "chr22");
        assert_eq!((prepared.start, prepared.stop), (20_000_000, 20_010_000));
        assert!(prepared.source.uri.ends_with("/HG00097.hap1.bed.gz"));
        assert!(prepared.index.uri.ends_with("/HG00097.hap1.bed.gz.tbi"));

        let error = prepare_methylation_attempt(
            &scratch_target(),
            &task(),
            "descriptor-1",
            Path::new(CANONICAL_METHYLATION_MANIFEST_PATH),
        )
        .unwrap_err();
        assert!(error.to_string().contains("not load-ready"));
    }

    #[test]
    fn retained_evaluation_contract_separates_source_labels_and_restricts_scope() {
        let target = evaluation_target();
        let hap1 = prepare_fixed_phased_methylation_source(
            &target,
            PHASED_METHYLATION_EVALUATION_AUTHORIZATION_ID,
            SourceHaplotype::Hap1,
            PHASED_METHYLATION_EVALUATION_START,
            PHASED_METHYLATION_EVALUATION_STOP,
        )
        .unwrap();
        let hap2 = prepare_fixed_phased_methylation_source(
            &target,
            PHASED_METHYLATION_EVALUATION_AUTHORIZATION_ID,
            SourceHaplotype::Hap2,
            PHASED_METHYLATION_EVALUATION_START,
            PHASED_METHYLATION_EVALUATION_STOP,
        )
        .unwrap();
        assert_eq!(hap1.sample_id, "HG00097");
        assert_eq!(hap2.sample_id, "HG00097");
        assert_eq!((hap1.start, hap1.stop), (47_040_000, 47_050_000));
        assert_eq!((hap2.start, hap2.stop), (47_040_000, 47_050_000));
        assert_eq!(hap1.source_haplotype, SourceHaplotype::Hap1);
        assert_eq!(hap2.source_haplotype, SourceHaplotype::Hap2);
        assert!(hap1.source.uri.ends_with("/HG00097.hap1.bed.gz"));
        assert!(hap2.source.uri.ends_with("/HG00097.hap2.bed.gz"));
        assert_ne!(hap1.source.generation, hap2.source.generation);

        assert!(validate_evaluation_database_name(PHASED_METHYLATION_EVALUATION_DATABASE).is_ok());
        assert!(
            validate_evaluation_database_name("gnomad_lr_y1_scratch_other_evaluation").is_err()
        );
    }

    #[test]
    fn fixed_rows_reject_cross_source_or_cross_region_records() {
        let prepared = prepare_fixed_phased_methylation_source(
            &evaluation_target(),
            PHASED_METHYLATION_EVALUATION_AUTHORIZATION_ID,
            SourceHaplotype::Hap2,
            PHASED_METHYLATION_EVALUATION_START,
            PHASED_METHYLATION_EVALUATION_STOP,
        )
        .unwrap();
        let fixture = |position, source_type| MethylationRecord {
            chrom: "chr22".into(),
            source_start0: position - 1,
            source_end0: position,
            position,
            methylation: 50.0,
            source_type,
            coverage: 2,
            estimated_modified_count: 1,
            estimated_unmodified_count: 1,
            discretized_methylation: 50.0,
        };
        assert!(fixed_phased_methylation_rows(
            &prepared,
            evaluation_target().database(),
            "single-owner-evaluation",
            vec![fixture(47_040_000, MethylationSourceType::Hap1)],
        )
        .is_err());
        assert!(fixed_phased_methylation_rows(
            &prepared,
            evaluation_target().database(),
            "single-owner-evaluation",
            vec![fixture(47_039_999, MethylationSourceType::Hap2)],
        )
        .is_err());
    }

    #[test]
    fn smoke_database_and_manifest_source_substitution_fail_closed() {
        for database in [
            "gnomad_lr_y1_scratch_v5_not_smoke",
            "gnomad_lr_y1_scratch_phased_methylation_smoke_v5_short",
            "gnomad_lr_y1_scratch_phased_methylation_smoke_v5_Uppercase_012345",
        ] {
            assert!(
                validate_smoke_database_name(database).is_err(),
                "{database}"
            );
        }

        let mut manifest: Value = serde_json::from_slice(include_bytes!(
            "../../sources/y1/methylation-phased-source-manifest.json"
        ))
        .unwrap();
        let entry = manifest["samples"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["sample_id"] == "HG00097")
            .unwrap();
        entry["objects"]["hap1_bed"]["immutable_identity"]["uri"] =
            Value::String("gs://substituted/HG00097.hap1.bed.gz".into());
        assert!(
            verified_canonical_manifest(&serde_json::to_vec(&manifest).unwrap())
                .unwrap_err()
                .to_string()
                .contains("pinned repository trust root")
        );
    }

    #[test]
    fn partial_insert_failure_never_attempts_or_emits_verified_readback() {
        let rows = smoke_fixture_rows();
        let expected = expected_smoke_readback(&rows).unwrap();
        let backend = FakeSmokeStorage {
            insert_failure: true,
            inserted_rows: std::cell::Cell::new(0),
            readback_calls: std::cell::Cell::new(0),
            readback: expected,
        };
        let error = insert_and_verify_smoke(&backend, &rows).unwrap_err();
        assert!(error.to_string().contains("partial insert failure"));
        assert_eq!(backend.inserted_rows.get(), 1);
        assert_eq!(backend.readback_calls.get(), 0);
    }

    #[test]
    fn exact_count_key_or_content_readback_mismatch_fails() {
        let rows = smoke_fixture_rows();
        let expected = expected_smoke_readback(&rows).unwrap();
        for readback in [
            SmokeReadback {
                row_count: 0,
                ..expected.clone()
            },
            SmokeReadback {
                key_sha256: "0".repeat(64),
                ..expected.clone()
            },
            SmokeReadback {
                content_sha256: "f".repeat(64),
                ..expected.clone()
            },
        ] {
            let backend = FakeSmokeStorage {
                insert_failure: false,
                inserted_rows: std::cell::Cell::new(0),
                readback_calls: std::cell::Cell::new(0),
                readback,
            };
            assert!(insert_and_verify_smoke(&backend, &rows)
                .unwrap_err()
                .to_string()
                .contains("readback mismatch"));
        }
    }

    #[test]
    #[ignore = "requires an explicitly supplied disposable local ClickHouse endpoint"]
    fn local_clickhouse_smoke_rowbinary_readback_matches_local_hashes() {
        let endpoint = std::env::var("GNOMAD_LR_LOCAL_CLICKHOUSE_SMOKE_URL")
            .expect("set GNOMAD_LR_LOCAL_CLICKHOUSE_SMOKE_URL to a disposable local endpoint");
        let database = format!(
            "{PHASED_METHYLATION_SMOKE_DATABASE_PREFIX}integration_{:012}",
            std::process::id()
        );
        let client = reqwest::blocking::Client::new();
        let execute = |query: &str| -> anyhow::Result<()> {
            let response = client.post(&endpoint).body(query.to_string()).send()?;
            if !response.status().is_success() {
                bail!("local ClickHouse query failed: {}", response.text()?);
            }
            Ok(())
        };
        execute(&format!("DROP DATABASE IF EXISTS {database} SYNC")).unwrap();
        execute(&format!("CREATE DATABASE {database}")).unwrap();
        let result = (|| -> anyhow::Result<()> {
            let target = ClickHouseTarget::new(
                &endpoint,
                &database,
                TargetKind::Scratch,
                AuthSource::None,
                false,
                false,
            )?;
            super::super::init_schema(&target)?;
            attest_fresh_y1_schema(&target)?;
            let prepared = prepare_phased_methylation_smoke(&target)?;
            let rows = smoke_rows(
                &prepared,
                target.database(),
                vec![MethylationRecord {
                    chrom: "chr22".into(),
                    source_start0: 19_999_999,
                    source_end0: 20_000_000,
                    position: 20_000_000,
                    methylation: 10.25,
                    source_type: MethylationSourceType::Hap1,
                    coverage: 4,
                    estimated_modified_count: 1,
                    estimated_unmodified_count: 3,
                    discretized_methylation: 25.0,
                }],
            )?;
            insert_and_verify_smoke(&ClickHouseSmokeInsertReadback(&target), &rows)?;
            assert!(attest_fresh_y1_schema(&target).is_err());
            Ok(())
        })();
        let cleanup = execute(&format!("DROP DATABASE {database} SYNC"));
        result.unwrap();
        cleanup.unwrap();
    }
}
