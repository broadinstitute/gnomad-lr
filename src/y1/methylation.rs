//! Typed, fail-closed Y1 methylation foundation.
//!
//! This module does not adapt the legacy methylation command or pool action. A
//! task names a checked manifest entry, never a path or sample ID. Preparation
//! resolves generation-bound read identities from the repository v2 manifest
//! and is restricted to a fenced scratch [`ClickHouseTarget`]. The checked D0
//! manifest is intentionally blocked, so no source read or database mutation is
//! currently authorized.

use super::contig::grch38_contig_length;
use super::{ClickHouseTarget, TargetKind};
use crate::loader::strict_bed_reader::{StrictBedLines, StrictBedStream, ValidatedBedRecord};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

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

pub fn parse_methylation_record(
    line: &str,
    expected_chrom: &str,
    expected_type: MethylationSourceType,
) -> anyhow::Result<MethylationRecord> {
    let record = parse_methylation_source_record(line)?;
    if record.chrom != expected_chrom {
        bail!(
            "methylation chromosome mismatch: expected {expected_chrom}, got {}",
            record.chrom
        );
    }
    if record.source_type != expected_type {
        bail!(
            "methylation source type mismatch: expected {:?}, got {:?}",
            expected_type,
            record.source_type
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

/// Resolve a typed task against the checked immutable manifest. This performs
/// no read and no ClickHouse mutation. The checked D0 manifest fails here with
/// its exact missing-metadata blockers.
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

    let raw = std::fs::read(manifest_path).with_context(|| {
        format!(
            "failed to read methylation manifest {}",
            manifest_path.display()
        )
    })?;
    let mut manifest: Value =
        serde_json::from_slice(&raw).context("invalid methylation manifest JSON")?;
    let recorded_hash = manifest
        .get("content_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("methylation manifest lacks content_sha256"))?
        .to_string();
    manifest
        .as_object_mut()
        .expect("JSON document is an object after content hash lookup")
        .remove("content_sha256");
    let canonical = serde_json::to_vec(&manifest)?;
    let actual_hash = format!("{:x}", Sha256::digest(canonical));
    if recorded_hash != actual_hash || task.source_manifest_hash != recorded_hash {
        bail!("methylation manifest canonical hash does not match the typed task identity");
    }
    if manifest.get("schema_version").and_then(Value::as_u64) != Some(2)
        || manifest.get("manifest_id").and_then(Value::as_str) != Some(&task.source_manifest_id)
    {
        bail!("typed task does not resolve to the required v2 methylation manifest");
    }

    let readiness = manifest
        .get("load_readiness")
        .ok_or_else(|| anyhow::anyhow!("methylation manifest lacks load_readiness"))?;
    if readiness.get("load_authorized").and_then(Value::as_bool) != Some(true) {
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
    // Shape validation of repository metadata is not runtime object validation.
    // Keep even a hand-edited/rehashed load_authorized manifest blocked until a
    // concrete GCS implementation revalidates metadata and binds both reads to
    // the declared generations without a check/read TOCTOU gap.
    if !runtime_immutable_reads_enabled() || !runtime_atomic_methylation_ledger_enabled() {
        bail!("Y1 methylation runtime generation/size/checksum verification, generation-bound reads, and atomic attempt/lease ownership are not implemented");
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

fn runtime_immutable_reads_enabled() -> bool {
    false
}

fn runtime_atomic_methylation_ledger_enabled() -> bool {
    false
}

fn resolve_immutable_object(
    value: Option<&Value>,
    slot: &str,
) -> anyhow::Result<ImmutableObjectIdentity> {
    let descriptor =
        value.ok_or_else(|| anyhow::anyhow!("manifest entry lacks object slot {slot}"))?;
    if descriptor.get("load_authorized").and_then(Value::as_bool) != Some(true) {
        bail!("manifest object slot {slot} is not load-authorized");
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
        immutable_read_uri,
    })
}

/// Open the generation-bound prepared source and parse strict records lazily.
pub fn open_prepared_methylation_records(
    prepared: &PreparedMethylationAttempt,
) -> anyhow::Result<MethylationRecordStream> {
    let expected_type = prepared.expected_type;
    let lines = StrictBedStream::open_region(
        &prepared.source.immutable_read_uri,
        &prepared.index.immutable_read_uri,
        &prepared.chrom,
        prepared.start,
        prepared.stop,
        move |line: &str| {
            let record = parse_methylation_source_record(line)?;
            if record.source_type != expected_type {
                bail!(
                    "methylation source type mismatch: expected {:?}, got {:?}",
                    expected_type,
                    record.source_type
                );
            }
            Ok(ValidatedBedRecord {
                chrom: record.chrom,
                start0: record.source_start0,
                end0: record.source_end0,
            })
        },
    )?
    .records();
    Ok(MethylationRecordStream {
        lines,
        expected_chrom: prepared.chrom.clone(),
        expected_type: prepared.expected_type,
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
                "b2124fa4a427b88f4446e519217ee9290593a068212f69167d7fc931688e9806".into(),
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
        assert!(message.contains("frozen Terra LR_sample entity snapshot"));
        assert!(message.contains("generation-pinned combined BED indexes"));
        assert!(message.contains("generation-bound read URIs"));
    }

    #[test]
    fn rehashed_load_authorized_manifest_still_cannot_bypass_runtime_identity_gate() {
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
            .contains("runtime generation/size/checksum verification"));
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
}
