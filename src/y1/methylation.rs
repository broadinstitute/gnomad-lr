//! Typed, fail-closed Y1 methylation loading and in-place raw finalization.
//!
//! This path never adapts the legacy free-form methylation command. A task names
//! an exact checked manifest entry plus run/task/attempt/lease ownership. Workers
//! append principal-bound receipts and write directly to the inactive canonical
//! raw table; finalization fences that principal, resolves one authoritative
//! ledger snapshot, removes failed-attempt prefixes, and freezes without an
//! active pointer or joined-serving authorization.

use super::contig::grch38_contig_length;
use super::{ClickHouseTarget, TargetKind, WorkerWriteFence};
use crate::loader::immutable_gcs::{HttpGcsBackend, ImmutableGcsObject};
use crate::loader::strict_bed_reader::{StrictBedLines, StrictBedStream, ValidatedBedRecord};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

    fn value(self) -> &'static str {
        match self {
            Self::SampleTotal => "sample_total",
            Self::SourcePhased => "source_phased",
        }
    }

    fn table(self) -> &'static str {
        match self {
            Self::SampleTotal => "lr_y1_methylation",
            Self::SourcePhased => "lr_y1_methylation_phased",
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
    /// Unix epoch milliseconds after which this assignment may not write or be finalized.
    pub lease_expires_at_ms: u64,
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
        if self.lease_expires_at_ms == 0 {
            bail!("methylation task requires a nonzero lease_expires_at_ms");
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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
    lease_expires_at_ms: u64,
    source_manifest_id: String,
    source_manifest_hash: String,
    manifest_entry_id: String,
    sample_id: String,
    data_layer: MethylationDataLayer,
    source_haplotype: Option<SourceHaplotype>,
    source_object_slot: String,
    source_index_object_slot: String,
    expected_type: MethylationSourceType,
    chrom: String,
    start: u32,
    stop: u32,
    source: ImmutableObjectIdentity,
    index: ImmutableObjectIdentity,
}

/// Resolve a typed task against the checked immutable manifest. This performs
/// no source read and no ClickHouse mutation.
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
    // Shape validation of repository metadata is not runtime object validation;
    // open_prepared_methylation_records performs exact-generation GCS checks.
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
        lease_expires_at_ms: task.lease_expires_at_ms,
        source_manifest_id: task.source_manifest_id.clone(),
        source_manifest_hash: recorded_hash,
        manifest_entry_id: task.manifest_entry_id.clone(),
        sample_id: sample_id.to_string(),
        data_layer: task.data_layer,
        source_haplotype: task.source_haplotype,
        source_object_slot: source_slot.to_string(),
        source_index_object_slot: index_slot.to_string(),
        expected_type,
        chrom: task.chrom.clone(),
        start: task.start,
        stop: task.stop,
        source,
        index,
    })
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
        updated_at: string("updated_at")?,
        immutable_read_uri,
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
    let expected_type = prepared.expected_type;
    let backend =
        Arc::new(HttpGcsBackend::new().context(
            "failed to initialize read-only GCS backend for immutable methylation source",
        )?);
    let lines = StrictBedStream::open_immutable_region(
        backend,
        &prepared.source.as_gcs_object(),
        &prepared.index.as_gcs_object(),
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MethylationAttemptCounts {
    pub source_rows: u64,
    pub canonical_rows: u64,
    pub reject_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MethylationAttemptReport {
    pub identity: MethylationTaskOwnerIdentity,
    pub worker_principal: String,
    pub source_manifest_id: String,
    pub source: ImmutableObjectIdentity,
    pub index: ImmutableObjectIdentity,
    pub state: MethylationLedgerState,
    pub counts: MethylationAttemptCounts,
    pub key_hash: String,
    pub content_hash: String,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    pub elapsed_ms: u128,
    pub canonical_table: String,
    pub error: Option<String>,
    pub published: bool,
    pub joined_serving_allowed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct MethylationAttemptReceipt {
    ancillary_run_id: String,
    modality: String,
    chrom: String,
    task_id: String,
    attempt_id: String,
    lease_id: String,
    lease_expires_at_ms: u64,
    worker_principal: String,
    release: String,
    cohort: String,
    reference_genome: String,
    sample_id: String,
    data_layer: String,
    source_haplotype: Option<u8>,
    source_manifest_id: String,
    source_manifest_hash: String,
    manifest_entry_id: String,
    source_object_slot: String,
    source_uri: String,
    source_generation: String,
    source_size_bytes: u64,
    source_checksum_algorithm: String,
    source_checksum: String,
    source_index_object_slot: String,
    source_index_uri: String,
    source_index_generation: String,
    source_index_size_bytes: u64,
    source_index_checksum_algorithm: String,
    source_index_checksum: String,
    interval_start: u32,
    interval_end: u32,
    state: String,
    source_rows: u64,
    staged_rows: u64,
    reject_rows: u64,
    key_hash: String,
    content_hash: String,
    error: Option<String>,
    started_at_ms: u64,
    finished_at_ms: u64,
    revision: u64,
}

impl MethylationAttemptReceipt {
    #[allow(clippy::too_many_arguments)]
    fn new(
        prepared: &PreparedMethylationAttempt,
        worker_principal: &str,
        state: MethylationLedgerState,
        counts: MethylationAttemptCounts,
        key_hash: String,
        content_hash: String,
        error: Option<String>,
        started_at_ms: u64,
        finished_at_ms: u64,
        revision: u64,
    ) -> Self {
        Self {
            ancillary_run_id: prepared.ancillary_run_id.clone(),
            modality: prepared.data_layer.modality().to_string(),
            chrom: prepared.chrom.clone(),
            task_id: prepared.task_id.clone(),
            attempt_id: prepared.attempt_id.clone(),
            lease_id: prepared.lease_id.clone(),
            lease_expires_at_ms: prepared.lease_expires_at_ms,
            worker_principal: worker_principal.to_string(),
            release: "y1".into(),
            cohort: "hgsvc_hprc".into(),
            reference_genome: "GRCh38".into(),
            sample_id: prepared.sample_id.clone(),
            data_layer: prepared.data_layer.value().to_string(),
            source_haplotype: prepared.source_haplotype.map(SourceHaplotype::value),
            source_manifest_id: prepared.source_manifest_id.clone(),
            source_manifest_hash: prepared.source_manifest_hash.clone(),
            manifest_entry_id: prepared.manifest_entry_id.clone(),
            source_object_slot: prepared.source_object_slot.clone(),
            source_uri: prepared.source.uri.clone(),
            source_generation: prepared.source.generation.clone(),
            source_size_bytes: prepared.source.byte_size,
            source_checksum_algorithm: prepared.source.checksum_algorithm.clone(),
            source_checksum: prepared.source.checksum.clone(),
            source_index_object_slot: prepared.source_index_object_slot.clone(),
            source_index_uri: prepared.index.uri.clone(),
            source_index_generation: prepared.index.generation.clone(),
            source_index_size_bytes: prepared.index.byte_size,
            source_index_checksum_algorithm: prepared.index.checksum_algorithm.clone(),
            source_index_checksum: prepared.index.checksum.clone(),
            interval_start: prepared.start,
            interval_end: prepared.stop,
            state: match state {
                MethylationLedgerState::Running => "running",
                MethylationLedgerState::Failed => "failed",
                MethylationLedgerState::Accepted => "accepted",
            }
            .into(),
            source_rows: counts.source_rows,
            staged_rows: counts.canonical_rows,
            reject_rows: counts.reject_rows,
            key_hash,
            content_hash,
            error,
            started_at_ms,
            finished_at_ms,
            revision,
        }
    }

    fn owner_identity(&self) -> MethylationTaskOwnerIdentity {
        MethylationTaskOwnerIdentity {
            ancillary_run_id: self.ancillary_run_id.clone(),
            task_id: self.task_id.clone(),
            attempt_id: self.attempt_id.clone(),
            lease_id: self.lease_id.clone(),
            data_layer: match self.data_layer.as_str() {
                "sample_total" => MethylationDataLayer::SampleTotal,
                "source_phased" => MethylationDataLayer::SourcePhased,
                _ => MethylationDataLayer::SampleTotal,
            },
            sample_id: self.sample_id.clone(),
            source_haplotype: match self.source_haplotype {
                None => None,
                Some(1) => Some(SourceHaplotype::Hap1),
                Some(2) => Some(SourceHaplotype::Hap2),
                Some(_) => None,
            },
            chrom: self.chrom.clone(),
            start: self.interval_start,
            stop: self.interval_end,
            source_manifest_hash: self.source_manifest_hash.clone(),
            manifest_entry_id: self.manifest_entry_id.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CanonicalAttemptSnapshot {
    rows: u64,
    unique_keys: u64,
    identity_violations: u64,
    min_position: u32,
    max_position: u32,
}

#[derive(Debug)]
struct CanonicalHashes {
    rows: u64,
    key_hash: String,
    content_hash: String,
}

fn epoch_ms() -> anyhow::Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn revision_now() -> anyhow::Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}

fn latest_task_receipts(
    target: &ClickHouseTarget,
    ancillary_run_id: &str,
    modality: &str,
    chrom: &str,
    task_id: Option<&str>,
) -> anyhow::Result<Vec<MethylationAttemptReceipt>> {
    let task_filter = if task_id.is_some() {
        " AND task_id = {task_id:String}"
    } else {
        ""
    };
    let query = format!(
        r#"WITH current_revisions AS (
    SELECT task_id, max(revision) AS revision
    FROM lr_y1_ancillary_task_attempts
    WHERE ancillary_run_id = {{run:String}} AND modality = {{modality:String}} AND chrom = {{chrom:String}}{task_filter}
    GROUP BY task_id
)
SELECT a.*
FROM lr_y1_ancillary_task_attempts AS a
INNER JOIN current_revisions AS c USING (task_id, revision)
WHERE a.ancillary_run_id = {{run:String}} AND a.modality = {{modality:String}} AND a.chrom = {{chrom:String}}{task_filter}
ORDER BY task_id, attempt_id, lease_id
FORMAT JSONEachRow"#
    );
    let mut parameters = vec![
        ("run", ancillary_run_id),
        ("modality", modality),
        ("chrom", chrom),
    ];
    if let Some(task_id) = task_id {
        parameters.push(("task_id", task_id));
    }
    let body = target.query_text(&query, &parameters)?;
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .context("authoritative methylation ledger snapshot returned malformed JSON")
        })
        .collect()
}

fn receipt_matches_prepared(
    receipt: &MethylationAttemptReceipt,
    prepared: &PreparedMethylationAttempt,
    worker_principal: &str,
) -> bool {
    receipt.ancillary_run_id == prepared.ancillary_run_id
        && receipt.modality == prepared.data_layer.modality()
        && receipt.chrom == prepared.chrom
        && receipt.task_id == prepared.task_id
        && receipt.attempt_id == prepared.attempt_id
        && receipt.lease_id == prepared.lease_id
        && receipt.lease_expires_at_ms == prepared.lease_expires_at_ms
        && receipt.worker_principal == worker_principal
        && receipt.release == "y1"
        && receipt.cohort == "hgsvc_hprc"
        && receipt.reference_genome == "GRCh38"
        && receipt.sample_id == prepared.sample_id
        && receipt.data_layer == prepared.data_layer.value()
        && receipt.source_haplotype == prepared.source_haplotype.map(SourceHaplotype::value)
        && receipt.source_manifest_id == prepared.source_manifest_id
        && receipt.source_manifest_hash == prepared.source_manifest_hash
        && receipt.manifest_entry_id == prepared.manifest_entry_id
        && receipt.source_object_slot == prepared.source_object_slot
        && receipt.source_uri == prepared.source.uri
        && receipt.source_generation == prepared.source.generation
        && receipt.source_size_bytes == prepared.source.byte_size
        && receipt.source_checksum_algorithm == prepared.source.checksum_algorithm
        && receipt.source_checksum == prepared.source.checksum
        && receipt.source_index_object_slot == prepared.source_index_object_slot
        && receipt.source_index_uri == prepared.index.uri
        && receipt.source_index_generation == prepared.index.generation
        && receipt.source_index_size_bytes == prepared.index.byte_size
        && receipt.source_index_checksum_algorithm == prepared.index.checksum_algorithm
        && receipt.source_index_checksum == prepared.index.checksum
        && receipt.interval_start == prepared.start
        && receipt.interval_end == prepared.stop
}

fn validate_claim_predecessor(
    current: &[MethylationAttemptReceipt],
    prepared: &PreparedMethylationAttempt,
    now_ms: u64,
) -> anyhow::Result<()> {
    if current.len() > 1 {
        bail!("methylation task has duplicate maximum-revision owners");
    }
    let Some(previous) = current.first() else {
        return Ok(());
    };
    if previous.attempt_id == prepared.attempt_id || previous.lease_id == prepared.lease_id {
        bail!("methylation retry must use a new attempt and lease identity");
    }
    match previous.state.as_str() {
        "failed" => Ok(()),
        "running" if previous.lease_expires_at_ms <= now_ms => Ok(()),
        "running" => bail!("methylation task already has an unexpired current lease owner"),
        "accepted" => bail!("methylation task already has an accepted current owner"),
        _ => bail!("methylation task has an unsupported current ledger state"),
    }
}

fn record_attempt_receipt(
    target: &ClickHouseTarget,
    receipt: &MethylationAttemptReceipt,
) -> anyhow::Result<()> {
    target.insert_json_each_row(
        "lr_y1_ancillary_task_attempts",
        std::slice::from_ref(receipt),
    )
}

fn ensure_current_claim(
    target: &ClickHouseTarget,
    prepared: &PreparedMethylationAttempt,
    worker_principal: &str,
    claim_revision: u64,
    now_ms: u64,
) -> anyhow::Result<()> {
    if now_ms >= prepared.lease_expires_at_ms {
        bail!("methylation attempt lease expired");
    }
    let current = latest_task_receipts(
        target,
        &prepared.ancillary_run_id,
        prepared.data_layer.modality(),
        &prepared.chrom,
        Some(&prepared.task_id),
    )?;
    if current.len() != 1
        || current[0].revision != claim_revision
        || current[0].state != "running"
        || !receipt_matches_prepared(&current[0], prepared, worker_principal)
    {
        bail!("methylation attempt no longer owns the exact current task/lease claim");
    }
    Ok(())
}

fn canonical_hashes(
    target: &ClickHouseTarget,
    prepared: &PreparedMethylationAttempt,
) -> anyhow::Result<CanonicalHashes> {
    let table = prepared.data_layer.table();
    let haplotype = prepared
        .source_haplotype
        .map(|value| value.value().to_string())
        .unwrap_or_default();
    let parameters = vec![
        ("run", prepared.ancillary_run_id.as_str()),
        ("task", prepared.task_id.as_str()),
        ("attempt", prepared.attempt_id.as_str()),
        ("lease", prepared.lease_id.as_str()),
        ("modality", prepared.data_layer.modality()),
        ("source_version", prepared.source_manifest_id.as_str()),
        ("manifest_hash", prepared.source_manifest_hash.as_str()),
        ("manifest_entry", prepared.manifest_entry_id.as_str()),
        ("chrom", prepared.chrom.as_str()),
        ("sample", prepared.sample_id.as_str()),
        ("haplotype", haplotype.as_str()),
    ];
    let filter = "ancillary_run_id = {run:String} AND task_id = {task:String} AND attempt_id = {attempt:String} AND lease_id = {lease:String}";
    let source_haplotype = if prepared.data_layer == MethylationDataLayer::SourcePhased {
        ", source_haplotype"
    } else {
        ""
    };
    let body = target.query_text(
        &format!(
            "SELECT count() AS rows, uniqExact(tuple(chrom, position, sample_id{source_haplotype})) AS unique_keys, countIf(release != 'y1' OR cohort != 'hgsvc_hprc' OR reference_genome != 'GRCh38' OR modality != {{modality:String}} OR source_version != {{source_version:String}} OR source_manifest_hash != {{manifest_hash:String}} OR manifest_entry_id != {{manifest_entry:String}} OR chrom != {{chrom:String}} OR sample_id != {{sample:String}} OR source_start0 + 1 != source_end0 OR source_end0 != position OR position < {} OR position > {}{}) AS identity_violations, if(count()=0,0,min(position)) AS min_position, if(count()=0,0,max(position)) AS max_position FROM {table} WHERE {filter} FORMAT JSONEachRow",
            prepared.start,
            prepared.stop,
            prepared.source_haplotype.map(|_| " OR source_haplotype != {haplotype:UInt8}".to_string()).unwrap_or_default(),
        ),
        &parameters,
    )?;
    let snapshot: CanonicalAttemptSnapshot =
        serde_json::from_str(body.trim()).context("invalid methylation canonical snapshot")?;
    if snapshot.identity_violations != 0
        || snapshot.unique_keys != snapshot.rows
        || (snapshot.rows != 0
            && (snapshot.min_position < prepared.start || snapshot.max_position > prepared.stop))
    {
        bail!("methylation canonical attempt contains duplicate or cross-identity rows");
    }
    let key_columns = format!("chrom,position,sample_id{source_haplotype}");
    let content_columns = if prepared.data_layer == MethylationDataLayer::SourcePhased {
        "ancillary_run_id,task_id,attempt_id,lease_id,release,cohort,reference_genome,modality,source_version,source_manifest_hash,manifest_entry_id,chrom,source_start0,source_end0,position,sample_id,source_haplotype,methylation,coverage,estimated_modified_count,estimated_unmodified_count,discretized_methylation"
    } else {
        "ancillary_run_id,task_id,attempt_id,lease_id,release,cohort,reference_genome,modality,source_version,source_manifest_hash,manifest_entry_id,chrom,source_start0,source_end0,position,sample_id,methylation,coverage,estimated_modified_count,estimated_unmodified_count,discretized_methylation"
    };
    let order = format!("chrom,position,sample_id{source_haplotype}");
    let domain = format!(
        "methylation-key-v1\0{}\0{}\0{}\0{}\0{}",
        prepared.ancillary_run_id,
        prepared.task_id,
        prepared.attempt_id,
        prepared.lease_id,
        snapshot.rows
    );
    let key_hash = target.query_sha256(
        &format!(
            "SELECT {key_columns} FROM {table} WHERE {filter} ORDER BY {order} FORMAT RowBinary"
        ),
        &parameters,
        domain.as_bytes(),
    )?;
    let content_hash = target.query_sha256(
        &format!("SELECT {content_columns} FROM {table} WHERE {filter} ORDER BY {order} FORMAT RowBinary"),
        &parameters,
        domain.replace("key-v1", "content-v1").as_bytes(),
    )?;
    Ok(CanonicalHashes {
        rows: snapshot.rows,
        key_hash,
        content_hash,
    })
}

fn claim_methylation_attempt(
    target: &ClickHouseTarget,
    prepared: &PreparedMethylationAttempt,
    worker_principal: &str,
    started_at_ms: u64,
) -> anyhow::Result<(u64, CanonicalHashes)> {
    let current = latest_task_receipts(
        target,
        &prepared.ancillary_run_id,
        prepared.data_layer.modality(),
        &prepared.chrom,
        Some(&prepared.task_id),
    )?;
    validate_claim_predecessor(&current, prepared, started_at_ms)?;
    let initial = canonical_hashes(target, prepared)?;
    if initial.rows != 0 {
        bail!("methylation attempt has orphan canonical rows before its claim");
    }
    let revision = revision_now()?;
    let receipt = MethylationAttemptReceipt::new(
        prepared,
        worker_principal,
        MethylationLedgerState::Running,
        MethylationAttemptCounts {
            source_rows: 0,
            canonical_rows: 0,
            reject_rows: 0,
        },
        initial.key_hash.clone(),
        initial.content_hash.clone(),
        None,
        started_at_ms,
        started_at_ms,
        revision,
    );
    record_attempt_receipt(target, &receipt)?;
    ensure_current_claim(target, prepared, worker_principal, revision, started_at_ms)?;
    Ok((revision, initial))
}

fn methylation_json_row(
    prepared: &PreparedMethylationAttempt,
    record: &MethylationRecord,
) -> Value {
    let mut row = serde_json::json!({
        "ancillary_run_id": prepared.ancillary_run_id,
        "task_id": prepared.task_id,
        "attempt_id": prepared.attempt_id,
        "lease_id": prepared.lease_id,
        "release": "y1",
        "cohort": "hgsvc_hprc",
        "reference_genome": "GRCh38",
        "modality": prepared.data_layer.modality(),
        "source_version": prepared.source_manifest_id,
        "source_manifest_hash": prepared.source_manifest_hash,
        "manifest_entry_id": prepared.manifest_entry_id,
        "chrom": record.chrom,
        "source_start0": record.source_start0,
        "source_end0": record.source_end0,
        "position": record.position,
        "sample_id": prepared.sample_id,
        "methylation": record.methylation,
        "coverage": record.coverage,
        "estimated_modified_count": record.estimated_modified_count,
        "estimated_unmodified_count": record.estimated_unmodified_count,
        "discretized_methylation": record.discretized_methylation,
    });
    if let Some(haplotype) = prepared.source_haplotype {
        row["source_haplotype"] = Value::from(haplotype.value());
    }
    row
}

/// Claim and execute one exact typed interval attempt against the frozen source
/// manifest. Every acknowledged batch is synchronously inserted into the
/// inactive direct-canonical table and fenced by a current lease recheck.
pub fn run_methylation_interval_attempt(
    target: &ClickHouseTarget,
    task: &Y1MethylationTaskSpec,
    descriptor_id: &str,
    manifest_path: &Path,
    worker_principal: &str,
    batch_records: usize,
) -> anyhow::Result<MethylationAttemptReport> {
    if batch_records == 0 {
        bail!("methylation batch_records must be greater than zero");
    }
    let prepared = prepare_methylation_attempt(target, task, descriptor_id, manifest_path)?;
    let authenticated = target
        .attest_current_user(worker_principal)
        .context("failed to bind methylation attempt to currentUser()")?;
    target.attest_synchronous_inserts()?;
    let started_at_ms = epoch_ms()?;
    if started_at_ms >= prepared.lease_expires_at_ms {
        bail!("methylation attempt lease is already expired");
    }
    let started = Instant::now();
    let (claim_revision, _) =
        claim_methylation_attempt(target, &prepared, &authenticated, started_at_ms)?;
    let mut source_rows = 0u64;
    let mut reject_rows = 0u64;
    let execution = (|| -> anyhow::Result<()> {
        let mut batch = Vec::with_capacity(batch_records);
        for record in open_prepared_methylation_records(&prepared)? {
            match record {
                Ok(record) => {
                    source_rows = source_rows.checked_add(1).context("source row overflow")?;
                    batch.push(methylation_json_row(&prepared, &record));
                }
                Err(error) => {
                    reject_rows = reject_rows.checked_add(1).context("reject row overflow")?;
                    return Err(error);
                }
            }
            if batch.len() == batch_records {
                ensure_current_claim(
                    target,
                    &prepared,
                    &authenticated,
                    claim_revision,
                    epoch_ms()?,
                )?;
                target.insert_json_each_row(prepared.data_layer.table(), &batch)?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            ensure_current_claim(
                target,
                &prepared,
                &authenticated,
                claim_revision,
                epoch_ms()?,
            )?;
            target.insert_json_each_row(prepared.data_layer.table(), &batch)?;
        }
        Ok(())
    })();

    ensure_current_claim(
        target,
        &prepared,
        &authenticated,
        claim_revision,
        epoch_ms()?,
    )
    .context("stale/superseded methylation worker may not emit a terminal receipt")?;
    let hashes = canonical_hashes(target, &prepared)?;
    let accepted =
        execution.is_ok() && reject_rows == 0 && hashes.rows == source_rows && source_rows != 0;
    let error = if accepted {
        None
    } else {
        Some(match execution {
            Ok(()) if source_rows == 0 => "methylation interval produced no records".to_string(),
            Ok(()) => format!(
                "canonical rows {} do not equal parsed source rows {} or rejects were observed",
                hashes.rows, source_rows
            ),
            Err(error) => format!("{error:#}"),
        })
    };
    let finished_at_ms = epoch_ms()?;
    ensure_current_claim(
        target,
        &prepared,
        &authenticated,
        claim_revision,
        finished_at_ms,
    )
    .context("methylation lease expired or was superseded before terminal persistence")?;
    let counts = MethylationAttemptCounts {
        source_rows,
        canonical_rows: hashes.rows,
        reject_rows,
    };
    let state = if accepted {
        MethylationLedgerState::Accepted
    } else {
        MethylationLedgerState::Failed
    };
    let terminal_revision = revision_now()?.max(
        claim_revision
            .checked_add(1)
            .context("methylation claim revision overflow")?,
    );
    let terminal = MethylationAttemptReceipt::new(
        &prepared,
        &authenticated,
        state,
        counts.clone(),
        hashes.key_hash.clone(),
        hashes.content_hash.clone(),
        error.clone(),
        started_at_ms,
        finished_at_ms,
        terminal_revision,
    );
    record_attempt_receipt(target, &terminal)?;
    let current = latest_task_receipts(
        target,
        &prepared.ancillary_run_id,
        prepared.data_layer.modality(),
        &prepared.chrom,
        Some(&prepared.task_id),
    )?;
    if current.len() != 1
        || current[0].revision != terminal_revision
        || current[0].state != terminal.state
        || !receipt_matches_prepared(&current[0], &prepared, &authenticated)
    {
        bail!("methylation terminal receipt was superseded or ambiguously persisted");
    }
    let report = MethylationAttemptReport {
        identity: terminal.owner_identity(),
        worker_principal: authenticated,
        source_manifest_id: prepared.source_manifest_id,
        source: prepared.source,
        index: prepared.index,
        state,
        counts,
        key_hash: hashes.key_hash,
        content_hash: hashes.content_hash,
        started_at_ms,
        finished_at_ms,
        elapsed_ms: started.elapsed().as_millis(),
        canonical_table: prepared.data_layer.table().into(),
        error: error.clone(),
        published: false,
        joined_serving_allowed: false,
    };
    if let Some(error) = error {
        bail!("methylation attempt failed after durable terminal receipt: {error}");
    }
    Ok(report)
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MethylationLedgerState {
    Running,
    Failed,
    Accepted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
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

/// Constructed only after one authoritative query has resolved both the exact
/// expected task owners and their latest attempt/lease states.
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FrozenMethylationAttempt {
    pub identity: MethylationTaskOwnerIdentity,
    pub lease_expires_at_ms: u64,
    pub worker_principal: String,
    pub source_manifest_id: String,
    pub source: ImmutableObjectIdentity,
    pub index: ImmutableObjectIdentity,
    pub counts: MethylationAttemptCounts,
    pub key_hash: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MethylationFinalizationReport {
    pub contract_version: u16,
    pub schema_version: u16,
    pub ancillary_run_id: String,
    pub release: String,
    pub cohort: String,
    pub reference_genome: String,
    pub modality: String,
    pub data_layer: MethylationDataLayer,
    pub chrom: String,
    pub source_manifest_id: String,
    pub source_manifest_hash: String,
    pub task_manifest_sha256: String,
    pub worker_principal: String,
    pub operator_identity: String,
    pub expected_tasks: u32,
    pub source_rows: u64,
    pub canonical_rows: u64,
    pub reject_rows: u64,
    pub key_hash: String,
    pub content_hash: String,
    pub frozen_at_ms: u64,
    pub attempts: Vec<FrozenMethylationAttempt>,
    pub frozen: bool,
    pub accepted: bool,
    pub pointer_activated: bool,
    pub published: bool,
    pub joined_serving_allowed: bool,
}

#[derive(Debug, Serialize)]
struct AncillaryRunReceipt<'a> {
    ancillary_run_id: &'a str,
    release: &'static str,
    cohort: &'static str,
    reference_genome: &'static str,
    modality: &'a str,
    data_layer: &'a str,
    chrom: &'a str,
    source_version: &'a str,
    source_manifest_id: &'a str,
    source_manifest_hash: &'a str,
    scope: &'static str,
    state: &'a str,
    expected_tasks: u32,
    source_rows: u64,
    canonical_rows: u64,
    reject_rows: u64,
    key_hash: &'a str,
    content_hash: &'a str,
    worker_principal: &'a str,
    peak_rss_bytes: u64,
    frozen_at_ms: u64,
    report_json: &'a str,
    revision: u64,
}

#[derive(Debug, Deserialize)]
struct PhysicalContribution {
    task_id: String,
    attempt_id: String,
    lease_id: String,
}

type SampleLayerIntervals = BTreeMap<(String, Option<u8>), Vec<(u32, u32)>>;

fn validate_finalization_tasks(
    target: &ClickHouseTarget,
    tasks: &[Y1MethylationTaskSpec],
    manifest_path: &Path,
) -> anyhow::Result<Vec<PreparedMethylationAttempt>> {
    let Some(first) = tasks.first() else {
        bail!("methylation finalization task manifest is empty");
    };
    let mut task_ids = BTreeSet::new();
    let mut attempts = BTreeSet::new();
    let mut intervals = SampleLayerIntervals::new();
    let mut prepared = Vec::with_capacity(tasks.len());
    for task in tasks {
        if task.ancillary_run_id != first.ancillary_run_id
            || task.release != first.release
            || task.cohort != first.cohort
            || task.reference_genome != first.reference_genome
            || task.source_manifest_id != first.source_manifest_id
            || task.source_manifest_hash != first.source_manifest_hash
            || task.data_layer != first.data_layer
            || task.chrom != first.chrom
        {
            bail!("methylation finalization tasks change a run/layer/contig/manifest identity");
        }
        if !task_ids.insert(task.task_id.clone())
            || !attempts.insert((task.attempt_id.clone(), task.lease_id.clone()))
        {
            bail!("methylation finalization manifest has duplicate task or attempt/lease identity");
        }
        let value =
            prepare_methylation_attempt(target, task, &task.coordinator_task_id, manifest_path)?;
        intervals
            .entry((
                value.sample_id.clone(),
                value.source_haplotype.map(SourceHaplotype::value),
            ))
            .or_default()
            .push((value.start, value.stop));
        prepared.push(value);
    }
    for task_intervals in intervals.values_mut() {
        task_intervals.sort_unstable();
        if task_intervals.windows(2).any(|pair| pair[1].0 <= pair[0].1) {
            bail!("methylation finalization manifest overlaps one sample/layer/haplotype interval");
        }
    }
    Ok(prepared)
}

fn all_ledger_attempts(
    target: &ClickHouseTarget,
    prepared: &PreparedMethylationAttempt,
) -> anyhow::Result<BTreeSet<(String, String, String)>> {
    let body = target.query_text(
        "SELECT task_id, attempt_id, lease_id FROM lr_y1_ancillary_task_attempts WHERE ancillary_run_id = {run:String} AND modality = {modality:String} AND chrom = {chrom:String} GROUP BY task_id, attempt_id, lease_id FORMAT JSONEachRow",
        &[
            ("run", &prepared.ancillary_run_id),
            ("modality", prepared.data_layer.modality()),
            ("chrom", &prepared.chrom),
        ],
    )?;
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let row: PhysicalContribution = serde_json::from_str(line)
                .context("methylation ledger attempt inventory is malformed")?;
            Ok((row.task_id, row.attempt_id, row.lease_id))
        })
        .collect()
}

fn physical_contributions(
    target: &ClickHouseTarget,
    prepared: &PreparedMethylationAttempt,
) -> anyhow::Result<Vec<PhysicalContribution>> {
    let body = target.query_text(
        &format!(
            "SELECT task_id, attempt_id, lease_id FROM {} WHERE ancillary_run_id = {{run:String}} GROUP BY task_id, attempt_id, lease_id ORDER BY task_id, attempt_id, lease_id FORMAT JSONEachRow",
            prepared.data_layer.table()
        ),
        &[("run", &prepared.ancillary_run_id)],
    )?;
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .context("methylation canonical contribution inventory is malformed")
        })
        .collect()
}

fn cleanup_nonaccepted_contributions(
    target: &ClickHouseTarget,
    prepared: &[PreparedMethylationAttempt],
) -> anyhow::Result<()> {
    let first = &prepared[0];
    let known = all_ledger_attempts(target, first)?;
    let accepted = prepared
        .iter()
        .map(|value| {
            (
                value.task_id.clone(),
                value.attempt_id.clone(),
                value.lease_id.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    for row in physical_contributions(target, first)? {
        let key = (row.task_id, row.attempt_id, row.lease_id);
        if !known.contains(&key) {
            bail!("orphan methylation canonical contribution has no attempt-ledger identity");
        }
        if !accepted.contains(&key) {
            target.execute_with_params(
                &format!(
                    "ALTER TABLE {} DELETE WHERE ancillary_run_id = {{run:String}} AND task_id = {{task:String}} AND attempt_id = {{attempt:String}} AND lease_id = {{lease:String}} SETTINGS mutations_sync = 2",
                    first.data_layer.table()
                ),
                &[
                    ("run", &first.ancillary_run_id),
                    ("task", &key.0),
                    ("attempt", &key.1),
                    ("lease", &key.2),
                ],
            )?;
        }
    }
    let remaining = physical_contributions(target, first)?
        .into_iter()
        .map(|row| (row.task_id, row.attempt_id, row.lease_id))
        .collect::<BTreeSet<_>>();
    if remaining != accepted {
        bail!("frozen methylation canonical contributions do not exactly equal accepted owners");
    }
    Ok(())
}

fn authoritative_snapshot(
    target: &ClickHouseTarget,
    prepared: &[PreparedMethylationAttempt],
    worker_principal: &str,
    frozen_at_ms: u64,
) -> anyhow::Result<(
    AuthoritativeMethylationLedgerSnapshot,
    Vec<MethylationAttemptReceipt>,
)> {
    let first = &prepared[0];
    let receipts = latest_task_receipts(
        target,
        &first.ancillary_run_id,
        first.data_layer.modality(),
        &first.chrom,
        None,
    )?;
    if receipts.len() != prepared.len() {
        bail!("authoritative methylation query did not return exactly one current owner per expected task");
    }
    let expected = prepared
        .iter()
        .map(|value| {
            (
                value.task_id.as_str(),
                MethylationTaskOwnerIdentity {
                    ancillary_run_id: value.ancillary_run_id.clone(),
                    task_id: value.task_id.clone(),
                    attempt_id: value.attempt_id.clone(),
                    lease_id: value.lease_id.clone(),
                    data_layer: value.data_layer,
                    sample_id: value.sample_id.clone(),
                    source_haplotype: value.source_haplotype,
                    chrom: value.chrom.clone(),
                    start: value.start,
                    stop: value.stop,
                    source_manifest_hash: value.source_manifest_hash.clone(),
                    manifest_entry_id: value.manifest_entry_id.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let by_task = prepared
        .iter()
        .map(|value| (value.task_id.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    let mut resolved = Vec::with_capacity(receipts.len());
    for receipt in &receipts {
        let value = by_task
            .get(receipt.task_id.as_str())
            .context("ledger contains a task outside the expected methylation manifest")?;
        if !receipt_matches_prepared(receipt, value, worker_principal)
            || receipt.state != "accepted"
            || receipt.lease_expires_at_ms <= frozen_at_ms
            || receipt.error.is_some()
            || receipt.reject_rows != 0
            || receipt.source_rows == 0
            || receipt.source_rows != receipt.staged_rows
            || receipt.key_hash.len() != 64
            || receipt.content_hash.len() != 64
        {
            bail!("authoritative methylation owner is stale, expired, nonaccepted, malformed, or cross-identity");
        }
        resolved.push(MethylationResolvedAttempt {
            identity: receipt.owner_identity(),
            state: MethylationLedgerState::Accepted,
            ownership: MethylationLeaseOwnership::Current,
        });
    }
    Ok((
        AuthoritativeMethylationLedgerSnapshot {
            atomically_resolved: true,
            expected_task_owners: expected.into_values().collect(),
            resolved_attempts: resolved,
        },
        receipts,
    ))
}

fn aggregate_attempt_hashes<'a>(
    domain: &[u8],
    values: impl Iterator<Item = (&'a str, &'a str, &'a str)>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    for (task, key, content) in values {
        for value in [task, key, content] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

fn build_frozen_report(
    target: &ClickHouseTarget,
    prepared: &[PreparedMethylationAttempt],
    receipts: &[MethylationAttemptReceipt],
    worker_principal: &str,
    operator_identity: &str,
    task_manifest_sha256: &str,
    frozen_at_ms: u64,
) -> anyhow::Result<MethylationFinalizationReport> {
    let first = &prepared[0];
    let by_task = receipts
        .iter()
        .map(|receipt| (receipt.task_id.as_str(), receipt))
        .collect::<BTreeMap<_, _>>();
    let mut attempts = Vec::with_capacity(prepared.len());
    let mut source_rows = 0u64;
    let mut canonical_rows = 0u64;
    for value in prepared {
        let receipt = by_task
            .get(value.task_id.as_str())
            .context("accepted methylation owner disappeared")?;
        let hashes = canonical_hashes(target, value)?;
        if hashes.rows != receipt.staged_rows
            || hashes.key_hash != receipt.key_hash
            || hashes.content_hash != receipt.content_hash
        {
            bail!("frozen methylation rows differ from their accepted attempt receipt");
        }
        source_rows = source_rows
            .checked_add(receipt.source_rows)
            .context("methylation source row total overflow")?;
        canonical_rows = canonical_rows
            .checked_add(hashes.rows)
            .context("methylation canonical row total overflow")?;
        attempts.push(FrozenMethylationAttempt {
            identity: receipt.owner_identity(),
            lease_expires_at_ms: receipt.lease_expires_at_ms,
            worker_principal: receipt.worker_principal.clone(),
            source_manifest_id: receipt.source_manifest_id.clone(),
            source: value.source.clone(),
            index: value.index.clone(),
            counts: MethylationAttemptCounts {
                source_rows: receipt.source_rows,
                canonical_rows: receipt.staged_rows,
                reject_rows: receipt.reject_rows,
            },
            key_hash: receipt.key_hash.clone(),
            content_hash: receipt.content_hash.clone(),
        });
    }
    attempts.sort_by(|left, right| left.identity.task_id.cmp(&right.identity.task_id));
    let key_hash = aggregate_attempt_hashes(
        b"gnomad-lr-y1-methylation-frozen-keys-v1\0",
        attempts.iter().map(|attempt| {
            (
                attempt.identity.task_id.as_str(),
                attempt.key_hash.as_str(),
                "",
            )
        }),
    );
    let content_hash = aggregate_attempt_hashes(
        b"gnomad-lr-y1-methylation-frozen-content-v1\0",
        attempts.iter().map(|attempt| {
            (
                attempt.identity.task_id.as_str(),
                attempt.key_hash.as_str(),
                attempt.content_hash.as_str(),
            )
        }),
    );
    Ok(MethylationFinalizationReport {
        contract_version: 1,
        schema_version: super::Y1_SCHEMA_VERSION,
        ancillary_run_id: first.ancillary_run_id.clone(),
        release: "y1".into(),
        cohort: "hgsvc_hprc".into(),
        reference_genome: "GRCh38".into(),
        modality: first.data_layer.modality().into(),
        data_layer: first.data_layer,
        chrom: first.chrom.clone(),
        source_manifest_id: first.source_manifest_id.clone(),
        source_manifest_hash: first.source_manifest_hash.clone(),
        task_manifest_sha256: task_manifest_sha256.into(),
        worker_principal: worker_principal.into(),
        operator_identity: operator_identity.into(),
        expected_tasks: u32::try_from(prepared.len())?,
        source_rows,
        canonical_rows,
        reject_rows: 0,
        key_hash,
        content_hash,
        frozen_at_ms,
        attempts,
        frozen: true,
        accepted: true,
        pointer_activated: false,
        published: false,
        joined_serving_allowed: false,
    })
}

fn record_ancillary_run(
    target: &ClickHouseTarget,
    prepared: &PreparedMethylationAttempt,
    state: &str,
    worker_principal: &str,
    report: Option<&MethylationFinalizationReport>,
    message: &str,
) -> anyhow::Result<()> {
    let zero = "0".repeat(64);
    let report_json = match report {
        Some(value) => serde_json::to_string(value)?,
        None => message.to_string(),
    };
    let row = AncillaryRunReceipt {
        ancillary_run_id: &prepared.ancillary_run_id,
        release: "y1",
        cohort: "hgsvc_hprc",
        reference_genome: "GRCh38",
        modality: prepared.data_layer.modality(),
        data_layer: prepared.data_layer.value(),
        chrom: &prepared.chrom,
        source_version: &prepared.source_manifest_id,
        source_manifest_id: &prepared.source_manifest_id,
        source_manifest_hash: &prepared.source_manifest_hash,
        scope: "bounded_intervals",
        state,
        expected_tasks: report.map(|value| value.expected_tasks).unwrap_or(0),
        source_rows: report.map(|value| value.source_rows).unwrap_or(0),
        canonical_rows: report.map(|value| value.canonical_rows).unwrap_or(0),
        reject_rows: report.map(|value| value.reject_rows).unwrap_or(0),
        key_hash: report.map(|value| value.key_hash.as_str()).unwrap_or(&zero),
        content_hash: report
            .map(|value| value.content_hash.as_str())
            .unwrap_or(&zero),
        worker_principal,
        peak_rss_bytes: 0,
        frozen_at_ms: report.map(|value| value.frozen_at_ms).unwrap_or(0),
        report_json: &report_json,
        revision: revision_now()?,
    };
    target.insert_json_each_row("lr_y1_ancillary_runs", std::slice::from_ref(&row))
}

fn read_durable_methylation_report(
    target: &ClickHouseTarget,
    prepared: &PreparedMethylationAttempt,
) -> anyhow::Result<Option<(String, MethylationFinalizationReport)>> {
    let body = target.query_text(
        "WITH max_revision AS (SELECT max(revision) AS revision FROM lr_y1_ancillary_runs WHERE ancillary_run_id = {run:String} AND modality = {modality:String} AND data_layer = {layer:String} AND chrom = {chrom:String}) SELECT state, report_json, worker_principal, source_manifest_id, source_manifest_hash, expected_tasks, source_rows, canonical_rows, reject_rows, key_hash, content_hash, frozen_at_ms FROM lr_y1_ancillary_runs WHERE ancillary_run_id = {run:String} AND modality = {modality:String} AND data_layer = {layer:String} AND chrom = {chrom:String} AND revision = (SELECT revision FROM max_revision) FORMAT JSONEachRow",
        &[
            ("run", &prepared.ancillary_run_id),
            ("modality", prepared.data_layer.modality()),
            ("layer", prepared.data_layer.value()),
            ("chrom", &prepared.chrom),
        ],
    )?;
    let lines = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return Ok(None);
    }
    if lines.len() != 1 {
        bail!("methylation run ledger has duplicate maximum-revision receipts");
    }
    #[derive(Deserialize)]
    struct Row {
        state: String,
        report_json: String,
        worker_principal: String,
        source_manifest_id: String,
        source_manifest_hash: String,
        expected_tasks: u32,
        source_rows: u64,
        canonical_rows: u64,
        reject_rows: u64,
        key_hash: String,
        content_hash: String,
        frozen_at_ms: u64,
    }
    let row: Row = serde_json::from_str(lines[0]).context("malformed methylation run receipt")?;
    if !matches!(row.state.as_str(), "frozen" | "accepted_frozen") {
        return Ok(None);
    }
    let report: MethylationFinalizationReport = serde_json::from_str(&row.report_json)
        .context("durable methylation frozen report is malformed")?;
    if row.worker_principal != report.worker_principal
        || row.source_manifest_id != report.source_manifest_id
        || row.source_manifest_hash != report.source_manifest_hash
        || row.expected_tasks != report.expected_tasks
        || row.source_rows != report.source_rows
        || row.canonical_rows != report.canonical_rows
        || row.reject_rows != report.reject_rows
        || row.key_hash != report.key_hash
        || row.content_hash != report.content_hash
        || row.frozen_at_ms != report.frozen_at_ms
    {
        bail!("durable methylation run columns disagree with report_json");
    }
    Ok(Some((row.state, report)))
}

/// Fence the exact writer, resolve one authoritative current-owner snapshot,
/// clean failed prefixes, and durably freeze raw rows in place. This never
/// writes `lr_y1_active_ancillary`, summary/availability tables, or joined data.
pub fn finalize_methylation_run(
    target: &ClickHouseTarget,
    fence: &WorkerWriteFence,
    task_manifest_path: &Path,
    source_manifest_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<MethylationFinalizationReport> {
    if target.kind() != TargetKind::Scratch || operator_identity.trim().is_empty() {
        bail!("methylation finalization requires a scratch target and operator identity");
    }
    let task_bytes = std::fs::read(task_manifest_path)
        .with_context(|| format!("failed to read {}", task_manifest_path.display()))?;
    let tasks: Vec<Y1MethylationTaskSpec> =
        serde_json::from_slice(&task_bytes).context("invalid typed methylation task manifest")?;
    let prepared = validate_finalization_tasks(target, &tasks, source_manifest_path)?;
    let first = &prepared[0];
    let task_manifest_sha256 = format!("{:x}", Sha256::digest(&task_bytes));

    if let Some((state, persisted)) = read_durable_methylation_report(target, first)? {
        fence.attest_fenced_and_drained(target)?;
        let (_, receipts) =
            authoritative_snapshot(target, &prepared, fence.principal(), persisted.frozen_at_ms)?;
        let verified = build_frozen_report(
            target,
            &prepared,
            &receipts,
            fence.principal(),
            operator_identity,
            &task_manifest_sha256,
            persisted.frozen_at_ms,
        )?;
        if verified != persisted {
            bail!("durable methylation frozen report differs from exact ledger/canonical revalidation");
        }
        if state == "frozen" {
            record_ancillary_run(
                target,
                first,
                "accepted_frozen",
                fence.principal(),
                Some(&verified),
                "",
            )?;
        }
        return Ok(verified);
    }

    record_ancillary_run(
        target,
        first,
        "freezing",
        fence.principal(),
        None,
        operator_identity,
    )?;
    let result = (|| -> anyhow::Result<MethylationFinalizationReport> {
        let before = epoch_ms()?;
        let (snapshot, _) = authoritative_snapshot(target, &prepared, fence.principal(), before)?;
        let spec = Y1MethylationFinalizationSpec {
            ancillary_run_id: first.ancillary_run_id.clone(),
            release: "y1".into(),
            cohort: "hgsvc_hprc".into(),
            reference_genome: "GRCh38".into(),
            source_manifest_hash: first.source_manifest_hash.clone(),
            data_layer: first.data_layer,
            chrom: first.chrom.clone(),
            expected_tasks: u32::try_from(prepared.len())?,
            activate: false,
        };
        reconcile_authoritative_attempts(&spec, &snapshot)?;
        fence.apply_and_drain(target)?;
        let frozen_at_ms = epoch_ms()?;
        let (snapshot, receipts) =
            authoritative_snapshot(target, &prepared, fence.principal(), frozen_at_ms)?;
        reconcile_authoritative_attempts(&spec, &snapshot)?;
        cleanup_nonaccepted_contributions(target, &prepared)?;
        let report = build_frozen_report(
            target,
            &prepared,
            &receipts,
            fence.principal(),
            operator_identity,
            &task_manifest_sha256,
            frozen_at_ms,
        )?;
        record_ancillary_run(
            target,
            first,
            "frozen",
            fence.principal(),
            Some(&report),
            "",
        )?;
        let (_, reread_receipts) =
            authoritative_snapshot(target, &prepared, fence.principal(), frozen_at_ms)?;
        let reread = build_frozen_report(
            target,
            &prepared,
            &reread_receipts,
            fence.principal(),
            operator_identity,
            &task_manifest_sha256,
            frozen_at_ms,
        )?;
        if reread != report {
            bail!("methylation canonical candidate changed after durable frozen receipt");
        }
        record_ancillary_run(
            target,
            first,
            "accepted_frozen",
            fence.principal(),
            Some(&report),
            "",
        )?;
        Ok(report)
    })();
    if let Err(error) = &result {
        if read_durable_methylation_report(target, first)
            .ok()
            .flatten()
            .is_none()
        {
            let _ = record_ancillary_run(
                target,
                first,
                "finalization_failed",
                fence.principal(),
                None,
                &format!("{operator_identity}: {error:#}"),
            );
        }
    }
    result
}

/// Caller-supplied accepted-attempt IDs are not part of this planning API. The
/// operational finalizer above is the only public path that constructs a live
/// authoritative snapshot.
pub fn plan_methylation_finalization(
    target: &ClickHouseTarget,
    spec: &Y1MethylationFinalizationSpec,
) -> anyhow::Result<MethylationFinalizationPlan> {
    validate_finalization_spec(target, spec)?;
    bail!("Y1 methylation finalization requires the fenced operational finalizer or an authoritative provider snapshot")
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
        bail!("raw methylation finalization cannot activate any ancillary pointer");
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
            staging_table: "lr_y1_methylation",
            canonical_table: "lr_y1_methylation",
            accepted_attempts,
            derive_total_summary: false,
            requires_unique_canonical_keys: true,
            materialize_availability_from_roster: false,
            joined_serving_allowed: false,
        },
        MethylationDataLayer::SourcePhased => MethylationFinalizationPlan {
            staging_table: "lr_y1_methylation_phased",
            canonical_table: "lr_y1_methylation_phased",
            accepted_attempts,
            derive_total_summary: false,
            requires_unique_canonical_keys: true,
            materialize_availability_from_roster: false,
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
            lease_expires_at_ms: u64::MAX,
            release: "y1".into(),
            cohort: "hgsvc_hprc".into(),
            reference_genome: "GRCh38".into(),
            source_manifest_id: "hgsvc-hprc-y1-phased-methylation-v2".into(),
            source_manifest_hash:
                "08e394bd5d4cb25f0d830403f54773b32b77eb072443df3931ca577ae54d5ec2".into(),
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
        total.lease_expires_at_ms = 0;
        assert!(total.validate("descriptor-1").is_err());
    }

    #[test]
    fn checked_load_ready_manifest_resolves_exact_generation_size_and_md5_identities() {
        let prepared = prepare_methylation_attempt(
            &scratch_target(),
            &task(),
            "descriptor-1",
            Path::new("sources/y1/methylation-phased-source-manifest.json"),
        )
        .unwrap();
        assert_eq!(prepared.sample_id, "HG00097");
        assert_eq!(prepared.source.checksum_algorithm, "md5_base64");
        assert_eq!(prepared.index.checksum_algorithm, "md5_base64");
        assert!(prepared.source.byte_size > 0);
        assert!(prepared.index.byte_size > 0);

        let mut stale = task();
        stale.source_manifest_hash =
            "f585cbc2b806dcb52944af2ecabe634338a41323f89e3938336235c7729e8743".into();
        assert!(prepare_methylation_attempt(
            &scratch_target(),
            &stale,
            "descriptor-1",
            Path::new("sources/y1/methylation-phased-source-manifest.json"),
        )
        .is_err());
    }

    #[test]
    fn claim_predecessor_rejects_live_concurrency_and_accepts_only_fenced_retry() {
        let source_path = Path::new("sources/y1/methylation-phased-source-manifest.json");
        let original =
            prepare_methylation_attempt(&scratch_target(), &task(), "descriptor-1", source_path)
                .unwrap();
        let receipt = |state: MethylationLedgerState, expires: u64| {
            let mut value = MethylationAttemptReceipt::new(
                &original,
                "writer_a",
                state,
                MethylationAttemptCounts {
                    source_rows: 1,
                    canonical_rows: 1,
                    reject_rows: 0,
                },
                "a".repeat(64),
                "b".repeat(64),
                None,
                1,
                2,
                3,
            );
            value.lease_expires_at_ms = expires;
            value
        };
        let mut retry_task = task();
        retry_task.attempt_id = "attempt-2".into();
        retry_task.lease_id = "lease-2".into();
        let retry = prepare_methylation_attempt(
            &scratch_target(),
            &retry_task,
            "descriptor-1",
            source_path,
        )
        .unwrap();

        assert!(validate_claim_predecessor(
            &[receipt(MethylationLedgerState::Running, 100)],
            &retry,
            50,
        )
        .is_err());
        assert!(validate_claim_predecessor(
            &[receipt(MethylationLedgerState::Running, 49)],
            &retry,
            50,
        )
        .is_ok());
        assert!(validate_claim_predecessor(
            &[receipt(MethylationLedgerState::Failed, 100)],
            &retry,
            50,
        )
        .is_ok());
        assert!(validate_claim_predecessor(
            &[receipt(MethylationLedgerState::Accepted, 100)],
            &retry,
            50,
        )
        .is_err());
        assert!(validate_claim_predecessor(
            &[
                receipt(MethylationLedgerState::Failed, 49),
                receipt(MethylationLedgerState::Failed, 49),
            ],
            &retry,
            50,
        )
        .is_err());

        let mut reused = retry.clone();
        reused.attempt_id = original.attempt_id.clone();
        assert!(validate_claim_predecessor(
            &[receipt(MethylationLedgerState::Failed, 49)],
            &reused,
            50,
        )
        .is_err());
    }

    #[test]
    fn receipt_identity_rejects_principal_source_index_and_haplotype_substitution() {
        let prepared = prepare_methylation_attempt(
            &scratch_target(),
            &task(),
            "descriptor-1",
            Path::new("sources/y1/methylation-phased-source-manifest.json"),
        )
        .unwrap();
        let base = MethylationAttemptReceipt::new(
            &prepared,
            "writer_a",
            MethylationLedgerState::Accepted,
            MethylationAttemptCounts {
                source_rows: 1,
                canonical_rows: 1,
                reject_rows: 0,
            },
            "a".repeat(64),
            "b".repeat(64),
            None,
            1,
            2,
            3,
        );
        assert!(receipt_matches_prepared(&base, &prepared, "writer_a"));
        assert!(!receipt_matches_prepared(&base, &prepared, "writer_b"));
        let mut substitutions = vec![base.clone(), base.clone(), base.clone()];
        substitutions[0].source_generation = "other".into();
        substitutions[1].source_index_checksum = "other".into();
        substitutions[2].source_haplotype = Some(2);
        for substitution in substitutions {
            assert!(!receipt_matches_prepared(
                &substitution,
                &prepared,
                "writer_a"
            ));
        }
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
    fn unfenced_planning_entrypoint_cannot_construct_an_authoritative_snapshot() {
        let spec = finalization_spec(MethylationDataLayer::SampleTotal);
        let error = plan_methylation_finalization(&scratch_target(), &spec).unwrap_err();
        assert!(error.to_string().contains("fenced operational finalizer"));
    }

    #[test]
    fn authoritative_snapshot_plans_direct_canonical_raw_only_and_unjoined() {
        let total_spec = finalization_spec(MethylationDataLayer::SampleTotal);
        let total = plan_methylation_finalization_from_snapshot(
            &scratch_target(),
            &total_spec,
            &accepted_snapshot(owner(&total_spec)),
        )
        .unwrap();
        assert!(!total.derive_total_summary);
        assert!(!total.materialize_availability_from_roster);
        assert_eq!(total.staging_table, total.canonical_table);
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
        assert!(!phased.materialize_availability_from_roster);
        assert_eq!(phased.staging_table, phased.canonical_table);
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
