//! Mirror-only chr22 source-phased methylation canary tasks.
//!
//! This is deliberately one worker task path, not a finalizer. It resolves every
//! descriptor against the checked accepted mirror ledger, writes only
//! attempt-scoped raw rows to candidate staging, and emits a deterministic
//! receipt for Genohype's coordinator-owned terminal receipt.

use super::{
    attest_exact_y1_schema, AuthSource, ClickHouseTarget, MethylationRecord, MethylationSourceType,
    SourceHaplotype, TargetKind, Y1_SCHEMA_VERSION,
};
use crate::loader::immutable_gcs::{HttpGcsBackend, ImmutableGcsObject};
use crate::loader::strict_bed_reader::{StrictBedStream, ValidatedBedRecord};
use anyhow::{bail, Context};
use base64::Engine;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const MIRROR_LEDGER_CONTENT_SHA256: &str =
    "97355c54eef458b56f31a318c740dddaff7261a0d76b1d83be5078b4efb13241";
pub const MIRROR_LEDGER_RAW_SHA256: &str =
    "7f4e15a93920c842b11fc24ed3ee96aebefcc42549e001431164c2631e54b78b";
pub const MIRROR_CONTRACT_ID: &str = "mirror-only-chr22-source-phased-canary-v1";
pub const MIRROR_RUN_ID: &str = "y1-phased-mirror-chr22-canary-v1";
pub const MIRROR_WORKER_PRINCIPAL: &str = "gnomad_lr_y1_phased_worker";
const MIRROR_PREFIX: &str =
    "gs://gnomad-lr-data/sources/y1/phased-methylation-v2/full-object-mirror/";
const SOURCE_MANIFEST_ID: &str = "hgsvc-hprc-y1-phased-methylation-v2";
const SOURCE_MANIFEST_SHA256: &str =
    "f585cbc2b806dcb52944af2ecabe634338a41323f89e3938336235c7729e8743";
const COPY_MANIFEST_SHA256: &str =
    "9ba362a055f74652c3852ce46e0389b2219acca48b054cc627839105bce4b2cc";
const CHR22_STOP: u32 = 50_818_468;
const CANDIDATE_DATABASE_PREFIX: &str = "gnomad_lr_y1_scratch_phased_canary_v5_";
const STAGING_TABLE: &str = "lr_y1_methylation_phased_staging";
const KEY_HASH_DOMAIN: &[u8] = b"phased-mirror-chr22-task-key-v1";
const CONTENT_HASH_DOMAIN: &[u8] = b"phased-mirror-chr22-task-content-v1";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhasedMirrorJobSpec {
    pub action: String,
    pub schema_version: u16,
    pub contract_id: String,
    pub run_id: String,
    pub ledger_content_sha256: String,
    pub ledger_raw_sha256: String,
    pub expected_backend_revision: String,
    pub expected_worker_build_identity: String,
    pub batch_records: usize,
    pub target: PhasedMirrorTargetSpec,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhasedMirrorTargetSpec {
    pub endpoint: String,
    pub database: String,
    pub authentication: String,
    pub worker_principal: String,
}

impl PhasedMirrorJobSpec {
    pub fn validate(&self, backend_revision: &str, build_identity: &str) -> anyhow::Result<()> {
        if self.action != "load_y1_phased_mirror_chr22"
            || self.schema_version != 1
            || self.contract_id != MIRROR_CONTRACT_ID
            || self.run_id != MIRROR_RUN_ID
            || self.ledger_content_sha256 != MIRROR_LEDGER_CONTENT_SHA256
            || self.ledger_raw_sha256 != MIRROR_LEDGER_RAW_SHA256
        {
            bail!("phased mirror job does not match the one exact repository canary contract");
        }
        if self.batch_records == 0 || self.batch_records > 10_000 {
            bail!("phased mirror batch_records must be in 1..=10000");
        }
        validate_release_identity(backend_revision, build_identity)?;
        if self.expected_backend_revision != backend_revision
            || self.expected_worker_build_identity != build_identity
        {
            bail!("phased mirror task expected a different backend revision/build identity");
        }
        if self.target.authentication != "named_passwordless_private_user"
            || self.target.worker_principal != MIRROR_WORKER_PRINCIPAL
        {
            bail!("phased mirror canary requires its exact named passwordless worker principal");
        }
        let candidate_suffix = self
            .target
            .database
            .strip_prefix(CANDIDATE_DATABASE_PREFIX)
            .unwrap_or_default();
        if candidate_suffix.is_empty() {
            bail!("phased mirror task requires a fresh candidate database with the exact canary-v5 prefix");
        }
        Ok(())
    }

    pub fn target(&self) -> anyhow::Result<ClickHouseTarget> {
        ClickHouseTarget::new(
            &self.target.endpoint,
            &self.target.database,
            TargetKind::Scratch,
            AuthSource::PasswordlessUser {
                username: self.target.worker_principal.clone(),
            },
            true,
            false,
        )
    }
}

fn validate_release_identity(revision: &str, identity: &str) -> anyhow::Result<()> {
    if revision.len() != 40
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        bail!("phased mirror worker requires a full lowercase commit revision");
    }
    let host = format!("gnomad-lr/{revision}/host-release/features-clickhouse");
    let linux = format!("gnomad-lr/{revision}/x86_64-linux-release/features-clickhouse");
    if identity != host && identity != linux {
        bail!("phased mirror worker requires an exact clean revision-bound release build identity");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorTaskObject {
    pub slot: String,
    pub uri: String,
    pub generation: String,
    pub byte_size: u64,
    pub md5_base64: String,
    pub immutable_read_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhasedMirrorTaskSpec {
    pub schema_version: u16,
    pub contract_id: String,
    pub coordinator_task_id: String,
    pub label: String,
    pub run_id: String,
    pub task_id: String,
    pub attempt_prefix: String,
    pub ledger_content_sha256: String,
    pub ledger_raw_sha256: String,
    pub sample: String,
    pub source_haplotype: SourceHaplotype,
    pub chrom: String,
    pub start: u32,
    pub stop: u32,
    pub bed: MirrorTaskObject,
    pub tbi: MirrorTaskObject,
    pub joinable_to_vcf: bool,
    pub orientation_status: String,
}

impl PhasedMirrorTaskSpec {
    pub fn validate_shape(&self, descriptor_id: &str) -> anyhow::Result<()> {
        if self.schema_version != 1
            || self.contract_id != MIRROR_CONTRACT_ID
            || self.coordinator_task_id != descriptor_id
            || self.run_id != MIRROR_RUN_ID
            || self.ledger_content_sha256 != MIRROR_LEDGER_CONTENT_SHA256
            || self.ledger_raw_sha256 != MIRROR_LEDGER_RAW_SHA256
            || self.chrom != "chr22"
            || self.start != 1
            || self.stop != CHR22_STOP
            || self.joinable_to_vcf
            || self.orientation_status != "UNCONFIRMED"
        {
            bail!("phased mirror task substituted its fixed contract, interval, or orientation boundary");
        }
        let hap = match self.source_haplotype {
            SourceHaplotype::Hap1 => "hap1",
            SourceHaplotype::Hap2 => "hap2",
        };
        if self.sample.is_empty()
            || self.label != format!("{} {hap} chr22", self.sample)
            || self.task_id != format!("{}:{hap}:chr22", self.sample)
            || self.attempt_prefix != format!("{MIRROR_RUN_ID}:{}:{hap}", self.sample)
            || self.bed.slot != format!("{hap}_bed")
            || self.tbi.slot != format!("{hap}_bed_index")
        {
            bail!("phased mirror task substituted its sample/haplotype/task identity");
        }
        validate_task_object(&self.bed)?;
        validate_task_object(&self.tbi)?;
        Ok(())
    }
}

fn validate_task_object(object: &MirrorTaskObject) -> anyhow::Result<()> {
    if !object.uri.starts_with(MIRROR_PREFIX)
        || object.uri.contains('?')
        || object.generation.is_empty()
        || object.generation.starts_with('0')
        || !object.generation.bytes().all(|byte| byte.is_ascii_digit())
        || object.byte_size == 0
        || object.immutable_read_uri != format!("{}?generation={}", object.uri, object.generation)
    {
        bail!("phased mirror task object is mutable or malformed");
    }
    let md5 = base64::engine::general_purpose::STANDARD
        .decode(&object.md5_base64)
        .context("phased mirror task object MD5 is not base64")?;
    if md5.len() != 16 {
        bail!("phased mirror task object MD5 is not 16 bytes");
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MirrorLedger {
    accepted_at: String,
    byte_count: u64,
    content_sha256: String,
    copy_manifest_canonical_sha256: String,
    copy_semantics: CopySemantics,
    destination_prefix: String,
    load_authorization_blockers: Vec<String>,
    load_authorized: bool,
    mirror_accepted: bool,
    object_count: usize,
    objects: Vec<LedgerObject>,
    reconciliation: Reconciliation,
    sample_count: usize,
    schema_version: u16,
    source_manifest_content_sha256: String,
    source_manifest_id: String,
    status: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CopySemantics {
    delete: bool,
    destination_precondition: String,
    overwrite: bool,
    public_access: bool,
    source: String,
}
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Reconciliation {
    duplicates: usize,
    extra: usize,
    identity_mismatches: usize,
    missing: usize,
    size_md5_equal_original: bool,
    unique_destination_generations: usize,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerObject {
    mirror: LedgerMirror,
    original: LedgerOriginal,
    sample_id: String,
    slot: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerOriginal {
    byte_size: u64,
    generation: String,
    immutable_read_uri: String,
    md5_base64: String,
    uri: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerMirror {
    byte_size: u64,
    crc32c_base64: String,
    created_at: String,
    generation: String,
    immutable_read_uri: String,
    md5_base64: String,
    uri: String,
}

fn verified_ledger() -> anyhow::Result<&'static MirrorLedger> {
    static LEDGER: OnceCell<MirrorLedger> = OnceCell::new();
    LEDGER.get_or_try_init(|| {
        verify_ledger_bytes(include_bytes!(
            "../../sources/y1/methylation-phased-mirror-ledger.json"
        ))
    })
}

fn verify_ledger_bytes(bytes: &[u8]) -> anyhow::Result<MirrorLedger> {
    if format!("{:x}", Sha256::digest(bytes)) != MIRROR_LEDGER_RAW_SHA256 {
        bail!("checked mirror ledger raw SHA-256 drift");
    }
    let mut value: Value =
        serde_json::from_slice(bytes).context("checked mirror ledger is invalid JSON")?;
    let recorded = value
        .get("content_sha256")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("mirror ledger must be an object"))?
        .remove("content_sha256");
    let canonical = format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?));
    if recorded != MIRROR_LEDGER_CONTENT_SHA256 || canonical != MIRROR_LEDGER_CONTENT_SHA256 {
        bail!("checked mirror ledger canonical content SHA-256 drift");
    }
    let ledger: MirrorLedger = serde_json::from_slice(bytes)?;
    validate_ledger(&ledger)?;
    Ok(ledger)
}

fn validate_ledger(ledger: &MirrorLedger) -> anyhow::Result<()> {
    if ledger.schema_version != 1
        || ledger.status != "accepted_pool_readable_mirror"
        || ledger.content_sha256 != MIRROR_LEDGER_CONTENT_SHA256
        || ledger.copy_manifest_canonical_sha256 != COPY_MANIFEST_SHA256
        || ledger.destination_prefix != MIRROR_PREFIX
        || ledger.source_manifest_id != SOURCE_MANIFEST_ID
        || ledger.source_manifest_content_sha256 != SOURCE_MANIFEST_SHA256
        || !ledger.mirror_accepted
        || ledger.load_authorized
        || ledger.object_count != 924
        || ledger.sample_count != 231
        || ledger.byte_count != 127_463_220_748
    {
        bail!("checked mirror ledger top-level accepted identity differs");
    }
    if ledger.copy_semantics
        != (CopySemantics {
            delete: false,
            destination_precondition: "does_not_exist".into(),
            overwrite: false,
            public_access: false,
            source: "exact original generation".into(),
        })
        || ledger.reconciliation
            != (Reconciliation {
                duplicates: 0,
                extra: 0,
                identity_mismatches: 0,
                missing: 0,
                size_md5_equal_original: true,
                unique_destination_generations: 924,
            })
    {
        bail!("checked mirror ledger copy/reconciliation contract is not mismatch-free");
    }
    if ledger.objects.len() != 924 {
        bail!("checked mirror ledger does not contain 924 objects");
    }
    let mut seen = BTreeSet::new();
    let mut generations = BTreeSet::new();
    let mut samples = BTreeSet::new();
    let mut bytes = 0u64;
    let slots = ["hap1_bed", "hap1_bed_index", "hap2_bed", "hap2_bed_index"];
    for object in &ledger.objects {
        if object.sample_id.is_empty()
            || !slots.contains(&object.slot.as_str())
            || !seen.insert((object.sample_id.clone(), object.slot.clone()))
        {
            bail!("mirror ledger contains an invalid or duplicate sample/slot quartet");
        }
        samples.insert(object.sample_id.clone());
        validate_ledger_identity(
            &object.original.uri,
            &object.original.generation,
            object.original.byte_size,
            &object.original.md5_base64,
            &object.original.immutable_read_uri,
            false,
        )?;
        validate_ledger_identity(
            &object.mirror.uri,
            &object.mirror.generation,
            object.mirror.byte_size,
            &object.mirror.md5_base64,
            &object.mirror.immutable_read_uri,
            true,
        )?;
        if object.original.byte_size != object.mirror.byte_size
            || object.original.md5_base64 != object.mirror.md5_base64
        {
            bail!("mirror ledger original/mirror size or MD5 mismatch");
        }
        if !generations.insert(object.mirror.generation.clone()) {
            bail!("mirror ledger destination generation is not unique");
        }
        bytes = bytes
            .checked_add(object.mirror.byte_size)
            .ok_or_else(|| anyhow::anyhow!("mirror ledger byte count overflow"))?;
    }
    if samples.len() != 231 || generations.len() != 924 || bytes != ledger.byte_count {
        bail!("mirror ledger sample/generation/byte counts differ");
    }
    for sample in samples {
        for slot in slots {
            if !seen.contains(&(sample.clone(), slot.into())) {
                bail!("mirror ledger sample lacks an exact four-slot quartet");
            }
        }
    }
    Ok(())
}

fn validate_ledger_identity(
    uri: &str,
    generation: &str,
    size: u64,
    md5: &str,
    immutable: &str,
    mirror: bool,
) -> anyhow::Result<()> {
    if !uri.starts_with("gs://")
        || uri.contains('?')
        || size == 0
        || generation.is_empty()
        || generation.starts_with('0')
        || !generation.bytes().all(|b| b.is_ascii_digit())
        || immutable != format!("{uri}?generation={generation}")
        || (mirror && !uri.starts_with(MIRROR_PREFIX))
    {
        bail!("mirror ledger contains a mutable or malformed object identity");
    }
    let decoded = base64::engine::general_purpose::STANDARD.decode(md5)?;
    if decoded.len() != 16 {
        bail!("mirror ledger MD5 is not 16 bytes");
    }
    Ok(())
}

fn exact_task_object(object: &LedgerObject) -> MirrorTaskObject {
    MirrorTaskObject {
        slot: object.slot.clone(),
        uri: object.mirror.uri.clone(),
        generation: object.mirror.generation.clone(),
        byte_size: object.mirror.byte_size,
        md5_base64: object.mirror.md5_base64.clone(),
        immutable_read_uri: object.mirror.immutable_read_uri.clone(),
    }
}

fn descriptor_ordinal(descriptor_id: &str) -> anyhow::Result<usize> {
    let encoded = descriptor_id
        .strip_prefix("custom_")
        .ok_or_else(|| anyhow::anyhow!("phased mirror descriptor ID is not canonical custom_N"))?;
    if encoded.is_empty()
        || !encoded.bytes().all(|byte| byte.is_ascii_digit())
        || (encoded.len() > 1 && encoded.starts_with('0'))
    {
        bail!("phased mirror descriptor ID is not canonical custom_N");
    }
    encoded
        .parse()
        .context("phased mirror descriptor ordinal is out of range")
}

fn canonical_task(descriptor_id: &str) -> anyhow::Result<&'static PhasedMirrorTaskSpec> {
    let ordinal = descriptor_ordinal(descriptor_id)?;
    static TASKS: OnceCell<Vec<PhasedMirrorTaskSpec>> = OnceCell::new();
    let tasks = TASKS.get_or_try_init(|| {
        let tasks: Vec<PhasedMirrorTaskSpec> = serde_json::from_str(include_str!(
            "../../manifests/y1/phased-methylation-mirror-chr22-canary.json"
        ))
        .context("checked phased mirror task manifest is invalid")?;
        if tasks.len() != 462 {
            bail!("checked phased mirror task manifest does not contain 462 tasks");
        }
        Ok::<_, anyhow::Error>(tasks)
    })?;
    let task = tasks
        .get(ordinal)
        .ok_or_else(|| anyhow::anyhow!("phased mirror descriptor ordinal is out of range"))?;
    if task.coordinator_task_id != descriptor_id {
        bail!("checked phased mirror task manifest is not canonically ordered");
    }
    Ok(task)
}

pub fn validate_task_against_ledger(
    task: &PhasedMirrorTaskSpec,
    descriptor_id: &str,
) -> anyhow::Result<()> {
    let expected = canonical_task(descriptor_id)?;
    if task != expected {
        bail!("phased mirror task does not equal its canonical manifest ordinal");
    }
    task.validate_shape(descriptor_id)?;
    let ledger = verified_ledger()?;
    let objects = ledger
        .objects
        .iter()
        .filter(|object| object.sample_id == task.sample)
        .map(|object| (object.slot.as_str(), object))
        .collect::<BTreeMap<_, _>>();
    if objects.len() != 4 {
        bail!("phased mirror task sample is absent or lacks its exact ledger quartet");
    }
    let bed = objects
        .get(task.bed.slot.as_str())
        .ok_or_else(|| anyhow::anyhow!("task BED slot is absent from ledger"))?;
    let tbi = objects
        .get(task.tbi.slot.as_str())
        .ok_or_else(|| anyhow::anyhow!("task TBI slot is absent from ledger"))?;
    if task.bed != exact_task_object(bed) || task.tbi != exact_task_object(tbi) {
        bail!("phased mirror task substituted exact ledger object identity");
    }
    Ok(())
}

fn gcs_object(object: &MirrorTaskObject) -> ImmutableGcsObject {
    ImmutableGcsObject {
        uri: object.uri.clone(),
        generation: object.generation.clone(),
        byte_size: object.byte_size,
        checksum_algorithm: "md5_base64".into(),
        checksum: object.md5_base64.clone(),
        immutable_read_uri: object.immutable_read_uri.clone(),
    }
}

fn open_records(
    task: &PhasedMirrorTaskSpec,
) -> anyhow::Result<impl Iterator<Item = anyhow::Result<MethylationRecord>>> {
    let expected = match task.source_haplotype {
        SourceHaplotype::Hap1 => MethylationSourceType::Hap1,
        SourceHaplotype::Hap2 => MethylationSourceType::Hap2,
    };
    let stream = StrictBedStream::open_immutable_region(
        Arc::new(HttpGcsBackend::new()?),
        &gcs_object(&task.bed),
        &gcs_object(&task.tbi),
        &task.chrom,
        task.start,
        task.stop,
        move |line: &str| {
            let record = super::methylation::parse_methylation_source_record(line)?;
            if record.source_type != expected {
                bail!("phased mirror source type substituted its haplotype");
            }
            Ok(ValidatedBedRecord {
                chrom: record.chrom,
                start0: record.source_start0,
                end0: record.source_end0,
            })
        },
    )?;
    let chrom = task.chrom.clone();
    Ok(stream.records().map(move |line| {
        let line = line?;
        super::methylation::parse_methylation_record(&line, &chrom, expected)
    }))
}

#[derive(Debug, Clone, Serialize)]
struct StagingRow {
    ancillary_run_id: String,
    attempt_id: String,
    release: &'static str,
    cohort: &'static str,
    reference_genome: &'static str,
    modality: &'static str,
    source_version: &'static str,
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

fn row(task: &PhasedMirrorTaskSpec, attempt_id: &str, record: MethylationRecord) -> StagingRow {
    StagingRow {
        ancillary_run_id: task.run_id.clone(),
        attempt_id: attempt_id.into(),
        release: "y1",
        cohort: "hgsvc_hprc",
        reference_genome: "GRCh38",
        modality: "per_haplotype_methylation",
        source_version: "accepted-phased-mirror-ledger-97355c54",
        chrom: record.chrom,
        source_start0: record.source_start0,
        source_end0: record.source_end0,
        position: record.position,
        sample_id: task.sample.clone(),
        source_haplotype: task.source_haplotype.value(),
        methylation: record.methylation,
        coverage: record.coverage,
        estimated_modified_count: record.estimated_modified_count,
        estimated_unmodified_count: record.estimated_unmodified_count,
        discretized_methylation: record.discretized_methylation,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Readback {
    rows: u64,
    key_sha256: String,
    content_sha256: String,
}

fn canonical_hasher(domain: &[u8]) -> Sha256 {
    let mut h = Sha256::new();
    h.update(b"gnomad-lr-y1-canonical-content-v1\0");
    h.update(domain);
    h.update([0]);
    h
}
fn encode_string(value: &str, out: &mut Vec<u8>) {
    let mut n = value.len() as u64;
    loop {
        let mut b = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(value.as_bytes());
}
fn encode_key(row: &StagingRow, out: &mut Vec<u8>) {
    encode_string(&row.ancillary_run_id, out);
    encode_string(&row.attempt_id, out);
    encode_string(&row.chrom, out);
    out.extend_from_slice(&row.position.to_le_bytes());
    encode_string(&row.sample_id, out);
    out.push(row.source_haplotype);
    out.extend_from_slice(&row.source_start0.to_le_bytes());
    out.extend_from_slice(&row.source_end0.to_le_bytes());
}
fn encode_content(row: &StagingRow, out: &mut Vec<u8>) {
    for value in [
        &row.ancillary_run_id,
        row.attempt_id.as_str(),
        row.release,
        row.cohort,
        row.reference_genome,
        row.modality,
        row.source_version,
        row.chrom.as_str(),
    ] {
        encode_string(value, out);
    }
    out.extend_from_slice(&row.source_start0.to_le_bytes());
    out.extend_from_slice(&row.source_end0.to_le_bytes());
    out.extend_from_slice(&row.position.to_le_bytes());
    encode_string(&row.sample_id, out);
    out.push(row.source_haplotype);
    out.extend_from_slice(&row.methylation.to_bits().to_le_bytes());
    out.extend_from_slice(&row.coverage.to_le_bytes());
    out.extend_from_slice(&row.estimated_modified_count.to_le_bytes());
    out.extend_from_slice(&row.estimated_unmodified_count.to_le_bytes());
    out.extend_from_slice(&row.discretized_methylation.to_bits().to_le_bytes());
}

fn readback(
    target: &ClickHouseTarget,
    task: &PhasedMirrorTaskSpec,
    attempt_id: &str,
) -> anyhow::Result<Readback> {
    let where_clause="WHERE ancillary_run_id={run:String} AND attempt_id={attempt:String} AND sample_id={sample:String} AND source_haplotype={hap:UInt8}";
    let hap = task.source_haplotype.value().to_string();
    let params = [
        ("run", task.run_id.as_str()),
        ("attempt", attempt_id),
        ("sample", task.sample.as_str()),
        ("hap", hap.as_str()),
    ];
    let count = target.query_text(
        &format!("SELECT count() FROM {STAGING_TABLE} {where_clause} FORMAT TabSeparated"),
        &params,
    )?;
    let rows = count
        .trim()
        .parse::<u64>()
        .context("malformed phased mirror staging count")?;
    let order="ancillary_run_id, attempt_id, chrom, position, sample_id, source_haplotype, source_start0, source_end0";
    let key=target.query_sha256(&format!("SELECT ancillary_run_id, attempt_id, chrom, position, sample_id, source_haplotype, source_start0, source_end0 FROM {STAGING_TABLE} {where_clause} ORDER BY {order} FORMAT RowBinary"),&params,KEY_HASH_DOMAIN)?;
    let content=target.query_sha256(&format!("SELECT ancillary_run_id, attempt_id, release, cohort, reference_genome, modality, source_version, chrom, source_start0, source_end0, position, sample_id, source_haplotype, methylation, coverage, estimated_modified_count, estimated_unmodified_count, discretized_methylation FROM {STAGING_TABLE} {where_clause} ORDER BY {order} FORMAT RowBinary"),&params,CONTENT_HASH_DOMAIN)?;
    Ok(Readback {
        rows,
        key_sha256: key,
        content_sha256: content,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct PhasedMirrorTaskReceipt {
    schema_version: u16,
    capability: &'static str,
    status: &'static str,
    contract_id: &'static str,
    run_id: String,
    coordinator_task_id: String,
    task_id: String,
    assignment_attempt: u64,
    attempt_id: String,
    sample: String,
    source_haplotype: SourceHaplotype,
    interval: ReceiptInterval,
    source: MirrorTaskObject,
    index: MirrorTaskObject,
    ledger_content_sha256: &'static str,
    ledger_raw_sha256: &'static str,
    backend_revision: &'static str,
    worker_build_identity: &'static str,
    worker_identity: String,
    authenticated_principal: String,
    schema_version_written: u16,
    table: &'static str,
    rows: u64,
    rejects: u64,
    key_sha256: String,
    content_sha256: String,
    joinable_to_vcf: bool,
    orientation_status: &'static str,
    final_or_serving_tables_written: bool,
}
#[derive(Debug, Clone, Serialize)]
struct ReceiptInterval {
    chrom: &'static str,
    start: u32,
    stop: u32,
    coordinate_convention: &'static str,
}

impl PhasedMirrorTaskReceipt {
    pub fn rows(&self) -> u64 {
        self.rows
    }
}

pub fn run_phased_mirror_task(
    target: &ClickHouseTarget,
    task: &PhasedMirrorTaskSpec,
    descriptor_id: &str,
    assignment_attempt: u64,
    attempt_id: &str,
    worker_identity: &str,
    backend_revision: &'static str,
    build_identity: &'static str,
    batch_records: usize,
) -> anyhow::Result<PhasedMirrorTaskReceipt> {
    validate_task_against_ledger(task, descriptor_id)?;
    validate_release_identity(backend_revision, build_identity)?;
    if assignment_attempt == 0 || attempt_id.is_empty() || worker_identity.is_empty() {
        bail!("phased mirror task lacks its exact assignment/worker identity");
    }
    let principal = target.attest_current_user(MIRROR_WORKER_PRINCIPAL)?;
    target.attest_synchronous_inserts()?;
    attest_exact_y1_schema(target)?;
    let actual = stage_records(target, task, attempt_id, batch_records, open_records(task)?)?;
    Ok(PhasedMirrorTaskReceipt {
        schema_version: 1,
        capability: "mirror_only_chr22_source_phased_task",
        status: "verified_candidate_staging_only",
        contract_id: MIRROR_CONTRACT_ID,
        run_id: task.run_id.clone(),
        coordinator_task_id: descriptor_id.into(),
        task_id: task.task_id.clone(),
        assignment_attempt,
        attempt_id: attempt_id.into(),
        sample: task.sample.clone(),
        source_haplotype: task.source_haplotype,
        interval: ReceiptInterval {
            chrom: "chr22",
            start: 1,
            stop: CHR22_STOP,
            coordinate_convention: "one_based_inclusive",
        },
        source: task.bed.clone(),
        index: task.tbi.clone(),
        ledger_content_sha256: MIRROR_LEDGER_CONTENT_SHA256,
        ledger_raw_sha256: MIRROR_LEDGER_RAW_SHA256,
        backend_revision,
        worker_build_identity: build_identity,
        worker_identity: worker_identity.into(),
        authenticated_principal: principal,
        schema_version_written: Y1_SCHEMA_VERSION,
        table: STAGING_TABLE,
        rows: actual.rows,
        rejects: 0,
        key_sha256: actual.key_sha256,
        content_sha256: actual.content_sha256,
        joinable_to_vcf: false,
        orientation_status: "UNCONFIRMED",
        final_or_serving_tables_written: false,
    })
}

fn stage_records<I>(
    target: &ClickHouseTarget,
    task: &PhasedMirrorTaskSpec,
    attempt_id: &str,
    batch_records: usize,
    records: I,
) -> anyhow::Result<Readback>
where
    I: IntoIterator<Item = anyhow::Result<MethylationRecord>>,
{
    if batch_records == 0 || batch_records > 10_000 {
        bail!("phased mirror batch_records must be in 1..=10000");
    }
    if readback(target, task, attempt_id)?.rows != 0 {
        bail!(
            "phased mirror attempt identity already has staging rows; refusing a silent duplicate"
        );
    }
    let mut key_hasher = canonical_hasher(KEY_HASH_DOMAIN);
    let mut content_hasher = canonical_hasher(CONTENT_HASH_DOMAIN);
    let mut count = 0u64;
    let mut batch = Vec::with_capacity(batch_records);
    let mut previous = None;
    for record in records {
        let record = record?;
        let key = (record.position, record.source_start0, record.source_end0);
        if previous.is_some_and(|prior| prior >= key) {
            bail!("phased mirror source contains duplicate or out-of-order canonical keys");
        }
        previous = Some(key);
        let staged = row(task, attempt_id, record);
        let mut encoded = Vec::new();
        encode_key(&staged, &mut encoded);
        key_hasher.update(&encoded);
        encoded.clear();
        encode_content(&staged, &mut encoded);
        content_hasher.update(&encoded);
        batch.push(staged);
        count += 1;
        if batch.len() == batch_records {
            target.insert_json_each_row(STAGING_TABLE, &batch)?;
            batch.clear();
        }
    }
    if count == 0 {
        bail!("phased mirror chr22 task returned zero records");
    }
    if !batch.is_empty() {
        target.insert_json_each_row(STAGING_TABLE, &batch)?;
    }
    let expected = Readback {
        rows: count,
        key_sha256: format!("{:x}", key_hasher.finalize()),
        content_sha256: format!("{:x}", content_hasher.finalize()),
    };
    let actual = readback(target, task, attempt_id)?;
    if actual != expected {
        bail!("phased mirror task staging readback count/content identity mismatch");
    }
    Ok(actual)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_task() -> PhasedMirrorTaskSpec {
        let tasks: Vec<PhasedMirrorTaskSpec> = serde_json::from_str(include_str!(
            "../../manifests/y1/phased-methylation-mirror-chr22-canary.json"
        ))
        .unwrap();
        tasks.into_iter().next().unwrap()
    }

    #[test]
    fn accepted_ledger_and_all_generated_tasks_resolve_exactly() {
        let ledger = verified_ledger().unwrap();
        assert_eq!(ledger.objects.len(), 924);
        let tasks: Vec<PhasedMirrorTaskSpec> = serde_json::from_str(include_str!(
            "../../manifests/y1/phased-methylation-mirror-chr22-canary.json"
        ))
        .unwrap();
        assert_eq!(tasks.len(), 462);
        for (index, task) in tasks.iter().enumerate() {
            validate_task_against_ledger(task, &format!("custom_{index}")).unwrap();
        }
    }

    #[test]
    fn complete_valid_task_cannot_move_to_another_ordinal() {
        let tasks: Vec<PhasedMirrorTaskSpec> = serde_json::from_str(include_str!(
            "../../manifests/y1/phased-methylation-mirror-chr22-canary.json"
        ))
        .unwrap();
        let mut moved = tasks[2].clone();
        moved.coordinator_task_id = "custom_0".into();
        assert!(validate_task_against_ledger(&moved, "custom_0").is_err());
    }

    #[test]
    fn noncanonical_and_out_of_range_descriptor_ids_fail() {
        let task = manifest_task();
        for descriptor_id in [
            "custom_00",
            "custom_01",
            "custom_",
            "custom_-1",
            "custom_+1",
            "custom_462",
            "custom_999999999999999999999999999999999999999999999999",
            "other_0",
        ] {
            let mut rewritten = task.clone();
            rewritten.coordinator_task_id = descriptor_id.into();
            assert!(
                validate_task_against_ledger(&rewritten, descriptor_id).is_err(),
                "{descriptor_id}"
            );
        }
    }

    #[test]
    fn substituted_task_source_interval_and_orientation_fail_before_io() {
        let original = manifest_task();
        let mut cases = Vec::new();
        let mut value = serde_json::to_value(&original).unwrap();
        value["bed"]["generation"] = Value::String("1".into());
        cases.push(value);
        let mut value = serde_json::to_value(&original).unwrap();
        value["sample"] = Value::String("HG00099".into());
        cases.push(value);
        let mut value = serde_json::to_value(&original).unwrap();
        value["stop"] = Value::from(CHR22_STOP - 1);
        cases.push(value);
        let mut value = serde_json::to_value(&original).unwrap();
        value["joinable_to_vcf"] = Value::Bool(true);
        cases.push(value);
        let mut value = serde_json::to_value(&original).unwrap();
        value["source_uri"] = Value::String("gs://mutable".into());
        assert!(serde_json::from_value::<PhasedMirrorTaskSpec>(value).is_err());
        for value in cases {
            let task: PhasedMirrorTaskSpec = serde_json::from_value(value).unwrap();
            assert!(validate_task_against_ledger(&task, "custom_0").is_err());
        }
    }

    #[test]
    fn job_rejects_principal_build_and_password_fields() {
        let revision = "a".repeat(40);
        let identity = format!("gnomad-lr/{revision}/host-release/features-clickhouse");
        let base = serde_json::json!({"action":"load_y1_phased_mirror_chr22","schema_version":1,"contract_id":MIRROR_CONTRACT_ID,"run_id":MIRROR_RUN_ID,"ledger_content_sha256":MIRROR_LEDGER_CONTENT_SHA256,"ledger_raw_sha256":MIRROR_LEDGER_RAW_SHA256,"expected_backend_revision":revision,"expected_worker_build_identity":identity,"batch_records":250,"target":{"endpoint":"http://127.0.0.1:8123","database":"gnomad_lr_y1_scratch_phased_canary_v5_unit","authentication":"named_passwordless_private_user","worker_principal":MIRROR_WORKER_PRINCIPAL}});
        let job: PhasedMirrorJobSpec = serde_json::from_value(base.clone()).unwrap();
        job.validate(&revision, &identity).unwrap();
        let mut changed = base.clone();
        changed["target"]["worker_principal"] = Value::String("default".into());
        let job: PhasedMirrorJobSpec = serde_json::from_value(changed).unwrap();
        assert!(job.validate(&revision, &identity).is_err());
        let mut changed = base.clone();
        changed["expected_worker_build_identity"] = Value::String(format!(
            "gnomad-lr/{revision}-dirty/host-release/features-clickhouse"
        ));
        let job: PhasedMirrorJobSpec = serde_json::from_value(changed).unwrap();
        assert!(job.validate(&revision, &identity).is_err());
        let mut changed = base;
        changed["target"]["password"] = Value::String("secret".into());
        assert!(serde_json::from_value::<PhasedMirrorJobSpec>(changed).is_err());
    }

    #[test]
    fn passwordless_target_pins_named_user_without_password_state() {
        let target = ClickHouseTarget::new(
            "http://10.1.2.3:8123",
            "gnomad_lr_y1_scratch_phased_canary_unit",
            TargetKind::Scratch,
            AuthSource::PasswordlessUser {
                username: MIRROR_WORKER_PRINCIPAL.into(),
            },
            true,
            false,
        )
        .unwrap();
        assert_eq!(target.database(), "gnomad_lr_y1_scratch_phased_canary_unit");
        assert!(ClickHouseTarget::new(
            "https://example.org:8123",
            "gnomad_lr_y1_scratch_phased_canary_unit",
            TargetKind::Scratch,
            AuthSource::PasswordlessUser {
                username: MIRROR_WORKER_PRINCIPAL.into()
            },
            true,
            false
        )
        .is_err());
    }

    fn fixture_record(haplotype: SourceHaplotype, position: u32) -> MethylationRecord {
        MethylationRecord {
            chrom: "chr22".into(),
            source_start0: position - 1,
            source_end0: position,
            position,
            methylation: 50.0,
            source_type: match haplotype {
                SourceHaplotype::Hap1 => MethylationSourceType::Hap1,
                SourceHaplotype::Hap2 => MethylationSourceType::Hap2,
            },
            coverage: 2,
            estimated_modified_count: 1,
            estimated_unmodified_count: 1,
            discretized_methylation: 50.0,
        }
    }

    #[test]
    #[ignore = "requires GNOMAD_LR_LOCAL_CLICKHOUSE_MIRROR_URL for a disposable local ClickHouse"]
    fn local_clickhouse_two_haplotype_tasks_touch_only_staging() {
        let endpoint = std::env::var("GNOMAD_LR_LOCAL_CLICKHOUSE_MIRROR_URL")
            .expect("set GNOMAD_LR_LOCAL_CLICKHOUSE_MIRROR_URL");
        let database = format!(
            "gnomad_lr_y1_scratch_phased_canary_v5_fixture_{:012}",
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
            let admin = ClickHouseTarget::new(
                &endpoint,
                &database,
                TargetKind::Scratch,
                AuthSource::None,
                false,
                false,
            )?;
            super::super::init_schema(&admin)?;
            execute(&format!(
                "CREATE USER IF NOT EXISTS {MIRROR_WORKER_PRINCIPAL} IDENTIFIED WITH no_password"
            ))?;
            execute(&format!(
                "GRANT SELECT ON {database}.* TO {MIRROR_WORKER_PRINCIPAL}"
            ))?;
            execute(&format!(
                "GRANT INSERT ON {database}.{STAGING_TABLE} TO {MIRROR_WORKER_PRINCIPAL}"
            ))?;
            let worker = ClickHouseTarget::new(
                &endpoint,
                &database,
                TargetKind::Scratch,
                AuthSource::PasswordlessUser {
                    username: MIRROR_WORKER_PRINCIPAL.into(),
                },
                false,
                false,
            )?;
            worker.attest_current_user(MIRROR_WORKER_PRINCIPAL)?;
            attest_exact_y1_schema(&worker)?;
            assert!(admin.attest_current_user(MIRROR_WORKER_PRINCIPAL).is_err());

            let tasks: Vec<PhasedMirrorTaskSpec> = serde_json::from_str(include_str!(
                "../../manifests/y1/phased-methylation-mirror-chr22-canary.json"
            ))?;
            for (index, task) in tasks.iter().take(2).enumerate() {
                validate_task_against_ledger(task, &format!("custom_{index}"))?;
                let receipt = stage_records(
                    &worker,
                    task,
                    &format!("fixture-attempt-{index}"),
                    1,
                    [Ok(fixture_record(
                        task.source_haplotype,
                        100 + index as u32,
                    ))],
                )?;
                assert_eq!(receipt.rows, 1);
            }
            assert_eq!(
                worker
                    .query_text(
                        &format!("SELECT count() FROM {STAGING_TABLE} FORMAT TabSeparated"),
                        &[],
                    )?
                    .trim(),
                "2"
            );
            for protected in [
                "lr_y1_methylation_phased",
                "lr_y1_active_ancillary",
                "lr_y1_methylation_summary",
                "lr_y1_methylation_availability",
            ] {
                assert_eq!(
                    worker
                        .query_text(
                            &format!("SELECT count() FROM {protected} FORMAT TabSeparated"),
                            &[],
                        )?
                        .trim(),
                    "0"
                );
            }
            Ok(())
        })();
        let cleanup_database = execute(&format!("DROP DATABASE {database} SYNC"));
        let cleanup_user = execute(&format!("DROP USER IF EXISTS {MIRROR_WORKER_PRINCIPAL}"));
        result.unwrap();
        cleanup_database.unwrap();
        cleanup_user.unwrap();
    }
}
