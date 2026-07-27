//! Fail-closed, deterministic Y1 sample metadata reconciliation.
//!
//! This module is deliberately independent of the legacy `lr_sample_metadata`
//! loader. All source bytes are pinned by a manifest and reconciliation occurs
//! completely in memory before any ClickHouse write.

use crate::domain::SUBPOP_TO_SUPERPOP;
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const VALID_SUPERPOPS: [&str; 7] = ["AFR", "AMR", "EAS", "EUR", "SAS", "ASJ", "OTH"];

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceChecksum {
    pub algorithm: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceIdentity {
    pub id: String,
    pub uri: String,
    pub immutable_version: String,
    pub byte_size: u64,
    pub checksum: SourceChecksum,
    pub acquired_on: String,
    pub license: String,
    pub provenance: String,
    pub intended_use: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RosterSource {
    #[serde(flatten)]
    pub artifact: SourceIdentity,
    pub derived_from_vcf: SourceIdentity,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetadataSources {
    pub roster: RosterSource,
    pub primary_hprc: SourceIdentity,
    pub supplemental_ancestry: SourceIdentity,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedMetadataFacts {
    pub roster_rows: usize,
    pub primary_source_rows: usize,
    pub primary_roster_overlap: usize,
    pub primary_absent_from_roster: usize,
    pub supplemental_source_rows: usize,
    pub supplemental_unique_samples: usize,
    pub supplemental_assignments: usize,
    pub primary_retained_conflicts: usize,
    pub exact_supplemental_duplicates: usize,
    pub distribution: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetadataSourceManifest {
    pub schema_version: u16,
    pub manifest_id: String,
    pub release: String,
    pub cohort: String,
    pub reference_genome: String,
    pub sources: MetadataSources,
    pub expected: ExpectedMetadataFacts,
}

impl MetadataSourceManifest {
    pub fn from_path(path: &Path) -> anyhow::Result<Self> {
        let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        let manifest: Self =
            serde_json::from_slice(&bytes).context("invalid metadata source manifest JSON")?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.schema_version != 1 {
            bail!(
                "unsupported metadata manifest schema version {}",
                self.schema_version
            );
        }
        if self.release != "y1" || self.cohort != "hgsvc_hprc" || self.reference_genome != "GRCh38"
        {
            bail!("metadata publication is restricted to y1/hgsvc_hprc/GRCh38");
        }
        if self.expected.roster_rows != 292 {
            bail!("Y1 roster expectation must be exactly 292");
        }
        let identities = [
            &self.sources.roster.artifact,
            &self.sources.roster.derived_from_vcf,
            &self.sources.primary_hprc,
            &self.sources.supplemental_ancestry,
        ];
        let mut ids = BTreeSet::new();
        for source in identities {
            for (field, value) in [
                ("id", source.id.as_str()),
                ("uri", source.uri.as_str()),
                ("immutable_version", source.immutable_version.as_str()),
                ("acquired_on", source.acquired_on.as_str()),
                ("license", source.license.as_str()),
                ("provenance", source.provenance.as_str()),
                ("intended_use", source.intended_use.as_str()),
            ] {
                if value.trim().is_empty() {
                    bail!("source {} has blank {}", source.id, field);
                }
            }
            if !ids.insert(&source.id) {
                bail!("duplicate source identity {}", source.id);
            }
            if source.byte_size == 0 {
                bail!("source {} has zero byte size", source.id);
            }
            if source.checksum.algorithm != "sha256" && source.checksum.algorithm != "md5_base64" {
                bail!("source {} uses unsupported checksum algorithm", source.id);
            }
            if source.checksum.value.trim().is_empty() {
                bail!("source {} has blank checksum", source.id);
            }
        }
        Ok(())
    }

    pub fn load_inputs(&self, manifest_path: &Path) -> anyhow::Result<PinnedInputs> {
        let base = manifest_path.parent().unwrap_or_else(|| Path::new("."));
        Ok(PinnedInputs {
            roster: load_and_verify(base, &self.sources.roster.artifact)?,
            primary: load_and_verify(base, &self.sources.primary_hprc)?,
            supplemental: load_and_verify(base, &self.sources.supplemental_ancestry)?,
        })
    }
}

pub struct PinnedInputs {
    pub roster: Vec<u8>,
    pub primary: Vec<u8>,
    pub supplemental: Vec<u8>,
}

fn load_and_verify(base: &Path, source: &SourceIdentity) -> anyhow::Result<Vec<u8>> {
    // Publication consumes repository-owned mirrors. The immutable upstream
    // identity remains in the manifest, but no moving network content is read.
    let path = source
        .uri
        .strip_prefix("file://")
        .map(PathBuf::from)
        .unwrap_or_else(|| base.join(&source.uri));
    let bytes = fs::read(&path).with_context(|| {
        format!(
            "failed to read pinned source {} at {}",
            source.id,
            path.display()
        )
    })?;
    if bytes.len() as u64 != source.byte_size {
        bail!(
            "source {} byte size mismatch: expected {}, observed {}",
            source.id,
            source.byte_size,
            bytes.len()
        );
    }
    if source.checksum.algorithm != "sha256" {
        bail!("local source {} must use sha256", source.id);
    }
    let observed = format!("{:x}", Sha256::digest(&bytes));
    if observed != source.checksum.value.to_ascii_lowercase() {
        bail!(
            "source {} checksum mismatch: expected {}, observed {}",
            source.id,
            source.checksum.value,
            observed
        );
    }
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReconciledMetadataRow {
    pub sample_id: String,
    pub roster_index: u16,
    pub subpopulation: String,
    pub superpopulation: String,
    pub population_descriptor: String,
    pub sex: String,
    pub collection: String,
    pub primary_metadata_present: u8,
    pub ancestry_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetadataAuditEvent {
    pub sample_id: String,
    pub roster_index: u16,
    pub event_type: String,
    pub primary_present: bool,
    pub primary_subpopulation: Option<String>,
    pub primary_superpopulation: Option<String>,
    pub supplemental_ethnicity: Option<String>,
    pub selected_superpopulation: Option<String>,
    pub selected_source: Option<String>,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReconciliationResult {
    pub rows: Vec<ReconciledMetadataRow>,
    pub audit: Vec<MetadataAuditEvent>,
    pub compact_audit: Vec<MetadataAuditEvent>,
    pub primary_source_rows: usize,
    pub primary_roster_overlap: usize,
    pub supplemental_source_rows: usize,
    pub supplemental_unique_samples: usize,
    pub distribution: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
struct PrimaryRow {
    subpopulation: String,
    descriptor: String,
    sex: String,
    collection: String,
}
#[derive(Debug, Clone)]
struct SupplementalRow {
    ethnicity: Option<String>,
    population: String,
    color: String,
}

pub fn reconcile_metadata(
    roster_bytes: &[u8],
    primary_bytes: &[u8],
    supplemental_bytes: &[u8],
) -> anyhow::Result<ReconciliationResult> {
    let roster_text = std::str::from_utf8(roster_bytes).context("roster is not UTF-8")?;
    let mut roster = Vec::new();
    let mut roster_seen = BTreeSet::new();
    for raw in roster_text.lines() {
        let id = raw.trim();
        if id.is_empty() {
            bail!("blank sample ID in roster");
        }
        if !roster_seen.insert(id.to_string()) {
            bail!("duplicate roster sample ID {id}");
        }
        roster.push(id.to_string());
    }
    if roster.len() > u16::MAX as usize {
        bail!("roster is too large for UInt16 roster_index");
    }

    let (primary, primary_source_rows) = parse_primary(primary_bytes)?;
    let (supplemental, supplemental_source_rows, mut duplicate_audit) =
        parse_supplemental(supplemental_bytes)?;
    let roster_indices: BTreeMap<_, _> = roster
        .iter()
        .enumerate()
        .map(|(index, sample)| (sample.as_str(), index as u16))
        .collect();
    for event in &mut duplicate_audit {
        if let Some(index) = roster_indices.get(event.sample_id.as_str()) {
            event.roster_index = *index;
        }
    }
    let primary_roster_overlap = roster.iter().filter(|id| primary.contains_key(*id)).count();
    let supplemental_unique_samples = supplemental.len();
    let mut rows = Vec::with_capacity(roster.len());
    let mut audit = Vec::with_capacity(roster.len() + duplicate_audit.len());

    for (zero_index, sample_id) in roster.iter().enumerate() {
        let roster_index = u16::try_from(zero_index).unwrap();
        let p = primary.get(sample_id);
        let s = supplemental
            .get(sample_id)
            .and_then(|row| row.ethnicity.clone());
        let primary_super = p
            .and_then(|row| SUBPOP_TO_SUPERPOP.get(row.subpopulation.as_str()).copied())
            .map(str::to_string);
        let (selected, source, event_type, details) = match (&primary_super, &s) {
            (Some(primary_value), Some(supplemental_value))
                if primary_value != supplemental_value =>
            {
                (
                    primary_value.clone(),
                    "hprc",
                    "source_conflict_primary_retained",
                    format!(
                        "primary {primary_value} retained over supplemental {supplemental_value}"
                    ),
                )
            }
            (Some(primary_value), _) => (
                primary_value.clone(),
                "hprc",
                "primary_assignment",
                String::new(),
            ),
            (None, Some(supplemental_value)) => (
                supplemental_value.clone(),
                "supplemental",
                "ancestry_from_supplemental",
                String::new(),
            ),
            (None, None) => {
                audit.push(make_event(
                    sample_id,
                    roster_index,
                    "unresolved_sample",
                    p,
                    primary_super,
                    s,
                    None,
                    None,
                    "neither source supplies a recognized superpopulation",
                ));
                continue;
            }
        };
        let (subpopulation, descriptor, sex, collection) = match p {
            Some(row) => (
                value_or_na(&row.subpopulation),
                value_or_na(&row.descriptor),
                value_or_na(&row.sex),
                value_or_na(&row.collection),
            ),
            None => (
                "N/A".into(),
                "HGSVC/HPRC ancestry label".into(),
                "N/A".into(),
                "N/A".into(),
            ),
        };
        rows.push(ReconciledMetadataRow {
            sample_id: sample_id.clone(),
            roster_index,
            subpopulation,
            superpopulation: selected.clone(),
            population_descriptor: descriptor,
            sex,
            collection,
            primary_metadata_present: u8::from(p.is_some()),
            ancestry_source: source.into(),
        });
        audit.push(make_event(
            sample_id,
            roster_index,
            event_type,
            p,
            primary_super,
            s,
            Some(selected),
            Some(source.into()),
            &details,
        ));
    }
    if rows.len() != roster.len() {
        let unresolved: Vec<_> = audit
            .iter()
            .filter(|e| e.event_type == "unresolved_sample")
            .map(|e| e.sample_id.as_str())
            .collect();
        bail!(
            "{} roster samples are unresolved: {}",
            unresolved.len(),
            unresolved.join(",")
        );
    }
    audit.extend(duplicate_audit);
    audit.sort_by(|a, b| (&a.sample_id, &a.event_type).cmp(&(&b.sample_id, &b.event_type)));
    let compact_audit = audit
        .iter()
        .filter(|e| {
            matches!(
                e.event_type.as_str(),
                "ancestry_from_supplemental" | "source_conflict_primary_retained"
            )
        })
        .cloned()
        .collect();
    let mut distribution = BTreeMap::new();
    for row in &rows {
        *distribution.entry(row.superpopulation.clone()).or_insert(0) += 1;
    }
    Ok(ReconciliationResult {
        rows,
        audit,
        compact_audit,
        primary_source_rows,
        primary_roster_overlap,
        supplemental_source_rows,
        supplemental_unique_samples,
        distribution,
    })
}

fn value_or_na(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        "N/A".into()
    } else {
        value.into()
    }
}

fn make_event(
    sample_id: &str,
    roster_index: u16,
    event_type: &str,
    primary: Option<&PrimaryRow>,
    primary_superpopulation: Option<String>,
    supplemental_ethnicity: Option<String>,
    selected_superpopulation: Option<String>,
    selected_source: Option<String>,
    details: &str,
) -> MetadataAuditEvent {
    MetadataAuditEvent {
        sample_id: sample_id.into(),
        roster_index,
        event_type: event_type.into(),
        primary_present: primary.is_some(),
        primary_subpopulation: primary.map(|p| p.subpopulation.clone()),
        primary_superpopulation,
        supplemental_ethnicity,
        selected_superpopulation,
        selected_source,
        details: details.into(),
    }
}

fn parse_primary(bytes: &[u8]) -> anyhow::Result<(BTreeMap<String, PrimaryRow>, usize)> {
    let mut reader = csv::ReaderBuilder::new().flexible(false).from_reader(bytes);
    let headers = reader
        .headers()
        .context("invalid primary CSV header")?
        .clone();
    let index = |name: &str| {
        headers
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| anyhow::anyhow!("primary CSV missing {name} column"))
    };
    let sample = index("sample_id")?;
    let subpop = index("population_abbreviation")?;
    let descriptor = index("population_descriptor")?;
    let sex = index("sex")?;
    let collection = index("collection")?;
    let mut rows = BTreeMap::new();
    let mut count = 0;
    for record in reader.records() {
        let record = record.context("invalid quoted primary CSV row")?;
        count += 1;
        let id = record.get(sample).unwrap_or("").trim();
        if id.is_empty() {
            bail!("blank primary sample ID at data row {count}");
        }
        let row = PrimaryRow {
            subpopulation: record.get(subpop).unwrap_or("").trim().into(),
            descriptor: record.get(descriptor).unwrap_or("").trim().into(),
            sex: record.get(sex).unwrap_or("").trim().into(),
            collection: record.get(collection).unwrap_or("").trim().into(),
        };
        if rows.insert(id.into(), row).is_some() {
            bail!("duplicate primary sample ID {id}");
        }
    }
    Ok((rows, count))
}

fn normalize_ethnicity(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value == "NA" {
        return None;
    }
    let value = if value == "ASK" { "ASJ" } else { value };
    VALID_SUPERPOPS.contains(&value).then(|| value.to_string())
}

fn parse_supplemental(
    bytes: &[u8],
) -> anyhow::Result<(
    BTreeMap<String, SupplementalRow>,
    usize,
    Vec<MetadataAuditEvent>,
)> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .flexible(false)
        .from_reader(bytes);
    let headers = reader
        .headers()
        .context("invalid supplemental TSV header")?
        .clone();
    let index = |name: &str| {
        headers
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| anyhow::anyhow!("supplemental TSV missing {name} column"))
    };
    let sample = index("sample")?;
    let ethnicity = index("ethnicity")?;
    let population = index("Population")?;
    let color = index("color")?;
    let mut rows: BTreeMap<String, SupplementalRow> = BTreeMap::new();
    let mut count = 0;
    let mut audit = Vec::new();
    for record in reader.records() {
        let record = record.context("invalid supplemental TSV row")?;
        count += 1;
        let id = record.get(sample).unwrap_or("").trim();
        if id.is_empty() {
            bail!("blank supplemental sample ID at data row {count}");
        }
        let row = SupplementalRow {
            ethnicity: normalize_ethnicity(record.get(ethnicity).unwrap_or("")),
            population: record.get(population).unwrap_or("").trim().into(),
            color: record.get(color).unwrap_or("").trim().into(),
        };
        if let Some(previous) = rows.get(id) {
            if previous.ethnicity != row.ethnicity {
                bail!("invalid_or_conflicting_duplicate: supplemental rows for {id} disagree on normalized ethnicity");
            }
            let ignored_differ =
                previous.population != row.population || previous.color != row.color;
            audit.push(MetadataAuditEvent {
                sample_id: id.into(),
                roster_index: u16::MAX,
                event_type: "exact_duplicate_supplemental_row".into(),
                primary_present: false,
                primary_subpopulation: None,
                primary_superpopulation: None,
                supplemental_ethnicity: row.ethnicity.clone(),
                selected_superpopulation: None,
                selected_source: None,
                details: if ignored_differ {
                    "consumed values match; ignored Population/color differ".into()
                } else {
                    "exact semantic duplicate accepted".into()
                },
            });
        } else {
            rows.insert(id.into(), row);
        }
    }
    Ok((rows, count, audit))
}

pub fn validate_expected(
    result: &ReconciliationResult,
    manifest: &MetadataSourceManifest,
) -> anyhow::Result<()> {
    let e = &manifest.expected;
    let checks = [
        ("output rows", result.rows.len(), e.roster_rows),
        (
            "primary source rows",
            result.primary_source_rows,
            e.primary_source_rows,
        ),
        (
            "primary roster overlap",
            result.primary_roster_overlap,
            e.primary_roster_overlap,
        ),
        (
            "supplemental source rows",
            result.supplemental_source_rows,
            e.supplemental_source_rows,
        ),
        (
            "supplemental unique samples",
            result.supplemental_unique_samples,
            e.supplemental_unique_samples,
        ),
        (
            "supplemental assignments",
            result
                .audit
                .iter()
                .filter(|a| a.event_type == "ancestry_from_supplemental")
                .count(),
            e.supplemental_assignments,
        ),
        (
            "primary-retained conflicts",
            result
                .audit
                .iter()
                .filter(|a| a.event_type == "source_conflict_primary_retained")
                .count(),
            e.primary_retained_conflicts,
        ),
        (
            "exact supplemental duplicates",
            result
                .audit
                .iter()
                .filter(|a| a.event_type == "exact_duplicate_supplemental_row")
                .count(),
            e.exact_supplemental_duplicates,
        ),
    ];
    for (label, observed, expected) in checks {
        if observed != expected {
            bail!("{label} mismatch: expected {expected}, observed {observed}");
        }
    }
    let absent = result
        .rows
        .iter()
        .filter(|r| r.primary_metadata_present == 0)
        .count();
    if absent != e.primary_absent_from_roster {
        bail!(
            "primary-absent roster rows mismatch: expected {}, observed {absent}",
            e.primary_absent_from_roster
        );
    }
    if result.distribution != e.distribution {
        bail!(
            "superpopulation distribution mismatch: expected {:?}, observed {:?}",
            e.distribution,
            result.distribution
        );
    }
    if result.rows.iter().any(|r| {
        r.sample_id.trim().is_empty()
            || r.superpopulation == "N/A"
            || r.superpopulation.trim().is_empty()
    }) {
        bail!("blank sample or unresolved superpopulation in output");
    }
    let unique: BTreeSet<_> = result.rows.iter().map(|r| &r.sample_id).collect();
    if unique.len() != result.rows.len() {
        bail!("output sample IDs are not unique");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct CarrierJoinValidation {
    pub carrier_run_id: String,
    pub carrier_rows: u64,
    pub distinct_carrier_samples: u64,
    pub unmatched_samples: u64,
    pub one_to_many_samples: u64,
}

#[derive(Debug, Serialize)]
struct MetadataReport<'a> {
    schema_version: u16,
    metadata_run_id: &'a str,
    source_manifest_id: &'a str,
    source_manifest_sha256: &'a str,
    rows_sha256: String,
    audit_sha256: String,
    counts: BTreeMap<&'static str, usize>,
    distribution: &'a BTreeMap<String, usize>,
    carrier_joins: &'a [CarrierJoinValidation],
    rows: &'a [ReconciledMetadataRow],
    audit: &'a [MetadataAuditEvent],
}

#[derive(Serialize)]
struct StoredMetadataRow<'a> {
    metadata_run_id: &'a str,
    release: &'a str,
    cohort: &'a str,
    reference_genome: &'a str,
    #[serde(flatten)]
    row: &'a ReconciledMetadataRow,
    source_manifest_id: &'a str,
    source_manifest_sha256: &'a str,
}

#[derive(Serialize)]
struct StoredAuditRow<'a> {
    metadata_run_id: &'a str,
    release: &'a str,
    cohort: &'a str,
    reference_genome: &'a str,
    sample_id: &'a str,
    roster_index: u16,
    event_type: &'a str,
    primary_present: u8,
    primary_subpopulation: &'a Option<String>,
    primary_superpopulation: &'a Option<String>,
    supplemental_ethnicity: &'a Option<String>,
    selected_superpopulation: &'a Option<String>,
    selected_source: &'a Option<String>,
    details: &'a str,
    source_manifest_id: &'a str,
    source_manifest_sha256: &'a str,
}

#[derive(Serialize)]
struct MetadataRunRow<'a> {
    metadata_run_id: &'a str,
    revision: u64,
    state: String,
    release: &'a str,
    cohort: &'a str,
    reference_genome: &'a str,
    source_manifest_id: &'a str,
    source_manifest_sha256: &'a str,
    source_manifest_json: &'a str,
    expected_roster_rows: u16,
    observed_roster_rows: u16,
    primary_source_rows: u16,
    primary_roster_overlap: u16,
    supplemental_source_rows: u16,
    supplemental_unique_samples: u16,
    output_rows: u16,
    supplemental_assignments: u16,
    primary_retained_conflicts: u16,
    exact_supplemental_duplicates: u16,
    audit_rows: u16,
    publisher_identity: &'a str,
    report_uri: &'a str,
    report_sha256: String,
    created_at_ms: u64,
    completed_at_ms: u64,
    failure_reason: String,
}

fn now_ms() -> anyhow::Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock precedes epoch")?
        .as_millis()
        .try_into()?)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn write_report_files(
    path: &Path,
    run_id: &str,
    manifest: &MetadataSourceManifest,
    manifest_sha: &str,
    result: &ReconciliationResult,
    joins: &[CarrierJoinValidation],
) -> anyhow::Result<String> {
    let rows_json = serde_json::to_vec(&result.rows)?;
    let audit_json = serde_json::to_vec(&result.audit)?;
    let mut counts = BTreeMap::new();
    counts.insert("roster_rows", result.rows.len());
    counts.insert("primary_source_rows", result.primary_source_rows);
    counts.insert("primary_roster_overlap", result.primary_roster_overlap);
    counts.insert(
        "primary_absent_rows",
        result
            .rows
            .iter()
            .filter(|r| r.primary_metadata_present == 0)
            .count(),
    );
    counts.insert("supplemental_source_rows", result.supplemental_source_rows);
    counts.insert(
        "supplemental_unique_samples",
        result.supplemental_unique_samples,
    );
    counts.insert(
        "supplemental_assignments",
        result
            .compact_audit
            .iter()
            .filter(|e| e.event_type == "ancestry_from_supplemental")
            .count(),
    );
    counts.insert(
        "primary_retained_conflicts",
        result
            .compact_audit
            .iter()
            .filter(|e| e.event_type == "source_conflict_primary_retained")
            .count(),
    );
    counts.insert(
        "exact_supplemental_duplicates",
        result
            .audit
            .iter()
            .filter(|e| e.event_type == "exact_duplicate_supplemental_row")
            .count(),
    );
    let report = MetadataReport {
        schema_version: 1,
        metadata_run_id: run_id,
        source_manifest_id: &manifest.manifest_id,
        source_manifest_sha256: manifest_sha,
        rows_sha256: sha256(&rows_json),
        audit_sha256: sha256(&audit_json),
        counts,
        distribution: &result.distribution,
        carrier_joins: joins,
        rows: &result.rows,
        audit: &result.audit,
    };
    let bytes = serde_json::to_vec_pretty(&report)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, &bytes)?;
    fs::rename(temporary, path)?;
    let compact_path = path.with_extension("compact.json");
    fs::write(
        &compact_path,
        serde_json::to_vec_pretty(&result.compact_audit)?,
    )?;
    let jsonl_path = path.with_extension("audit.jsonl");
    let mut jsonl = Vec::new();
    for event in &result.audit {
        serde_json::to_writer(&mut jsonl, event)?;
        jsonl.push(b'\n');
    }
    fs::write(jsonl_path, jsonl)?;
    Ok(sha256(&bytes))
}

fn parse_u64_fields(text: &str, expected: usize, label: &str) -> anyhow::Result<Vec<u64>> {
    let values: Vec<_> = text
        .trim()
        .split('\t')
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse::<u64>()
                .with_context(|| format!("invalid {label} result {v}"))
        })
        .collect::<Result<_, _>>()?;
    if values.len() != expected {
        bail!(
            "{label} returned {} fields, expected {expected}",
            values.len()
        );
    }
    Ok(values)
}

pub fn reconcile_and_publish(
    target: &super::ClickHouseTarget,
    metadata_run_id: &str,
    manifest_path: &Path,
    report_path: &Path,
    publisher_identity: &str,
    carrier_run_ids: &[String],
) -> anyhow::Result<Vec<CarrierJoinValidation>> {
    if metadata_run_id.trim().is_empty() || publisher_identity.trim().is_empty() {
        bail!("metadata run ID and publisher identity must be non-blank");
    }
    super::storage::init_schema(target)?;
    let manifest_bytes = fs::read(manifest_path)?;
    let manifest_sha = sha256(&manifest_bytes);
    let manifest_json = String::from_utf8(manifest_bytes).context("manifest is not UTF-8")?;
    let manifest = MetadataSourceManifest::from_path(manifest_path)?;
    let existing = target.query_text("SELECT count() FROM lr_y1_metadata_runs WHERE metadata_run_id = {metadata_run_id:String} FORMAT TabSeparated", &[("metadata_run_id", metadata_run_id)])?;
    if existing.trim() != "0" {
        bail!("metadata run ID {metadata_run_id} already exists and is immutable");
    }
    let inputs = manifest.load_inputs(manifest_path)?;
    let result = reconcile_metadata(&inputs.roster, &inputs.primary, &inputs.supplemental)?;
    validate_expected(&result, &manifest)?;
    // Required pre-staging artifact. It is replaced atomically with the final
    // carrier-validation report after candidate queries complete.
    let initial_report_sha = write_report_files(
        report_path,
        metadata_run_id,
        &manifest,
        &manifest_sha,
        &result,
        &[],
    )?;
    let created = now_ms()?;
    let counts = |kind: &str| result.audit.iter().filter(|e| e.event_type == kind).count() as u16;
    let ledger = |revision: u64,
                  state: &str,
                  completed_at_ms: u64,
                  failure_reason: &str,
                  report_sha: &str| MetadataRunRow {
        metadata_run_id,
        revision,
        state: state.to_string(),
        release: &manifest.release,
        cohort: &manifest.cohort,
        reference_genome: &manifest.reference_genome,
        source_manifest_id: &manifest.manifest_id,
        source_manifest_sha256: &manifest_sha,
        source_manifest_json: &manifest_json,
        expected_roster_rows: manifest.expected.roster_rows as u16,
        observed_roster_rows: result.rows.len() as u16,
        primary_source_rows: result.primary_source_rows as u16,
        primary_roster_overlap: result.primary_roster_overlap as u16,
        supplemental_source_rows: result.supplemental_source_rows as u16,
        supplemental_unique_samples: result.supplemental_unique_samples as u16,
        output_rows: result.rows.len() as u16,
        supplemental_assignments: counts("ancestry_from_supplemental"),
        primary_retained_conflicts: counts("source_conflict_primary_retained"),
        exact_supplemental_duplicates: counts("exact_duplicate_supplemental_row"),
        audit_rows: result.audit.len() as u16,
        publisher_identity,
        report_uri: report_path.to_str().unwrap_or(""),
        report_sha256: report_sha.to_string(),
        created_at_ms: created,
        completed_at_ms,
        failure_reason: failure_reason.to_string(),
    };
    target.insert_json_each_row(
        "lr_y1_metadata_runs",
        &[ledger(created, "created", 0, "", &initial_report_sha)],
    )?;

    let operation = || -> anyhow::Result<Vec<CarrierJoinValidation>> {
        let stored: Vec<_> = result
            .rows
            .iter()
            .map(|row| StoredMetadataRow {
                metadata_run_id,
                release: &manifest.release,
                cohort: &manifest.cohort,
                reference_genome: &manifest.reference_genome,
                row,
                source_manifest_id: &manifest.manifest_id,
                source_manifest_sha256: &manifest_sha,
            })
            .collect();
        let audit: Vec<_> = result
            .audit
            .iter()
            .map(|event| StoredAuditRow {
                metadata_run_id,
                release: &manifest.release,
                cohort: &manifest.cohort,
                reference_genome: &manifest.reference_genome,
                sample_id: &event.sample_id,
                roster_index: event.roster_index,
                event_type: &event.event_type,
                primary_present: u8::from(event.primary_present),
                primary_subpopulation: &event.primary_subpopulation,
                primary_superpopulation: &event.primary_superpopulation,
                supplemental_ethnicity: &event.supplemental_ethnicity,
                selected_superpopulation: &event.selected_superpopulation,
                selected_source: &event.selected_source,
                details: &event.details,
                source_manifest_id: &manifest.manifest_id,
                source_manifest_sha256: &manifest_sha,
            })
            .collect();
        target.insert_json_each_row("lr_y1_sample_metadata_staging", &stored)?;
        target.insert_json_each_row("lr_y1_metadata_audit_staging", &audit)?;
        let candidate = target.query_text("SELECT count(), uniqExact(sample_id), countIf(empty(sample_id)), countIf(empty(superpopulation) OR superpopulation = 'N/A'), countIf(primary_metadata_present = 1), countIf(primary_metadata_present = 0), countIf(ancestry_source = 'supplemental') FROM lr_y1_sample_metadata_staging WHERE metadata_run_id = {metadata_run_id:String} AND release = {release:String} AND cohort = {cohort:String} AND reference_genome = {reference_genome:String} FORMAT TabSeparated", &[("metadata_run_id", metadata_run_id), ("release", &manifest.release), ("cohort", &manifest.cohort), ("reference_genome", &manifest.reference_genome)])?;
        let c = parse_u64_fields(&candidate, 7, "candidate validation")?;
        let expected = [
            manifest.expected.roster_rows as u64,
            manifest.expected.roster_rows as u64,
            0,
            0,
            manifest.expected.primary_roster_overlap as u64,
            manifest.expected.primary_absent_from_roster as u64,
            manifest.expected.supplemental_assignments as u64,
        ];
        if c.as_slice() != expected {
            bail!(
                "candidate acceptance counts mismatch: expected {:?}, observed {:?}",
                expected,
                c
            );
        }
        let distribution_text = target.query_text("SELECT superpopulation, count() FROM lr_y1_sample_metadata_staging WHERE metadata_run_id = {metadata_run_id:String} GROUP BY superpopulation ORDER BY superpopulation FORMAT TabSeparated", &[("metadata_run_id", metadata_run_id)])?;
        let mut candidate_distribution = BTreeMap::new();
        for line in distribution_text.lines() {
            let (population, count) = line
                .split_once('\t')
                .context("invalid candidate distribution row")?;
            candidate_distribution.insert(population.to_string(), count.parse::<usize>()?);
        }
        if candidate_distribution != manifest.expected.distribution {
            bail!(
                "candidate distribution mismatch: expected {:?}, observed {:?}",
                manifest.expected.distribution,
                candidate_distribution
            );
        }
        let audit_counts = target.query_text("SELECT count(), countIf(event_type = 'ancestry_from_supplemental'), countIf(event_type = 'source_conflict_primary_retained'), countIf(event_type = 'exact_duplicate_supplemental_row') FROM lr_y1_metadata_audit_staging WHERE metadata_run_id = {metadata_run_id:String} FORMAT TabSeparated", &[("metadata_run_id", metadata_run_id)])?;
        let observed_audit = parse_u64_fields(&audit_counts, 4, "candidate audit validation")?;
        let expected_audit = [
            result.audit.len() as u64,
            manifest.expected.supplemental_assignments as u64,
            manifest.expected.primary_retained_conflicts as u64,
            manifest.expected.exact_supplemental_duplicates as u64,
        ];
        if observed_audit.as_slice() != expected_audit {
            bail!(
                "candidate audit counts mismatch: expected {:?}, observed {:?}",
                expected_audit,
                observed_audit
            );
        }
        let mut joins = Vec::new();
        for carrier_run_id in carrier_run_ids {
            let query = "WITH carriers AS (SELECT sample_id, count() AS rows FROM lr_y1_carriers WHERE run_id = {carrier_run_id:String} AND release = {release:String} AND cohort = {cohort:String} AND reference_genome = {reference_genome:String} GROUP BY sample_id), metadata AS (SELECT sample_id, count() AS matches FROM lr_y1_sample_metadata_staging WHERE metadata_run_id = {metadata_run_id:String} AND release = {release:String} AND cohort = {cohort:String} AND reference_genome = {reference_genome:String} GROUP BY sample_id) SELECT sum(carriers.rows), count(), countIf(ifNull(metadata.matches, 0) = 0), countIf(ifNull(metadata.matches, 0) > 1) FROM carriers LEFT JOIN metadata USING sample_id FORMAT TabSeparated";
            let text = target.query_text(
                query,
                &[
                    ("carrier_run_id", carrier_run_id),
                    ("metadata_run_id", metadata_run_id),
                    ("release", &manifest.release),
                    ("cohort", &manifest.cohort),
                    ("reference_genome", &manifest.reference_genome),
                ],
            )?;
            let v = parse_u64_fields(&text, 4, "carrier join validation")?;
            if v[2] != 0 || v[3] != 0 {
                bail!("carrier run {carrier_run_id} metadata join failed: {} unmatched, {} one-to-many", v[2], v[3]);
            }
            joins.push(CarrierJoinValidation {
                carrier_run_id: carrier_run_id.clone(),
                carrier_rows: v[0],
                distinct_carrier_samples: v[1],
                unmatched_samples: v[2],
                one_to_many_samples: v[3],
            });
        }
        target.execute_with_params("INSERT INTO lr_y1_sample_metadata SELECT * FROM lr_y1_sample_metadata_staging WHERE metadata_run_id = {metadata_run_id:String}", &[("metadata_run_id", metadata_run_id)])?;
        target.execute_with_params("INSERT INTO lr_y1_metadata_audit SELECT * FROM lr_y1_metadata_audit_staging WHERE metadata_run_id = {metadata_run_id:String}", &[("metadata_run_id", metadata_run_id)])?;
        Ok(joins)
    };
    match operation() {
        Ok(joins) => {
            let report_sha = write_report_files(
                report_path,
                metadata_run_id,
                &manifest,
                &manifest_sha,
                &result,
                &joins,
            )?;
            let completed = now_ms()?;
            target.insert_json_each_row(
                "lr_y1_metadata_runs",
                &[ledger(completed, "accepted", completed, "", &report_sha)],
            )?;
            Ok(joins)
        }
        Err(error) => {
            let completed = now_ms()?;
            let reason = format!("{error:#}");
            target.insert_json_each_row(
                "lr_y1_metadata_runs",
                &[ledger(
                    completed,
                    "rejected",
                    completed,
                    &reason,
                    &initial_report_sha,
                )],
            )?;
            Err(error)
        }
    }
}

pub fn activate_metadata(
    target: &super::ClickHouseTarget,
    metadata_run_id: &str,
    activated_by: &str,
) -> anyhow::Result<String> {
    if target.kind() != super::TargetKind::Serving {
        bail!("metadata activation and rollback require a serving target");
    }
    let accepted = target.query_text("SELECT argMax(state, revision) FROM lr_y1_metadata_runs WHERE metadata_run_id = {metadata_run_id:String} FORMAT TabSeparated", &[("metadata_run_id", metadata_run_id)])?;
    if accepted.trim() != "accepted" {
        bail!("metadata run {metadata_run_id} is not accepted");
    }
    let rows = target.query_text("SELECT count(), uniqExact(sample_id) FROM lr_y1_sample_metadata WHERE metadata_run_id = {metadata_run_id:String} AND release = 'y1' AND cohort = 'hgsvc_hprc' AND reference_genome = 'GRCh38' FORMAT TabSeparated", &[("metadata_run_id", metadata_run_id)])?;
    if parse_u64_fields(&rows, 2, "published metadata validation")? != [292, 292] {
        bail!("metadata run {metadata_run_id} does not have 292 unique published rows");
    }
    let pointer_state = target.query_text("SELECT argMax(metadata_run_id, revision), max(revision) FROM lr_y1_active_metadata WHERE release = 'y1' AND cohort = 'hgsvc_hprc' AND reference_genome = 'GRCh38' FORMAT TabSeparated", &[])?;
    let mut fields = pointer_state.trim().split('\t');
    let previous = fields.next().unwrap_or("").to_string();
    let previous_revision = fields
        .next()
        .unwrap_or("0")
        .parse::<u64>()
        .context("invalid active metadata revision")?;
    let revision = now_ms()?.max(previous_revision.saturating_add(1));
    #[derive(Serialize)]
    struct Pointer<'a> {
        release: &'a str,
        cohort: &'a str,
        reference_genome: &'a str,
        revision: u64,
        metadata_run_id: &'a str,
        previous_metadata_run_id: &'a str,
        activated_at_ms: u64,
        activated_by: &'a str,
    }
    target.insert_json_each_row(
        "lr_y1_active_metadata",
        &[Pointer {
            release: "y1",
            cohort: "hgsvc_hprc",
            reference_genome: "GRCh38",
            revision,
            metadata_run_id,
            previous_metadata_run_id: &previous,
            activated_at_ms: revision,
            activated_by,
        }],
    )?;
    let resolved = target.query_text("SELECT argMax(metadata_run_id, revision) FROM lr_y1_active_metadata WHERE release = 'y1' AND cohort = 'hgsvc_hprc' AND reference_genome = 'GRCh38' FORMAT TabSeparated", &[])?.trim().to_string();
    if resolved != metadata_run_id {
        bail!("active metadata pointer did not resolve to requested run");
    }
    Ok(previous)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(
        roster: &str,
        primary: &str,
        supplemental: &str,
    ) -> anyhow::Result<ReconciliationResult> {
        reconcile_metadata(
            roster.as_bytes(),
            primary.as_bytes(),
            supplemental.as_bytes(),
        )
    }
    const PH: &str = "sample_id,population_descriptor,population_abbreviation,sex,collection\n";
    const SH: &str = "sample\tethnicity\tPopulation\tcolor\n";

    #[test]
    fn quoted_comma_primary_precedence_and_ignored_fields() {
        let result = run(
            "HG02486\n",
            &format!("{PH}HG02486,\"Barbados, Caribbean\",ACB,female,foo\n"),
            &format!("{SH}HG02486\tEAS\tKHV\t#bad\n"),
        )
        .unwrap();
        let row = &result.rows[0];
        assert_eq!(
            (
                &row.superpopulation,
                &row.subpopulation,
                &row.population_descriptor
            ),
            (&"AFR".into(), &"ACB".into(), &"Barbados, Caribbean".into())
        );
        assert_eq!(
            result.compact_audit[0].event_type,
            "source_conflict_primary_retained"
        );
        assert!(!serde_json::to_string(row).unwrap().contains("#bad"));
    }

    #[test]
    fn supplemental_fallback_normalizes_ask_and_uses_placeholders() {
        let result = run(
            "HG002\nMISSING\n",
            &format!("{PH}HG002,desc,N/A,male,coll\n"),
            &format!("{SH}HG002\tASK\tTSI\tx\nMISSING\tEAS\tKHV\ty\n"),
        )
        .unwrap();
        assert_eq!(result.rows[0].superpopulation, "ASJ");
        assert_eq!(result.rows[0].population_descriptor, "desc");
        assert_eq!(result.rows[1].subpopulation, "N/A");
        assert_eq!(
            result.rows[1].population_descriptor,
            "HGSVC/HPRC ancestry label"
        );
        assert_eq!(result.rows[1].sex, "N/A");
    }

    #[test]
    fn exact_duplicates_are_audited_and_conflicting_consumed_values_fail() {
        let ok = run("A\n", PH, &format!("{SH}A\tAFR\tYRI\tx\nA\tAFR\tTSI\ty\n")).unwrap();
        assert_eq!(
            ok.audit
                .iter()
                .filter(|e| e.event_type == "exact_duplicate_supplemental_row")
                .count(),
            1
        );
        assert!(
            run("A\n", PH, &format!("{SH}A\tAFR\tYRI\tx\nA\tEAS\tKHV\ty\n"))
                .unwrap_err()
                .to_string()
                .contains("invalid_or_conflicting_duplicate")
        );
    }

    #[test]
    fn blank_na_and_unknown_ethnicity_are_unavailable() {
        for value in ["", "NA", "BOGUS"] {
            assert!(run("A\n", PH, &format!("{SH}A\t{value}\tYRI\tx\n")).is_err());
        }
    }

    #[test]
    fn duplicate_and_blank_roster_ids_fail() {
        assert!(run("A\nA\n", PH, &format!("{SH}A\tAFR\tYRI\tx\n")).is_err());
        assert!(run("A\n \n", PH, &format!("{SH}A\tAFR\tYRI\tx\n")).is_err());
    }

    #[test]
    fn checked_in_sources_match_authoritative_facts() {
        let manifest_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("sources/y1/metadata-source-manifest.json");
        let manifest = MetadataSourceManifest::from_path(&manifest_path).unwrap();
        let input = manifest.load_inputs(&manifest_path).unwrap();
        let first = reconcile_metadata(&input.roster, &input.primary, &input.supplemental).unwrap();
        validate_expected(&first, &manifest).unwrap();
        let second =
            reconcile_metadata(&input.roster, &input.primary, &input.supplemental).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.compact_audit.len(), 64);
        assert_eq!(
            first
                .rows
                .iter()
                .find(|r| r.sample_id == "HG02486")
                .unwrap()
                .superpopulation,
            "AFR"
        );
        assert_eq!(
            first
                .rows
                .iter()
                .find(|r| r.sample_id == "HG06807")
                .unwrap()
                .superpopulation,
            "AFR"
        );
    }
}
