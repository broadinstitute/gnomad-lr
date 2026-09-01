//! Foundation for the optional, source-derived primary-motif product.
//!
//! This module deliberately does not publish or query the product. It freezes the
//! pure sequence/aggregation contract, validates the candidate registry, and exposes
//! only generation-qualified source reads. A later producer/finalizer must require a
//! reviewed registry before accepting a product run.

use crate::loader::immutable_gcs::{ImmutableGcsBackend, ImmutableGcsObject};
use crate::loader::vcf_reader::VcfStream;
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const METRIC: &str = "WHOLE_RECORD_EXACT_PRIMARY_MOTIF_UNITS_V1";
pub const ANCHOR_RULE: &str = "TRID_ENVELOPE_LEFT_PADDING_BASE_V1";
pub const REGISTRY_CONTRACT: &str = "Y1_PRIMARY_REPEAT_REGISTRY_V1";
pub const MAX_ALT_IDENTITIES: usize = u16::MAX as usize;
pub const MAX_REPRESENTED_SEQUENCE_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_PRODUCER_BINS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryMotifRunState {
    Planned,
    Producing,
    Produced,
    IndependentlyVerified,
    AcceptedFrozen,
    Failed,
}

/// Product runs have their own append-only lifecycle; rows are never silently
/// attached to an already frozen primary run.
pub fn validate_run_state_transition(
    from: PrimaryMotifRunState,
    to: PrimaryMotifRunState,
    registry: &PrimaryRepeatRegistry,
    bound_registry_digest: &str,
) -> anyhow::Result<()> {
    registry.validate()?;
    require_sha256(bound_registry_digest, "run-bound registry")?;
    if registry.content_sha256 != bound_registry_digest {
        bail!("primary-motif run is not bound to the supplied registry digest");
    }
    let allowed = matches!(
        (from, to),
        (
            PrimaryMotifRunState::Planned,
            PrimaryMotifRunState::Producing
        ) | (
            PrimaryMotifRunState::Producing,
            PrimaryMotifRunState::Produced
        ) | (
            PrimaryMotifRunState::Produced,
            PrimaryMotifRunState::IndependentlyVerified
        ) | (
            PrimaryMotifRunState::IndependentlyVerified,
            PrimaryMotifRunState::AcceptedFrozen
        ) | (PrimaryMotifRunState::Planned, PrimaryMotifRunState::Failed)
            | (
                PrimaryMotifRunState::Producing,
                PrimaryMotifRunState::Failed
            )
            | (PrimaryMotifRunState::Produced, PrimaryMotifRunState::Failed)
            | (
                PrimaryMotifRunState::IndependentlyVerified,
                PrimaryMotifRunState::Failed
            )
    );
    if !allowed {
        bail!("invalid primary-motif product run state transition");
    }
    if to == PrimaryMotifRunState::AcceptedFrozen {
        registry.require_production_approval()?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RegistryApprovalState {
    CandidatePendingScience,
    Reviewed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrimaryRepeatSelectionBasis {
    ExactMainCatalogComponent,
    LrSoleComponent,
    ReviewedPrimaryRepeatRegistry,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TridComponent {
    pub start0: u32,
    pub end0: u32,
    pub motif: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryRepeatRegistryEntry {
    pub registry_entry_id: String,
    pub catalog_id: String,
    pub canonical_locus_id: String,
    pub source_variant_id: String,
    pub chrom: String,
    /// The exact one-based VCF POS. For this TRID envelope contract it equals
    /// the zero-based start of the leftmost represented component.
    pub source_position: u32,
    pub ordered_components: Vec<TridComponent>,
    /// Zero-based and never inferred from motif equality.
    pub component_index: usize,
    pub motif: String,
    pub selection_basis: PrimaryRepeatSelectionBasis,
    pub biological_role: Option<String>,
    pub approval_state: RegistryApprovalState,
    pub reviewer: Option<String>,
    pub approval_receipt: Option<String>,
    pub catalog_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryRepeatRegistry {
    pub schema_version: u16,
    pub contract: String,
    pub release: String,
    pub reference_genome: String,
    pub source_inventory_sha256: String,
    pub approval_state: RegistryApprovalState,
    pub design_authorization_receipt: String,
    pub entries: Vec<PrimaryRepeatRegistryEntry>,
    pub content_sha256: String,
}

impl PrimaryRepeatRegistry {
    pub fn from_slice(bytes: &[u8]) -> anyhow::Result<Self> {
        let value: Value =
            serde_json::from_slice(bytes).context("primary-repeat registry is not JSON")?;
        let registry: Self = serde_json::from_value(value.clone())
            .context("primary-repeat registry does not match its strict schema")?;
        let actual = canonical_registry_digest(value)?;
        if actual != registry.content_sha256 {
            bail!(
                "primary-repeat registry digest mismatch: declared {} actual {actual}",
                registry.content_sha256
            );
        }
        registry.validate()?;
        Ok(registry)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let actual_digest = canonical_registry_digest(
            serde_json::to_value(self).context("failed to serialize primary-repeat registry")?,
        )?;
        if actual_digest != self.content_sha256 {
            bail!("primary-repeat registry content no longer matches its bound digest");
        }
        if self.schema_version != 1
            || self.contract != REGISTRY_CONTRACT
            || self.release != "Y1"
            || self.reference_genome != "GRCh38"
        {
            bail!("unsupported primary-repeat registry identity");
        }
        require_sha256(&self.source_inventory_sha256, "source inventory")?;
        require_sha256(&self.content_sha256, "registry content")?;
        if self.design_authorization_receipt.trim().is_empty() {
            bail!("registry must retain the operator-approved design receipt");
        }
        if self.entries.is_empty() {
            bail!("primary-repeat registry has no entries");
        }
        let mut ids = BTreeSet::new();
        let mut loci = BTreeSet::new();
        for entry in &self.entries {
            entry.validate()?;
            if entry.approval_state != self.approval_state {
                bail!("registry and entry approval states differ");
            }
            if !ids.insert(entry.registry_entry_id.as_str())
                || !loci.insert((
                    entry.canonical_locus_id.as_str(),
                    entry.source_variant_id.as_str(),
                ))
            {
                bail!("primary-repeat registry contains a duplicate entry or locus/source pair");
            }
        }
        Ok(())
    }

    /// Acceptance/finalization must call this. Candidate fixtures can exercise
    /// the pure producer contract, but cannot authorize persisted product rows.
    pub fn require_production_approval(&self) -> anyhow::Result<()> {
        if self.approval_state != RegistryApprovalState::Reviewed {
            bail!("primary-repeat registry is candidate_pending_science, not reviewed");
        }
        Ok(())
    }
}

impl PrimaryRepeatRegistryEntry {
    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, value) in [
            ("registry_entry_id", self.registry_entry_id.as_str()),
            ("catalog_id", self.catalog_id.as_str()),
            ("canonical_locus_id", self.canonical_locus_id.as_str()),
            ("source_variant_id", self.source_variant_id.as_str()),
            ("chrom", self.chrom.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("primary-repeat registry entry has empty {name}");
            }
        }
        validate_motif(&self.motif)?;
        if self.ordered_components.is_empty() {
            bail!("primary-repeat registry entry has no ordered components");
        }
        for component in &self.ordered_components {
            validate_motif(&component.motif)?;
            if component.start0 >= component.end0 {
                bail!("TRID component must have non-empty zero-based half-open bounds");
            }
        }
        let selected = self
            .ordered_components
            .get(self.component_index)
            .ok_or_else(|| anyhow::anyhow!("primary-repeat component index is out of range"))?;
        if selected.motif != self.motif {
            bail!("primary-repeat motif does not exactly equal stored component orientation");
        }
        let envelope_start = self
            .ordered_components
            .iter()
            .map(|c| c.start0)
            .min()
            .unwrap();
        if self.source_position != envelope_start {
            bail!(
                "source VCF POS does not equal the TRID envelope start required by {ANCHOR_RULE}"
            );
        }
        match self.approval_state {
            RegistryApprovalState::CandidatePendingScience => {
                if self.reviewer.is_some() || self.approval_receipt.is_some() {
                    bail!("candidate registry entry must not claim a science reviewer or approval receipt");
                }
            }
            RegistryApprovalState::Reviewed => {
                if self
                    .reviewer
                    .as_deref()
                    .map(str::trim)
                    .is_none_or(str::is_empty)
                    || self
                        .approval_receipt
                        .as_deref()
                        .map(str::trim)
                        .is_none_or(str::is_empty)
                    || self.catalog_digest.is_none()
                {
                    bail!("reviewed registry entry requires reviewer, approval receipt, and catalog digest");
                }
            }
        }
        if let Some(digest) = &self.catalog_digest {
            require_sha256(digest, "catalog")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepresentedAlleles {
    pub reference: String,
    pub alternates: Vec<String>,
    pub removed_anchor: char,
    pub represented_sequence_bytes: usize,
}

/// Validate and remove exactly one shared left padding base. No other trimming,
/// normalization, rotation, or component allocation is performed.
pub fn apply_trid_envelope_left_padding(
    entry: &PrimaryRepeatRegistryEntry,
    reference: &str,
    alternates: &[String],
) -> anyhow::Result<RepresentedAlleles> {
    entry.validate()?;
    if alternates.is_empty() || alternates.len() > MAX_ALT_IDENTITIES {
        bail!("source record ALT cardinality is outside the complete producer bounds");
    }
    let envelope_start = entry
        .ordered_components
        .iter()
        .map(|c| c.start0)
        .min()
        .unwrap();
    let envelope_end = entry
        .ordered_components
        .iter()
        .map(|c| c.end0)
        .max()
        .unwrap();
    let expected_length = usize::try_from(envelope_end - envelope_start).unwrap();
    let mut all = Vec::with_capacity(alternates.len() + 1);
    all.push(reference);
    all.extend(alternates.iter().map(String::as_str));
    for allele in &all {
        validate_concrete_sequence(allele)?;
    }
    if reference.len().checked_sub(1) != Some(expected_length) {
        bail!("REF minus one padding base does not equal the TRID envelope length");
    }
    let anchor = reference.as_bytes()[0].to_ascii_uppercase();
    if all
        .iter()
        .any(|allele| allele.as_bytes()[0].to_ascii_uppercase() != anchor)
    {
        bail!("REF and every ALT do not share the declared left padding base");
    }
    let reference = reference[1..].to_ascii_uppercase();
    let alternates = alternates
        .iter()
        .map(|allele| allele[1..].to_ascii_uppercase())
        .collect::<Vec<_>>();
    let represented_sequence_bytes =
        reference.len() + alternates.iter().map(String::len).sum::<usize>();
    if represented_sequence_bytes > MAX_REPRESENTED_SEQUENCE_BYTES {
        bail!("complete represented sequence bytes exceed the producer bound");
    }
    Ok(RepresentedAlleles {
        reference,
        alternates,
        removed_anchor: char::from(anchor),
        represented_sequence_bytes,
    })
}

/// Count case-insensitive, exact, full-length, non-overlapping motif strings
/// from left to right. Other motifs do not compete and unmatched bases remain
/// unmatched.
pub fn count_exact_primary_motif_units(sequence: &str, motif: &str) -> anyhow::Result<u32> {
    validate_concrete_sequence(sequence)?;
    validate_motif(motif)?;
    let sequence = sequence.as_bytes();
    let motif = motif.as_bytes();
    let mut index = 0usize;
    let mut count = 0u32;
    while index + motif.len() <= sequence.len() {
        if sequence[index..index + motif.len()].eq_ignore_ascii_case(motif) {
            count = count
                .checked_add(1)
                .context("exact motif count exceeds UInt32")?;
            index += motif.len();
        } else {
            index += 1;
        }
    }
    Ok(count)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifAlleleBin {
    pub exact_units: u32,
    pub allele_copies: u64,
    pub reference_copies: u64,
    pub alternate_copies: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifAlleleDistribution {
    pub metric: &'static str,
    pub anchor_rule: &'static str,
    pub an: u64,
    pub alt_ac_sum: u64,
    pub reference_copies: u64,
    pub alt_count: usize,
    pub represented_sequence_bytes: usize,
    /// Digest of represented REF/ALTs plus this stratum's exact AC/AN inputs.
    /// The persistent source-record digest remains a separate whole-line receipt.
    pub allele_frequency_receipt_sha256: String,
    pub bins: Vec<PrimaryMotifAlleleBin>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryMotifStratumInput {
    pub division: String,
    pub ancestry: Option<String>,
    pub sex: Option<String>,
    pub alt_ac: Vec<u32>,
    pub an: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifStratumDistribution {
    pub division: String,
    pub ancestry: Option<String>,
    pub sex: Option<String>,
    pub distribution: PrimaryMotifAlleleDistribution,
}

/// Build one complete aggregate-only stratum. ALT identities are consumed only
/// offline and are not retained in the returned bins.
pub fn aggregate_primary_motif_alleles(
    represented: &RepresentedAlleles,
    motif: &str,
    alt_ac: &[u32],
    an: u32,
) -> anyhow::Result<PrimaryMotifAlleleDistribution> {
    if represented.alternates.len() != alt_ac.len() {
        bail!("ALT sequence and AC cardinalities differ");
    }
    if represented.alternates.is_empty() || represented.alternates.len() > MAX_ALT_IDENTITIES {
        bail!("ALT cardinality is outside the complete producer bounds");
    }
    let alt_ac_sum = alt_ac.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(u64::from(*value))
            .context("ALT AC sum overflow")
    })?;
    let an = u64::from(an);
    let reference_copies = an
        .checked_sub(alt_ac_sum)
        .ok_or_else(|| anyhow::anyhow!("negative REF copies: sum ALT AC exceeds AN"))?;
    let mut bins: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
    let ref_units = count_exact_primary_motif_units(&represented.reference, motif)?;
    bins.insert(ref_units, (reference_copies, reference_copies));
    for (sequence, copies) in represented.alternates.iter().zip(alt_ac) {
        let units = count_exact_primary_motif_units(sequence, motif)?;
        let bin = bins.entry(units).or_default();
        bin.0 = bin
            .0
            .checked_add(u64::from(*copies))
            .context("bin copy overflow")?;
    }
    if bins.len() > MAX_PRODUCER_BINS {
        bail!("complete aggregate exceeds the producer bin bound");
    }
    let bins = bins
        .into_iter()
        .map(
            |(exact_units, (allele_copies, reference_copies))| PrimaryMotifAlleleBin {
                exact_units,
                allele_copies,
                reference_copies,
                alternate_copies: allele_copies - reference_copies,
            },
        )
        .collect::<Vec<_>>();
    let observed = bins.iter().try_fold(0u64, |sum, bin| {
        sum.checked_add(bin.allele_copies)
            .context("histogram copy overflow")
    })?;
    if observed != an {
        bail!("aggregate allele copies do not reconcile to AN");
    }
    Ok(PrimaryMotifAlleleDistribution {
        metric: METRIC,
        anchor_rule: ANCHOR_RULE,
        an,
        alt_ac_sum,
        reference_copies,
        alt_count: represented.alternates.len(),
        represented_sequence_bytes: represented.represented_sequence_bytes,
        allele_frequency_receipt_sha256: allele_frequency_receipt_digest(represented, alt_ac, an),
        bins,
    })
}

/// Aggregate a caller-declared complete stratum grid. Keys must be unique and an
/// explicit overall `all` stratum is mandatory; no partial grid is synthesized.
pub fn aggregate_primary_motif_strata(
    represented: &RepresentedAlleles,
    motif: &str,
    strata: Vec<PrimaryMotifStratumInput>,
) -> anyhow::Result<Vec<PrimaryMotifStratumDistribution>> {
    let mut keys = BTreeSet::new();
    let mut has_all = false;
    let mut output = Vec::with_capacity(strata.len());
    for stratum in strata {
        if stratum.division.trim().is_empty() {
            bail!("primary-motif stratum division must not be empty");
        }
        let key = (
            stratum.division.clone(),
            stratum.ancestry.clone(),
            stratum.sex.clone(),
        );
        if !keys.insert(key) {
            bail!("duplicate primary-motif stratum identity");
        }
        if stratum.division == "all" && stratum.ancestry.is_none() && stratum.sex.is_none() {
            has_all = true;
        }
        output.push(PrimaryMotifStratumDistribution {
            division: stratum.division,
            ancestry: stratum.ancestry,
            sex: stratum.sex,
            distribution: aggregate_primary_motif_alleles(
                represented,
                motif,
                &stratum.alt_ac,
                stratum.an,
            )?,
        });
    }
    if !has_all {
        bail!("complete primary-motif stratum grid lacks the overall all stratum");
    }
    Ok(output)
}

/// Parse one exact registered VCF line and produce its overall aggregate. This
/// remains pure: generation-qualified I/O is separately forced by
/// `read_generation_bound_registered_record`.
pub fn produce_registered_overall_from_vcf_line(
    entry: &PrimaryRepeatRegistryEntry,
    line: &str,
) -> anyhow::Result<PrimaryMotifAlleleDistribution> {
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() < 8 {
        bail!("registered VCF source line has fewer than eight columns");
    }
    if fields[0] != entry.chrom
        || fields[1] != entry.source_position.to_string()
        || fields[2] != entry.source_variant_id
    {
        bail!("VCF record identity does not exactly equal the registry entry");
    }
    let alternates = fields[4].split(',').map(str::to_string).collect::<Vec<_>>();
    let mut info = BTreeMap::new();
    for item in fields[7].split(';') {
        let Some((name, value)) = item.split_once('=') else {
            // Unrelated declared INFO flags do not participate in this metric.
            continue;
        };
        if matches!(name, "AC" | "AN") && info.insert(name, value).is_some() {
            bail!("VCF INFO contains duplicate field {name}");
        }
    }
    let alt_ac = info
        .get("AC")
        .ok_or_else(|| anyhow::anyhow!("VCF INFO lacks AC"))?
        .split(',')
        .map(|value| value.parse::<u32>().context("VCF INFO/AC is not UInt32"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let an = info
        .get("AN")
        .ok_or_else(|| anyhow::anyhow!("VCF INFO lacks AN"))?
        .parse::<u32>()
        .context("VCF INFO/AN is not UInt32")?;
    let represented = apply_trid_envelope_left_padding(entry, fields[3], &alternates)?;
    aggregate_primary_motif_alleles(&represented, &entry.motif, &alt_ac, an)
}

/// The only source reader exposed by this product foundation. Metadata, index,
/// header, and data ranges remain generation-qualified through the existing
/// immutable reader, and exactly one registered record is required.
pub fn read_generation_bound_registered_record(
    backend: Arc<dyn ImmutableGcsBackend>,
    source: &ImmutableGcsObject,
    index: &ImmutableGcsObject,
    entry: &PrimaryRepeatRegistryEntry,
) -> anyhow::Result<String> {
    entry.validate()?;
    let stream = VcfStream::open_immutable_region(
        backend,
        source,
        index,
        &entry.chrom,
        entry.source_position,
        entry.source_position,
    )?;
    let mut matches = Vec::new();
    for record in stream.records() {
        let record = record?;
        let fields = record.split('\t').collect::<Vec<_>>();
        if fields.len() < 8 {
            bail!("generation-bound source record is malformed");
        }
        if fields[0] == entry.chrom
            && fields[1] == entry.source_position.to_string()
            && fields[2] == entry.source_variant_id
        {
            matches.push(record);
        }
    }
    if matches.len() != 1 {
        bail!(
            "registered immutable source query returned {} exact records; expected one",
            matches.len()
        );
    }
    Ok(matches.pop().unwrap())
}

fn canonical_registry_digest(mut value: Value) -> anyhow::Result<String> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("primary-repeat registry root must be an object"))?;
    object
        .remove("content_sha256")
        .ok_or_else(|| anyhow::anyhow!("primary-repeat registry lacks content_sha256"))?;
    let bytes =
        serde_json::to_vec(&value).context("failed to canonicalize primary-repeat registry")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn allele_frequency_receipt_digest(
    represented: &RepresentedAlleles,
    alt_ac: &[u32],
    an: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"Y1_PRIMARY_MOTIF_ALLELE_FREQUENCY_INPUT_V1\0");
    update_len_prefixed(&mut hasher, represented.reference.as_bytes());
    for (allele, ac) in represented.alternates.iter().zip(alt_ac) {
        update_len_prefixed(&mut hasher, allele.as_bytes());
        hasher.update(ac.to_be_bytes());
    }
    hasher.update(an.to_be_bytes());
    format!("{:x}", hasher.finalize())
}

fn update_len_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn validate_motif(value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|base| matches!(base, b'A' | b'C' | b'G' | b'T'))
    {
        bail!("motif must be a non-empty uppercase concrete DNA string");
    }
    Ok(())
}

fn validate_concrete_sequence(value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|base| matches!(base.to_ascii_uppercase(), b'A' | b'C' | b'G' | b'T'))
    {
        bail!("allele must be a non-empty concrete DNA sequence");
    }
    Ok(())
}

fn require_sha256(value: &str, label: &str) -> anyhow::Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} digest must be 64 lowercase hexadecimal characters");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(motif: &str, start0: u32, end0: u32) -> PrimaryRepeatRegistryEntry {
        PrimaryRepeatRegistryEntry {
            registry_entry_id: "fixture".into(),
            catalog_id: "fixture".into(),
            canonical_locus_id: format!("chr1-{start0}-TRV-{}", end0 - start0),
            source_variant_id: format!("chr1-{start0}-TRV-{}", end0 - start0),
            chrom: "chr1".into(),
            source_position: start0,
            ordered_components: vec![TridComponent {
                start0,
                end0,
                motif: motif.into(),
            }],
            component_index: 0,
            motif: motif.into(),
            selection_basis: PrimaryRepeatSelectionBasis::ExactMainCatalogComponent,
            biological_role: None,
            approval_state: RegistryApprovalState::CandidatePendingScience,
            reviewer: None,
            approval_receipt: None,
            catalog_digest: None,
        }
    }

    #[test]
    fn exact_count_is_case_insensitive_left_to_right_and_non_overlapping() {
        assert_eq!(
            count_exact_primary_motif_units("cagCAGTCAGCA", "CAG").unwrap(),
            3
        );
        assert_eq!(count_exact_primary_motif_units("AAAAA", "AAA").unwrap(), 1);
        assert_eq!(count_exact_primary_motif_units("CA", "CAG").unwrap(), 0);
        assert_eq!(
            count_exact_primary_motif_units("TGCTGCCAG", "TGC").unwrap(),
            2
        );
    }

    #[test]
    fn anchor_and_ref_accounting_are_complete_and_aggregate_only() {
        let entry = entry("CAG", 100, 106);
        let represented =
            apply_trid_envelope_left_padding(&entry, "ACAGCAG", &["ACAG".into(), "ATGCCAG".into()])
                .unwrap();
        let result = aggregate_primary_motif_alleles(&represented, "CAG", &[3, 2], 8).unwrap();
        assert_eq!(result.reference_copies, 3);
        assert_eq!(
            result.bins.iter().map(|bin| bin.allele_copies).sum::<u64>(),
            8
        );
        assert_eq!(
            result.bins,
            vec![
                PrimaryMotifAlleleBin {
                    exact_units: 1,
                    allele_copies: 5,
                    reference_copies: 0,
                    alternate_copies: 5
                },
                PrimaryMotifAlleleBin {
                    exact_units: 2,
                    allele_copies: 3,
                    reference_copies: 3,
                    alternate_copies: 0
                },
            ]
        );
    }

    #[test]
    fn malformed_anchor_incomplete_cardinality_and_negative_ref_fail_closed() {
        let entry = entry("CAG", 100, 106);
        assert!(apply_trid_envelope_left_padding(&entry, "ACAGCAG", &["CCAG".into()]).is_err());
        assert!(apply_trid_envelope_left_padding(&entry, "ACAG", &["ACAG".into()]).is_err());
        let represented =
            apply_trid_envelope_left_padding(&entry, "ACAGCAG", &["ACAG".into()]).unwrap();
        assert!(aggregate_primary_motif_alleles(&represented, "CAG", &[], 2).is_err());
        assert!(aggregate_primary_motif_alleles(&represented, "CAG", &[3], 2).is_err());
        assert!(aggregate_primary_motif_strata(
            &represented,
            "CAG",
            vec![PrimaryMotifStratumInput {
                division: "afr".into(),
                ancestry: Some("afr".into()),
                sex: None,
                alt_ac: vec![1],
                an: 2,
            }],
        )
        .is_err());
    }

    #[test]
    fn exact_registered_vcf_line_produces_ref_and_all_alt_bins() {
        let entry = entry("TGC", 100, 106);
        let result = produce_registered_overall_from_vcf_line(
            &entry,
            "chr1\t100\tchr1-100-TRV-6\tATGCTGC\tATGC,ACAGTGC\t.\tPASS\tAC=3,2;AN=8",
        )
        .unwrap();
        assert_eq!(result.reference_copies, 3);
        assert_eq!(result.alt_count, 2);
        assert_eq!(
            result.bins.iter().map(|bin| bin.allele_copies).sum::<u64>(),
            8
        );
        let represented =
            apply_trid_envelope_left_padding(&entry, "ATGCTGC", &["ATGC".into(), "ACAGTGC".into()])
                .unwrap();
        let strata = aggregate_primary_motif_strata(
            &represented,
            "TGC",
            vec![
                PrimaryMotifStratumInput {
                    division: "all".into(),
                    ancestry: None,
                    sex: None,
                    alt_ac: vec![3, 2],
                    an: 8,
                },
                PrimaryMotifStratumInput {
                    division: "afr_XX".into(),
                    ancestry: Some("afr".into()),
                    sex: Some("XX".into()),
                    alt_ac: vec![1, 0],
                    an: 2,
                },
            ],
        )
        .unwrap();
        assert_eq!(strata.len(), 2);
        assert_eq!(strata[1].distribution.reference_copies, 1);
    }

    #[test]
    fn checked_registry_fixtures_preserve_orientation_role_and_candidate_state() {
        let registry = PrimaryRepeatRegistry::from_slice(include_bytes!(
            "../../sources/y1/primary-repeat-registry.json"
        ))
        .unwrap();
        let values = registry
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.catalog_id.as_str(),
                    entry.motif.as_str(),
                    entry.component_index,
                    entry.biological_role.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                ("HTT", "CAG", 0, Some("coding polyglutamine repeat")),
                (
                    "ATXN1",
                    "TGC",
                    0,
                    Some("stored-orientation disease-associated repeat")
                ),
                ("RFC1", "AAAAG", 0, Some("benign reference motif")),
            ]
        );
        assert!(registry.require_production_approval().is_err());
        let mut falsely_reviewed = registry.clone();
        falsely_reviewed.approval_state = RegistryApprovalState::Reviewed;
        for entry in &mut falsely_reviewed.entries {
            entry.approval_state = RegistryApprovalState::Reviewed;
            entry.reviewer = Some("reviewer".into());
            entry.approval_receipt = Some("receipt".into());
            entry.catalog_digest = Some("0".repeat(64));
        }
        falsely_reviewed.entries[0].reviewer = Some("   ".into());
        falsely_reviewed.content_sha256 =
            canonical_registry_digest(serde_json::to_value(&falsely_reviewed).unwrap()).unwrap();
        assert!(falsely_reviewed.validate().is_err());
        assert!(validate_run_state_transition(
            PrimaryMotifRunState::Produced,
            PrimaryMotifRunState::IndependentlyVerified,
            &registry,
            &registry.content_sha256,
        )
        .is_ok());
        assert!(validate_run_state_transition(
            PrimaryMotifRunState::Produced,
            PrimaryMotifRunState::IndependentlyVerified,
            &registry,
            &"0".repeat(64),
        )
        .is_err());
        assert!(validate_run_state_transition(
            PrimaryMotifRunState::IndependentlyVerified,
            PrimaryMotifRunState::AcceptedFrozen,
            &registry,
            &registry.content_sha256,
        )
        .is_err());
        assert!(validate_run_state_transition(
            PrimaryMotifRunState::Produced,
            PrimaryMotifRunState::AcceptedFrozen,
            &registry,
            &registry.content_sha256,
        )
        .is_err());
    }
}
