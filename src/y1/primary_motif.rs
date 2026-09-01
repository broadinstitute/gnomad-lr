//! Foundation for the optional, source-derived primary-motif product.
//!
//! This module deliberately does not publish or query the product. It freezes the
//! pure sequence/aggregation contract, validates the candidate registry, and exposes
//! only generation-qualified source reads. A later producer/finalizer must require a
//! reviewed registry before accepting a product run.

use crate::loader::immutable_gcs::{ImmutableGcsBackend, ImmutableGcsObject};
use crate::loader::vcf_reader::VcfStream;
use crate::y1::model::Cohort;
use crate::y1::parser::{
    parse_complete_source_genotypes, CompleteGenotypeCall, CompleteGenotypeTotals,
    CompleteSourceGenotypes, Y1Header,
};
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
pub const MAX_SOURCE_RECORDS_PER_LOCUS: usize = 1;
pub const MAX_SOURCE_DIVISIONS: usize = 21;
pub const MAX_GENOTYPE_PAIRS_PER_STRATUM: usize = 5_000;
pub const MAX_GENOTYPE_CELLS_PER_STRATUM: usize = 5_000;
pub const MAX_SERIALIZED_AGGREGATE_BYTES: usize = 1024 * 1024;
pub const AOU_GENOTYPE_UNAVAILABLE_REASON: &str = "AGGREGATE_ONLY_SOURCE_NO_GT_PAIRING";

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifGenotypeStratumAggregation {
    pub division: String,
    pub ancestry: Option<String>,
    pub sex: Option<String>,
    pub aggregate: PrimaryMotifGenotypeAggregation,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrimaryMotifGenotypeStatus {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifGenotypePair {
    /// Internal anonymous source-allele identities. Zero is REF; positive values
    /// are exact one-based ALT indices. They are retained only to prove margins.
    pub shorter_allele_index: u16,
    pub longer_allele_index: u16,
    pub shorter_exact_units: u32,
    pub longer_exact_units: u32,
    pub people: u32,
    pub phased_people: u32,
    pub unphased_people: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifGenotypeCell {
    pub shorter_exact_units: u32,
    pub longer_exact_units: u32,
    pub people: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifGenotypeAlleleMargin {
    pub allele_index: u16,
    pub expected_copies: u32,
    pub paired_copies: u32,
    /// Called alleles in partial or non-diploid GTs. They are not represented
    /// as diploid cells but remain necessary for an exact INFO margin proof.
    pub excluded_from_pairs_copies: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifGenotypeAggregation {
    pub status: PrimaryMotifGenotypeStatus,
    pub reason_code: Option<&'static str>,
    pub called_diploid_people: u32,
    pub partial_diploid_people: u32,
    pub no_call_people: u32,
    pub non_diploid_people: u32,
    pub observed_an: Option<u32>,
    pub header_roster_sha256: Option<String>,
    pub genotype_content_sha256: Option<String>,
    pub margins: Vec<PrimaryMotifGenotypeAlleleMargin>,
    /// Pair rows are an internal verifier/storage shape, never person rows.
    pub internal_pairs: Vec<PrimaryMotifGenotypePair>,
    /// Public-ready anonymous unit-count cells. A later API may serve only these.
    pub cells: Vec<PrimaryMotifGenotypeCell>,
    pub serialized_bytes: usize,
}

/// Aggregate complete source GTs into anonymous exact-unit cells while retaining
/// only bounded allele-index pair counts needed to prove REF/ALT margins.
pub fn aggregate_primary_motif_genotypes(
    cohort: Cohort,
    represented: &RepresentedAlleles,
    motif: &str,
    expected_alt_ac: &[u32],
    expected_an: u32,
    complete: Option<&CompleteSourceGenotypes>,
) -> anyhow::Result<PrimaryMotifGenotypeAggregation> {
    if cohort == Cohort::Aou {
        if complete.is_some() {
            bail!("AoU aggregate-only source must not supply synthetic GT pairing");
        }
        let mut output = PrimaryMotifGenotypeAggregation {
            status: PrimaryMotifGenotypeStatus::Unavailable,
            reason_code: Some(AOU_GENOTYPE_UNAVAILABLE_REASON),
            called_diploid_people: 0,
            partial_diploid_people: 0,
            no_call_people: 0,
            non_diploid_people: 0,
            observed_an: None,
            header_roster_sha256: None,
            genotype_content_sha256: None,
            margins: Vec::new(),
            internal_pairs: Vec::new(),
            cells: Vec::new(),
            serialized_bytes: 0,
        };
        finalize_genotype_serialized_bytes(&mut output)?;
        return Ok(output);
    }
    let complete =
        complete.context("HGSVC/HPRC genotype aggregate requires complete source GTs")?;
    if represented.alternates.len() != expected_alt_ac.len()
        || expected_alt_ac.is_empty()
        || expected_alt_ac.len() > MAX_ALT_IDENTITIES
    {
        bail!("complete genotype ALT sequence and INFO/AC cardinalities differ or exceed bounds");
    }
    if complete.calls.len() != complete.totals.total_people as usize {
        bail!("complete genotype call vector does not match its total-person receipt");
    }
    let expected_ref = expected_alt_ac
        .iter()
        .try_fold(0u32, |sum, value| {
            sum.checked_add(*value).context("INFO/AC sum overflow")
        })
        .and_then(|sum| {
            expected_an
                .checked_sub(sum)
                .context("INFO ALT AC exceeds AN")
        })?;
    let mut expected_margins = Vec::with_capacity(expected_alt_ac.len() + 1);
    expected_margins.push(expected_ref);
    expected_margins.extend_from_slice(expected_alt_ac);
    if complete.observed_an != expected_an || complete.observed_allele_margins != expected_margins {
        bail!("complete source GT receipt does not exactly reproduce INFO REF/ALT AC/AN margins");
    }
    require_sha256(&complete.header_roster_sha256, "header roster")?;
    require_sha256(&complete.genotype_content_sha256, "genotype content")?;
    if complete.totals.partial_diploid_people != 0 || complete.totals.non_diploid_people != 0 {
        bail!("complete autosomal genotype pairing is unavailable when any GT is partial or non-diploid");
    }

    let mut allele_units = Vec::with_capacity(represented.alternates.len() + 1);
    allele_units.push(count_exact_primary_motif_units(
        &represented.reference,
        motif,
    )?);
    for alternate in &represented.alternates {
        allele_units.push(count_exact_primary_motif_units(alternate, motif)?);
    }
    let mut pair_counts = BTreeMap::<(u32, u16, u32, u16), (u32, u32)>::new();
    let mut cell_counts = BTreeMap::<(u32, u32), u32>::new();
    let mut paired_margins = vec![0u32; allele_units.len()];
    let mut excluded_margins = vec![0u32; allele_units.len()];

    for call in &complete.calls {
        let called = call.alleles.iter().flatten().copied().collect::<Vec<_>>();
        if call.alleles.len() == 2 && called.len() == 2 {
            let left = called[0];
            let right = called[1];
            let left_units = *allele_units
                .get(left as usize)
                .context("complete GT allele index exceeds represented alleles")?;
            let right_units = *allele_units
                .get(right as usize)
                .context("complete GT allele index exceeds represented alleles")?;
            let (shorter_units, shorter, longer_units, longer) =
                if (left_units, left) <= (right_units, right) {
                    (left_units, left, right_units, right)
                } else {
                    (right_units, right, left_units, left)
                };
            let phase_counts = pair_counts
                .entry((shorter_units, shorter, longer_units, longer))
                .or_default();
            if call.phased {
                phase_counts.0 = phase_counts
                    .0
                    .checked_add(1)
                    .context("phased pair count overflow")?;
            } else {
                phase_counts.1 = phase_counts
                    .1
                    .checked_add(1)
                    .context("unphased pair count overflow")?;
            }
            let cell = cell_counts
                .entry((shorter_units, longer_units))
                .or_default();
            *cell = cell
                .checked_add(1)
                .context("genotype cell count overflow")?;
            for allele in [left, right] {
                paired_margins[allele as usize] = paired_margins[allele as usize]
                    .checked_add(1)
                    .context("paired allele margin overflow")?;
            }
        } else {
            for allele in called {
                let margin = excluded_margins
                    .get_mut(allele as usize)
                    .context("complete GT allele index exceeds represented alleles")?;
                *margin = margin
                    .checked_add(1)
                    .context("excluded allele margin overflow")?;
            }
        }
    }
    if pair_counts.len() > MAX_GENOTYPE_PAIRS_PER_STRATUM {
        bail!("complete genotype pair aggregate exceeds the all-or-nothing producer bound");
    }
    if cell_counts.len() > MAX_GENOTYPE_CELLS_PER_STRATUM {
        bail!("complete genotype cell aggregate exceeds the all-or-nothing producer bound");
    }
    let internal_pairs = pair_counts
        .into_iter()
        .map(
            |(
                (
                    shorter_exact_units,
                    shorter_allele_index,
                    longer_exact_units,
                    longer_allele_index,
                ),
                (phased_people, unphased_people),
            )| {
                Ok(PrimaryMotifGenotypePair {
                    shorter_allele_index,
                    longer_allele_index,
                    shorter_exact_units,
                    longer_exact_units,
                    people: phased_people
                        .checked_add(unphased_people)
                        .context("pair people overflow")?,
                    phased_people,
                    unphased_people,
                })
            },
        )
        .collect::<anyhow::Result<Vec<_>>>()?;
    let cells = cell_counts
        .into_iter()
        .map(
            |((shorter_exact_units, longer_exact_units), people)| PrimaryMotifGenotypeCell {
                shorter_exact_units,
                longer_exact_units,
                people,
            },
        )
        .collect::<Vec<_>>();
    let cell_people = cells.iter().try_fold(0u32, |sum, cell| {
        sum.checked_add(cell.people)
            .context("cell people total overflow")
    })?;
    if cell_people != complete.totals.called_diploid_people {
        bail!("anonymous genotype cells do not reconcile to called diploid people");
    }
    let margins = expected_margins
        .iter()
        .enumerate()
        .map(|(index, expected)| {
            if paired_margins[index].checked_add(excluded_margins[index]) != Some(*expected) {
                bail!("anonymous genotype pair margins do not exactly reproduce REF/ALT AC/AN");
            }
            Ok(PrimaryMotifGenotypeAlleleMargin {
                allele_index: u16::try_from(index).context("allele index exceeds UInt16")?,
                expected_copies: *expected,
                paired_copies: paired_margins[index],
                excluded_from_pairs_copies: excluded_margins[index],
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut output = PrimaryMotifGenotypeAggregation {
        status: PrimaryMotifGenotypeStatus::Available,
        reason_code: None,
        called_diploid_people: complete.totals.called_diploid_people,
        partial_diploid_people: complete.totals.partial_diploid_people,
        no_call_people: complete.totals.no_call_people,
        non_diploid_people: complete.totals.non_diploid_people,
        observed_an: Some(complete.observed_an),
        header_roster_sha256: Some(complete.header_roster_sha256.clone()),
        genotype_content_sha256: Some(complete.genotype_content_sha256.clone()),
        margins,
        internal_pairs,
        cells,
        serialized_bytes: 0,
    };
    finalize_genotype_serialized_bytes(&mut output)?;
    Ok(output)
}

fn finalize_genotype_serialized_bytes(
    output: &mut PrimaryMotifGenotypeAggregation,
) -> anyhow::Result<()> {
    for _ in 0..8 {
        let serialized_bytes = serde_json::to_vec(output)
            .context("failed to serialize complete genotype aggregate")?
            .len();
        if serialized_bytes == output.serialized_bytes {
            if serialized_bytes > MAX_SERIALIZED_AGGREGATE_BYTES {
                bail!(
                    "complete genotype aggregate exceeds the all-or-nothing serialized-byte bound"
                );
            }
            return Ok(());
        }
        output.serialized_bytes = serialized_bytes;
    }
    bail!("complete genotype serialized-byte receipt did not reach a fixed point")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifSampleMetadata {
    pub ancestry: String,
    pub sex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrimaryMotifMetadataBindingReceipt {
    pub metadata_run_id: String,
    pub accepted_metadata_receipt_sha256: String,
    pub metadata_manifest_sha256: String,
    pub header_roster_sha256: String,
    pub header_mapping_sha256: String,
    pub metadata_row_count: u32,
    pub mapped_sample_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataBoundGenotypeCall {
    genotype: CompleteGenotypeCall,
    metadata: PrimaryMotifSampleMetadata,
}

/// Opaque in-memory binding. Individual GT/metadata associations are private
/// and non-serializable; only the aggregate producer may consume them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBoundCompleteGenotypes {
    calls: Vec<MetadataBoundGenotypeCall>,
    pub receipt: PrimaryMotifMetadataBindingReceipt,
}

/// Compute the expected one-to-one header/metadata mapping digest. Identifiers
/// participate in the digest but are not returned in any product receipt or row.
pub fn primary_motif_metadata_mapping_sha256(
    header: &Y1Header,
    metadata_by_sample: &BTreeMap<String, PrimaryMotifSampleMetadata>,
) -> anyhow::Result<String> {
    if metadata_by_sample.len() != header.sample_names.len()
        || metadata_by_sample
            .keys()
            .any(|sample| !header.sample_names.contains(sample))
    {
        bail!("metadata rows do not exactly equal the complete VCF header roster");
    }
    let mut hasher = Sha256::new();
    hasher.update(b"Y1_PRIMARY_MOTIF_HEADER_METADATA_MAPPING_V1\0");
    for sample_id in &header.sample_names {
        let metadata = metadata_by_sample
            .get(sample_id)
            .context("VCF header sample lacks exactly one metadata row")?;
        if metadata.ancestry.trim().is_empty() || metadata.sex.trim().is_empty() {
            bail!("primary-motif metadata ancestry and sex must be nonempty");
        }
        for value in [
            sample_id.as_bytes(),
            metadata.ancestry.as_bytes(),
            metadata.sex.as_bytes(),
        ] {
            update_len_prefixed(&mut hasher, value);
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Bind complete calls to an accepted metadata snapshot in memory. The returned
/// value contains strata labels and receipts only, never source identifiers.
pub fn bind_complete_genotypes_to_metadata(
    header: &Y1Header,
    complete: &CompleteSourceGenotypes,
    metadata_by_sample: &BTreeMap<String, PrimaryMotifSampleMetadata>,
    metadata_run_id: &str,
    accepted_metadata_receipt_sha256: &str,
    metadata_manifest_sha256: &str,
    expected_mapping_sha256: &str,
) -> anyhow::Result<MetadataBoundCompleteGenotypes> {
    if metadata_run_id.trim().is_empty() {
        bail!("metadata binding requires an accepted metadata run identity");
    }
    require_sha256(
        accepted_metadata_receipt_sha256,
        "accepted metadata ledger receipt",
    )?;
    require_sha256(metadata_manifest_sha256, "metadata manifest")?;
    require_sha256(expected_mapping_sha256, "expected metadata mapping")?;
    if complete.calls.len() != header.sample_names.len() {
        bail!("complete genotype calls do not align to the VCF header roster");
    }
    let actual_mapping = primary_motif_metadata_mapping_sha256(header, metadata_by_sample)?;
    if actual_mapping != expected_mapping_sha256 {
        bail!("VCF-header-to-metadata mapping digest differs from its accepted receipt");
    }
    let mut divisions = BTreeSet::from(["all".to_string()]);
    for metadata in metadata_by_sample.values() {
        divisions.insert(metadata.ancestry.clone());
        divisions.insert(metadata.sex.clone());
        divisions.insert(format!("{}_{}", metadata.ancestry, metadata.sex));
    }
    if divisions.len() > MAX_SOURCE_DIVISIONS {
        bail!("metadata-derived complete division set exceeds the all-or-nothing producer bound");
    }
    let mut roster_hasher = Sha256::new();
    roster_hasher.update(b"Y1_PRIMARY_MOTIF_HEADER_ROSTER_V1\0");
    let calls = header
        .sample_names
        .iter()
        .zip(&complete.calls)
        .map(|(sample_id, genotype)| {
            update_len_prefixed(&mut roster_hasher, sample_id.as_bytes());
            Ok(MetadataBoundGenotypeCall {
                genotype: genotype.clone(),
                metadata: metadata_by_sample
                    .get(sample_id)
                    .context("VCF header sample lacks metadata during binding")?
                    .clone(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let header_roster_sha256 = format!("{:x}", roster_hasher.finalize());
    if header_roster_sha256 != complete.header_roster_sha256 {
        bail!("metadata binding roster digest differs from the genotype source roster");
    }
    Ok(MetadataBoundCompleteGenotypes {
        calls,
        receipt: PrimaryMotifMetadataBindingReceipt {
            metadata_run_id: metadata_run_id.to_string(),
            accepted_metadata_receipt_sha256: accepted_metadata_receipt_sha256.to_string(),
            metadata_manifest_sha256: metadata_manifest_sha256.to_string(),
            header_roster_sha256,
            header_mapping_sha256: actual_mapping,
            metadata_row_count: u32::try_from(metadata_by_sample.len())
                .context("metadata row count exceeds UInt32")?,
            mapped_sample_count: u32::try_from(header.sample_names.len())
                .context("mapped sample count exceeds UInt32")?,
        },
    })
}

/// Build every metadata-qualified HGSVC/HPRC genotype stratum named by the
/// checked source frequency grid. Source AC/AN values are authoritative and
/// each in-memory metadata subset must reproduce them exactly. No sample ID is
/// copied into the returned aggregate-only values.
pub fn aggregate_bound_primary_motif_genotype_strata(
    represented: &RepresentedAlleles,
    motif: &str,
    source_strata: &[PrimaryMotifStratumInput],
    bound: &MetadataBoundCompleteGenotypes,
) -> anyhow::Result<Vec<PrimaryMotifGenotypeStratumAggregation>> {
    let mut output = Vec::with_capacity(source_strata.len());
    for stratum in source_strata {
        let calls = bound
            .calls
            .iter()
            .filter(|call| metadata_matches_division(&call.metadata, &stratum.division))
            .map(|call| call.genotype.clone())
            .collect::<Vec<_>>();
        let complete = subset_complete_genotypes(&calls, represented.alternates.len() + 1)?;
        let aggregate = aggregate_primary_motif_genotypes(
            Cohort::HgsvcHprc,
            represented,
            motif,
            &stratum.alt_ac,
            stratum.an,
            Some(&complete),
        )?;
        output.push(PrimaryMotifGenotypeStratumAggregation {
            division: stratum.division.clone(),
            ancestry: stratum.ancestry.clone(),
            sex: stratum.sex.clone(),
            aggregate,
        });
    }
    Ok(output)
}

fn metadata_matches_division(metadata: &PrimaryMotifSampleMetadata, division: &str) -> bool {
    if division == "all" {
        return true;
    }
    if matches!(division, "XX" | "XY") {
        return metadata.sex == division;
    }
    if let Some((ancestry, sex)) = division.rsplit_once('_') {
        return metadata.ancestry == ancestry && metadata.sex == sex;
    }
    metadata.ancestry == division
}

fn subset_complete_genotypes(
    calls: &[CompleteGenotypeCall],
    allele_count: usize,
) -> anyhow::Result<CompleteSourceGenotypes> {
    if allele_count == 0 || allele_count > MAX_ALT_IDENTITIES + 1 {
        bail!("subset genotype allele cardinality is outside producer bounds");
    }
    let mut totals = CompleteGenotypeTotals::default();
    for call in calls {
        totals.total_people = totals
            .total_people
            .checked_add(1)
            .context("subset people overflow")?;
        let called = call.alleles.iter().flatten().count();
        match (call.alleles.len(), called) {
            (2, 2) => {
                totals.called_diploid_people = totals
                    .called_diploid_people
                    .checked_add(1)
                    .context("subset called people overflow")?
            }
            (2, 0) => {
                totals.no_call_people = totals
                    .no_call_people
                    .checked_add(1)
                    .context("subset no-call people overflow")?
            }
            (2, _) => {
                totals.partial_diploid_people = totals
                    .partial_diploid_people
                    .checked_add(1)
                    .context("subset partial people overflow")?
            }
            (_, 0) => {
                totals.no_call_people = totals
                    .no_call_people
                    .checked_add(1)
                    .context("subset no-call people overflow")?
            }
            _ => {
                totals.non_diploid_people = totals
                    .non_diploid_people
                    .checked_add(1)
                    .context("subset non-diploid people overflow")?
            }
        }
        if call
            .alleles
            .iter()
            .flatten()
            .any(|allele| *allele as usize >= allele_count)
        {
            bail!("subset genotype allele index exceeds source allele cardinality");
        }
    }
    let mut observed_allele_margins = vec![0u32; allele_count];
    let mut observed_an = 0u32;
    let mut content = Sha256::new();
    content.update(b"Y1_PRIMARY_MOTIF_METADATA_SUBSET_GENOTYPES_V1\0");
    for call in calls {
        content.update([u8::from(call.phased)]);
        content.update((call.alleles.len() as u64).to_be_bytes());
        for allele in &call.alleles {
            match allele {
                Some(allele) => {
                    content.update([1]);
                    content.update(allele.to_be_bytes());
                    observed_allele_margins[*allele as usize] = observed_allele_margins
                        [*allele as usize]
                        .checked_add(1)
                        .context("subset allele margin overflow")?;
                    observed_an = observed_an.checked_add(1).context("subset AN overflow")?;
                }
                None => content.update([0, 0, 0]),
            }
        }
    }
    Ok(CompleteSourceGenotypes {
        calls: calls.to_vec(),
        totals,
        observed_allele_margins,
        observed_an,
        header_roster_sha256: bound_digest(&bound_roster_material(calls)),
        genotype_content_sha256: format!("{:x}", content.finalize()),
    })
}

fn bound_roster_material(calls: &[CompleteGenotypeCall]) -> Vec<u8> {
    // Subset aggregates have no sample roster. This deterministic typed digest
    // binds only the cardinality and is never used as a metadata mapping receipt.
    (calls.len() as u64).to_be_bytes().to_vec()
}

fn bound_digest(material: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"Y1_PRIMARY_MOTIF_ANONYMOUS_SUBSET_ROSTER_V1\0");
    hasher.update(material);
    format!("{:x}", hasher.finalize())
}

/// Bind the anonymous aggregate to its product run, frozen primary run,
/// registry, and (for available HGSVC/HPRC cells) accepted metadata receipt.
pub fn primary_motif_genotype_receipt_sha256(
    aggregate: &PrimaryMotifGenotypeAggregation,
    product_run_id: &str,
    primary_run_id: &str,
    source_variant_id: &str,
    registry_digest: &str,
    metadata: Option<&PrimaryMotifMetadataBindingReceipt>,
) -> anyhow::Result<String> {
    if [product_run_id, primary_run_id, source_variant_id]
        .iter()
        .any(|value| value.trim().is_empty())
    {
        bail!("genotype receipt requires product, primary-run, and source-record identities");
    }
    require_sha256(registry_digest, "genotype receipt registry")?;
    validate_primary_motif_genotype_aggregation(aggregate)?;
    match aggregate.status {
        PrimaryMotifGenotypeStatus::Available => {
            let metadata =
                metadata.context("available genotype aggregate lacks metadata binding")?;
            require_sha256(
                &metadata.accepted_metadata_receipt_sha256,
                "accepted metadata ledger receipt",
            )?;
            require_sha256(&metadata.metadata_manifest_sha256, "metadata manifest")?;
            require_sha256(&metadata.header_roster_sha256, "header roster")?;
            require_sha256(&metadata.header_mapping_sha256, "header mapping")?;
            if metadata.metadata_run_id.trim().is_empty()
                || aggregate.header_roster_sha256.as_deref()
                    != Some(metadata.header_roster_sha256.as_str())
                || metadata.metadata_row_count != metadata.mapped_sample_count
                || metadata.mapped_sample_count
                    != aggregate.called_diploid_people
                        + aggregate.partial_diploid_people
                        + aggregate.no_call_people
                        + aggregate.non_diploid_people
            {
                bail!("available genotype aggregate does not exactly reconcile to metadata binding totals");
            }
        }
        PrimaryMotifGenotypeStatus::Unavailable => {
            if metadata.is_some()
                || aggregate.reason_code != Some(AOU_GENOTYPE_UNAVAILABLE_REASON)
                || !aggregate.internal_pairs.is_empty()
                || !aggregate.cells.is_empty()
                || !aggregate.margins.is_empty()
            {
                bail!(
                    "typed unavailable genotype aggregate contains contradictory pairing evidence"
                );
            }
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(b"Y1_PRIMARY_MOTIF_GENOTYPE_RUN_RECEIPT_V1\0");
    for value in [
        product_run_id.as_bytes(),
        primary_run_id.as_bytes(),
        source_variant_id.as_bytes(),
        registry_digest.as_bytes(),
    ] {
        update_len_prefixed(&mut hasher, value);
    }
    update_len_prefixed(
        &mut hasher,
        &serde_json::to_vec(aggregate).context("failed to serialize genotype receipt aggregate")?,
    );
    if let Some(metadata) = metadata {
        update_len_prefixed(
            &mut hasher,
            &serde_json::to_vec(metadata)
                .context("failed to serialize metadata binding receipt")?,
        );
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Independent structural verifier for a staged anonymous aggregate. A
/// finalizer can call this without any person-level source rows.
pub fn validate_primary_motif_genotype_aggregation(
    aggregate: &PrimaryMotifGenotypeAggregation,
) -> anyhow::Result<()> {
    let actual_serialized_bytes = serde_json::to_vec(aggregate)
        .context("failed to validate genotype receipt serialization")?
        .len();
    if actual_serialized_bytes != aggregate.serialized_bytes
        || actual_serialized_bytes > MAX_SERIALIZED_AGGREGATE_BYTES
        || aggregate.internal_pairs.len() > MAX_GENOTYPE_PAIRS_PER_STRATUM
        || aggregate.cells.len() > MAX_GENOTYPE_CELLS_PER_STRATUM
        || aggregate.margins.len() > MAX_ALT_IDENTITIES + 1
    {
        bail!("genotype receipt violates an exact all-or-nothing output bound");
    }
    if aggregate.status == PrimaryMotifGenotypeStatus::Unavailable {
        if aggregate.reason_code != Some(AOU_GENOTYPE_UNAVAILABLE_REASON)
            || aggregate.called_diploid_people != 0
            || aggregate.partial_diploid_people != 0
            || aggregate.no_call_people != 0
            || aggregate.non_diploid_people != 0
            || aggregate.observed_an.is_some()
            || aggregate.header_roster_sha256.is_some()
            || aggregate.genotype_content_sha256.is_some()
            || !aggregate.internal_pairs.is_empty()
            || !aggregate.cells.is_empty()
            || !aggregate.margins.is_empty()
        {
            bail!("typed unavailable genotype aggregate contains contradictory pairing evidence");
        }
        return Ok(());
    }
    if aggregate.reason_code.is_some()
        || aggregate.margins.is_empty()
        || aggregate.partial_diploid_people != 0
        || aggregate.non_diploid_people != 0
    {
        bail!("available genotype aggregate has unavailable/incomplete pairing evidence or no margins");
    }
    require_sha256(
        aggregate
            .header_roster_sha256
            .as_deref()
            .context("available genotype aggregate lacks source roster digest")?,
        "header roster",
    )?;
    require_sha256(
        aggregate
            .genotype_content_sha256
            .as_deref()
            .context("available genotype aggregate lacks source content digest")?,
        "genotype content",
    )?;
    let mut reconstructed_margins = vec![0u32; aggregate.margins.len()];
    let mut reconstructed_cells = BTreeMap::<(u32, u32), u32>::new();
    let mut unique_pairs = BTreeSet::new();
    let mut pair_people = 0u32;
    for pair in &aggregate.internal_pairs {
        let key = (
            pair.shorter_exact_units,
            pair.shorter_allele_index,
            pair.longer_exact_units,
            pair.longer_allele_index,
        );
        if !unique_pairs.insert(key)
            || (pair.shorter_exact_units, pair.shorter_allele_index)
                > (pair.longer_exact_units, pair.longer_allele_index)
            || pair.people == 0
            || Some(pair.people) != pair.phased_people.checked_add(pair.unphased_people)
        {
            bail!("anonymous genotype pair row is duplicate, misordered, empty, or phase-inconsistent");
        }
        for allele_index in [pair.shorter_allele_index, pair.longer_allele_index] {
            let margin = reconstructed_margins
                .get_mut(allele_index as usize)
                .context("genotype pair allele index exceeds margin identities")?;
            *margin = margin
                .checked_add(pair.people)
                .context("reconstructed pair margin overflow")?;
        }
        let cell = reconstructed_cells
            .entry((pair.shorter_exact_units, pair.longer_exact_units))
            .or_default();
        *cell = cell
            .checked_add(pair.people)
            .context("reconstructed cell overflow")?;
        pair_people = pair_people
            .checked_add(pair.people)
            .context("reconstructed pair people overflow")?;
    }
    if pair_people != aggregate.called_diploid_people {
        bail!("anonymous pair people do not reconcile to called diploid people");
    }
    let mut supplied_cells = BTreeMap::new();
    for cell in &aggregate.cells {
        if cell.shorter_exact_units > cell.longer_exact_units
            || cell.people == 0
            || supplied_cells
                .insert(
                    (cell.shorter_exact_units, cell.longer_exact_units),
                    cell.people,
                )
                .is_some()
        {
            bail!("anonymous genotype cell is duplicate, misordered, or empty");
        }
    }
    if supplied_cells != reconstructed_cells {
        bail!("anonymous genotype cells do not exactly equal grouped internal pairs");
    }
    let mut expected_an = 0u32;
    for (index, margin) in aggregate.margins.iter().enumerate() {
        if margin.allele_index as usize != index
            || margin.paired_copies != reconstructed_margins[index]
            || margin
                .paired_copies
                .checked_add(margin.excluded_from_pairs_copies)
                != Some(margin.expected_copies)
        {
            bail!("genotype margin identities or exact paired/excluded proof differ");
        }
        expected_an = expected_an
            .checked_add(margin.expected_copies)
            .context("expected genotype AN overflow")?;
    }
    if aggregate.observed_an != Some(expected_an) {
        bail!("genotype margin copies do not reconcile to observed AN");
    }
    Ok(())
}

/// Parse one exact registered HGSVC/HPRC VCF line before carrier loss and build
/// its aggregate-only genotype product. The line must already come from the
/// generation-bound registered-record reader.
pub fn produce_registered_genotypes_from_vcf_line(
    header: &Y1Header,
    entry: &PrimaryRepeatRegistryEntry,
    line: &str,
) -> anyhow::Result<PrimaryMotifGenotypeAggregation> {
    entry.validate()?;
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() < 8
        || fields[0] != entry.chrom
        || fields[1] != entry.source_position.to_string()
        || fields[2] != entry.source_variant_id
    {
        bail!("VCF record identity does not exactly equal the registered genotype locus");
    }
    if header.cohort == Cohort::Aou {
        if fields.len() != 8 {
            bail!("AoU aggregate-only source unexpectedly contains FORMAT/sample pairing");
        }
        return aggregate_primary_motif_genotypes(
            Cohort::Aou,
            &RepresentedAlleles {
                reference: String::new(),
                alternates: Vec::new(),
                removed_anchor: 'N',
                represented_sequence_bytes: 0,
            },
            &entry.motif,
            &[],
            0,
            None,
        );
    }
    if fields.len() < 9 {
        bail!("HGSVC/HPRC registered genotype source lacks FORMAT/sample columns");
    }
    let alternates = fields[4].split(',').map(str::to_string).collect::<Vec<_>>();
    let mut ac = None;
    let mut an = None;
    for item in fields[7].split(';') {
        if let Some(value) = item.strip_prefix("AC=") {
            if ac.replace(value).is_some() {
                bail!("VCF INFO contains duplicate AC");
            }
        } else if let Some(value) = item.strip_prefix("AN=") {
            if an.replace(value).is_some() {
                bail!("VCF INFO contains duplicate AN");
            }
        }
    }
    let alt_ac = ac
        .context("VCF INFO lacks AC")?
        .split(',')
        .map(|value| value.parse::<u32>().context("VCF INFO/AC is not UInt32"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let an = an
        .context("VCF INFO lacks AN")?
        .parse::<u32>()
        .context("VCF INFO/AN is not UInt32")?;
    let represented = apply_trid_envelope_left_padding(entry, fields[3], &alternates)?;
    let complete =
        parse_complete_source_genotypes(header, line, &alt_ac, an).map_err(anyhow::Error::new)?;
    aggregate_primary_motif_genotypes(
        Cohort::HgsvcHprc,
        &represented,
        &entry.motif,
        &alt_ac,
        an,
        Some(&complete),
    )
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

    fn synthetic_header(sample_count: usize) -> Y1Header {
        Y1Header {
            cohort: Cohort::HgsvcHprc,
            reference_genome: crate::y1::model::ReferenceGenome::Grch38,
            sample_names: (0..sample_count)
                .map(|index| format!("private-{index}"))
                .collect(),
            frequency_divisions: Vec::new(),
            info_fields: BTreeMap::new(),
            format_fields: BTreeMap::new(),
        }
    }

    fn synthetic_complete_line(sample_count: usize, no_calls: usize, alt_count: usize) -> String {
        let mut fields = vec![
            "chr1".to_string(),
            "100".to_string(),
            "chr1-100-TRV-6".to_string(),
            "ACAGCAG".to_string(),
            std::iter::repeat_n("ACAG", alt_count)
                .collect::<Vec<_>>()
                .join(","),
            ".".to_string(),
            "PASS".to_string(),
            ".".to_string(),
            "GT".to_string(),
        ];
        fields.extend(
            (0..sample_count).map(|index| if index < no_calls { "./." } else { "0|1" }.to_string()),
        );
        fields.join("\t")
    }

    #[test]
    fn source_expectation_fixtures_reproduce_292_292_and_291_plus_one_no_call() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/y1/primary_motif_genotype_source_expectations.json"
        ))
        .unwrap();
        let observed = fixture["loci"]
            .as_array()
            .unwrap()
            .iter()
            .map(|locus| {
                let called = locus["called_diploid_people"].as_u64().unwrap() as usize;
                let no_calls = locus["no_call_people"].as_u64().unwrap() as usize;
                let sample_count = called + no_calls;
                let alt_count = locus["source_alt_count"].as_u64().unwrap() as usize;
                let expected_an = locus["info_an"].as_u64().unwrap() as u32;
                let mut expected_ac = vec![0u32; alt_count];
                expected_ac[0] = called as u32;
                let header = synthetic_header(sample_count);
                let line = synthetic_complete_line(sample_count, no_calls, alt_count);
                let complete =
                    parse_complete_source_genotypes(&header, &line, &expected_ac, expected_an)
                        .unwrap();
                let registry_entry = entry(locus["motif"].as_str().unwrap(), 100, 106);
                let alternates =
                    std::iter::repeat_n("ACAG".to_string(), alt_count).collect::<Vec<_>>();
                let represented =
                    apply_trid_envelope_left_padding(&registry_entry, "ACAGCAG", &alternates)
                        .unwrap();
                let aggregate = aggregate_primary_motif_genotypes(
                    Cohort::HgsvcHprc,
                    &represented,
                    locus["motif"].as_str().unwrap(),
                    &expected_ac,
                    expected_an,
                    Some(&complete),
                )
                .unwrap();
                assert_eq!(
                    aggregate.cells.iter().map(|cell| cell.people).sum::<u32>(),
                    called as u32
                );
                assert_eq!(aggregate.no_call_people, no_calls as u32);
                (
                    locus["catalog_id"].as_str().unwrap().to_string(),
                    called,
                    no_calls,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            observed,
            vec![
                ("HTT".to_string(), 292, 0),
                ("ATXN1".to_string(), 292, 0),
                ("RFC1".to_string(), 291, 1),
            ]
        );
    }

    #[test]
    fn partial_no_call_and_non_diploid_are_partitioned_and_margins_remain_exact() {
        let header = synthetic_header(4);
        let mut fields = synthetic_complete_line(4, 0, 1)
            .split('\t')
            .map(str::to_string)
            .collect::<Vec<_>>();
        fields[9..].clone_from_slice(&["0/1".into(), "./1".into(), "./.".into(), "1".into()]);
        let complete =
            parse_complete_source_genotypes(&header, &fields.join("\t"), &[3], 4).unwrap();
        assert_eq!(complete.totals.called_diploid_people, 1);
        assert_eq!(complete.totals.partial_diploid_people, 1);
        assert_eq!(complete.totals.no_call_people, 1);
        assert_eq!(complete.totals.non_diploid_people, 1);
        let represented =
            apply_trid_envelope_left_padding(&entry("CAG", 100, 106), "ACAGCAG", &["ACAG".into()])
                .unwrap();
        assert_eq!(complete.observed_allele_margins, vec![1, 3]);
        assert!(aggregate_primary_motif_genotypes(
            Cohort::HgsvcHprc,
            &represented,
            "CAG",
            &[3],
            4,
            Some(&complete),
        )
        .is_err());
        let mut malformed = synthetic_complete_line(4, 0, 1)
            .split('\t')
            .map(str::to_string)
            .collect::<Vec<_>>();
        malformed[9] = "0|".into();
        assert!(parse_complete_source_genotypes(&header, &malformed.join("\t"), &[4], 8).is_err());
    }

    #[test]
    fn aou_is_typed_unavailable_and_metadata_binding_emits_no_identifiers() {
        let unavailable = aggregate_primary_motif_genotypes(
            Cohort::Aou,
            &RepresentedAlleles {
                reference: String::new(),
                alternates: Vec::new(),
                removed_anchor: 'N',
                represented_sequence_bytes: 0,
            },
            "CAG",
            &[],
            0,
            None,
        )
        .unwrap();
        assert_eq!(unavailable.status, PrimaryMotifGenotypeStatus::Unavailable);
        assert_eq!(
            unavailable.reason_code,
            Some(AOU_GENOTYPE_UNAVAILABLE_REASON)
        );
        assert_eq!(
            unavailable.serialized_bytes,
            serde_json::to_vec(&unavailable).unwrap().len()
        );
        assert!(primary_motif_genotype_receipt_sha256(
            &unavailable,
            "aou-product",
            "primary-run",
            "source-record",
            &"b".repeat(64),
            None,
        )
        .is_ok());

        let header = synthetic_header(2);
        let line = synthetic_complete_line(2, 0, 1);
        let complete = parse_complete_source_genotypes(&header, &line, &[2], 4).unwrap();
        let metadata = header
            .sample_names
            .iter()
            .map(|sample| {
                (
                    sample.clone(),
                    PrimaryMotifSampleMetadata {
                        ancestry: "afr".into(),
                        sex: "XX".into(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mapping = primary_motif_metadata_mapping_sha256(&header, &metadata).unwrap();
        let bound = bind_complete_genotypes_to_metadata(
            &header,
            &complete,
            &metadata,
            "accepted-metadata-run",
            &"c".repeat(64),
            &"a".repeat(64),
            &mapping,
        )
        .unwrap();
        let serialized_receipt = serde_json::to_string(&bound.receipt).unwrap();
        assert!(!serialized_receipt.contains("private-0"));
        assert!(!serialized_receipt.contains("private-1"));
        assert_eq!(bound.calls.len(), 2);
        assert_eq!(bound.receipt.mapped_sample_count, 2);
        let represented =
            apply_trid_envelope_left_padding(&entry("CAG", 100, 106), "ACAGCAG", &["ACAG".into()])
                .unwrap();
        let source_strata = vec![
            PrimaryMotifStratumInput {
                division: "all".into(),
                ancestry: None,
                sex: None,
                alt_ac: vec![2],
                an: 4,
            },
            PrimaryMotifStratumInput {
                division: "afr_XX".into(),
                ancestry: Some("afr".into()),
                sex: Some("XX".into()),
                alt_ac: vec![2],
                an: 4,
            },
        ];
        let genotype_strata = aggregate_bound_primary_motif_genotype_strata(
            &represented,
            "CAG",
            &source_strata,
            &bound,
        )
        .unwrap();
        assert_eq!(genotype_strata.len(), 2);
        assert_eq!(genotype_strata[1].aggregate.called_diploid_people, 2);
        let mut corrupted_strata = source_strata;
        corrupted_strata[1].alt_ac[0] = 1;
        assert!(aggregate_bound_primary_motif_genotype_strata(
            &represented,
            "CAG",
            &corrupted_strata,
            &bound,
        )
        .is_err());
        let available = aggregate_primary_motif_genotypes(
            Cohort::HgsvcHprc,
            &represented,
            "CAG",
            &[2],
            4,
            Some(&complete),
        )
        .unwrap();
        let receipt = primary_motif_genotype_receipt_sha256(
            &available,
            "product-run",
            "primary-run",
            "source-record",
            &"b".repeat(64),
            Some(&bound.receipt),
        )
        .unwrap();
        assert_eq!(receipt.len(), 64);
        assert!(primary_motif_genotype_receipt_sha256(
            &available,
            "product-run",
            "primary-run",
            "source-record",
            &"b".repeat(64),
            None,
        )
        .is_err());
        let mut corrupted_cells = available.clone();
        corrupted_cells.cells[0].people += 1;
        finalize_genotype_serialized_bytes(&mut corrupted_cells).unwrap();
        assert!(primary_motif_genotype_receipt_sha256(
            &corrupted_cells,
            "product-run",
            "primary-run",
            "source-record",
            &"b".repeat(64),
            Some(&bound.receipt),
        )
        .is_err());
        let mut corrupted_size = available;
        corrupted_size.serialized_bytes += 1;
        assert!(primary_motif_genotype_receipt_sha256(
            &corrupted_size,
            "product-run",
            "primary-run",
            "source-record",
            &"b".repeat(64),
            Some(&bound.receipt),
        )
        .is_err());
        let mut incomplete = metadata;
        incomplete.remove("private-1");
        assert!(primary_motif_metadata_mapping_sha256(&header, &incomplete).is_err());
    }

    #[test]
    fn corrupted_info_margin_is_rejected_before_aggregation() {
        let header = synthetic_header(2);
        let line = synthetic_complete_line(2, 0, 1);
        assert!(parse_complete_source_genotypes(&header, &line, &[1], 4).is_err());
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
