//! Supported primary-motif planner/producer and source-independent rereconciler.
//!
//! The producer persists only aggregate rows. Every source/header/index read is
//! generation qualified. Re-entry is deterministic: a table is inserted only
//! when empty for the run, while an already-complete table must byte-for-byte
//! match the freshly recomputed JSON row set.

use super::primary_motif::{
    aggregate_bound_primary_motif_genotype_strata, aggregate_primary_motif_genotypes,
    aggregate_primary_motif_strata, apply_trid_envelope_left_padding,
    bind_complete_genotypes_to_metadata, PrimaryMotifGenotypeAggregation,
    PrimaryMotifGenotypeStatus, PrimaryMotifMetadataBindingReceipt, PrimaryMotifSampleMetadata,
    PrimaryMotifStratumInput, PrimaryRepeatRegistry, RegistryApprovalState, ANCHOR_RULE,
    AOU_GENOTYPE_UNAVAILABLE_REASON, MAX_ALT_IDENTITIES, MAX_GENOTYPE_CELLS_PER_STRATUM,
    MAX_GENOTYPE_PAIRS_PER_STRATUM, MAX_PRODUCER_BINS, MAX_REPRESENTED_SEQUENCE_BYTES,
    MAX_SERIALIZED_AGGREGATE_BYTES, MAX_SOURCE_DIVISIONS, METRIC,
};
use super::primary_motif_product::{
    append_product_run_transition, independent_receipt_digest, snapshot_product_rows,
    IndependentProductReceipt, ResolvedPrimaryProductInput,
};
use super::{ClickHouseTarget, Cohort, Y1Header};
use crate::loader::immutable_gcs::HttpGcsBackend;
use crate::loader::vcf_reader::read_immutable_header_text;
use crate::y1::parser::{parse_complete_source_genotypes, transform_record};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const ALGORITHM_VERSION: &str = "Y1_PRIMARY_MOTIF_PRODUCER_V1";
const PRODUCT_TABLE_SPECS: &[(&str, &str)] = &[
    ("lr_y1_primary_motif_loci", "chrom, source_position, source_variant_id"),
    ("lr_y1_primary_motif_allele_bins", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), exact_units"),
    ("lr_y1_primary_motif_genotype_pairs", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), shorter_exact_units, longer_exact_units, shorter_allele_index, longer_allele_index"),
    ("lr_y1_primary_motif_genotype_margins", "chrom, source_variant_id, division, ifNull(ancestry, ''), ifNull(sex, ''), allele_index"),
];

#[derive(Debug, Clone, Serialize)]
pub struct ProductProductionReport {
    pub product_run_id: String,
    pub primary_run_id: String,
    pub state: String,
    pub resumed: bool,
    pub physical: super::primary_motif_product::ProductPhysicalSnapshot,
}

#[derive(Debug, Clone)]
pub struct ProductMetadataSelection {
    pub metadata_run_id: String,
    pub accepted_receipt_sha256: String,
    pub manifest_sha256: String,
    pub header_mapping_sha256: String,
    pub rows: BTreeMap<String, PrimaryMotifSampleMetadata>,
}

#[derive(Deserialize)]
struct MetadataRunRow {
    state: String,
    source_manifest_sha256: String,
    report_sha256: String,
    output_rows: u16,
}

#[derive(Deserialize)]
struct MetadataRow {
    sample_id: String,
    superpopulation: String,
    sex: String,
}

pub fn resolve_product_metadata(
    target: &ClickHouseTarget,
    cohort: Cohort,
    metadata_run_id: Option<&str>,
    header: &Y1Header,
) -> anyhow::Result<Option<ProductMetadataSelection>> {
    if cohort == Cohort::Aou {
        if metadata_run_id.is_some() {
            bail!("AoU product must not bind sample metadata");
        }
        return Ok(None);
    }
    let metadata_run_id = metadata_run_id
        .filter(|value| !value.trim().is_empty())
        .context("HGSVC/HPRC product requires an accepted metadata run")?;
    let body = target.query_text(
        "SELECT state, source_manifest_sha256, report_sha256, output_rows FROM lr_y1_metadata_runs FINAL WHERE metadata_run_id = {metadata_run_id:String} FORMAT JSONEachRow",
        &[("metadata_run_id", metadata_run_id)],
    )?;
    let run: MetadataRunRow = exactly_one_json(&body, "accepted metadata run")?;
    if run.state != "accepted" || run.output_rows != 292 {
        bail!("metadata run is not one accepted 292-row contract");
    }
    require_sha256(&run.source_manifest_sha256, "metadata manifest")?;
    require_sha256(&run.report_sha256, "metadata receipt")?;
    let body = target.query_text(
        "SELECT sample_id, superpopulation, sex FROM lr_y1_sample_metadata WHERE metadata_run_id = {metadata_run_id:String} AND release = 'y1' AND cohort = 'hgsvc_hprc' AND reference_genome = 'GRCh38' ORDER BY sample_id FORMAT JSONEachRow",
        &[("metadata_run_id", metadata_run_id)],
    )?;
    let mut rows = BTreeMap::new();
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let row: MetadataRow =
            serde_json::from_str(line).context("invalid accepted metadata row")?;
        let ancestry = match row.superpopulation.as_str() {
            "AFR" => "afr",
            "AMR" => "amr",
            "ASJ" => "asj",
            "EAS" => "eas",
            "EUR" => "nfe",
            "SAS" => "sas",
            value => {
                bail!("metadata superpopulation {value:?} has no checked source-frequency stratum")
            }
        };
        let sex = match row.sex.to_ascii_lowercase().as_str() {
            "female" | "xx" => "XX",
            "male" | "xy" => "XY",
            value => bail!("metadata sex {value:?} has no checked source-frequency stratum"),
        };
        if rows
            .insert(
                row.sample_id,
                PrimaryMotifSampleMetadata {
                    ancestry: ancestry.into(),
                    sex: sex.into(),
                },
            )
            .is_some()
        {
            bail!("accepted metadata contains a duplicate sample ID");
        }
    }
    if rows.len() != 292 {
        bail!("accepted metadata physical rows are not exactly 292 unique samples");
    }
    let header_mapping_sha256 =
        super::primary_motif::primary_motif_metadata_mapping_sha256(header, &rows)?;
    Ok(Some(ProductMetadataSelection {
        metadata_run_id: metadata_run_id.into(),
        accepted_receipt_sha256: run.report_sha256,
        manifest_sha256: run.source_manifest_sha256,
        header_mapping_sha256,
        rows,
    }))
}

struct PreparedRows {
    loci: Vec<Value>,
    bins: Vec<Value>,
    pairs: Vec<Value>,
    margins: Vec<Value>,
    registered_locus_ids: Vec<String>,
    metadata_receipt: Option<PrimaryMotifMetadataBindingReceipt>,
    called_diploid_people: u64,
    partial_diploid_people: u64,
    no_call_people: u64,
    non_diploid_people: u64,
    serialized_bytes: u64,
}

impl PreparedRows {
    fn table_rows(&self) -> [(&'static str, &'static str, &[Value]); 4] {
        [
            (
                PRODUCT_TABLE_SPECS[0].0,
                PRODUCT_TABLE_SPECS[0].1,
                &self.loci,
            ),
            (
                PRODUCT_TABLE_SPECS[1].0,
                PRODUCT_TABLE_SPECS[1].1,
                &self.bins,
            ),
            (
                PRODUCT_TABLE_SPECS[2].0,
                PRODUCT_TABLE_SPECS[2].1,
                &self.pairs,
            ),
            (
                PRODUCT_TABLE_SPECS[3].0,
                PRODUCT_TABLE_SPECS[3].1,
                &self.margins,
            ),
        ]
    }
}

pub fn produce_product(
    reader: &ClickHouseTarget,
    writer: &ClickHouseTarget,
    registry: &PrimaryRepeatRegistry,
    resolved: &ResolvedPrimaryProductInput,
    cohort: Cohort,
    product_run_id: &str,
    metadata_run_id: Option<&str>,
    operator_identity: &str,
    message: &str,
) -> anyhow::Result<ProductProductionReport> {
    validate_request(
        registry,
        resolved,
        cohort,
        product_run_id,
        operator_identity,
        message,
    )?;
    super::primary_motif_product::attest_primary_motif_schema(reader)?;
    if !reader.same_destination(writer) || !writer.uses_dedicated_principal_auth() {
        bail!("producer requires a dedicated writer credential for the same scratch destination");
    }
    let reader_user = reader.query_text("SELECT currentUser() FORMAT TabSeparated", &[])?;
    let writer_user = writer.query_text("SELECT currentUser() FORMAT TabSeparated", &[])?;
    if reader_user.trim() == writer_user.trim() {
        bail!("producer reader/operator and dedicated writer principals must differ");
    }
    writer.attest_synchronous_inserts()?;
    let backend = Arc::new(HttpGcsBackend::new()?);
    let header_text = read_immutable_header_text(backend.clone(), &resolved.source.vcf_object())
        .context("failed to generation-read product VCF header")?;
    let header = Y1Header::parse(&header_text, cohort).map_err(anyhow::Error::new)?;
    let metadata = resolve_product_metadata(reader, cohort, metadata_run_id, &header)?;
    let resumed = ensure_planned_run(
        writer,
        registry,
        resolved,
        cohort,
        product_run_id,
        metadata.as_ref(),
        operator_identity,
        message,
    )?;
    let state = latest_state(writer, product_run_id)?;
    if state == "planned" {
        append_product_run_transition(
            writer,
            product_run_id,
            super::primary_motif::PrimaryMotifRunState::Producing,
            registry,
            operator_identity,
            "generation-qualified aggregate production started",
        )?;
    } else if state == "produced" {
        let physical = snapshot_product_rows(writer, product_run_id)?;
        return Ok(ProductProductionReport {
            product_run_id: product_run_id.into(),
            primary_run_id: resolved.primary_run_id.clone(),
            state,
            resumed: true,
            physical,
        });
    } else if state != "producing" {
        bail!("product run cannot be produced from state {state:?}");
    }

    let prepared = prepare_rows(
        registry,
        resolved,
        cohort,
        product_run_id,
        &header,
        metadata.as_ref(),
        backend,
    )?;
    for (table, order, rows) in prepared.table_rows() {
        ensure_exact_rows(writer, product_run_id, table, order, rows, true)?;
    }
    let physical = snapshot_product_rows(writer, product_run_id)?;
    append_produced_revision(
        writer,
        registry,
        product_run_id,
        &prepared,
        &physical,
        operator_identity,
        message,
    )?;
    Ok(ProductProductionReport {
        product_run_id: product_run_id.into(),
        primary_run_id: resolved.primary_run_id.clone(),
        state: "produced".into(),
        resumed,
        physical,
    })
}

pub fn reconcile_product(
    target: &ClickHouseTarget,
    registry: &PrimaryRepeatRegistry,
    resolved: &ResolvedPrimaryProductInput,
    cohort: Cohort,
    product_run_id: &str,
    metadata_run_id: Option<&str>,
) -> anyhow::Result<IndependentProductReceipt> {
    validate_request(
        registry,
        resolved,
        cohort,
        product_run_id,
        "independent-reconciler",
        "reconcile",
    )?;
    super::primary_motif_product::attest_primary_motif_schema(target)?;
    if latest_state(target, product_run_id)? != "produced" {
        bail!("independent reconciler requires a produced product run");
    }
    let backend = Arc::new(HttpGcsBackend::new()?);
    let header_text = read_immutable_header_text(backend.clone(), &resolved.source.vcf_object())
        .context("failed to independently generation-read product VCF header")?;
    let header = Y1Header::parse(&header_text, cohort).map_err(anyhow::Error::new)?;
    let metadata = resolve_product_metadata(target, cohort, metadata_run_id, &header)?;
    let prepared = prepare_rows(
        registry,
        resolved,
        cohort,
        product_run_id,
        &header,
        metadata.as_ref(),
        backend,
    )?;
    for (table, order, rows) in prepared.table_rows() {
        ensure_exact_rows(target, product_run_id, table, order, rows, false)?;
    }
    let physical = snapshot_product_rows(target, product_run_id)?;
    let approval = approval_name(registry.approval_state);
    let metadata_receipt = prepared.metadata_receipt.as_ref();
    let mut receipt = IndependentProductReceipt {
        contract: "Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1".into(),
        product_run_id: product_run_id.into(),
        primary_run_id: resolved.primary_run_id.clone(),
        release: "y1".into(),
        cohort: cohort.as_str().into(),
        reference_genome: "GRCh38".into(),
        chrom: resolved.chrom.clone(),
        source_inventory_sha256: registry.source_inventory_sha256.clone(),
        source_manifest_sha256: resolved.source.manifest_sha256.clone(),
        source_uri: resolved.source.source_uri.clone(),
        source_generation: resolved.source.source_generation.clone(),
        source_size_bytes: resolved.source.source_size_bytes,
        source_md5_base64: resolved.source.source_md5_base64.clone(),
        source_index_uri: resolved.source.source_index_uri.clone(),
        source_index_generation: resolved.source.source_index_generation.clone(),
        source_index_size_bytes: resolved.source.source_index_size_bytes,
        source_index_md5_base64: resolved.source.source_index_md5_base64.clone(),
        registry_digest: registry.content_sha256.clone(),
        registry_approval_state: approval.into(),
        registered_locus_ids: prepared.registered_locus_ids,
        complete_strata: true,
        no_truncation: true,
        exact_ac_an_and_genotype_margins: true,
        metadata_run_id: metadata_receipt.map(|value| value.metadata_run_id.clone()),
        metadata_receipt_sha256: metadata_receipt
            .map(|value| value.accepted_metadata_receipt_sha256.clone()),
        metadata_manifest_sha256: metadata_receipt
            .map(|value| value.metadata_manifest_sha256.clone()),
        physical,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = independent_receipt_digest(&receipt)?;
    receipt.validate()?;
    Ok(receipt)
}

fn prepare_rows(
    registry: &PrimaryRepeatRegistry,
    resolved: &ResolvedPrimaryProductInput,
    cohort: Cohort,
    product_run_id: &str,
    header: &Y1Header,
    metadata: Option<&ProductMetadataSelection>,
    backend: Arc<HttpGcsBackend>,
) -> anyhow::Result<PreparedRows> {
    let mut output = PreparedRows {
        loci: Vec::new(),
        bins: Vec::new(),
        pairs: Vec::new(),
        margins: Vec::new(),
        registered_locus_ids: Vec::new(),
        metadata_receipt: None,
        called_diploid_people: 0,
        partial_diploid_people: 0,
        no_call_people: 0,
        non_diploid_people: 0,
        serialized_bytes: 0,
    };
    let source = resolved.source.vcf_object();
    let index = resolved.source.index_object();
    let mut entries = registry
        .entries
        .iter()
        .filter(|entry| entry.chrom == resolved.chrom)
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| (entry.source_position, entry.source_variant_id.as_str()));
    for entry in entries {
        let canonical = resolved
            .loci
            .iter()
            .find(|locus| {
                locus.source_variant_id == entry.source_variant_id
                    && locus.position == entry.source_position
            })
            .context("resolved primary input lacks registered canonical locus")?;
        let line = super::primary_motif::read_generation_bound_registered_record(
            backend.clone(),
            &source,
            &index,
            entry,
        )?;
        let transformed = transform_record(header, &line).map_err(anyhow::Error::new)?;
        let represented = apply_trid_envelope_left_padding(
            entry,
            &transformed.summary.ref_allele,
            &transformed.summary.alts,
        )?;
        let stratum_inputs = transformed
            .summary
            .frequencies
            .iter()
            .map(|frequency| {
                let (ancestry, sex) = division_dimensions(&frequency.division)?;
                Ok(PrimaryMotifStratumInput {
                    division: frequency.division.clone(),
                    ancestry,
                    sex,
                    alt_ac: frequency
                        .ac
                        .clone()
                        .context("registered source stratum lacks AC")?,
                    an: frequency.an.context("registered source stratum lacks AN")?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let allele_strata =
            aggregate_primary_motif_strata(&represented, &entry.motif, stratum_inputs.clone())?;
        let (genotype_strata, metadata_receipt) = match cohort {
            Cohort::Aou => {
                let unavailable = aggregate_primary_motif_genotypes(
                    Cohort::Aou,
                    &represented,
                    &entry.motif,
                    &[],
                    0,
                    None,
                )?;
                (vec![("all".to_string(), None, None, unavailable)], None)
            }
            Cohort::HgsvcHprc => {
                let metadata = metadata.context("HGSVC/HPRC prepared rows lack metadata")?;
                let complete = parse_complete_source_genotypes(
                    header,
                    &line,
                    &transformed.summary.ac,
                    transformed.summary.an,
                )
                .map_err(anyhow::Error::new)?;
                let bound = bind_complete_genotypes_to_metadata(
                    header,
                    &complete,
                    &metadata.rows,
                    &metadata.metadata_run_id,
                    &metadata.accepted_receipt_sha256,
                    &metadata.manifest_sha256,
                    &metadata.header_mapping_sha256,
                )?;
                let receipt = bound.receipt.clone();
                let strata = aggregate_bound_primary_motif_genotype_strata(
                    &represented,
                    &entry.motif,
                    &stratum_inputs,
                    &bound,
                )?
                .into_iter()
                .map(|value| (value.division, value.ancestry, value.sex, value.aggregate))
                .collect();
                (strata, Some(receipt))
            }
        };
        if let Some(receipt) = &metadata_receipt {
            match &output.metadata_receipt {
                Some(existing) if existing != receipt => {
                    bail!("metadata binding changed between registered loci")
                }
                None => output.metadata_receipt = Some(receipt.clone()),
                _ => {}
            }
        }
        append_locus_rows(
            &mut output,
            registry,
            resolved,
            product_run_id,
            entry,
            canonical,
            &line,
            &represented,
            &allele_strata,
            &genotype_strata,
            metadata_receipt.as_ref(),
        )?;
    }
    if output.loci.len() != output.registered_locus_ids.len() || output.loci.is_empty() {
        bail!("prepared product locus inventory is empty or incomplete");
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn append_locus_rows(
    output: &mut PreparedRows,
    registry: &PrimaryRepeatRegistry,
    resolved: &ResolvedPrimaryProductInput,
    product_run_id: &str,
    entry: &super::primary_motif::PrimaryRepeatRegistryEntry,
    canonical: &super::primary_motif_product::CanonicalPrimaryLocus,
    line: &str,
    represented: &super::primary_motif::RepresentedAlleles,
    allele_strata: &[super::primary_motif::PrimaryMotifStratumDistribution],
    genotype_strata: &[(
        String,
        Option<String>,
        Option<String>,
        PrimaryMotifGenotypeAggregation,
    )],
    metadata: Option<&PrimaryMotifMetadataBindingReceipt>,
) -> anyhow::Result<()> {
    let base = |division: &str, ancestry: &Option<String>, sex: &Option<String>| {
        json!({
            "product_run_id": product_run_id, "release": "y1", "cohort": resolved.cohort,
            "reference_genome": "GRCh38", "chrom": resolved.chrom,
            "primary_run_id": resolved.primary_run_id, "source_variant_id": entry.source_variant_id,
            "canonical_locus_id": entry.canonical_locus_id, "registry_digest": registry.content_sha256,
            "metric": METRIC, "division": division, "ancestry": ancestry, "sex": sex
        })
    };
    for stratum in allele_strata {
        for bin in &stratum.distribution.bins {
            let mut row = base(&stratum.division, &stratum.ancestry, &stratum.sex);
            extend(
                &mut row,
                json!({
                    "exact_units": bin.exact_units, "allele_copies": bin.allele_copies,
                    "reference_copies": bin.reference_copies, "alternate_copies": bin.alternate_copies,
                    "stratum_an": stratum.distribution.an, "stratum_alt_ac": stratum.distribution.alt_ac_sum,
                    "stratum_ref_copies": stratum.distribution.reference_copies,
                    "stratum_receipt_sha256": stratum.distribution.allele_frequency_receipt_sha256
                }),
            )?;
            output.bins.push(row);
        }
    }
    for (division, ancestry, sex, aggregate) in genotype_strata {
        super::primary_motif::validate_primary_motif_genotype_aggregation(aggregate)?;
        let aggregate_digest = sha256_json(b"Y1_PRIMARY_MOTIF_GENOTYPE_STRATUM_V1\0", aggregate)?;
        for pair in &aggregate.internal_pairs {
            let mut row = base(division, ancestry, sex);
            extend(
                &mut row,
                json!({
                    "shorter_allele_index": pair.shorter_allele_index,
                    "longer_allele_index": pair.longer_allele_index,
                    "shorter_exact_units": pair.shorter_exact_units,
                    "longer_exact_units": pair.longer_exact_units, "people": pair.people,
                    "phased_people": pair.phased_people, "unphased_people": pair.unphased_people,
                    "pair_receipt_sha256": aggregate_digest
                }),
            )?;
            output.pairs.push(row);
        }
        for margin in &aggregate.margins {
            let mut row = base(division, ancestry, sex);
            extend(
                &mut row,
                json!({
                    "allele_index": margin.allele_index, "expected_copies": margin.expected_copies,
                    "paired_copies": margin.paired_copies,
                    "excluded_from_pairs_copies": margin.excluded_from_pairs_copies,
                    "margin_receipt_sha256": aggregate_digest
                }),
            )?;
            output.margins.push(row);
        }
    }
    let overall = allele_strata
        .iter()
        .find(|value| value.division == "all")
        .context("prepared allele strata lack all")?;
    let genotype = genotype_strata
        .iter()
        .find(|value| value.0 == "all")
        .context("prepared genotype strata lack all")?;
    let aggregate = &genotype.3;
    let allele_receipt = sha256_json(b"Y1_PRIMARY_MOTIF_ALL_STRATA_V1\0", allele_strata)?;
    let genotype_receipt = sha256_json(
        b"Y1_PRIMARY_MOTIF_ALL_GENOTYPE_STRATA_V1\0",
        genotype_strata,
    )?;
    let component_digest = sha256_json(
        b"Y1_PRIMARY_MOTIF_COMPONENTS_V1\0",
        &entry.ordered_components,
    )?;
    let envelope_start = entry
        .ordered_components
        .iter()
        .map(|value| value.start0)
        .min()
        .unwrap();
    let envelope_end = entry
        .ordered_components
        .iter()
        .map(|value| value.end0)
        .max()
        .unwrap();
    let serialized_bytes = genotype_strata.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(value.3.serialized_bytes as u64)
            .context("serialized aggregate bytes overflow")
    })?;
    let pair_count = genotype_strata
        .iter()
        .map(|value| value.3.internal_pairs.len() as u64)
        .sum::<u64>();
    let cell_count = genotype_strata
        .iter()
        .map(|value| value.3.cells.len() as u64)
        .sum::<u64>();
    let margin_count = genotype_strata
        .iter()
        .map(|value| value.3.margins.len() as u64)
        .sum::<u64>();
    let (metadata_run_id, metadata_receipt, metadata_manifest, header_roster, header_mapping) =
        metadata
            .map(|value| {
                (
                    Some(value.metadata_run_id.clone()),
                    Some(value.accepted_metadata_receipt_sha256.clone()),
                    Some(value.metadata_manifest_sha256.clone()),
                    Some(value.header_roster_sha256.clone()),
                    Some(value.header_mapping_sha256.clone()),
                )
            })
            .unwrap_or((None, None, None, None, None));
    let status = match aggregate.status {
        PrimaryMotifGenotypeStatus::Available => "AVAILABLE",
        PrimaryMotifGenotypeStatus::Unavailable => "UNAVAILABLE",
    };
    output.loci.push(json!({
        "product_run_id": product_run_id, "release": "y1", "cohort": resolved.cohort,
        "reference_genome": "GRCh38", "chrom": resolved.chrom, "primary_run_id": resolved.primary_run_id,
        "primary_task_id": canonical.task_id, "primary_attempt_id": canonical.attempt_id,
        "source_variant_id": entry.source_variant_id, "canonical_locus_id": entry.canonical_locus_id,
        "source_position": entry.source_position, "source_uri": resolved.source.source_uri,
        "source_generation": resolved.source.source_generation, "source_size_bytes": resolved.source.source_size_bytes,
        "source_md5_base64": resolved.source.source_md5_base64, "source_index_uri": resolved.source.source_index_uri,
        "source_index_generation": resolved.source.source_index_generation,
        "source_index_size_bytes": resolved.source.source_index_size_bytes,
        "source_index_md5_base64": resolved.source.source_index_md5_base64,
        "source_record_sha256": format!("{:x}", Sha256::digest(line.as_bytes())),
        "genotype_content_sha256": aggregate.genotype_content_sha256,
        "component_starts0": entry.ordered_components.iter().map(|value| value.start0).collect::<Vec<_>>(),
        "component_ends0": entry.ordered_components.iter().map(|value| value.end0).collect::<Vec<_>>(),
        "component_motifs": entry.ordered_components.iter().map(|value| value.motif.clone()).collect::<Vec<_>>(),
        "component_digest": component_digest, "primary_component_index": entry.component_index,
        "primary_motif": entry.motif, "selection_basis": enum_name(&entry.selection_basis)?,
        "biological_role": entry.biological_role, "catalog_id": entry.catalog_id,
        "catalog_digest": entry.catalog_digest, "registry_digest": registry.content_sha256,
        "registry_approval_state": approval_name(registry.approval_state), "metric": METRIC,
        "algorithm_version": ALGORITHM_VERSION, "algorithm_sha256": algorithm_sha256(),
        "anchor_rule": ANCHOR_RULE, "anchor_base": represented.removed_anchor.to_string(),
        "trid_envelope_start0": envelope_start, "trid_envelope_end0": envelope_end,
        "ref_with_anchor_bytes": represented.reference.len() + 1, "represented_ref_bytes": represented.reference.len(),
        "alts_checked": represented.alternates.len(), "represented_sequence_bytes": represented.represented_sequence_bytes,
        "stratum_count": allele_strata.len(), "bin_count": allele_strata.iter().map(|value| value.distribution.bins.len()).sum::<usize>(),
        "overall_an": overall.distribution.an, "overall_alt_ac": overall.distribution.alt_ac_sum,
        "overall_ref_copies": overall.distribution.reference_copies, "genotype_status": status,
        "genotype_reason_code": aggregate.reason_code, "called_diploid_people": aggregate.called_diploid_people,
        "partial_diploid_people": aggregate.partial_diploid_people, "no_call_people": aggregate.no_call_people,
        "non_diploid_people": aggregate.non_diploid_people, "genotype_observed_an": aggregate.observed_an,
        "genotype_pair_count": pair_count, "genotype_cell_count": cell_count, "genotype_margin_count": margin_count,
        "metadata_run_id": metadata_run_id, "accepted_metadata_receipt_sha256": metadata_receipt,
        "metadata_manifest_sha256": metadata_manifest, "header_roster_sha256": header_roster,
        "header_mapping_sha256": header_mapping, "bounds_status": "complete_no_truncation", "status": "complete",
        "reason_code": if status == "UNAVAILABLE" { Some(AOU_GENOTYPE_UNAVAILABLE_REASON) } else { None },
        "allele_receipt_sha256": allele_receipt, "genotype_receipt_sha256": genotype_receipt,
        "serialized_bytes": serialized_bytes
    }));
    output
        .registered_locus_ids
        .push(entry.registry_entry_id.clone());
    output.called_diploid_people += u64::from(aggregate.called_diploid_people);
    output.partial_diploid_people += u64::from(aggregate.partial_diploid_people);
    output.no_call_people += u64::from(aggregate.no_call_people);
    output.non_diploid_people += u64::from(aggregate.non_diploid_people);
    output.serialized_bytes = output
        .serialized_bytes
        .checked_add(serialized_bytes)
        .context("run serialized bytes overflow")?;
    Ok(())
}

fn ensure_planned_run(
    target: &ClickHouseTarget,
    registry: &PrimaryRepeatRegistry,
    resolved: &ResolvedPrimaryProductInput,
    cohort: Cohort,
    product_run_id: &str,
    metadata: Option<&ProductMetadataSelection>,
    operator_identity: &str,
    message: &str,
) -> anyhow::Result<bool> {
    let body = target.query_text(
        "SELECT count() FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} FORMAT TabSeparated",
        &[("product_run_id", product_run_id)],
    )?;
    if body.trim() != "0" {
        attest_run_identity(target, registry, resolved, cohort, product_run_id, metadata)?;
        return Ok(true);
    }
    for (table, _) in PRODUCT_TABLE_SPECS {
        let count = target.query_text(
            &format!("SELECT count() FROM {table} WHERE product_run_id = {{product_run_id:String}} FORMAT TabSeparated"),
            &[("product_run_id", product_run_id)],
        )?;
        if count.trim() != "0" {
            bail!("product rows exist without their planned run ledger");
        }
    }
    let now = now_parts()?;
    let empty = empty_content_sha256();
    let executable = executable_sha256()?;
    let (metadata_run_id, metadata_receipt, metadata_manifest, header_mapping) = metadata
        .map(|value| {
            (
                Some(value.metadata_run_id.clone()),
                Some(value.accepted_receipt_sha256.clone()),
                Some(value.manifest_sha256.clone()),
                Some(value.header_mapping_sha256.clone()),
            )
        })
        .unwrap_or((None, None, None, None));
    let mut row = json!({
        "product_run_id": product_run_id, "revision": now.0, "state": "planned", "release": "y1",
        "cohort": cohort.as_str(), "reference_genome": "GRCh38", "chrom": resolved.chrom,
        "primary_database": resolved.primary_database, "primary_run_id": resolved.primary_run_id,
        "registry_digest": registry.content_sha256, "registry_approval_state": approval_name(registry.approval_state),
        "metric": METRIC, "algorithm_version": ALGORITHM_VERSION, "algorithm_sha256": algorithm_sha256(),
        "executable_revision": option_env!("GNOMAD_LR_BUILD_REVISION").unwrap_or(env!("CARGO_PKG_VERSION")),
        "executable_sha256": executable, "anchor_rule": ANCHOR_RULE,
        "source_inventory_sha256": registry.source_inventory_sha256,
        "max_alt_identities": MAX_ALT_IDENTITIES, "max_represented_sequence_bytes": MAX_REPRESENTED_SEQUENCE_BYTES,
        "max_producer_bins": MAX_PRODUCER_BINS, "max_source_divisions": MAX_SOURCE_DIVISIONS,
        "max_genotype_pairs_per_stratum": MAX_GENOTYPE_PAIRS_PER_STRATUM,
        "max_genotype_cells_per_stratum": MAX_GENOTYPE_CELLS_PER_STRATUM,
        "max_serialized_aggregate_bytes": MAX_SERIALIZED_AGGREGATE_BYTES,
        "bounds_status": "planned", "metadata_run_id": metadata_run_id,
        "accepted_metadata_receipt_sha256": metadata_receipt, "metadata_manifest_sha256": metadata_manifest,
        "header_roster_sha256": Value::Null, "header_mapping_sha256": header_mapping,
        "locus_rows": 0, "bin_rows": 0, "genotype_pair_rows": 0, "genotype_margin_rows": 0,
        "called_diploid_people": 0, "partial_diploid_people": 0, "no_call_people": 0,
        "non_diploid_people": 0, "serialized_bytes": 0, "locus_content_sha256": empty,
        "bin_content_sha256": empty, "genotype_pair_content_sha256": Value::Null,
        "genotype_margin_content_sha256": Value::Null, "receipt_sha256": empty_content_sha256(),
        "created_at": now.1, "updated_at": now.1, "operator_identity": operator_identity, "message": message
    });
    let receipt = sha256_json(
        b"Y1_PRIMARY_MOTIF_RUN_REVISION_V1\0",
        &without_key(&row, "receipt_sha256")?,
    )?;
    row["receipt_sha256"] = Value::from(receipt);
    target.insert_json_each_row("lr_y1_primary_motif_runs", &[row])?;
    attest_run_identity(target, registry, resolved, cohort, product_run_id, metadata)?;
    Ok(false)
}

fn append_produced_revision(
    target: &ClickHouseTarget,
    registry: &PrimaryRepeatRegistry,
    product_run_id: &str,
    prepared: &PreparedRows,
    physical: &super::primary_motif_product::ProductPhysicalSnapshot,
    operator_identity: &str,
    message: &str,
) -> anyhow::Result<()> {
    let body = target.query_text(
        "SELECT * FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT JSONEachRow",
        &[("product_run_id", product_run_id)],
    )?;
    let mut row: Value = exactly_one_json(&body, "producing product run")?;
    if row["state"] != "producing" || row["registry_digest"] != registry.content_sha256 {
        bail!("latest run revision is not the bound producing run");
    }
    let now = now_parts()?;
    let object = row
        .as_object_mut()
        .context("run revision is not an object")?;
    object.insert("revision".into(), Value::from(now.0));
    object.insert("state".into(), Value::from("produced"));
    object.insert(
        "bounds_status".into(),
        Value::from("complete_no_truncation"),
    );
    object.insert("updated_at".into(), Value::from(now.1));
    object.insert("operator_identity".into(), Value::from(operator_identity));
    object.insert("message".into(), Value::from(message));
    for (name, value) in [
        ("locus_rows", physical.locus_rows),
        ("bin_rows", physical.bin_rows),
        ("genotype_pair_rows", physical.genotype_pair_rows),
        ("genotype_margin_rows", physical.genotype_margin_rows),
        ("called_diploid_people", prepared.called_diploid_people),
        ("partial_diploid_people", prepared.partial_diploid_people),
        ("no_call_people", prepared.no_call_people),
        ("non_diploid_people", prepared.non_diploid_people),
        ("serialized_bytes", prepared.serialized_bytes),
    ] {
        object.insert(name.into(), Value::from(value));
    }
    object.insert(
        "locus_content_sha256".into(),
        Value::from(physical.locus_content_sha256.clone()),
    );
    object.insert(
        "bin_content_sha256".into(),
        Value::from(physical.bin_content_sha256.clone()),
    );
    object.insert(
        "genotype_pair_content_sha256".into(),
        if physical.genotype_pair_rows == 0 {
            Value::Null
        } else {
            Value::from(physical.genotype_pair_content_sha256.clone())
        },
    );
    object.insert(
        "genotype_margin_content_sha256".into(),
        if physical.genotype_margin_rows == 0 {
            Value::Null
        } else {
            Value::from(physical.genotype_margin_content_sha256.clone())
        },
    );
    if let Some(receipt) = &prepared.metadata_receipt {
        object.insert(
            "header_roster_sha256".into(),
            Value::from(receipt.header_roster_sha256.clone()),
        );
        object.insert(
            "header_mapping_sha256".into(),
            Value::from(receipt.header_mapping_sha256.clone()),
        );
    }
    object.insert("receipt_sha256".into(), Value::from(empty_content_sha256()));
    let receipt = sha256_json(
        b"Y1_PRIMARY_MOTIF_RUN_REVISION_V1\0",
        &without_key(&row, "receipt_sha256")?,
    )?;
    row.as_object_mut()
        .context("run revision is not an object")?
        .insert("receipt_sha256".into(), Value::from(receipt));
    target.insert_json_each_row("lr_y1_primary_motif_runs", &[row])?;
    if latest_state(target, product_run_id)? != "produced" {
        bail!("produced revision was concurrently superseded");
    }
    Ok(())
}

fn attest_run_identity(
    target: &ClickHouseTarget,
    registry: &PrimaryRepeatRegistry,
    resolved: &ResolvedPrimaryProductInput,
    cohort: Cohort,
    product_run_id: &str,
    metadata: Option<&ProductMetadataSelection>,
) -> anyhow::Result<()> {
    let body = target.query_text(
        "SELECT cohort, chrom, primary_database, primary_run_id, registry_digest, registry_approval_state, source_inventory_sha256, metadata_run_id, accepted_metadata_receipt_sha256, metadata_manifest_sha256 FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT JSONEachRow",
        &[("product_run_id", product_run_id)],
    )?;
    let value: Value = exactly_one_json(&body, "bound product run")?;
    let expected_metadata = metadata.map(|value| value.metadata_run_id.as_str());
    let expected_metadata_receipt = metadata.map(|value| value.accepted_receipt_sha256.as_str());
    let expected_metadata_manifest = metadata.map(|value| value.manifest_sha256.as_str());
    let expected_header_mapping = metadata.map(|value| value.header_mapping_sha256.as_str());
    if value["cohort"] != cohort.as_str()
        || value["chrom"] != resolved.chrom
        || value["primary_database"] != resolved.primary_database
        || value["primary_run_id"] != resolved.primary_run_id
        || value["registry_digest"] != registry.content_sha256
        || value["registry_approval_state"] != approval_name(registry.approval_state)
        || value["source_inventory_sha256"] != registry.source_inventory_sha256
        || value["metadata_run_id"].as_str() != expected_metadata
        || value["accepted_metadata_receipt_sha256"].as_str() != expected_metadata_receipt
        || value["metadata_manifest_sha256"].as_str() != expected_metadata_manifest
        || value["header_mapping_sha256"].as_str() != expected_header_mapping
    {
        bail!("existing product run identity differs from requested immutable inputs");
    }
    Ok(())
}

fn ensure_exact_rows(
    target: &ClickHouseTarget,
    product_run_id: &str,
    table: &str,
    order: &str,
    expected: &[Value],
    allow_insert: bool,
) -> anyhow::Result<()> {
    let body = target.query_text(
        &format!("SELECT * FROM {table} WHERE product_run_id = {{product_run_id:String}} ORDER BY {order} FORMAT JSONEachRow"),
        &[("product_run_id", product_run_id)],
    )?;
    let observed = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).context("invalid persisted product row"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    if observed.is_empty() && !expected.is_empty() && allow_insert {
        target.insert_json_each_row(table, expected)?;
        return ensure_exact_rows(target, product_run_id, table, order, expected, false);
    }
    if normalize_values(observed) != normalize_values(expected.to_vec()) {
        bail!("{table} rows differ from complete generation-qualified recomputation");
    }
    Ok(())
}

fn normalize_values(mut values: Vec<Value>) -> Vec<Value> {
    for value in &mut values {
        normalize_value(value);
    }
    values.sort_by_cached_key(|value| serde_json::to_string(value).unwrap_or_default());
    values
}

fn normalize_value(value: &mut Value) {
    match value {
        Value::String(text) if text.bytes().all(|byte| byte.is_ascii_digit()) => {
            if let Ok(number) = text.parse::<u64>() {
                *value = Value::from(number);
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_value(value);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                normalize_value(value);
            }
        }
        _ => {}
    }
}

fn validate_request(
    registry: &PrimaryRepeatRegistry,
    resolved: &ResolvedPrimaryProductInput,
    cohort: Cohort,
    product_run_id: &str,
    operator_identity: &str,
    message: &str,
) -> anyhow::Result<()> {
    registry.validate()?;
    if product_run_id.trim().is_empty()
        || operator_identity.trim().is_empty()
        || message.trim().is_empty()
    {
        bail!("product run, operator, and message must be nonempty");
    }
    if resolved.cohort != cohort.as_str()
        || resolved.chrom.trim().is_empty()
        || resolved.primary_run_id.trim().is_empty()
    {
        bail!("resolved primary input differs from requested cohort or lacks identity");
    }
    let expected = registry
        .entries
        .iter()
        .filter(|entry| entry.chrom == resolved.chrom)
        .map(|entry| entry.source_variant_id.as_str())
        .collect::<BTreeSet<_>>();
    let observed = resolved
        .loci
        .iter()
        .map(|locus| locus.source_variant_id.as_str())
        .collect::<BTreeSet<_>>();
    if expected != observed || expected.len() != resolved.loci.len() {
        bail!("resolved canonical locus inventory differs from the complete registry contig inventory");
    }
    Ok(())
}

fn division_dimensions(division: &str) -> anyhow::Result<(Option<String>, Option<String>)> {
    if division == "all" {
        return Ok((None, None));
    }
    if matches!(division, "XX" | "XY") {
        return Ok((None, Some(division.into())));
    }
    if let Some((ancestry, sex)) = division.rsplit_once('_') {
        if matches!(sex, "XX" | "XY")
            && !ancestry.is_empty()
            && ancestry.bytes().all(|byte| byte.is_ascii_lowercase())
        {
            return Ok((Some(ancestry.into()), Some(sex.into())));
        }
    }
    if division.bytes().all(|byte| byte.is_ascii_lowercase()) {
        return Ok((Some(division.into()), None));
    }
    bail!("unsupported source stratum identity {division:?}")
}

fn latest_state(target: &ClickHouseTarget, product_run_id: &str) -> anyhow::Result<String> {
    let body = target.query_text(
        "SELECT state FROM lr_y1_primary_motif_runs WHERE product_run_id = {product_run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT TabSeparated",
        &[("product_run_id", product_run_id)],
    )?;
    let state = body.trim().to_string();
    if state.is_empty() {
        bail!("product run has no ledger revision");
    }
    Ok(state)
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

fn extend(target: &mut Value, source: Value) -> anyhow::Result<()> {
    let target = target
        .as_object_mut()
        .context("target row is not an object")?;
    let source = source.as_object().context("source row is not an object")?;
    target.extend(source.clone());
    Ok(())
}

fn without_key(value: &Value, key: &str) -> anyhow::Result<Value> {
    let mut value = value.clone();
    value
        .as_object_mut()
        .context("digest value is not an object")?
        .remove(key);
    Ok(value)
}

fn sha256_json<T: Serialize + ?Sized>(domain: &[u8], value: &T) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(serde_json::to_vec(value)?);
    Ok(format!("{:x}", hasher.finalize()))
}

fn algorithm_sha256() -> String {
    let mut hasher = Sha256::new();
    hasher.update(include_bytes!("primary_motif.rs"));
    hasher.update(include_bytes!("primary_motif_producer.rs"));
    format!("{:x}", hasher.finalize())
}

fn executable_sha256() -> anyhow::Result<String> {
    let path = std::env::current_exe()?;
    Ok(format!("{:x}", Sha256::digest(std::fs::read(path)?)))
}

fn empty_content_sha256() -> String {
    format!("{:x}", Sha256::digest([]))
}

fn now_parts() -> anyhow::Result<(u64, u64)> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok((u64::try_from(elapsed.as_nanos())?, elapsed.as_secs()))
}

fn enum_name<T: Serialize>(value: &T) -> anyhow::Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(str::to_string)
        .context("enum did not serialize as text")
}

fn approval_name(value: RegistryApprovalState) -> &'static str {
    match value {
        RegistryApprovalState::CandidatePendingScience => "CANDIDATE_PENDING_SCIENCE",
        RegistryApprovalState::Reviewed => "REVIEWED",
    }
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

    #[test]
    fn source_divisions_map_to_typed_metadata_dimensions() {
        assert_eq!(division_dimensions("all").unwrap(), (None, None));
        assert_eq!(
            division_dimensions("XX").unwrap(),
            (None, Some("XX".into()))
        );
        assert_eq!(
            division_dimensions("nfe").unwrap(),
            (Some("nfe".into()), None)
        );
        assert_eq!(
            division_dimensions("afr_XY").unwrap(),
            (Some("afr".into()), Some("XY".into()))
        );
        assert!(division_dimensions("AFR_XY").is_err());
    }

    #[test]
    fn candidate_registry_name_stays_fail_closed() {
        let registry = PrimaryRepeatRegistry::from_slice(include_bytes!(
            "../../sources/y1/primary-repeat-registry.json"
        ))
        .unwrap();
        assert_eq!(
            approval_name(registry.approval_state),
            "CANDIDATE_PENDING_SCIENCE"
        );
        assert!(registry.require_production_approval().is_err());
    }

    #[test]
    fn persisted_rows_forbid_person_level_keys() {
        let forbidden = ["sample_id", "person_id", "raw_gt", "gt_alleles"];
        for ddl in [
            include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_loci.sql"),
            include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_allele_bins.sql"),
            include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_genotype_pairs.sql"),
            include_str!("../../sql/y1/primary_motif/lr_y1_primary_motif_genotype_margins.sql"),
        ] {
            for key in forbidden {
                assert!(!ddl.contains(key));
            }
        }
    }
}
