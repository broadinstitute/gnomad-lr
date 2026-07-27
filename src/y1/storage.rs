use super::model::*;
use super::target::{ClickHouseTarget, TargetKind};
use anyhow::{bail, Context};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

pub const Y1_SCHEMA_VERSION: u16 = 1;

const SUMMARY_COLUMNS: &str = "run_id, release, cohort, reference_genome, chrom, position, source_variant_id, ref_allele, alts, allele_type, qual, filters, ac, an, af, allele_lengths, length_provenance, source_allele_length, source_svlen, source_svlen_present, frequencies_json, source_info_json";
const ALLELE_COLUMNS: &str = "run_id, release, cohort, reference_genome, chrom, position, reference_end, xpos, source_variant_id, alt_index, ref_allele, alt, allele_type, qual, filters, ac, an, af, allele_length, length_provenance, source_info_json";
const FREQUENCY_COLUMNS: &str = "run_id, release, cohort, reference_genome, chrom, position, source_variant_id, alt_index, division, ac, an, af, values_available";
const CARRIER_COLUMNS: &str = "run_id, release, cohort, reference_genome, chrom, position, source_variant_id, alt_index, alt, sample_id, genotype_position, gt_alleles, gt_phased, genotype_fields_json, position_fields_json";

const ACCEPTED_ATTEMPTS: &str = r#"
SELECT task_id, any(attempt_id) AS attempt_id
FROM (
    SELECT task_id, attempt_id, argMax(state, revision) AS state
    FROM lr_y1_task_attempts
    WHERE run_id = {run_id:String}
    GROUP BY task_id, attempt_id
)
WHERE state = 'accepted'
GROUP BY task_id
HAVING count() = 1
"#;

pub fn init_schema(target: &ClickHouseTarget) -> anyhow::Result<()> {
    let schemas: &[(&str, &str)] = &[
        (
            "lr_y1_load_runs",
            include_str!("../../sql/y1/lr_y1_load_runs.sql"),
        ),
        (
            "lr_y1_task_attempts",
            include_str!("../../sql/y1/lr_y1_task_attempts.sql"),
        ),
        (
            "lr_y1_active_partitions",
            include_str!("../../sql/y1/lr_y1_active_partitions.sql"),
        ),
        (
            "lr_y1_rejects_staging",
            include_str!("../../sql/y1/lr_y1_rejects_staging.sql"),
        ),
        (
            "lr_y1_summaries_staging",
            include_str!("../../sql/y1/lr_y1_summaries_staging.sql"),
        ),
        (
            "lr_y1_alleles_staging",
            include_str!("../../sql/y1/lr_y1_alleles_staging.sql"),
        ),
        (
            "lr_y1_frequencies_staging",
            include_str!("../../sql/y1/lr_y1_frequencies_staging.sql"),
        ),
        (
            "lr_y1_carriers_staging",
            include_str!("../../sql/y1/lr_y1_carriers_staging.sql"),
        ),
        (
            "lr_y1_summaries",
            include_str!("../../sql/y1/lr_y1_summaries.sql"),
        ),
        (
            "lr_y1_alleles",
            include_str!("../../sql/y1/lr_y1_alleles.sql"),
        ),
        (
            "lr_y1_frequencies",
            include_str!("../../sql/y1/lr_y1_frequencies.sql"),
        ),
        (
            "lr_y1_carriers",
            include_str!("../../sql/y1/lr_y1_carriers.sql"),
        ),
    ];

    for (name, ddl) in schemas {
        target
            .execute(ddl)
            .with_context(|| format!("failed to initialize Y1 table {name}"))?;
    }
    Ok(())
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
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
    source_info_json: String,
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
                    source_info_json: source_info_json.clone(),
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

pub fn stage_attempt(
    target: &ClickHouseTarget,
    context: &AttemptContext,
    batch: &TransformationBatch,
) -> anyhow::Result<StagedCounts> {
    let rows = StageRows::from_batch(context, batch)?;
    let counts = rows.counts(batch.report.source_records)?;

    target.insert_json_each_row("lr_y1_summaries_staging", &rows.summaries)?;
    target.insert_json_each_row("lr_y1_alleles_staging", &rows.alleles)?;
    target.insert_json_each_row("lr_y1_frequencies_staging", &rows.frequencies)?;
    target.insert_json_each_row("lr_y1_carriers_staging", &rows.carriers)?;
    target.insert_json_each_row("lr_y1_rejects_staging", &rows.rejects)?;
    Ok(counts)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptState {
    Failed,
    Accepted,
}

impl AttemptState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Accepted => "accepted",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TaskAttemptLedgerRow {
    pub run_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub revision: u64,
    pub state: String,
    pub chrom: String,
    pub interval_start: u32,
    pub interval_end: u32,
    pub source_records: u64,
    pub summary_rows: u64,
    pub allele_rows: u64,
    pub frequency_rows: u64,
    pub carrier_rows: u64,
    pub rejected_records: u64,
    pub report_json: String,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub error: String,
}

impl TaskAttemptLedgerRow {
    pub fn new(
        context: &AttemptContext,
        revision: u64,
        state: AttemptState,
        counts: StagedCounts,
        report: &TransformationReport,
        error: impl Into<String>,
    ) -> anyhow::Result<Self> {
        context.validate()?;
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
            started_at_ms: revision,
            updated_at_ms: revision,
            error: error.into(),
        })
    }
}

pub fn record_task_attempt(
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

#[derive(Debug, Clone)]
pub struct PublicationRequest {
    pub run_id: String,
    pub scope: LoadScope,
    pub release: Release,
    pub cohort: Cohort,
    pub reference_genome: ReferenceGenome,
    pub chrom: String,
    pub interval_start: u32,
    pub interval_end: u32,
    pub expected_tasks: u32,
    pub expected_counts: StagedCounts,
    pub source_uri: String,
    pub source_generation: String,
    pub source_checksum: String,
}

impl PublicationRequest {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.run_id.is_empty() || self.chrom.is_empty() {
            bail!("publication run_id and chromosome must not be empty");
        }
        if self.interval_start == 0 || self.interval_start > self.interval_end {
            bail!("publication interval must be one-based and non-empty");
        }
        if self.expected_tasks == 0 {
            bail!("publication must contain at least one accepted task");
        }
        if self.expected_counts.rejects != 0 {
            bail!("publication is blocked while transformed records are rejected");
        }
        if self.expected_counts.summaries != self.expected_counts.source_records {
            bail!("publication requires one canonical summary per source record");
        }
        Ok(())
    }
}

struct PublishedTable {
    published: &'static str,
    staging: &'static str,
    columns: &'static str,
    unique_key: &'static str,
    expected: u64,
}

pub fn publish_staged_run(
    target: &ClickHouseTarget,
    request: &PublicationRequest,
) -> anyhow::Result<()> {
    request.validate()?;
    if target.kind() == TargetKind::Serving && request.scope != LoadScope::FullChromosome {
        bail!("interval and synthetic runs cannot be materialized in a serving Y1 target");
    }

    let accepted = accepted_counts(target, request)?;
    if accepted != request.expected_counts {
        bail!(
            "accepted task ledger counts {accepted:?} do not match expected counts {:?}",
            request.expected_counts
        );
    }

    let tables = published_tables(request);
    for table in &tables {
        validate_staging_table(target, request, table)?;
    }
    validate_reject_staging(target, request)?;

    let active = active_run(target, request)?;
    if active.as_deref() == Some(request.run_id.as_str()) {
        bail!("the active run cannot be replaced in place; publish a new run_id");
    }

    let params = publication_parameters(request);
    for table in &tables {
        let query = format!(
            "ALTER TABLE {} DROP PARTITION tuple({{release:String}}, {{cohort:String}}, {{reference_genome:String}}, {{chrom:String}}, {{run_id:String}})",
            table.published
        );
        target.execute_with_params(&query, &params)?;
    }

    for table in &tables {
        let selected_columns = prefixed_columns(table.columns, "s");
        let query = format!(
            "INSERT INTO {published} ({columns})\nSELECT {selected_columns}\nFROM {staging} AS s\nINNER JOIN ({accepted}) AS a\n  ON s.task_id = a.task_id AND s.attempt_id = a.attempt_id\nWHERE s.run_id = {{run_id:String}}",
            published = table.published,
            columns = table.columns,
            staging = table.staging,
            accepted = ACCEPTED_ATTEMPTS,
        );
        target.execute_with_params(&query, &params)?;
    }

    for table in &tables {
        let actual = published_row_count(target, request, table.published)?;
        if actual != table.expected {
            bail!(
                "published table {} has {actual} rows for run {}; expected {}",
                table.published,
                request.run_id,
                table.expected
            );
        }
    }
    Ok(())
}

pub fn activate_published_run(
    target: &ClickHouseTarget,
    request: &PublicationRequest,
    independent_source_records: u64,
    activated_by: &str,
) -> anyhow::Result<()> {
    request.validate()?;
    if target.kind() != TargetKind::Serving {
        bail!("only an explicitly acknowledged serving target can be activated");
    }
    if request.scope != LoadScope::FullChromosome {
        bail!("only a full-chromosome run can be activated");
    }
    let expected_end = grch38_chromosome_length(&request.chrom)?;
    if request.interval_start != 1 || request.interval_end != expected_end {
        bail!(
            "full-chromosome activation for {} must cover 1-{expected_end}",
            request.chrom
        );
    }
    if independent_source_records == 0
        || independent_source_records != request.expected_counts.source_records
    {
        bail!("independent source count does not match the validated publication count");
    }
    validate_surveyed_source_identity(request)?;
    if activated_by.is_empty() {
        bail!("activation requires a non-empty operator identity");
    }

    for table in published_tables(request) {
        let actual = published_row_count(target, request, table.published)?;
        if actual != table.expected {
            bail!(
                "cannot activate: {} has {actual} rows, expected {}",
                table.published,
                table.expected
            );
        }
    }

    let previous_run_id = active_run(target, request)?.unwrap_or_default();
    if previous_run_id == request.run_id {
        return Ok(());
    }
    let revision = now_revision()?;
    let row = ActivePartitionRow {
        release: request.release.as_str().to_string(),
        cohort: request.cohort.as_str().to_string(),
        reference_genome: request.reference_genome.as_str().to_string(),
        chrom: request.chrom.clone(),
        revision,
        run_id: request.run_id.clone(),
        previous_run_id,
        activated_at_ms: revision / 1_000_000,
        activated_by: activated_by.to_string(),
    };
    target.insert_json_each_row("lr_y1_active_partitions", std::slice::from_ref(&row))?;
    if active_run(target, request)?.as_deref() != Some(request.run_id.as_str()) {
        bail!("active-partition pointer did not resolve to the requested run");
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct ActivePartitionRow {
    release: String,
    cohort: String,
    reference_genome: String,
    chrom: String,
    revision: u64,
    run_id: String,
    previous_run_id: String,
    activated_at_ms: u64,
    activated_by: String,
}

fn accepted_counts(
    target: &ClickHouseTarget,
    request: &PublicationRequest,
) -> anyhow::Result<StagedCounts> {
    let query = r#"
SELECT
    count(),
    uniqExact(task_id),
    coalesce(sum(source_records), 0),
    coalesce(sum(summary_rows), 0),
    coalesce(sum(allele_rows), 0),
    coalesce(sum(frequency_rows), 0),
    coalesce(sum(carrier_rows), 0),
    coalesce(sum(rejected_records), 0)
FROM (
    SELECT
        task_id,
        attempt_id,
        argMax(state, revision) AS state,
        argMax(source_records, revision) AS source_records,
        argMax(summary_rows, revision) AS summary_rows,
        argMax(allele_rows, revision) AS allele_rows,
        argMax(frequency_rows, revision) AS frequency_rows,
        argMax(carrier_rows, revision) AS carrier_rows,
        argMax(rejected_records, revision) AS rejected_records
    FROM lr_y1_task_attempts
    WHERE run_id = {run_id:String}
    GROUP BY task_id, attempt_id
)
WHERE state = 'accepted'
FORMAT TabSeparated
"#;
    let body = target.query_text(query, &[("run_id", &request.run_id)])?;
    let values = parse_u64_row(&body, 8, "accepted task counts")?;
    if values[0] != u64::from(request.expected_tasks) || values[1] != values[0] {
        bail!(
            "run {} has {} accepted attempts across {} tasks; expected {} distinct tasks",
            request.run_id,
            values[0],
            values[1],
            request.expected_tasks
        );
    }
    Ok(StagedCounts {
        source_records: values[2],
        summaries: values[3],
        alleles: values[4],
        frequencies: values[5],
        carriers: values[6],
        rejects: values[7],
    })
}

fn validate_staging_table(
    target: &ClickHouseTarget,
    request: &PublicationRequest,
    table: &PublishedTable,
) -> anyhow::Result<()> {
    let query = format!(
        "SELECT count(), uniqExact({key}), countIf(s.release != {{release:String}} OR s.cohort != {{cohort:String}} OR s.reference_genome != {{reference_genome:String}} OR s.chrom != {{chrom:String}})\nFROM {staging} AS s\nINNER JOIN ({accepted}) AS a\n  ON s.task_id = a.task_id AND s.attempt_id = a.attempt_id\nWHERE s.run_id = {{run_id:String}}\nFORMAT TabSeparated",
        key = table.unique_key,
        staging = table.staging,
        accepted = ACCEPTED_ATTEMPTS,
    );
    let params = publication_parameters(request);
    let body = target.query_text(&query, &params)?;
    let values = parse_u64_row(&body, 3, table.staging)?;
    if values[0] != table.expected {
        bail!(
            "{} has {} accepted staging rows; expected {}",
            table.staging,
            values[0],
            table.expected
        );
    }
    if values[1] != values[0] {
        bail!(
            "{} contains duplicate accepted keys ({} rows, {} unique)",
            table.staging,
            values[0],
            values[1]
        );
    }
    if values[2] != 0 {
        bail!(
            "{} contains {} rows outside the requested cohort partition",
            table.staging,
            values[2]
        );
    }
    Ok(())
}

fn validate_reject_staging(
    target: &ClickHouseTarget,
    request: &PublicationRequest,
) -> anyhow::Result<()> {
    let query = format!(
        "SELECT count()\nFROM lr_y1_rejects_staging AS s\nINNER JOIN ({ACCEPTED_ATTEMPTS}) AS a\n  ON s.task_id = a.task_id AND s.attempt_id = a.attempt_id\nWHERE s.run_id = {{run_id:String}}\nFORMAT TabSeparated"
    );
    let body = target.query_text(&query, &[("run_id", &request.run_id)])?;
    let values = parse_u64_row(&body, 1, "reject staging")?;
    if values[0] != request.expected_counts.rejects {
        bail!(
            "accepted reject staging has {} rows; expected {}",
            values[0],
            request.expected_counts.rejects
        );
    }
    Ok(())
}

fn published_row_count(
    target: &ClickHouseTarget,
    request: &PublicationRequest,
    table: &str,
) -> anyhow::Result<u64> {
    let query = format!(
        "SELECT count() FROM {table} WHERE run_id = {{run_id:String}} AND release = {{release:String}} AND cohort = {{cohort:String}} AND reference_genome = {{reference_genome:String}} AND chrom = {{chrom:String}} FORMAT TabSeparated"
    );
    let body = target.query_text(&query, &publication_parameters(request))?;
    Ok(parse_u64_row(&body, 1, table)?[0])
}

fn active_run(
    target: &ClickHouseTarget,
    request: &PublicationRequest,
) -> anyhow::Result<Option<String>> {
    let query = "SELECT argMax(run_id, revision) FROM lr_y1_active_partitions WHERE release = {release:String} AND cohort = {cohort:String} AND reference_genome = {reference_genome:String} AND chrom = {chrom:String} FORMAT TabSeparated";
    let body = target.query_text(query, &publication_parameters(request))?;
    let value = body.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn published_tables(request: &PublicationRequest) -> [PublishedTable; 4] {
    [
        PublishedTable {
            published: "lr_y1_summaries",
            staging: "lr_y1_summaries_staging",
            columns: SUMMARY_COLUMNS,
            unique_key: "tuple(s.release, s.cohort, s.source_variant_id)",
            expected: request.expected_counts.summaries,
        },
        PublishedTable {
            published: "lr_y1_alleles",
            staging: "lr_y1_alleles_staging",
            columns: ALLELE_COLUMNS,
            unique_key: "tuple(s.release, s.cohort, s.source_variant_id, s.alt_index)",
            expected: request.expected_counts.alleles,
        },
        PublishedTable {
            published: "lr_y1_frequencies",
            staging: "lr_y1_frequencies_staging",
            columns: FREQUENCY_COLUMNS,
            unique_key:
                "tuple(s.release, s.cohort, s.source_variant_id, s.alt_index, s.division)",
            expected: request.expected_counts.frequencies,
        },
        PublishedTable {
            published: "lr_y1_carriers",
            staging: "lr_y1_carriers_staging",
            columns: CARRIER_COLUMNS,
            unique_key: "tuple(s.release, s.cohort, s.source_variant_id, s.alt_index, s.sample_id, s.genotype_position)",
            expected: request.expected_counts.carriers,
        },
    ]
}

fn publication_parameters(request: &PublicationRequest) -> [(&'static str, &str); 5] {
    [
        ("run_id", request.run_id.as_str()),
        ("release", request.release.as_str()),
        ("cohort", request.cohort.as_str()),
        ("reference_genome", request.reference_genome.as_str()),
        ("chrom", request.chrom.as_str()),
    ]
}

fn prefixed_columns(columns: &str, prefix: &str) -> String {
    columns
        .split(',')
        .map(|column| format!("{prefix}.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_u64_row(body: &str, expected: usize, label: &str) -> anyhow::Result<Vec<u64>> {
    let fields: Vec<&str> = body.trim().split('\t').collect();
    if fields.len() != expected {
        bail!(
            "{label} returned {} fields; expected {expected}",
            fields.len()
        );
    }
    fields
        .into_iter()
        .map(|field| {
            field
                .parse::<u64>()
                .with_context(|| format!("invalid UInt64 {field:?} returned by {label}"))
        })
        .collect()
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

fn validate_surveyed_source_identity(request: &PublicationRequest) -> anyhow::Result<()> {
    let expected_name = format!(
        "gnomAD_LR_Y1.{}.{}.vcf.gz",
        request.cohort.as_str(),
        request.chrom
    );
    if !request.source_uri.ends_with(&expected_name) {
        bail!("serving activation source URI must end with {expected_name}");
    }
    if request.source_generation.is_empty() || request.source_checksum.is_empty() {
        bail!("serving activation requires immutable source generation and checksum values");
    }
    Ok(())
}

fn grch38_chromosome_length(chrom: &str) -> anyhow::Result<u32> {
    let raw = chrom.strip_prefix("chr").unwrap_or(chrom);
    let length = match raw {
        "1" => 248_956_422,
        "2" => 242_193_529,
        "3" => 198_295_559,
        "4" => 190_214_555,
        "5" => 181_538_259,
        "6" => 170_805_979,
        "7" => 159_345_973,
        "8" => 145_138_636,
        "9" => 138_394_717,
        "10" => 133_797_422,
        "11" => 135_086_622,
        "12" => 133_275_309,
        "13" => 114_364_328,
        "14" => 107_043_718,
        "15" => 101_991_189,
        "16" => 90_338_345,
        "17" => 83_257_441,
        "18" => 80_373_285,
        "19" => 58_617_616,
        "20" => 64_444_167,
        "21" => 46_709_983,
        "22" => 50_818_468,
        "X" => 156_040_895,
        "Y" => 57_227_415,
        "M" | "MT" => 16_569,
        _ => bail!("unsupported GRCh38 chromosome {chrom:?}"),
    };
    Ok(length)
}

fn now_revision() -> anyhow::Result<u64> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    u64::try_from(nanos).context("timestamp exceeds UInt64")
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn prefixing_preserves_column_order() {
        assert_eq!(
            prefixed_columns("run_id, release, source_variant_id", "s"),
            "s.run_id, s.release, s.source_variant_id"
        );
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
    fn activation_requires_a_serving_full_chromosome_target() {
        let target = ClickHouseTarget::new(
            "http://127.0.0.1:8123",
            "gnomad_lr_y1_scratch_unit",
            TargetKind::Scratch,
            super::super::target::AuthSource::None,
            false,
            false,
        )
        .unwrap();
        let request = PublicationRequest {
            run_id: "unit".to_string(),
            scope: LoadScope::Interval,
            release: Release::Y1,
            cohort: Cohort::Aou,
            reference_genome: ReferenceGenome::Grch38,
            chrom: "chr22".to_string(),
            interval_start: 20_000_000,
            interval_end: 20_010_000,
            expected_tasks: 1,
            expected_counts: StagedCounts {
                source_records: 1,
                summaries: 1,
                alleles: 1,
                frequencies: 1,
                carriers: 0,
                rejects: 0,
            },
            source_uri: "fixture.vcf".to_string(),
            source_generation: "fixture".to_string(),
            source_checksum: "fixture".to_string(),
        };
        assert!(activate_published_run(&target, &request, 1, "unit-test").is_err());
    }

    #[test]
    fn grch38_chr22_length_is_fixed() {
        assert_eq!(grch38_chromosome_length("chr22").unwrap(), 50_818_468);
    }

    #[test]
    fn local_clickhouse_retry_publication_is_count_stable() {
        let Ok(endpoint) = std::env::var("GNOMAD_LR_Y1_TEST_ENDPOINT") else {
            return;
        };
        let database = std::env::var("GNOMAD_LR_Y1_TEST_DATABASE")
            .unwrap_or_else(|_| "gnomad_lr_y1_scratch_ci".to_string());
        let target = ClickHouseTarget::new(
            &endpoint,
            &database,
            TargetKind::Scratch,
            super::super::target::AuthSource::None,
            false,
            false,
        )
        .unwrap();
        init_schema(&target).unwrap();

        exercise_fixture_publication(
            &target,
            HGSVC_FIXTURE,
            Cohort::HgsvcHprc,
            StagedCounts {
                source_records: 1,
                summaries: 1,
                alleles: 13,
                frequencies: 273,
                carriers: 214,
                rejects: 0,
            },
        );
        exercise_fixture_publication(
            &target,
            AOU_FIXTURE,
            Cohort::Aou,
            StagedCounts {
                source_records: 1,
                summaries: 1,
                alleles: 1,
                frequencies: 6,
                carriers: 0,
                rejects: 0,
            },
        );
    }

    fn exercise_fixture_publication(
        target: &ClickHouseTarget,
        fixture: &str,
        cohort: Cohort,
        expected: StagedCounts,
    ) {
        let batch = fixture_batch(fixture, cohort);
        let run_id = format!(
            "fixture-{}-{}-{}",
            cohort.as_str(),
            std::process::id(),
            now_revision().unwrap()
        );
        let revision = now_revision().unwrap();
        let run = LoadRunLedgerRow {
            run_id: run_id.clone(),
            revision,
            state: "validated".to_string(),
            load_scope: LoadScope::Synthetic.as_str().to_string(),
            release: Release::Y1.as_str().to_string(),
            cohort: cohort.as_str().to_string(),
            reference_genome: ReferenceGenome::Grch38.as_str().to_string(),
            chrom: "chr22".to_string(),
            interval_start: 20_000_000,
            interval_end: 20_010_000,
            source_uri: "checked-in-fixture.vcf".to_string(),
            source_generation: "git".to_string(),
            source_checksum_algorithm: "git_blob".to_string(),
            source_checksum: "fixture".to_string(),
            source_index_uri: "checked-in-fixture.vcf".to_string(),
            source_index_generation: "git".to_string(),
            source_index_checksum: "fixture".to_string(),
            schema_version: Y1_SCHEMA_VERSION,
            loader_version: env!("CARGO_PKG_VERSION").to_string(),
            expected_tasks: 1,
            expected_source_records: expected.source_records,
            summary_rows: expected.summaries,
            allele_rows: expected.alleles,
            frequency_rows: expected.frequencies,
            carrier_rows: expected.carriers,
            rejected_records: expected.rejects,
            created_at_ms: revision / 1_000_000,
            updated_at_ms: revision / 1_000_000,
            message: "local synthetic integration".to_string(),
        };
        record_load_run(target, &run).unwrap();

        let base_context = AttemptContext {
            run_id: run_id.clone(),
            task_id: "chr22-20000000-20010000".to_string(),
            attempt_id: "failed-attempt".to_string(),
            cohort,
            chrom: "chr22".to_string(),
            interval_start: 20_000_000,
            interval_end: 20_010_000,
        };

        let failed_counts = stage_attempt(target, &base_context, &batch).unwrap();
        assert_eq!(failed_counts, expected);
        let failed = TaskAttemptLedgerRow::new(
            &base_context,
            now_revision().unwrap(),
            AttemptState::Failed,
            failed_counts,
            &batch.report,
            "synthetic retry injection",
        )
        .unwrap();
        record_task_attempt(target, &failed).unwrap();

        let accepted_context = AttemptContext {
            attempt_id: "accepted-attempt".to_string(),
            ..base_context
        };
        let accepted_counts = stage_attempt(target, &accepted_context, &batch).unwrap();
        assert_eq!(accepted_counts, expected);
        let accepted = TaskAttemptLedgerRow::new(
            &accepted_context,
            now_revision().unwrap(),
            AttemptState::Accepted,
            accepted_counts,
            &batch.report,
            "",
        )
        .unwrap();
        record_task_attempt(target, &accepted).unwrap();

        let request = PublicationRequest {
            run_id,
            scope: LoadScope::Synthetic,
            release: Release::Y1,
            cohort,
            reference_genome: ReferenceGenome::Grch38,
            chrom: "chr22".to_string(),
            interval_start: 20_000_000,
            interval_end: 20_010_000,
            expected_tasks: 1,
            expected_counts: expected,
            source_uri: "checked-in-fixture.vcf".to_string(),
            source_generation: "git".to_string(),
            source_checksum: "fixture".to_string(),
        };

        publish_staged_run(target, &request).unwrap();
        let first_counts: Vec<u64> = published_tables(&request)
            .iter()
            .map(|table| published_row_count(target, &request, table.published).unwrap())
            .collect();
        publish_staged_run(target, &request).unwrap();
        let second_counts: Vec<u64> = published_tables(&request)
            .iter()
            .map(|table| published_row_count(target, &request, table.published).unwrap())
            .collect();
        assert_eq!(first_counts, second_counts);
        assert_eq!(
            second_counts,
            vec![
                expected.summaries,
                expected.alleles,
                expected.frequencies,
                expected.carriers
            ]
        );
        assert_eq!(active_run(target, &request).unwrap(), None);
    }
}
