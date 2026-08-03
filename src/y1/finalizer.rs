use super::{
    contig::grch38_contig_length, record_load_run, storage::delete_attempt_rows, ClickHouseTarget,
    LoadRunLedgerRow, LoadScope, PoolY1TaskSpec, ReferenceGenome, Release, StagedCounts,
    TargetKind, WorkerWriteFence, Y1_SCHEMA_VERSION,
};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentExpectedCounts {
    pub contract_version: u8,
    pub run_id: String,
    pub cohort: String,
    pub chrom: String,
    pub evidence_uri: String,
    pub producer: String,
    pub source_generation: String,
    pub source_checksum: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_load_mode: Option<super::PrimaryLoadMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carrier_loading_status: Option<super::CarrierLoadingStatus>,
    pub counts: StagedCounts,
    pub facts: IndependentReconciliationFacts,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndependentReconciliationFacts {
    pub source_records: u64,
    pub alt_alleles: u64,
    pub frequency_rows: u64,
    pub genotype_calls: u64,
    pub called_alleles: u64,
    pub carrier_alt_copies: u64,
    pub fully_missing_genotypes: u64,
    pub partially_called_genotypes: u64,
    pub annotated_alt_alleles: u64,
    pub source_content_sha256: String,
    pub genotype_content_sha256: String,
    pub annotation_content_sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
struct AttemptView {
    task_id: String,
    attempt_id: String,
    state: String,
    chrom: String,
    interval_start: u32,
    interval_end: u32,
    source_records: u64,
    summary_rows: u64,
    allele_rows: u64,
    frequency_rows: u64,
    carrier_rows: u64,
    rejected_records: u64,
    report_json: String,
}

#[derive(Debug, Deserialize)]
struct TerminalAttemptPrincipalView {
    attempt_id: String,
    report_json: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptTransformationReport {
    source_records: u64,
    summary_rows: u64,
    carrier_rows: u64,
    genotype_calls: u64,
    missing_genotypes: u64,
    partially_called_genotypes: u64,
    reference_genotypes: u64,
    rejected_records: u64,
    rejects: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct PhysicalAttemptView {
    table: String,
    task_id: String,
    attempt_id: String,
    rows: u64,
    unique_keys: u64,
    identity_violations: u64,
    min_position: u32,
    max_position: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalContentDigest {
    pub table: String,
    pub task_id: String,
    pub attempt_id: String,
    pub rows: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalTableCount {
    pub table: String,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadAcceptanceReceipt {
    pub contract_version: u8,
    pub schema_version: u16,
    pub worker_principal: String,
    pub run_id: String,
    pub cohort: String,
    pub chrom: String,
    pub manifest_sha256: String,
    pub independent_counts_sha256: String,
    pub source_uri: String,
    pub source_generation: String,
    pub source_checksum: String,
    pub source_index_uri: String,
    pub source_index_generation: String,
    pub source_index_checksum: String,
    pub primary_load_mode: Option<super::PrimaryLoadMode>,
    pub carrier_loading_status: super::CarrierLoadingStatus,
    pub accepted_attempts: BTreeMap<String, String>,
    pub failed_attempts: BTreeMap<String, Vec<String>>,
    pub expected_counts: StagedCounts,
    pub source_content_sha256: String,
    pub genotype_content_sha256: String,
    pub annotation_content_sha256: String,
    pub canonical_counts: Vec<CanonicalTableCount>,
    pub canonical_digests: Vec<CanonicalContentDigest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizationReport {
    pub run_id: String,
    pub cohort: String,
    pub chrom: String,
    pub operator_identity: String,
    pub manifest_tasks: usize,
    pub manifest_sha256: String,
    pub accepted_attempts: BTreeMap<String, String>,
    pub failed_attempts: BTreeMap<String, Vec<String>>,
    pub expected_counts: StagedCounts,
    pub independent_evidence_uri: String,
    pub independent_counts_sha256: String,
    pub acceptance_receipt_sha256: String,
    pub acceptance: LoadAcceptanceReceipt,
    pub accepted: bool,
    pub frozen: bool,
    pub published: bool,
}

struct LedgerCoverage {
    accepted: BTreeMap<String, String>,
    failed: BTreeMap<String, Vec<String>>,
    nonaccepted: Vec<(String, String)>,
    counts: Vec<CanonicalTableCount>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PhysicalPhase {
    BeforeCleanup,
    Frozen,
}

#[derive(Clone, Copy)]
struct TableSpec {
    label: &'static str,
    table: &'static str,
    unique_key: &'static str,
    columns: &'static str,
    order_by: &'static str,
    has_primary_identity: bool,
}

const TABLES: [TableSpec; 5] = [
    TableSpec {
        label: "summaries",
        table: "lr_y1_summaries",
        unique_key: "tuple(position, source_variant_id)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,source_variant_id,ref_allele,alts,allele_type,qual,filters,ac,an,af,allele_lengths,length_provenance,source_allele_length,source_svlen,source_svlen_present,frequencies_json,source_info_json",
        order_by: "chrom,position,source_variant_id",
        has_primary_identity: true,
    },
    TableSpec {
        label: "alleles",
        table: "lr_y1_alleles",
        unique_key: "tuple(position, source_variant_id, alt_index)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,reference_end,xpos,source_variant_id,alt_index,ref_allele,alt,allele_type,qual,filters,ac,an,af,allele_length,length_provenance,rsids,cadd_phred,phylop,major_consequence,short_read_match_id,short_read_match_type,short_read_match_source",
        order_by: "chrom,position,source_variant_id,alt_index",
        has_primary_identity: true,
    },
    TableSpec {
        label: "frequencies",
        table: "lr_y1_frequencies",
        unique_key: "tuple(position, source_variant_id, alt_index, division)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,source_variant_id,alt_index,division,ac,an,af,values_available",
        order_by: "chrom,position,source_variant_id,alt_index,division",
        has_primary_identity: true,
    },
    TableSpec {
        label: "carriers",
        table: "lr_y1_carriers",
        unique_key: "tuple(position, source_variant_id, alt_index, sample_id, genotype_position)",
        columns: "run_id,task_id,attempt_id,release,cohort,reference_genome,chrom,position,source_variant_id,alt_index,alt,sample_id,genotype_position,gt_alleles,gt_phased,genotype_fields_json,position_fields_json",
        order_by: "chrom,position,source_variant_id,alt_index,sample_id,genotype_position",
        has_primary_identity: true,
    },
    TableSpec {
        label: "rejects",
        table: "lr_y1_rejects_staging",
        unique_key: "tuple(record_number, source_variant_id, reject_code, message)",
        columns: "run_id,task_id,attempt_id,record_number,source_variant_id,reject_code,message",
        order_by: "reject_code,record_number,source_variant_id,message",
        has_primary_identity: false,
    },
];

/// Backward-compatible chr22-only entry point with its original command name.
pub fn finalize_chr22_run(
    target: &ClickHouseTarget,
    fence: &WorkerWriteFence,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<FinalizationReport> {
    finalize_contig_run_inner(
        target,
        fence,
        manifest_path,
        expected_path,
        operator_identity,
        Some("chr22"),
        false,
    )
}

/// Freeze and accept exactly one complete canonical GRCh38 contig in place.
pub fn finalize_contig_run(
    target: &ClickHouseTarget,
    fence: &WorkerWriteFence,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<FinalizationReport> {
    finalize_contig_run_inner(
        target,
        fence,
        manifest_path,
        expected_path,
        operator_identity,
        None,
        false,
    )
}

fn finalize_contig_run_inner(
    target: &ClickHouseTarget,
    fence: &WorkerWriteFence,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
    required_chrom: Option<&str>,
    stop_after_frozen: bool,
) -> anyhow::Result<FinalizationReport> {
    if target.kind() != TargetKind::Scratch {
        bail!("contig candidate finalization is restricted to an isolated scratch target");
    }
    if operator_identity.trim().is_empty() {
        bail!("finalization requires a non-empty operator identity");
    }

    let manifest_bytes = std::fs::read(manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let tasks: Vec<PoolY1TaskSpec> = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("invalid task manifest {}", manifest_path.display()))?;
    let run = validate_manifest(&tasks)?;
    if required_chrom.is_some_and(|chrom| run.chrom != chrom) {
        bail!("the legacy chr22 finalizer accepts only chr22 manifests");
    }
    let manifest_sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));

    let expected_bytes = std::fs::read(expected_path)
        .with_context(|| format!("failed to read {}", expected_path.display()))?;
    let expected: IndependentExpectedCounts = serde_json::from_slice(&expected_bytes)
        .with_context(|| format!("invalid independent counts {}", expected_path.display()))?;
    validate_independent_reconciliation(&expected, &run)?;
    let independent_counts_sha256 = format!("{:x}", Sha256::digest(&expected_bytes));

    let durable = read_durable_report(target, &run.run_id)?;
    if let Some((state, persisted)) = durable {
        validate_terminal_worker_principals(target, &run.run_id, fence.principal())?;
        fence.attest_fenced_and_drained(target)?;
        attest_no_active_task_leases(target, &run.run_id)?;
        let verified = rebuild_report(
            target,
            fence,
            &run,
            &tasks,
            &expected,
            operator_identity,
            manifest_sha256,
            independent_counts_sha256,
        )?;
        if persisted != verified {
            bail!("durable frozen machine report differs from exact manifest/count/digest revalidation");
        }
        if state == "accepted_frozen" {
            return Ok(persisted);
        }
        record_report_state(
            target,
            &run,
            &expected,
            tasks.len(),
            "accepted_frozen",
            &persisted,
        )?;
        validate_persisted_report(target, &run.run_id, "accepted_frozen", &persisted)?;
        return Ok(persisted);
    }

    record_state(
        target,
        &run,
        &expected,
        tasks.len(),
        revision_now()?,
        "freezing",
        operator_identity,
    )?;

    let result = (|| -> anyhow::Result<FinalizationReport> {
        // The ledger marker blocks cooperative writers. It is not the database
        // fence: terminal leases are attested before and after the dedicated
        // worker principal is made read-only, then already-running inserts drain.
        attest_no_active_task_leases(target, &run.run_id)?;
        // Reject missing, mixed, or A-load/B-finalize identity before changing
        // any ClickHouse principal. Coverage validates it again after the fence.
        validate_terminal_worker_principals(target, &run.run_id, fence.principal())?;
        fence.apply_and_drain(target)?;
        attest_no_active_task_leases(target, &run.run_id)?;

        let before = validate_ledger_coverage(
            target,
            &run,
            &tasks,
            fence.principal(),
            PhysicalPhase::BeforeCleanup,
        )?;
        validate_expected_counts(&before.counts, expected.counts)?;
        for (task_id, attempt_id) in &before.nonaccepted {
            delete_attempt_rows(target, &run.run_id, task_id, attempt_id)?;
        }

        let report = rebuild_report(
            target,
            fence,
            &run,
            &tasks,
            &expected,
            operator_identity,
            manifest_sha256,
            independent_counts_sha256,
        )?;
        record_report_state(target, &run, &expected, tasks.len(), "frozen", &report)?;
        validate_persisted_report(target, &run.run_id, "frozen", &report)?;
        if stop_after_frozen {
            bail!("test stop after durable frozen state");
        }

        let reread = rebuild_report(
            target,
            fence,
            &run,
            &tasks,
            &expected,
            operator_identity,
            report.manifest_sha256.clone(),
            report.independent_counts_sha256.clone(),
        )?;
        if reread != report {
            bail!("canonical rows, ledger, inputs, or digests changed after durable freeze");
        }
        record_report_state(
            target,
            &run,
            &expected,
            tasks.len(),
            "accepted_frozen",
            &report,
        )?;
        validate_persisted_report(target, &run.run_id, "accepted_frozen", &report)?;
        Ok(report)
    })();

    if let Err(error) = &result {
        let latest = latest_run_state(target, &run.run_id).unwrap_or_default();
        if !matches!(latest.as_str(), "frozen" | "accepted_frozen") {
            let _ = record_state(
                target,
                &run,
                &expected,
                tasks.len(),
                revision_now()?,
                "finalization_failed",
                &format!("{operator_identity}: {error:#}"),
            );
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn rebuild_report(
    target: &ClickHouseTarget,
    fence: &WorkerWriteFence,
    run: &PoolY1TaskSpec,
    tasks: &[PoolY1TaskSpec],
    expected: &IndependentExpectedCounts,
    operator_identity: &str,
    manifest_sha256: String,
    independent_counts_sha256: String,
) -> anyhow::Result<FinalizationReport> {
    fence.attest_fenced_and_drained(target)?;
    attest_no_active_task_leases(target, &run.run_id)?;
    let frozen =
        validate_ledger_coverage(target, run, tasks, fence.principal(), PhysicalPhase::Frozen)?;
    validate_expected_counts(&frozen.counts, expected.counts)?;
    let digests = read_canonical_digests(target, run, &frozen.accepted, &frozen.counts)?;
    let acceptance = build_acceptance_receipt(
        run,
        expected,
        &frozen,
        fence.principal(),
        manifest_sha256.clone(),
        independent_counts_sha256.clone(),
        digests,
    );
    let acceptance_json = serde_json::to_string(&acceptance)?;
    let acceptance_receipt_sha256 = format!("{:x}", Sha256::digest(acceptance_json.as_bytes()));
    Ok(FinalizationReport {
        run_id: run.run_id.clone(),
        cohort: run.cohort.clone(),
        chrom: run.chrom.clone(),
        operator_identity: operator_identity.to_string(),
        manifest_tasks: tasks.len(),
        manifest_sha256,
        accepted_attempts: frozen.accepted,
        failed_attempts: frozen.failed,
        expected_counts: expected.counts,
        independent_evidence_uri: expected.evidence_uri.clone(),
        independent_counts_sha256,
        acceptance_receipt_sha256,
        acceptance,
        accepted: true,
        frozen: true,
        published: false,
    })
}

fn latest_run_state(target: &ClickHouseTarget, run_id: &str) -> anyhow::Result<String> {
    Ok(target
        .query_text(
            "SELECT state FROM lr_y1_load_runs WHERE run_id = {run_id:String} ORDER BY revision DESC LIMIT 1 FORMAT TabSeparated",
            &[("run_id", run_id)],
        )?
        .trim()
        .to_string())
}

fn read_durable_report(
    target: &ClickHouseTarget,
    run_id: &str,
) -> anyhow::Result<Option<(String, FinalizationReport)>> {
    let state = latest_run_state(target, run_id)?;
    if !matches!(state.as_str(), "frozen" | "accepted_frozen") {
        return Ok(None);
    }
    let body = target.query_text(
        "SELECT message FROM lr_y1_load_runs WHERE run_id = {run_id:String} AND state = {state:String} ORDER BY revision DESC LIMIT 1 FORMAT JSONEachRow",
        &[("run_id", run_id), ("state", &state)],
    )?;
    #[derive(Deserialize)]
    struct Row {
        message: String,
    }
    let row: Row = serde_json::from_str(body.trim())
        .context("durable frozen machine report row is missing or malformed")?;
    let report = serde_json::from_str(&row.message)
        .context("durable frozen machine report JSON is malformed")?;
    Ok(Some((state, report)))
}

fn validate_persisted_report(
    target: &ClickHouseTarget,
    run_id: &str,
    state: &str,
    report: &FinalizationReport,
) -> anyhow::Result<()> {
    let persisted = read_durable_report(target, run_id)?
        .filter(|(persisted_state, _)| persisted_state == state)
        .map(|(_, report)| report)
        .context("durable frozen machine report is missing")?;
    if persisted != *report {
        bail!("durable frozen machine report differs from the verified report");
    }
    Ok(())
}

fn record_report_state(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
    expected: &IndependentExpectedCounts,
    task_count: usize,
    state: &str,
    report: &FinalizationReport,
) -> anyhow::Result<()> {
    record_state(
        target,
        run,
        expected,
        task_count,
        revision_now()?,
        state,
        &serde_json::to_string(report)?,
    )
}

fn attest_no_active_task_leases(target: &ClickHouseTarget, run_id: &str) -> anyhow::Result<()> {
    let active = target.query_text(
        r#"SELECT count() FROM (
SELECT task_id, attempt_id, argMax(state, revision) AS state
FROM lr_y1_task_attempts
WHERE run_id = {run_id:String}
GROUP BY task_id, attempt_id
HAVING state = 'running'
) FORMAT TabSeparated"#,
        &[("run_id", run_id)],
    )?;
    if active.trim() != "0" {
        bail!("active Y1 task leases remain; refusing canonical snapshot");
    }
    Ok(())
}

fn validate_terminal_worker_principals(
    target: &ClickHouseTarget,
    run_id: &str,
    expected_worker_principal: &str,
) -> anyhow::Result<()> {
    let body = target.query_text(
        r#"SELECT attempt_id, argMax(report_json, revision) AS report_json
FROM lr_y1_task_attempts
WHERE run_id = {run_id:String}
GROUP BY task_id, attempt_id
HAVING argMax(state, revision) IN ('accepted', 'failed')
FORMAT JSONEachRow"#,
        &[("run_id", run_id)],
    )?;
    validate_terminal_worker_principal_rows(&body, expected_worker_principal)
}

fn validate_terminal_worker_principal_rows(
    body: &str,
    expected_worker_principal: &str,
) -> anyhow::Result<()> {
    let mut terminal_attempts = 0usize;
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let row: TerminalAttemptPrincipalView =
            serde_json::from_str(line).context("invalid terminal attempt principal JSON")?;
        let report: serde_json::Value = serde_json::from_str(&row.report_json)
            .context("invalid durable terminal attempt report JSON")?;
        validate_worker_provenance(&report, &row.attempt_id, expected_worker_principal)?;
        terminal_attempts += 1;
    }
    if terminal_attempts == 0 {
        bail!("run has no terminal attempt principal provenance");
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_independent_reconciliation(
    expected: &IndependentExpectedCounts,
    run: &PoolY1TaskSpec,
) -> anyhow::Result<()> {
    if expected.contract_version != 1
        || expected.run_id != run.run_id
        || expected.cohort != run.cohort
        || expected.chrom != run.chrom
        || expected.source_generation != run.source_generation
        || expected.source_checksum != run.source_checksum
        || expected.primary_load_mode != run.primary_load_mode
        || expected.carrier_loading_status
            != run.primary_load_mode.map(|_| run.carrier_loading_status())
    {
        bail!("independent reconciliation identity does not match the manifest run/source/mode");
    }
    let aggregate_only =
        run.primary_load_mode == Some(super::PrimaryLoadMode::AggregateOnlyNoCarriers);
    let facts = &expected.facts;
    if expected.evidence_uri.trim().is_empty()
        || expected.producer.trim().is_empty()
        || expected.counts.source_records == 0
        || facts.source_records != expected.counts.source_records
        || facts.alt_alleles != expected.counts.alleles
        || facts.frequency_rows != expected.counts.frequencies
        || facts.carrier_alt_copies != expected.counts.carriers
        || facts.fully_missing_genotypes > facts.genotype_calls
        || facts.partially_called_genotypes > facts.genotype_calls
        || facts
            .fully_missing_genotypes
            .checked_add(facts.partially_called_genotypes)
            .is_none_or(|value| value > facts.genotype_calls)
        || facts.annotated_alt_alleles > facts.alt_alleles
        || facts.carrier_alt_copies > facts.called_alleles
        || (run.cohort == "hgsvc_hprc"
            && !aggregate_only
            && (facts.genotype_calls == 0 || facts.called_alleles == 0))
        || (aggregate_only
            && (expected.counts.carriers != 0
                || facts.genotype_calls != 0
                || facts.called_alleles != 0
                || facts.carrier_alt_copies != 0
                || facts.fully_missing_genotypes != 0
                || facts.partially_called_genotypes != 0))
        || !valid_sha256(&facts.source_content_sha256)
        || !valid_sha256(&facts.genotype_content_sha256)
        || !valid_sha256(&facts.annotation_content_sha256)
    {
        bail!(
            "independent reconciliation facts are incomplete or inconsistent with expected counts"
        );
    }
    if expected.counts.rejects != 0 || expected.counts.summaries != expected.counts.source_records {
        bail!("independent counts require zero rejects and one summary per source record");
    }
    if run.cohort == "aou"
        && (expected.counts.carriers != 0
            || facts.genotype_calls != 0
            || facts.called_alleles != 0
            || facts.carrier_alt_copies != 0
            || facts.fully_missing_genotypes != 0
            || facts.partially_called_genotypes != 0)
    {
        bail!("AoU independent reconciliation must contain no genotype or carrier observations");
    }
    Ok(())
}

fn validate_expected_counts(
    counts: &[CanonicalTableCount],
    expected: StagedCounts,
) -> anyhow::Result<()> {
    let rows = |table: &str| {
        counts
            .iter()
            .find(|snapshot| snapshot.table == table)
            .map(|snapshot| snapshot.rows)
            .with_context(|| format!("canonical snapshot is missing {table}"))
    };
    let observed = StagedCounts {
        source_records: rows("summaries")?,
        summaries: rows("summaries")?,
        alleles: rows("alleles")?,
        frequencies: rows("frequencies")?,
        carriers: rows("carriers")?,
        rejects: rows("rejects")?,
    };
    if observed != expected {
        bail!("frozen canonical counts {observed:?} do not equal independent expected counts {expected:?}");
    }
    Ok(())
}

fn build_acceptance_receipt(
    run: &PoolY1TaskSpec,
    expected: &IndependentExpectedCounts,
    ledger: &LedgerCoverage,
    worker_principal: &str,
    manifest_sha256: String,
    independent_counts_sha256: String,
    canonical_digests: Vec<CanonicalContentDigest>,
) -> LoadAcceptanceReceipt {
    LoadAcceptanceReceipt {
        contract_version: 4,
        schema_version: Y1_SCHEMA_VERSION,
        worker_principal: worker_principal.to_string(),
        run_id: run.run_id.clone(),
        cohort: run.cohort.clone(),
        chrom: run.chrom.clone(),
        manifest_sha256,
        independent_counts_sha256,
        source_uri: run.source_uri.clone(),
        source_generation: run.source_generation.clone(),
        source_checksum: run.source_checksum.clone(),
        source_index_uri: run.source_index_uri.clone(),
        source_index_generation: run.source_index_generation.clone(),
        source_index_checksum: run.source_index_checksum.clone(),
        primary_load_mode: run.primary_load_mode,
        carrier_loading_status: run.carrier_loading_status(),
        accepted_attempts: ledger.accepted.clone(),
        failed_attempts: ledger.failed.clone(),
        expected_counts: expected.counts,
        source_content_sha256: expected.facts.source_content_sha256.clone(),
        genotype_content_sha256: expected.facts.genotype_content_sha256.clone(),
        annotation_content_sha256: expected.facts.annotation_content_sha256.clone(),
        canonical_counts: ledger.counts.clone(),
        canonical_digests,
    }
}

fn validate_manifest(tasks: &[PoolY1TaskSpec]) -> anyhow::Result<PoolY1TaskSpec> {
    let Some(first) = tasks.first() else {
        bail!("task manifest must not be empty")
    };
    let contig_length = grch38_contig_length(&first.chrom)?;
    let mut previous_stop = 0;
    let mut task_ids = BTreeSet::new();
    for (index, task) in tasks.iter().enumerate() {
        task.validate(&format!("custom_{index}"))?;
        if task.run_id != first.run_id
            || task.cohort != first.cohort
            || task.chrom != first.chrom
            || task.source_uri != first.source_uri
            || task.source_generation != first.source_generation
            || task.source_checksum != first.source_checksum
            || task.source_size_bytes != first.source_size_bytes
            || task.source_index_uri != first.source_index_uri
            || task.source_index_generation != first.source_index_generation
            || task.source_index_checksum != first.source_index_checksum
            || task.source_index_size_bytes != first.source_index_size_bytes
            || task.primary_load_mode != first.primary_load_mode
        {
            bail!("task {index} changes a run or immutable source identity");
        }
        if task.start != previous_stop + 1 {
            bail!("task {index} does not begin immediately after the previous task");
        }
        if !task_ids.insert(task.task_id.clone()) {
            bail!("task manifest contains duplicate task_id {}", task.task_id);
        }
        previous_stop = task.stop;
    }
    if previous_stop != contig_length {
        bail!(
            "task manifest must cover {}:1-{contig_length} exactly",
            first.chrom
        );
    }
    Ok(first.clone())
}

fn validate_worker_provenance(
    report: &serde_json::Value,
    attempt_id: &str,
    expected_worker_principal: &str,
) -> anyhow::Result<()> {
    let field = |name: &str| {
        report
            .get(name)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("attempt {attempt_id} has no {name}"))
    };
    let worker_identity = field("worker_identity")?;
    let build_identity = field("worker_build_version")?;
    let backend_revision = field("backend_revision")?;
    let worker_principal = field("worker_principal")?;
    if worker_principal != expected_worker_principal {
        bail!(
            "attempt {attempt_id} ClickHouse principal {worker_principal:?} does not equal fenced principal {expected_worker_principal:?}"
        );
    }
    if matches!(worker_identity, "unknown" | "unknown-worker")
        || matches!(
            build_identity,
            "unknown" | "unknown-build" | "unversioned-development-build"
        )
    {
        bail!("attempt {attempt_id} has placeholder worker/build identity");
    }
    if !matches!(backend_revision.len(), 40 | 64)
        || !backend_revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || !build_identity.contains(backend_revision)
    {
        bail!("attempt {attempt_id} worker build is not bound to a full backend revision");
    }
    Ok(())
}

fn validate_attempt_carrier_loading(
    report: &serde_json::Value,
    manifest_task: &PoolY1TaskSpec,
    attempt_id: &str,
) -> anyhow::Result<()> {
    let expected_status = match manifest_task.carrier_loading_status() {
        super::CarrierLoadingStatus::Loaded => "loaded",
        super::CarrierLoadingStatus::UnavailableNotLoaded => "unavailable_not_loaded",
        super::CarrierLoadingStatus::NotApplicableAggregateSource => {
            "not_applicable_aggregate_source"
        }
    };
    let mode_matches = match manifest_task.primary_load_mode {
        Some(mode) => {
            report.get("primary_load_mode") == Some(&serde_json::to_value(Some(mode))?)
                && report
                    .get("carrier_loading_status")
                    .and_then(|value| value.as_str())
                    == Some(expected_status)
        }
        None => {
            // Reports committed before this field existed are accepted only for
            // ordinary manifests. The exceptional aggregate-only path always
            // requires explicit mode and unavailability provenance.
            report
                .get("primary_load_mode")
                .is_none_or(serde_json::Value::is_null)
                && report
                    .get("carrier_loading_status")
                    .is_none_or(|value| value.as_str() == Some(expected_status))
        }
    };
    if !mode_matches {
        bail!("attempt {attempt_id} report has inconsistent primary carrier-loading mode/status");
    }
    Ok(())
}

fn validate_aggregate_only_attempt_transformation(
    report: &serde_json::Value,
    report_counts: &StagedCounts,
    manifest_task: &PoolY1TaskSpec,
    attempt_id: &str,
) -> anyhow::Result<()> {
    if manifest_task.primary_load_mode != Some(super::PrimaryLoadMode::AggregateOnlyNoCarriers) {
        return Ok(());
    }
    if report_counts.carriers != 0 {
        bail!("attempt {attempt_id} aggregate-only report declares carrier rows");
    }
    let transformation: AttemptTransformationReport = serde_json::from_value(
        report
            .get("transformation")
            .cloned()
            .with_context(|| format!("attempt {attempt_id} has no transformation report"))?,
    )
    .with_context(|| format!("attempt {attempt_id} has an invalid transformation report"))?;
    for (field, value) in [
        ("carrier_rows", transformation.carrier_rows),
        ("genotype_calls", transformation.genotype_calls),
        ("missing_genotypes", transformation.missing_genotypes),
        (
            "partially_called_genotypes",
            transformation.partially_called_genotypes,
        ),
        ("reference_genotypes", transformation.reference_genotypes),
    ] {
        if value != 0 {
            bail!(
                "attempt {attempt_id} aggregate-only transformation field {field} must be zero, found {value}"
            );
        }
    }
    if transformation.source_records != report_counts.source_records
        || transformation.summary_rows != report_counts.summaries
        || transformation.carrier_rows != report_counts.carriers
        || transformation.rejected_records != report_counts.rejects
        || u64::try_from(transformation.rejects.len())? != transformation.rejected_records
    {
        bail!("attempt {attempt_id} transformation report disagrees with its staged counts");
    }
    Ok(())
}

fn validate_attempt_source_identity(
    report: &serde_json::Value,
    manifest_task: &PoolY1TaskSpec,
    attempt_id: &str,
) -> anyhow::Result<()> {
    for (field, expected) in [
        ("source_uri", manifest_task.source_uri.as_str()),
        (
            "source_generation",
            manifest_task.source_generation.as_str(),
        ),
        (
            "source_checksum_algorithm",
            manifest_task.source_checksum_algorithm.as_str(),
        ),
        ("source_checksum", manifest_task.source_checksum.as_str()),
        ("source_index_uri", manifest_task.source_index_uri.as_str()),
        (
            "source_index_generation",
            manifest_task.source_index_generation.as_str(),
        ),
        (
            "source_index_checksum_algorithm",
            manifest_task.source_index_checksum_algorithm.as_str(),
        ),
        (
            "source_index_checksum",
            manifest_task.source_index_checksum.as_str(),
        ),
    ] {
        if report.get(field).and_then(|value| value.as_str()) != Some(expected) {
            bail!(
                "attempt {attempt_id} report {field} does not match its manifest task source identity"
            );
        }
    }
    for (field, expected) in [
        ("source_size_bytes", manifest_task.source_size_bytes),
        (
            "source_index_size_bytes",
            manifest_task.source_index_size_bytes,
        ),
    ] {
        if report.get(field).and_then(|value| value.as_u64()) != Some(expected) {
            bail!(
                "attempt {attempt_id} report {field} does not match its manifest task source identity"
            );
        }
    }
    Ok(())
}

fn validate_ledger_coverage(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
    tasks: &[PoolY1TaskSpec],
    expected_worker_principal: &str,
    phase: PhysicalPhase,
) -> anyhow::Result<LedgerCoverage> {
    let body = target.query_text(
        r#"
SELECT task_id, attempt_id, argMax(state, revision) AS state,
       argMax(chrom, revision) AS chrom,
       argMax(interval_start, revision) AS interval_start,
       argMax(interval_end, revision) AS interval_end,
       argMax(source_records, revision) AS source_records,
       argMax(summary_rows, revision) AS summary_rows,
       argMax(allele_rows, revision) AS allele_rows,
       argMax(frequency_rows, revision) AS frequency_rows,
       argMax(carrier_rows, revision) AS carrier_rows,
       argMax(rejected_records, revision) AS rejected_records,
       argMax(report_json, revision) AS report_json
FROM lr_y1_task_attempts
WHERE run_id = {run_id:String}
GROUP BY task_id, attempt_id
FORMAT JSONEachRow
"#,
        &[("run_id", &run.run_id)],
    )?;
    let physical = read_physical_attempts(target, run)?;
    let expected: BTreeMap<&str, (u32, u32)> = tasks
        .iter()
        .map(|task| (task.task_id.as_str(), (task.start, task.stop)))
        .collect();
    let mut accepted = BTreeMap::new();
    let mut failed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut terminal_counts = BTreeMap::new();
    let mut terminal_states = BTreeMap::new();

    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let row: AttemptView = serde_json::from_str(line).context("invalid attempt ledger JSON")?;
        if row.attempt_id.trim().is_empty() {
            bail!("attempt ledger contains an empty attempt ID");
        }
        let bounds = expected.get(row.task_id.as_str()).with_context(|| {
            format!(
                "run ledger contains task {} absent from manifest",
                row.task_id
            )
        })?;
        if row.chrom != run.chrom || (row.interval_start, row.interval_end) != *bounds {
            bail!(
                "attempt {} has bounds inconsistent with its manifest task",
                row.attempt_id
            );
        }
        let report: serde_json::Value = serde_json::from_str(&row.report_json)
            .context("invalid durable attempt report JSON")?;
        validate_worker_provenance(&report, &row.attempt_id, expected_worker_principal)?;
        let manifest_task = tasks
            .iter()
            .find(|task| task.task_id == row.task_id)
            .expect("ledger task was checked above");
        for (field, expected_value) in [
            ("run_id", run.run_id.as_str()),
            ("task_id", row.task_id.as_str()),
            ("attempt_id", row.attempt_id.as_str()),
            ("cohort", manifest_task.cohort.as_str()),
            ("chrom", run.chrom.as_str()),
        ] {
            if report.get(field).and_then(|value| value.as_str()) != Some(expected_value) {
                bail!("attempt {} report has inconsistent {field}", row.attempt_id);
            }
        }
        validate_attempt_source_identity(&report, manifest_task, &row.attempt_id)?;
        validate_attempt_carrier_loading(&report, manifest_task, &row.attempt_id)?;
        let counts = StagedCounts {
            source_records: row.source_records,
            summaries: row.summary_rows,
            alleles: row.allele_rows,
            frequencies: row.frequency_rows,
            carriers: row.carrier_rows,
            rejects: row.rejected_records,
        };
        let key = (row.task_id.clone(), row.attempt_id.clone());
        if terminal_counts.insert(key.clone(), counts).is_some()
            || terminal_states.insert(key, row.state.clone()).is_some()
        {
            bail!("attempt ledger query returned a duplicate terminal attempt");
        }
        let report_counts: StagedCounts = serde_json::from_value(
            report
                .get("counts")
                .cloned()
                .context("attempt report has no counts")?,
        )?;
        let inserted_rows = report
            .pointer("/inserted/rows")
            .and_then(|value| value.as_u64());
        let inserted_bytes = report
            .pointer("/inserted/bytes")
            .and_then(|value| value.as_u64());
        if report.get("start").and_then(|value| value.as_u64())
            != Some(u64::from(manifest_task.start))
            || report.get("stop").and_then(|value| value.as_u64())
                != Some(u64::from(manifest_task.stop))
            || report.get("published").and_then(|value| value.as_bool()) != Some(false)
            || report_counts != counts
            || inserted_rows.is_none()
            || inserted_bytes.is_none()
            || report
                .get("started_at_ms")
                .and_then(|value| value.as_u64())
                .is_none()
            || report
                .get("finished_at_ms")
                .and_then(|value| value.as_u64())
                .is_none()
            || report
                .get("elapsed_ms")
                .and_then(|value| value.as_u64())
                .is_none()
            || report.get("linux_peak_rss_bytes").is_none()
        {
            bail!(
                "attempt {} report is incomplete or inconsistent with its declared source identity",
                row.attempt_id
            );
        }
        match row.state.as_str() {
            "accepted" => {
                validate_aggregate_only_attempt_transformation(
                    &report,
                    &report_counts,
                    manifest_task,
                    &row.attempt_id,
                )?;
                let expected_inserted = [
                    counts.summaries,
                    counts.alleles,
                    counts.frequencies,
                    counts.carriers,
                    counts.rejects,
                ]
                .into_iter()
                .try_fold(0u64, |total, value| total.checked_add(value))
                .context("attempt row total exceeds UInt64")?;
                if report.get("state").and_then(|value| value.as_str()) != Some("accepted")
                    || !report
                        .get("failure")
                        .is_some_and(serde_json::Value::is_null)
                    || inserted_rows != Some(expected_inserted)
                    || (expected_inserted > 0 && inserted_bytes == Some(0))
                    || accepted
                        .insert(row.task_id.clone(), row.attempt_id.clone())
                        .is_some()
                {
                    bail!(
                        "task {} lacks exactly one complete accepted attempt",
                        row.task_id
                    );
                }
            }
            "failed" => {
                if report.get("state").and_then(|value| value.as_str()) != Some("failed")
                    || !report
                        .get("failure")
                        .is_some_and(serde_json::Value::is_object)
                {
                    bail!(
                        "failed attempt {} has no structured failure result",
                        row.attempt_id
                    );
                }
                failed.entry(row.task_id).or_default().push(row.attempt_id);
            }
            state => bail!("attempt ledger has unsupported nonterminal state {state:?}"),
        }
    }
    for attempts in failed.values_mut() {
        attempts.sort();
    }
    if accepted.len() != tasks.len()
        || tasks
            .iter()
            .any(|task| !accepted.contains_key(&task.task_id))
    {
        bail!("run does not have exactly one accepted terminal attempt for every manifest task");
    }
    for task in tasks
        .iter()
        .filter(|task| task.controlled_fail_once.is_some())
    {
        let failures = failed
            .get(&task.task_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let initial_matches = failures.len() == 1
            && (failures[0] == task.attempt_id
                || crate::pool::durable_attempt_matches_prefix(&failures[0], &task.attempt_id));
        let retry_matches = task.retry_attempt_id.as_ref().is_some_and(|prefix| {
            accepted.get(&task.task_id).is_some_and(|attempt| {
                attempt == prefix || crate::pool::durable_attempt_matches_prefix(attempt, prefix)
            })
        });
        if !initial_matches || !retry_matches {
            bail!(
                "controlled fail-once task {} lacks its failed initial and accepted retry evidence",
                task.task_id
            );
        }
    }

    let counts = validate_physical_attempts(
        tasks,
        &accepted,
        &terminal_counts,
        &terminal_states,
        &physical,
        phase,
    )?;
    let mut nonaccepted = terminal_states
        .iter()
        .filter(|(_, state)| state.as_str() != "accepted")
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    nonaccepted.sort();
    Ok(LedgerCoverage {
        accepted,
        failed,
        nonaccepted,
        counts,
    })
}

fn read_physical_attempts(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
) -> anyhow::Result<Vec<PhysicalAttemptView>> {
    let mut result = Vec::new();
    for spec in TABLES {
        let identity = if spec.has_primary_identity {
            "countIf(release != 'y1' OR cohort != {cohort:String} OR reference_genome != 'GRCh38' OR chrom != {chrom:String})"
        } else {
            "toUInt64(0)"
        };
        let positions = if spec.has_primary_identity {
            "min(position), max(position)"
        } else {
            "toUInt32(0), toUInt32(0)"
        };
        let query = format!(
            "SELECT '{}' AS table, task_id, attempt_id, count() AS rows, uniqExact({}) AS unique_keys, {} AS identity_violations, {} AS min_position, {} AS max_position FROM {} WHERE run_id = {{run_id:String}} GROUP BY task_id, attempt_id FORMAT JSONEachRow",
            spec.label,
            spec.unique_key,
            identity,
            positions.split(',').next().expect("position expression"),
            positions.split(',').nth(1).expect("position expression"),
            spec.table,
        );
        let body = target.query_text(
            &query,
            &[
                ("run_id", &run.run_id),
                ("cohort", &run.cohort),
                ("chrom", &run.chrom),
            ],
        )?;
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            result.push(serde_json::from_str(line).context("invalid canonical aggregate JSON")?);
        }
    }
    Ok(result)
}

fn validate_physical_attempts(
    tasks: &[PoolY1TaskSpec],
    accepted: &BTreeMap<String, String>,
    terminal_counts: &BTreeMap<(String, String), StagedCounts>,
    terminal_states: &BTreeMap<(String, String), String>,
    physical: &[PhysicalAttemptView],
    phase: PhysicalPhase,
) -> anyhow::Result<Vec<CanonicalTableCount>> {
    let task_bounds: BTreeMap<&str, (u32, u32)> = tasks
        .iter()
        .map(|task| (task.task_id.as_str(), (task.start, task.stop)))
        .collect();
    let accepted_terminal_count = terminal_states
        .values()
        .filter(|state| state.as_str() == "accepted")
        .count();
    if accepted.len() != tasks.len()
        || accepted_terminal_count != tasks.len()
        || tasks.iter().any(|task| {
            accepted
                .get(&task.task_id)
                .and_then(|attempt| terminal_states.get(&(task.task_id.clone(), attempt.clone())))
                .map(String::as_str)
                != Some("accepted")
        })
    {
        bail!("physical acceptance lacks exactly one accepted terminal attempt per task");
    }
    let mut aggregates = BTreeMap::new();
    for row in physical {
        if !TABLES.iter().any(|spec| spec.label == row.table) {
            bail!("canonical snapshot contains an unknown table");
        }
        let attempt_key = (row.task_id.clone(), row.attempt_id.clone());
        if !terminal_counts.contains_key(&attempt_key) {
            bail!(
                "stale or orphan canonical contribution for task {}/attempt {}",
                row.task_id,
                row.attempt_id
            );
        }
        let bounds = task_bounds
            .get(row.task_id.as_str())
            .context("canonical row names a task absent from the manifest")?;
        if row.identity_violations != 0 {
            bail!(
                "canonical {} contains cross-run/cohort/contig identity violations",
                row.table
            );
        }
        if row.table != "rejects"
            && row.rows != 0
            && (row.min_position < bounds.0 || row.max_position > bounds.1)
        {
            bail!(
                "canonical {} escapes task {} bounds",
                row.table,
                row.task_id
            );
        }
        if row.unique_keys != row.rows {
            bail!(
                "canonical {} contains duplicate keys for task {}/attempt {}",
                row.table,
                row.task_id,
                row.attempt_id
            );
        }
        let key = (
            row.table.clone(),
            row.task_id.clone(),
            row.attempt_id.clone(),
        );
        if aggregates.insert(key, row.rows).is_some() {
            bail!("canonical snapshot contains a duplicate attempt aggregate");
        }
    }

    let mut accepted_rows = BTreeMap::new();
    for (attempt_key, counts) in terminal_counts {
        if counts.summaries.checked_add(counts.rejects) != Some(counts.source_records) {
            bail!(
                "terminal attempt counts do not reconcile source records to summaries plus rejects"
            );
        }
        let expected_rows = [
            counts.summaries,
            counts.alleles,
            counts.frequencies,
            counts.carriers,
            counts.rejects,
        ];
        let is_accepted = accepted.get(&attempt_key.0) == Some(&attempt_key.1)
            && terminal_states.get(attempt_key).map(String::as_str) == Some("accepted");
        for (spec, expected) in TABLES.into_iter().zip(expected_rows) {
            let rows = aggregates
                .get(&(
                    spec.label.to_string(),
                    attempt_key.0.clone(),
                    attempt_key.1.clone(),
                ))
                .copied()
                .unwrap_or(0);
            if is_accepted {
                if rows != expected {
                    bail!(
                        "canonical {} rows disagree with accepted ledger for task {}/attempt {}",
                        spec.label,
                        attempt_key.0,
                        attempt_key.1
                    );
                }
                let total = accepted_rows.entry(spec.label).or_insert(0u64);
                *total = total
                    .checked_add(rows)
                    .context("canonical row total exceeds UInt64")?;
            } else {
                // A failed request can commit any strict prefix of the five
                // table inserts, including a server commit whose response was
                // lost before its ledger counters advanced. Identity, task
                // bounds, terminal-attempt ownership, and uniqueness were all
                // checked above; only accepted attempts require count parity.
                if phase == PhysicalPhase::Frozen && rows != 0 {
                    bail!(
                        "nonaccepted canonical {} rows survived synchronous freeze cleanup",
                        spec.label
                    );
                }
            }
        }
        if is_accepted && counts.rejects != 0 {
            bail!("accepted terminal attempt contains rejected records");
        }
    }
    Ok(TABLES
        .into_iter()
        .map(|spec| CanonicalTableCount {
            table: spec.label.to_string(),
            rows: accepted_rows.get(spec.label).copied().unwrap_or(0),
        })
        .collect())
}

fn read_canonical_digests(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
    accepted: &BTreeMap<String, String>,
    counts: &[CanonicalTableCount],
) -> anyhow::Result<Vec<CanonicalContentDigest>> {
    let total = |label: &str| {
        counts
            .iter()
            .find(|count| count.table == label)
            .map(|count| count.rows)
            .unwrap_or(0)
    };
    let mut result = Vec::with_capacity(accepted.len() * TABLES.len());
    let mut digest_totals: BTreeMap<&str, u64> = BTreeMap::new();
    for (task_id, attempt_id) in accepted {
        for spec in TABLES {
            let count_query = format!(
                "SELECT count() FROM {} WHERE run_id = {{run_id:String}} AND task_id = {{task_id:String}} AND attempt_id = {{attempt_id:String}} FORMAT TabSeparated",
                spec.table
            );
            let parameters = [
                ("run_id", run.run_id.as_str()),
                ("task_id", task_id.as_str()),
                ("attempt_id", attempt_id.as_str()),
            ];
            let rows = target
                .query_text(&count_query, &parameters)?
                .trim()
                .parse::<u64>()
                .context("invalid canonical digest row count")?;
            let query = format!(
                "SELECT {} FROM {} WHERE run_id = {{run_id:String}} AND task_id = {{task_id:String}} AND attempt_id = {{attempt_id:String}} ORDER BY {} FORMAT RowBinary",
                spec.columns, spec.table, spec.order_by
            );
            let domain = format!("{}\0{}\0{}\0{}", spec.label, task_id, attempt_id, rows);
            let sha256 = target.query_sha256(&query, &parameters, domain.as_bytes())?;
            *digest_totals.entry(spec.label).or_default() += rows;
            result.push(CanonicalContentDigest {
                table: spec.label.to_string(),
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
                rows,
                sha256,
            });
        }
    }
    for spec in TABLES {
        if digest_totals.get(spec.label).copied().unwrap_or(0) != total(spec.label) {
            bail!(
                "per-attempt {} digest counts do not equal the frozen canonical count",
                spec.label
            );
        }
    }
    Ok(result)
}

fn record_state(
    target: &ClickHouseTarget,
    run: &PoolY1TaskSpec,
    expected: &IndependentExpectedCounts,
    task_count: usize,
    revision: u64,
    state: &str,
    message: &str,
) -> anyhow::Result<()> {
    record_load_run(
        target,
        &LoadRunLedgerRow {
            run_id: run.run_id.clone(),
            revision,
            state: state.to_string(),
            load_scope: LoadScope::FullChromosome.as_str().to_string(),
            release: Release::Y1.as_str().to_string(),
            cohort: run.cohort.clone(),
            reference_genome: ReferenceGenome::Grch38.as_str().to_string(),
            chrom: run.chrom.clone(),
            interval_start: 1,
            interval_end: grch38_contig_length(&run.chrom)?,
            source_uri: run.source_uri.clone(),
            source_generation: run.source_generation.clone(),
            source_checksum_algorithm: run.source_checksum_algorithm.clone(),
            source_checksum: run.source_checksum.clone(),
            source_index_uri: run.source_index_uri.clone(),
            source_index_generation: run.source_index_generation.clone(),
            source_index_checksum: run.source_index_checksum.clone(),
            schema_version: Y1_SCHEMA_VERSION,
            loader_version: env!("CARGO_PKG_VERSION").to_string(),
            expected_tasks: u32::try_from(task_count)?,
            expected_source_records: expected.counts.source_records,
            summary_rows: expected.counts.summaries,
            allele_rows: expected.counts.alleles,
            frequency_rows: expected.counts.frequencies,
            carrier_rows: expected.counts.carriers,
            rejected_records: expected.counts.rejects,
            created_at_ms: revision / 1_000_000,
            updated_at_ms: revision / 1_000_000,
            message: message.to_string(),
        },
    )
}

fn revision_now() -> anyhow::Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_for(chrom: &str, index: usize, start: u32, stop: u32) -> PoolY1TaskSpec {
        PoolY1TaskSpec {
            coordinator_task_id: format!("custom_{index}"),
            label: "test".into(),
            run_id: "run".into(),
            task_id: format!("task-{index}"),
            attempt_id: format!("attempt-{index}"),
            release: "y1".into(),
            cohort: "aou".into(),
            reference_genome: "GRCh38".into(),
            chrom: chrom.into(),
            start,
            stop,
            source_uri: format!(
                "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.{chrom}.vcf.gz"
            ),
            source_generation: "1".into(),
            source_checksum_algorithm: "md5_base64".into(),
            source_checksum: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            source_size_bytes: 1,
            source_index_uri: format!(
                "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.{chrom}.vcf.gz.tbi"
            ),
            source_index_generation: "2".into(),
            source_index_checksum_algorithm: "md5_base64".into(),
            source_index_checksum: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            source_index_size_bytes: 1,
            primary_load_mode: None,
            retry_attempt_id: None,
            controlled_fail_once: None,
        }
    }

    fn independent_for(task: &PoolY1TaskSpec) -> IndependentExpectedCounts {
        IndependentExpectedCounts {
            contract_version: 1,
            run_id: task.run_id.clone(),
            cohort: task.cohort.clone(),
            chrom: task.chrom.clone(),
            evidence_uri: "file://independent.json".into(),
            producer: "independent-test".into(),
            source_generation: task.source_generation.clone(),
            source_checksum: task.source_checksum.clone(),
            primary_load_mode: task.primary_load_mode,
            carrier_loading_status: task
                .primary_load_mode
                .map(|_| task.carrier_loading_status()),
            counts: StagedCounts {
                source_records: 1,
                summaries: 1,
                alleles: 2,
                frequencies: 3,
                carriers: 0,
                rejects: 0,
            },
            facts: IndependentReconciliationFacts {
                source_records: 1,
                alt_alleles: 2,
                frequency_rows: 3,
                genotype_calls: 0,
                called_alleles: 0,
                carrier_alt_copies: 0,
                fully_missing_genotypes: 0,
                partially_called_genotypes: 0,
                annotated_alt_alleles: 2,
                source_content_sha256: "a".repeat(64),
                genotype_content_sha256: "b".repeat(64),
                annotation_content_sha256: "c".repeat(64),
            },
        }
    }

    type TestSnapshot = (
        Vec<PoolY1TaskSpec>,
        BTreeMap<String, String>,
        BTreeMap<(String, String), StagedCounts>,
        BTreeMap<(String, String), String>,
        Vec<PhysicalAttemptView>,
    );

    fn snapshot() -> TestSnapshot {
        let task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        let counts = StagedCounts {
            source_records: 1,
            summaries: 1,
            alleles: 2,
            frequencies: 3,
            carriers: 0,
            rejects: 0,
        };
        let accepted = BTreeMap::from([(task.task_id.clone(), "accepted".into())]);
        let terminal_counts = BTreeMap::from([
            ((task.task_id.clone(), "accepted".into()), counts),
            ((task.task_id.clone(), "failed".into()), counts),
        ]);
        let terminal_states = BTreeMap::from([
            ((task.task_id.clone(), "accepted".into()), "accepted".into()),
            ((task.task_id.clone(), "failed".into()), "failed".into()),
        ]);
        let mut physical = Vec::new();
        for attempt in ["accepted", "failed"] {
            for (table, rows) in [("summaries", 1), ("alleles", 2), ("frequencies", 3)] {
                physical.push(PhysicalAttemptView {
                    table: table.into(),
                    task_id: task.task_id.clone(),
                    attempt_id: attempt.into(),
                    rows,
                    unique_keys: rows,
                    identity_violations: 0,
                    min_position: 1,
                    max_position: 1,
                });
            }
        }
        (
            vec![task],
            accepted,
            terminal_counts,
            terminal_states,
            physical,
        )
    }

    #[test]
    fn manifest_requires_exact_adjacency_full_coverage_and_one_identity() {
        for chrom in (1..=22)
            .map(|n| format!("chr{n}"))
            .chain(["chrX".into(), "chrY".into()])
        {
            let length = grch38_contig_length(&chrom).unwrap();
            assert!(validate_manifest(&[task_for(&chrom, 0, 1, length)]).is_ok());
            assert!(validate_manifest(&[task_for(&chrom, 0, 1, length - 1)]).is_err());
            assert!(validate_manifest(&[
                task_for(&chrom, 0, 1, 10),
                task_for(&chrom, 1, 12, length)
            ])
            .is_err());
        }
        assert!(validate_manifest(&[task_for("chrM", 0, 1, 16_569)]).is_err());
    }

    #[test]
    fn independent_reconciliation_rejects_malformed_hash_source_drop_and_cross_identity() {
        let task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        assert!(validate_independent_reconciliation(&independent_for(&task), &task).is_ok());
        let mut malformed = independent_for(&task);
        malformed.facts.source_content_sha256 = "bad".into();
        assert!(validate_independent_reconciliation(&malformed, &task).is_err());
        let mut dropped = independent_for(&task);
        dropped.facts.source_records = 2;
        assert!(validate_independent_reconciliation(&dropped, &task).is_err());
        let mut cross = independent_for(&task);
        cross.source_generation = "other".into();
        assert!(validate_independent_reconciliation(&cross, &task).is_err());
    }

    #[test]
    fn aggregate_only_reconciliation_requires_explicit_unavailable_carrier_receipt() {
        let mut task = task_for("chrX", 0, 1, grch38_contig_length("chrX").unwrap());
        task.cohort = "hgsvc_hprc".into();
        task.source_uri =
            "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chrX.vcf.gz"
                .into();
        task.source_index_uri = format!("{}.tbi", task.source_uri);
        task.primary_load_mode = Some(super::super::PrimaryLoadMode::AggregateOnlyNoCarriers);
        task.validate(&task.coordinator_task_id).unwrap();

        let expected = independent_for(&task);
        assert_eq!(expected.counts.carriers, 0);
        assert_eq!(expected.facts.genotype_calls, 0);
        assert_eq!(
            expected.carrier_loading_status,
            Some(super::super::CarrierLoadingStatus::UnavailableNotLoaded)
        );
        validate_independent_reconciliation(&expected, &task).unwrap();

        let mut missing_status = independent_for(&task);
        missing_status.carrier_loading_status = None;
        assert!(validate_independent_reconciliation(&missing_status, &task).is_err());

        let mut fabricated_carrier = independent_for(&task);
        fabricated_carrier.counts.carriers = 1;
        fabricated_carrier.facts.carrier_alt_copies = 1;
        fabricated_carrier.facts.called_alleles = 1;
        assert!(validate_independent_reconciliation(&fabricated_carrier, &task).is_err());
    }

    #[test]
    fn ordinary_legacy_attempt_reports_may_omit_mode_but_aggregate_only_may_not() {
        let ordinary = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        validate_attempt_carrier_loading(&serde_json::json!({}), &ordinary, "legacy").unwrap();
        validate_attempt_carrier_loading(
            &serde_json::json!({
                "primary_load_mode": null,
                "carrier_loading_status": "not_applicable_aggregate_source"
            }),
            &ordinary,
            "current",
        )
        .unwrap();
        assert!(validate_attempt_carrier_loading(
            &serde_json::json!({"carrier_loading_status": "loaded"}),
            &ordinary,
            "wrong",
        )
        .is_err());

        let mut aggregate = task_for("chrX", 0, 1, grch38_contig_length("chrX").unwrap());
        aggregate.cohort = "hgsvc_hprc".into();
        aggregate.source_uri =
            "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chrX.vcf.gz"
                .into();
        aggregate.source_index_uri = format!("{}.tbi", aggregate.source_uri);
        aggregate.primary_load_mode = Some(super::super::PrimaryLoadMode::AggregateOnlyNoCarriers);
        assert!(validate_attempt_carrier_loading(
            &serde_json::json!({}),
            &aggregate,
            "missing-mode",
        )
        .is_err());
        validate_attempt_carrier_loading(
            &serde_json::json!({
                "primary_load_mode": "aggregate_only_no_carriers",
                "carrier_loading_status": "unavailable_not_loaded"
            }),
            &aggregate,
            "explicit-mode",
        )
        .unwrap();
    }

    #[test]
    fn aggregate_only_attempt_transformation_must_have_no_genotype_or_carrier_evidence() {
        let mut task = task_for("chrX", 0, 1, grch38_contig_length("chrX").unwrap());
        task.cohort = "hgsvc_hprc".into();
        task.source_uri =
            "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chrX.vcf.gz"
                .into();
        task.source_index_uri = format!("{}.tbi", task.source_uri);
        task.primary_load_mode = Some(super::super::PrimaryLoadMode::AggregateOnlyNoCarriers);
        let counts = StagedCounts {
            source_records: 1,
            summaries: 1,
            alleles: 2,
            frequencies: 3,
            carriers: 0,
            rejects: 0,
        };
        let report = serde_json::json!({
            "transformation": {
                "source_records": 1,
                "summary_rows": 1,
                "carrier_rows": 0,
                "genotype_calls": 0,
                "missing_genotypes": 0,
                "partially_called_genotypes": 0,
                "reference_genotypes": 0,
                "rejected_records": 0,
                "rejects": []
            }
        });

        validate_aggregate_only_attempt_transformation(&report, &counts, &task, "happy").unwrap();

        for field in [
            "carrier_rows",
            "genotype_calls",
            "missing_genotypes",
            "partially_called_genotypes",
            "reference_genotypes",
        ] {
            let mut tampered = report.clone();
            tampered["transformation"][field] = serde_json::json!(1);
            let error =
                validate_aggregate_only_attempt_transformation(&tampered, &counts, &task, field)
                    .unwrap_err();
            assert!(error.to_string().contains(field), "{error:#}");
        }

        for field in [
            "gt_fields_parsed",
            "format_fields_parsed",
            "carrier_evidence",
            "haplotype_rows",
        ] {
            let mut unexpected_evidence = report.clone();
            unexpected_evidence["transformation"][field] = serde_json::json!(1);
            assert!(validate_aggregate_only_attempt_transformation(
                &unexpected_evidence,
                &counts,
                &task,
                field,
            )
            .is_err());
        }

        let mut top_level_carriers = counts;
        top_level_carriers.carriers = 1;
        assert!(validate_aggregate_only_attempt_transformation(
            &report,
            &top_level_carriers,
            &task,
            "top-level-carriers",
        )
        .is_err());
        assert!(validate_aggregate_only_attempt_transformation(
            &serde_json::json!({}),
            &counts,
            &task,
            "missing-transformation",
        )
        .is_err());

        let ordinary = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        validate_aggregate_only_attempt_transformation(
            &serde_json::json!({}),
            &counts,
            &ordinary,
            "ordinary-legacy",
        )
        .unwrap();
    }

    #[test]
    fn stale_but_valid_older_tbi_attempt_is_rejected_by_newer_finalization_manifest() {
        let mut manifest_task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        manifest_task.source_index_generation = "3".into();
        manifest_task.source_index_checksum = "AQEBAQEBAQEBAQEBAQEBAQ==".into();

        let mut older_task = manifest_task.clone();
        older_task.source_index_generation = "2".into();
        older_task.source_index_checksum = "AAAAAAAAAAAAAAAAAAAAAA==".into();
        older_task
            .validate(&older_task.coordinator_task_id)
            .unwrap();
        manifest_task
            .validate(&manifest_task.coordinator_task_id)
            .unwrap();

        let accepted_older_attempt = serde_json::json!({
            "state": "accepted",
            "source_uri": older_task.source_uri.as_str(),
            "source_generation": older_task.source_generation.as_str(),
            "source_size_bytes": older_task.source_size_bytes,
            "source_checksum_algorithm": older_task.source_checksum_algorithm.as_str(),
            "source_checksum": older_task.source_checksum.as_str(),
            "source_index_uri": older_task.source_index_uri.as_str(),
            "source_index_generation": older_task.source_index_generation.as_str(),
            "source_index_size_bytes": older_task.source_index_size_bytes,
            "source_index_checksum_algorithm": older_task.source_index_checksum_algorithm.as_str(),
            "source_index_checksum": older_task.source_index_checksum.as_str(),
        });
        validate_attempt_source_identity(
            &accepted_older_attempt,
            &older_task,
            "accepted-older-generation",
        )
        .unwrap();

        let error = validate_attempt_source_identity(
            &accepted_older_attempt,
            &manifest_task,
            "accepted-older-generation",
        )
        .unwrap_err();
        assert!(error.to_string().contains("source_index_generation"));

        let mut legacy_report = accepted_older_attempt;
        legacy_report
            .as_object_mut()
            .unwrap()
            .remove("source_checksum");
        assert!(validate_attempt_source_identity(
            &legacy_report,
            &older_task,
            "legacy-without-vcf-checksum"
        )
        .is_err());
    }

    #[test]
    fn retry_rows_are_verified_then_must_be_removed_before_freeze() {
        let (tasks, accepted, counts, states, physical) = snapshot();
        let before = validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &physical,
            PhysicalPhase::BeforeCleanup,
        )
        .unwrap();
        assert_eq!(
            before
                .iter()
                .find(|row| row.table == "summaries")
                .unwrap()
                .rows,
            1
        );
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &physical,
            PhysicalPhase::Frozen
        )
        .is_err());
        let accepted_only = physical
            .into_iter()
            .filter(|row| row.attempt_id == "accepted")
            .collect::<Vec<_>>();
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &accepted_only,
            PhysicalPhase::Frozen
        )
        .is_ok());
    }

    #[test]
    fn physical_acceptance_allows_attributable_partial_failure_but_rejects_stale_and_cross_identity_rows(
    ) {
        let (tasks, accepted, counts, states, physical) = snapshot();
        assert!(validate_physical_attempts(
            &tasks,
            &BTreeMap::new(),
            &counts,
            &states,
            &physical,
            PhysicalPhase::BeforeCleanup
        )
        .is_err());
        let mut stale = physical.clone();
        let mut orphan = stale[0].clone();
        orphan.attempt_id = "orphan".into();
        stale.push(orphan);
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &stale,
            PhysicalPhase::BeforeCleanup
        )
        .is_err());
        let mut cross = physical.clone();
        cross[0].identity_violations = 1;
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &cross,
            PhysicalPhase::BeforeCleanup
        )
        .is_err());
        let mut partial = physical;
        partial
            .iter_mut()
            .find(|row| row.attempt_id == "failed" && row.table == "alleles")
            .unwrap()
            .rows = 1;
        partial
            .iter_mut()
            .find(|row| row.attempt_id == "failed" && row.table == "alleles")
            .unwrap()
            .unique_keys = 1;
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &partial,
            PhysicalPhase::BeforeCleanup
        )
        .is_ok());
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &counts,
            &states,
            &partial,
            PhysicalPhase::Frozen
        )
        .is_err());
    }

    #[test]
    fn freeze_state_rejects_late_primary_writes() {
        for state in [
            "freezing",
            "frozen",
            "accepted_frozen",
            "finalization_failed",
        ] {
            assert!(super::super::storage::validate_primary_write_state(Some(state)).is_err());
        }
        assert!(super::super::storage::validate_primary_write_state(None).is_ok());
        assert!(super::super::storage::validate_primary_write_state(Some("validated")).is_ok());
    }

    #[test]
    fn future_finalization_requires_revision_bound_worker_provenance() {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let valid = serde_json::json!({
            "worker_identity": "worker-7",
            "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
            "backend_revision": revision,
            "worker_principal": "writer_a",
        });
        assert!(validate_worker_provenance(&valid, "attempt-7", "writer_a").is_ok());
        let invalid = serde_json::json!({
            "worker_identity": "unknown-worker",
            "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
            "backend_revision": revision,
            "worker_principal": "writer_a",
        });
        assert!(validate_worker_provenance(&invalid, "attempt-7", "writer_a").is_err());
    }

    #[test]
    fn a_load_b_finalize_and_missing_principal_fail_closed() {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let mut report = serde_json::json!({
            "worker_identity": "worker-7",
            "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
            "backend_revision": revision,
            "worker_principal": "writer_a",
        });
        assert!(validate_worker_provenance(&report, "attempt-a", "writer_b").is_err());
        report.as_object_mut().unwrap().remove("worker_principal");
        assert!(validate_worker_provenance(&report, "attempt-a", "writer_a").is_err());
    }

    #[test]
    fn mixed_terminal_attempt_principals_fail_closed() {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let report = |principal: &str| {
            serde_json::json!({
                "worker_identity": "worker-7",
                "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
                "backend_revision": revision,
                "worker_principal": principal,
            })
            .to_string()
        };
        let body = [
            serde_json::json!({"attempt_id": "attempt-a", "report_json": report("writer_a")}),
            serde_json::json!({"attempt_id": "attempt-b", "report_json": report("writer_b")}),
        ]
        .into_iter()
        .map(|row| row.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        assert!(validate_terminal_worker_principal_rows(&body, "writer_a").is_err());
    }

    fn record_terminal_fixture_attempt(
        target: &ClickHouseTarget,
        task: &PoolY1TaskSpec,
        context: &super::super::AttemptContext,
        counts: StagedCounts,
        transformation: &super::super::TransformationReport,
        accepted: bool,
        worker_principal: &str,
    ) {
        let revision = revision_now().unwrap();
        let inserted_rows = counts.summaries
            + counts.alleles
            + counts.frequencies
            + counts.carriers
            + counts.rejects;
        let report = super::super::PoolY1AttemptReport {
            run_id: task.run_id.clone(),
            task_id: task.task_id.clone(),
            attempt_id: context.attempt_id.clone(),
            cohort: context.cohort,
            chrom: task.chrom.clone(),
            start: task.start,
            stop: task.stop,
            source_uri: task.source_uri.clone(),
            source_generation: task.source_generation.clone(),
            source_size_bytes: task.source_size_bytes,
            source_checksum_algorithm: task.source_checksum_algorithm.clone(),
            source_checksum: task.source_checksum.clone(),
            source_index_uri: task.source_index_uri.clone(),
            source_index_generation: task.source_index_generation.clone(),
            source_index_size_bytes: task.source_index_size_bytes,
            source_index_checksum_algorithm: task.source_index_checksum_algorithm.clone(),
            source_index_checksum: task.source_index_checksum.clone(),
            primary_load_mode: task.primary_load_mode,
            carrier_loading_status: task.carrier_loading_status(),
            counts,
            transformation: transformation.clone(),
            inserted: super::super::InsertStats {
                rows: inserted_rows,
                bytes: 1,
                requests: 1,
            },
            started_at_ms: 1,
            finished_at_ms: 2,
            elapsed_ms: 1,
            parse_transform_insert_ms: 1,
            linux_peak_rss_bytes: None,
            worker_identity: "integration-worker".to_string(),
            worker_build_version:
                "gnomad-lr/0123456789abcdef0123456789abcdef01234567/x86_64-linux-release"
                    .to_string(),
            backend_revision: "0123456789abcdef0123456789abcdef01234567".to_string(),
            worker_principal: worker_principal.to_string(),
            state: if accepted { "accepted" } else { "failed" }.to_string(),
            failure: None,
            published: false,
        };
        let ledger = super::super::storage::TaskAttemptLedgerRow::new(
            context,
            revision,
            if accepted {
                super::super::storage::AttemptState::Accepted
            } else {
                super::super::storage::AttemptState::Failed
            },
            counts,
            &report,
            if accepted { "" } else { "fixture retry" },
        )
        .unwrap();
        super::super::storage::record_task_attempt(target, &ledger).unwrap();
    }

    #[test]
    fn local_clickhouse_freezes_canonical_rows_in_place_and_cleans_retry() {
        let Ok(endpoint) = std::env::var("GNOMAD_LR_Y1_TEST_ENDPOINT") else {
            return;
        };
        let database = std::env::var("GNOMAD_LR_Y1_TEST_DATABASE")
            .unwrap_or_else(|_| "gnomad_lr_y1_scratch_v5_ci".to_string());
        let target = ClickHouseTarget::new(
            &endpoint,
            &database,
            TargetKind::Scratch,
            super::super::AuthSource::None,
            false,
            false,
        )
        .unwrap();
        super::super::init_schema(&target).unwrap();
        let worker_principal = format!("gnomad_lr_y1_worker_{}", std::process::id());
        let worker_password = format!("local_test_{}", std::process::id());
        target
            .execute(&format!("DROP USER IF EXISTS {worker_principal}"))
            .unwrap();
        target
            .execute(&format!(
                "CREATE USER {worker_principal} IDENTIFIED WITH plaintext_password BY '{worker_password}' SETTINGS async_insert = 0"
            ))
            .unwrap();
        target
            .execute(&format!(
                "GRANT SELECT, INSERT ON {}.* TO {worker_principal}",
                target.database()
            ))
            .unwrap();
        std::env::set_var(super::super::Y1_WORKER_USERNAME_ENV, &worker_principal);
        std::env::set_var(super::super::Y1_WORKER_PASSWORD_ENV, &worker_password);
        let worker = ClickHouseTarget::new(
            &endpoint,
            &database,
            TargetKind::Scratch,
            super::super::AuthSource::Environment {
                username_variable: super::super::Y1_WORKER_USERNAME_ENV.to_string(),
                password_variable: super::super::Y1_WORKER_PASSWORD_ENV.to_string(),
            },
            false,
            false,
        )
        .unwrap();
        let fence = WorkerWriteFence::new(&target, worker.clone(), &worker_principal).unwrap();

        let fixture = include_str!("../../tests/fixtures/y1/aou_summary_only_ins.vcf");
        let header = super::super::Y1Header::parse(fixture, super::super::Cohort::Aou).unwrap();
        let batch = super::super::transform_records(
            &header,
            fixture
                .lines()
                .filter(|line| !line.is_empty() && !line.starts_with('#')),
        );
        let mut task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        task.run_id = format!("freeze-integration-{}", revision_now().unwrap());
        let base = super::super::AttemptContext {
            run_id: task.run_id.clone(),
            task_id: task.task_id.clone(),
            attempt_id: "failed".into(),
            cohort: super::super::Cohort::Aou,
            chrom: task.chrom.clone(),
            interval_start: task.start,
            interval_end: task.stop,
        };
        super::super::stage_attempt(&worker, &base, &batch).unwrap();
        // Model a request that committed physical rows but failed before its
        // per-table counters advanced in the terminal report.
        record_terminal_fixture_attempt(
            &worker,
            &task,
            &base,
            StagedCounts::default(),
            &batch.report,
            false,
            &worker_principal,
        );
        let accepted_context = super::super::AttemptContext {
            attempt_id: "accepted".into(),
            ..base.clone()
        };
        let accepted_counts =
            super::super::stage_attempt(&worker, &accepted_context, &batch).unwrap();
        record_terminal_fixture_attempt(
            &worker,
            &task,
            &accepted_context,
            accepted_counts,
            &batch.report,
            true,
            &worker_principal,
        );

        let mut expected = independent_for(&task);
        expected.counts = accepted_counts;
        expected.facts.source_records = accepted_counts.source_records;
        expected.facts.alt_alleles = accepted_counts.alleles;
        expected.facts.frequency_rows = accepted_counts.frequencies;
        expected.facts.annotated_alt_alleles = accepted_counts.alleles;
        let nonce = format!("{}-{}", std::process::id(), revision_now().unwrap());
        let manifest_path = std::env::temp_dir().join(format!("gnomad-lr-manifest-{nonce}.json"));
        let expected_path = std::env::temp_dir().join(format!("gnomad-lr-expected-{nonce}.json"));
        std::fs::write(
            &manifest_path,
            serde_json::to_vec(&vec![task.clone()]).unwrap(),
        )
        .unwrap();
        std::fs::write(&expected_path, serde_json::to_vec(&expected).unwrap()).unwrap();
        // This precheck intentionally happens before the database fence. The
        // delayed INSERT below is issued only after the durable acceptance.
        super::super::storage::ensure_run_accepts_primary_writes(&worker, &task.run_id).unwrap();
        let stopped = finalize_contig_run_inner(
            &target,
            &fence,
            &manifest_path,
            &expected_path,
            "integration-test",
            None,
            true,
        );
        let stopped = stopped.unwrap_err();
        assert!(stopped.to_string().contains("test stop"), "{stopped:#}");
        assert_eq!(latest_run_state(&target, &task.run_id).unwrap(), "frozen");
        assert!(finalize_contig_run(
            &target,
            &fence,
            &manifest_path,
            &expected_path,
            "mismatched-operator",
        )
        .is_err());
        assert_eq!(latest_run_state(&target, &task.run_id).unwrap(), "frozen");

        let report = finalize_contig_run(
            &target,
            &fence,
            &manifest_path,
            &expected_path,
            "integration-test",
        )
        .unwrap();
        let accepted_retry = finalize_contig_run(
            &target,
            &fence,
            &manifest_path,
            &expected_path,
            "integration-test",
        )
        .unwrap();
        assert_eq!(accepted_retry, report);
        assert!(finalize_contig_run(
            &target,
            &fence,
            &manifest_path,
            &expected_path,
            "mismatched-operator",
        )
        .is_err());
        assert_eq!(
            latest_run_state(&target, &task.run_id).unwrap(),
            "accepted_frozen"
        );
        let _ = std::fs::remove_file(manifest_path);
        let _ = std::fs::remove_file(expected_path);
        assert!(report.accepted && report.frozen && !report.published);
        assert!(report
            .acceptance
            .canonical_digests
            .iter()
            .all(|digest| valid_sha256(&digest.sha256)));

        let rows = target.query_text(
            "SELECT countIf(attempt_id = 'failed'), countIf(attempt_id = 'accepted') FROM lr_y1_summaries WHERE run_id = {run_id:String} FORMAT TabSeparated",
            &[("run_id", &task.run_id)],
        ).unwrap();
        assert_eq!(rows.trim(), "0\t1");
        let delayed = worker.execute_with_params(
            "INSERT INTO lr_y1_summaries SELECT * FROM lr_y1_summaries WHERE run_id = {run_id:String} LIMIT 1",
            &[("run_id", &task.run_id)],
        );
        assert!(
            delayed.is_err(),
            "database fence accepted a delayed worker insert"
        );
        target
            .execute(&format!("DROP USER IF EXISTS {worker_principal}"))
            .unwrap();
        std::env::remove_var(super::super::Y1_WORKER_USERNAME_ENV);
        std::env::remove_var(super::super::Y1_WORKER_PASSWORD_ENV);
    }
}
