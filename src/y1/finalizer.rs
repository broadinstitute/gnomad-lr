use super::{
    contig::grch38_contig_length,
    record_load_run,
    storage::{publish_accepted_staged_run, validate_load_acceptance_receipt},
    ClickHouseTarget, Cohort, LoadRunLedgerRow, LoadScope, PoolY1TaskSpec, PublicationRequest,
    ReferenceGenome, Release, StagedCounts, TargetKind, Y1_SCHEMA_VERSION,
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
    signature: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingContentSignature {
    pub table: String,
    pub rows: u64,
    pub signature: u64,
}

struct LedgerCoverage {
    accepted: BTreeMap<String, String>,
    failed: BTreeMap<String, Vec<String>>,
    staging_signatures: Vec<StagingContentSignature>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadAcceptanceReceipt {
    pub contract_version: u8,
    pub schema_version: u16,
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
    pub accepted_attempts: BTreeMap<String, String>,
    pub failed_attempts: BTreeMap<String, Vec<String>>,
    pub expected_counts: StagedCounts,
    pub source_content_sha256: String,
    pub genotype_content_sha256: String,
    pub annotation_content_sha256: String,
    pub staging_signatures: Vec<StagingContentSignature>,
}

#[derive(Debug, Serialize)]
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
    pub published: bool,
}

/// Backward-compatible chr22-only entry point with its original signature.
pub fn finalize_chr22_run(
    target: &ClickHouseTarget,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<FinalizationReport> {
    finalize_contig_run_inner(
        target,
        manifest_path,
        expected_path,
        operator_identity,
        Some("chr22"),
    )
}

/// Finalize and publish exactly one complete canonical GRCh38 contig.
pub fn finalize_contig_run(
    target: &ClickHouseTarget,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<FinalizationReport> {
    finalize_contig_run_inner(
        target,
        manifest_path,
        expected_path,
        operator_identity,
        None,
    )
}

fn finalize_contig_run_inner(
    target: &ClickHouseTarget,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
    required_chrom: Option<&str>,
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
    let contig_length = grch38_contig_length(&run.chrom)?;
    let manifest_sha256 = format!("{:x}", Sha256::digest(&manifest_bytes));

    let expected_bytes = std::fs::read(expected_path)
        .with_context(|| format!("failed to read {}", expected_path.display()))?;
    let expected: IndependentExpectedCounts = serde_json::from_slice(&expected_bytes)
        .with_context(|| format!("invalid independent counts {}", expected_path.display()))?;
    validate_independent_reconciliation(&expected, &run)?;
    let independent_counts_sha256 = format!("{:x}", Sha256::digest(&expected_bytes));

    let ledger = validate_ledger_coverage(target, &run.run_id, &run.chrom, &tasks)?;
    validate_expected_staging_counts(&ledger.staging_signatures, expected.counts)?;
    let acceptance = build_acceptance_receipt(
        &run,
        &expected,
        &ledger,
        manifest_sha256.clone(),
        independent_counts_sha256.clone(),
    );
    let acceptance_json = serde_json::to_string(&acceptance)?;
    let acceptance_receipt_sha256 = format!("{:x}", Sha256::digest(acceptance_json.as_bytes()));
    let accepted_revision = revision_now()?;
    record_state(
        target,
        &run,
        &expected,
        tasks.len(),
        accepted_revision,
        "accepted",
        &acceptance_json,
    )?;

    // Re-read both the durable receipt and the complete physical staging set.
    // Full-contig publication receives a capability only after both still match.
    let persisted_acceptance = validate_load_acceptance_receipt(
        target,
        &run.run_id,
        &acceptance_json,
        &acceptance_receipt_sha256,
    )?;
    let current_ledger = validate_ledger_coverage(target, &run.run_id, &run.chrom, &tasks)?;
    validate_expected_staging_counts(&current_ledger.staging_signatures, expected.counts)?;
    let current_acceptance = build_acceptance_receipt(
        &run,
        &expected,
        &current_ledger,
        manifest_sha256.clone(),
        independent_counts_sha256.clone(),
    );
    if current_acceptance != acceptance {
        bail!("staging or attempt ledger changed after the durable load acceptance was recorded");
    }
    let cohort = parse_cohort(&run.cohort)?;
    let request = PublicationRequest {
        run_id: run.run_id.clone(),
        scope: LoadScope::FullChromosome,
        release: Release::Y1,
        cohort,
        reference_genome: ReferenceGenome::Grch38,
        chrom: run.chrom.clone(),
        interval_start: 1,
        interval_end: contig_length,
        expected_tasks: u32::try_from(tasks.len())?,
        expected_counts: expected.counts,
        source_uri: run.source_uri.clone(),
        source_generation: run.source_generation.clone(),
        source_checksum: run.source_checksum.clone(),
    };

    let created = revision_now()?;
    record_state(
        target,
        &run,
        &expected,
        tasks.len(),
        created,
        "finalizing",
        operator_identity,
    )?;
    if let Err(error) = publish_accepted_staged_run(target, &request, &persisted_acceptance) {
        let _ = record_state(
            target,
            &run,
            &expected,
            tasks.len(),
            revision_now()?,
            "finalization_failed",
            &format!("{operator_identity}: {error:#}"),
        );
        return Err(error.context(format!("guarded full-{} publication failed", run.chrom)));
    }
    record_state(
        target,
        &run,
        &expected,
        tasks.len(),
        revision_now()?,
        "published",
        operator_identity,
    )?;

    Ok(FinalizationReport {
        run_id: run.run_id,
        cohort: run.cohort,
        chrom: run.chrom,
        operator_identity: operator_identity.to_string(),
        manifest_tasks: tasks.len(),
        manifest_sha256,
        accepted_attempts: ledger.accepted,
        failed_attempts: ledger.failed,
        expected_counts: expected.counts,
        independent_evidence_uri: expected.evidence_uri,
        independent_counts_sha256,
        acceptance_receipt_sha256,
        acceptance,
        published: true,
    })
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
    {
        bail!("independent reconciliation identity does not match the manifest run/source");
    }
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
        || (run.cohort == "hgsvc_hprc" && (facts.genotype_calls == 0 || facts.called_alleles == 0))
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

fn validate_expected_staging_counts(
    signatures: &[StagingContentSignature],
    expected: StagedCounts,
) -> anyhow::Result<()> {
    let rows = |table: &str| {
        signatures
            .iter()
            .find(|signature| signature.table == table)
            .map(|signature| signature.rows)
            .with_context(|| format!("acceptance snapshot is missing {table}"))
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
        bail!("physical accepted staging counts {observed:?} do not equal independent expected counts {expected:?}");
    }
    Ok(())
}

fn build_acceptance_receipt(
    run: &PoolY1TaskSpec,
    expected: &IndependentExpectedCounts,
    ledger: &LedgerCoverage,
    manifest_sha256: String,
    independent_counts_sha256: String,
) -> LoadAcceptanceReceipt {
    LoadAcceptanceReceipt {
        contract_version: 1,
        schema_version: Y1_SCHEMA_VERSION,
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
        accepted_attempts: ledger.accepted.clone(),
        failed_attempts: ledger.failed.clone(),
        expected_counts: expected.counts,
        source_content_sha256: expected.facts.source_content_sha256.clone(),
        genotype_content_sha256: expected.facts.genotype_content_sha256.clone(),
        annotation_content_sha256: expected.facts.annotation_content_sha256.clone(),
        staging_signatures: ledger.staging_signatures.clone(),
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

fn validate_worker_provenance(report: &serde_json::Value, attempt_id: &str) -> anyhow::Result<()> {
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

    if matches!(worker_identity, "unknown" | "unknown-worker") {
        bail!("attempt {attempt_id} has placeholder worker identity");
    }
    if matches!(
        build_identity,
        "unknown" | "unknown-build" | "unversioned-development-build"
    ) {
        bail!("attempt {attempt_id} has placeholder worker build identity");
    }
    if !matches!(backend_revision.len(), 40 | 64)
        || !backend_revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("attempt {attempt_id} backend revision is not a full Git object ID");
    }
    if !build_identity.contains(backend_revision) {
        bail!("attempt {attempt_id} worker build identity is not bound to its backend revision");
    }
    Ok(())
}

fn validate_ledger_coverage(
    target: &ClickHouseTarget,
    run_id: &str,
    chrom: &str,
    tasks: &[PoolY1TaskSpec],
) -> anyhow::Result<LedgerCoverage> {
    let query = r#"
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
"#;
    let body = target.query_text(query, &[("run_id", run_id)])?;
    let physical = read_physical_attempts(target, run_id, &tasks[0].cohort, chrom)?;
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
        let Some(bounds) = expected.get(row.task_id.as_str()) else {
            bail!(
                "run ledger contains task {} absent from the checked manifest",
                row.task_id
            );
        };
        if row.chrom != chrom || (row.interval_start, row.interval_end) != *bounds {
            bail!(
                "attempt {} has bounds inconsistent with its manifest task",
                row.attempt_id
            );
        }
        let report: serde_json::Value = serde_json::from_str(&row.report_json)
            .context("invalid durable attempt report JSON")?;
        validate_worker_provenance(&report, &row.attempt_id)?;
        let manifest_task = tasks
            .iter()
            .find(|task| task.task_id == row.task_id)
            .expect("task was checked against the manifest above");
        for (field, expected_value) in [
            ("run_id", run_id),
            ("task_id", row.task_id.as_str()),
            ("attempt_id", row.attempt_id.as_str()),
            ("cohort", manifest_task.cohort.as_str()),
            ("chrom", chrom),
        ] {
            if report.get(field).and_then(|value| value.as_str()) != Some(expected_value) {
                bail!("attempt {} report has inconsistent {field}", row.attempt_id);
            }
        }
        let ledger_counts = StagedCounts {
            source_records: row.source_records,
            summaries: row.summary_rows,
            alleles: row.allele_rows,
            frequencies: row.frequency_rows,
            carriers: row.carrier_rows,
            rejects: row.rejected_records,
        };
        let attempt_key = (row.task_id.clone(), row.attempt_id.clone());
        if terminal_counts
            .insert(attempt_key.clone(), ledger_counts)
            .is_some()
            || terminal_states
                .insert(attempt_key, row.state.clone())
                .is_some()
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
        if report.get("source_uri").and_then(|value| value.as_str())
            != Some(manifest_task.source_uri.as_str())
            || report
                .get("source_generation")
                .and_then(|value| value.as_str())
                != Some(manifest_task.source_generation.as_str())
            || report
                .get("source_size_bytes")
                .and_then(|value| value.as_u64())
                != Some(manifest_task.source_size_bytes)
            || report.get("start").and_then(|value| value.as_u64())
                != Some(u64::from(manifest_task.start))
            || report.get("stop").and_then(|value| value.as_u64())
                != Some(u64::from(manifest_task.stop))
            || report.get("published").and_then(|value| value.as_bool()) != Some(false)
            || report_counts != ledger_counts
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
            bail!("attempt {} report is incomplete or inconsistent with its immutable ledger/source identity", row.attempt_id);
        }
        if row.state == "accepted" {
            let expected_inserted = [
                row.summary_rows,
                row.allele_rows,
                row.frequency_rows,
                row.carrier_rows,
                row.rejected_records,
            ]
            .into_iter()
            .try_fold(0u64, |total, value| {
                total
                    .checked_add(value)
                    .context("attempt row total exceeds UInt64")
            })?;
            if report.get("state").and_then(|value| value.as_str()) != Some("accepted")
                || !report
                    .get("failure")
                    .is_some_and(serde_json::Value::is_null)
                || inserted_rows != Some(expected_inserted)
                || (expected_inserted > 0 && inserted_bytes == Some(0))
            {
                bail!(
                    "accepted attempt {} does not contain a complete successful worker result",
                    row.attempt_id
                );
            }
            if accepted
                .insert(row.task_id.clone(), row.attempt_id)
                .is_some()
            {
                bail!("task {} has more than one accepted attempt", row.task_id);
            }
        } else if row.state == "failed" {
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
            failed
                .entry(row.task_id.clone())
                .or_default()
                .push(row.attempt_id);
        } else {
            bail!(
                "attempt ledger has unsupported terminal state {:?}",
                row.state
            );
        }
    }
    for attempts in failed.values_mut() {
        attempts.sort();
    }
    if accepted.len() != tasks.len() {
        bail!(
            "run has {} accepted manifest tasks; expected {}",
            accepted.len(),
            tasks.len()
        );
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
            bail!("controlled fail-once task {} does not prove failed initial and deterministically accepted retry attempts", task.task_id);
        }
    }
    let staging_signatures = validate_physical_attempts(
        tasks,
        &accepted,
        &terminal_counts,
        &terminal_states,
        &physical,
    )?;
    Ok(LedgerCoverage {
        accepted,
        failed,
        staging_signatures,
    })
}

fn read_physical_attempts(
    target: &ClickHouseTarget,
    run_id: &str,
    cohort: &str,
    chrom: &str,
) -> anyhow::Result<Vec<PhysicalAttemptView>> {
    const TABLES: [(&str, &str, &str); 4] = [
        (
            "summaries",
            "tuple(position, source_variant_id)",
            "release,cohort,reference_genome,chrom,position,source_variant_id,ref_allele,alts,allele_type,qual,filters,ac,an,af,allele_lengths,length_provenance,source_allele_length,source_svlen,source_svlen_present,frequencies_json,source_info_json",
        ),
        (
            "alleles",
            "tuple(position, source_variant_id, alt_index)",
            "release,cohort,reference_genome,chrom,position,reference_end,xpos,source_variant_id,alt_index,ref_allele,alt,allele_type,qual,filters,ac,an,af,allele_length,length_provenance,rsids,cadd_phred,phylop,major_consequence,short_read_match_id,short_read_match_type,short_read_match_source",
        ),
        (
            "frequencies",
            "tuple(position, source_variant_id, alt_index, division)",
            "release,cohort,reference_genome,chrom,position,source_variant_id,alt_index,division,ac,an,af,values_available",
        ),
        (
            "carriers",
            "tuple(position, source_variant_id, alt_index, sample_id, genotype_position)",
            "release,cohort,reference_genome,chrom,position,source_variant_id,alt_index,alt,sample_id,genotype_position,gt_alleles,gt_phased,genotype_fields_json,position_fields_json",
        ),
    ];
    let mut result = Vec::new();
    for (table, unique_key, signature_columns) in TABLES {
        let query = format!(
            "SELECT '{table}' AS table, task_id, attempt_id, count() AS rows, \
             uniqExact({unique_key}) AS unique_keys, \
             countIf(release != 'y1' OR cohort != {{cohort:String}} OR reference_genome != 'GRCh38' OR chrom != {{chrom:String}}) AS identity_violations, \
             min(position) AS min_position, max(position) AS max_position, \
             groupBitXor(cityHash64(toJSONString(tuple({signature_columns})))) AS signature \
             FROM lr_y1_{table}_staging WHERE run_id = {{run_id:String}} \
             GROUP BY task_id, attempt_id FORMAT JSONEachRow"
        );
        let body = target.query_text(
            &query,
            &[("run_id", run_id), ("cohort", cohort), ("chrom", chrom)],
        )?;
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            result.push(
                serde_json::from_str(line).context("invalid physical staging aggregate JSON")?,
            );
        }
    }
    let rejects = target.query_text(
        "SELECT 'rejects' AS table, task_id, attempt_id, count() AS rows, count() AS unique_keys, 0 AS identity_violations, 0 AS min_position, 0 AS max_position, groupBitXor(cityHash64(toJSONString(tuple(record_number, source_variant_id, reject_code, message)))) AS signature FROM lr_y1_rejects_staging WHERE run_id = {run_id:String} GROUP BY task_id, attempt_id FORMAT JSONEachRow",
        &[("run_id", run_id)],
    )?;
    for line in rejects.lines().filter(|line| !line.trim().is_empty()) {
        result.push(serde_json::from_str(line).context("invalid reject staging aggregate JSON")?);
    }
    Ok(result)
}

fn validate_physical_attempts(
    tasks: &[PoolY1TaskSpec],
    accepted: &BTreeMap<String, String>,
    terminal_counts: &BTreeMap<(String, String), StagedCounts>,
    terminal_states: &BTreeMap<(String, String), String>,
    physical: &[PhysicalAttemptView],
) -> anyhow::Result<Vec<StagingContentSignature>> {
    const TABLES: [&str; 5] = ["summaries", "alleles", "frequencies", "carriers", "rejects"];
    let task_bounds: BTreeMap<&str, (u32, u32)> = tasks
        .iter()
        .map(|task| (task.task_id.as_str(), (task.start, task.stop)))
        .collect();
    if accepted.len() != tasks.len()
        || tasks
            .iter()
            .any(|task| !accepted.contains_key(&task.task_id))
    {
        bail!("physical acceptance snapshot lacks exactly one accepted attempt per manifest task");
    }
    let accepted_terminal_count = terminal_states
        .values()
        .filter(|state| state.as_str() == "accepted")
        .count();
    if accepted_terminal_count != tasks.len()
        || accepted.iter().any(|(task_id, attempt_id)| {
            terminal_states
                .get(&(task_id.clone(), attempt_id.clone()))
                .map(String::as_str)
                != Some("accepted")
        })
    {
        bail!("terminal ledger contains a missing, duplicate, or inconsistent accepted attempt");
    }
    let mut aggregates = BTreeMap::new();
    for row in physical {
        if !TABLES.contains(&row.table.as_str()) {
            bail!("physical staging snapshot contains an unknown table");
        }
        let attempt_key = (row.task_id.clone(), row.attempt_id.clone());
        if !terminal_counts.contains_key(&attempt_key) {
            bail!(
                "stale or orphan staging contribution for task {}/attempt {}",
                row.task_id,
                row.attempt_id
            );
        }
        let bounds = task_bounds
            .get(row.task_id.as_str())
            .context("physical staging row names a task absent from the manifest")?;
        if row.identity_violations != 0 {
            bail!(
                "physical {} staging contains cross-run/cohort/contig identity violations",
                row.table
            );
        }
        if row.table != "rejects"
            && row.rows != 0
            && (row.min_position < bounds.0 || row.max_position > bounds.1)
        {
            bail!(
                "physical {} staging escapes task {} bounds",
                row.table,
                row.task_id
            );
        }
        if row.unique_keys != row.rows {
            bail!(
                "physical {} staging contains duplicate keys for task {}/attempt {}",
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
        if aggregates.insert(key, row).is_some() {
            bail!("physical staging snapshot contains a duplicate attempt aggregate");
        }
    }

    let mut accepted_rows = BTreeMap::new();
    let mut accepted_signatures = BTreeMap::new();
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
        for (table, expected) in TABLES.into_iter().zip(expected_rows) {
            let physical = aggregates.get(&(
                table.to_string(),
                attempt_key.0.clone(),
                attempt_key.1.clone(),
            ));
            let rows = physical.map_or(0, |row| row.rows);
            if rows != expected {
                bail!(
                    "physical {table} rows disagree with terminal ledger for task {}/attempt {}",
                    attempt_key.0,
                    attempt_key.1
                );
            }
            if accepted.get(&attempt_key.0) == Some(&attempt_key.1) {
                let total = accepted_rows.entry(table).or_insert(0u64);
                *total = total
                    .checked_add(rows)
                    .context("accepted staging row total exceeds UInt64")?;
                *accepted_signatures.entry(table).or_insert(0u64) ^=
                    physical.map_or(0, |row| row.signature);
            }
        }
        if terminal_states.get(attempt_key).map(String::as_str) == Some("accepted")
            && counts.rejects != 0
        {
            bail!("accepted terminal attempt contains rejected records");
        }
    }

    Ok(TABLES
        .into_iter()
        .map(|table| StagingContentSignature {
            table: table.to_string(),
            rows: accepted_rows.get(table).copied().unwrap_or(0),
            signature: accepted_signatures.get(table).copied().unwrap_or(0),
        })
        .collect())
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

fn parse_cohort(value: &str) -> anyhow::Result<Cohort> {
    match value {
        "hgsvc_hprc" => Ok(Cohort::HgsvcHprc),
        "aou" => Ok(Cohort::Aou),
        _ => bail!("unsupported cohort"),
    }
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
            source_checksum: "x".into(),
            source_size_bytes: 1,
            source_index_uri: format!(
                "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.{chrom}.vcf.gz.tbi"
            ),
            source_index_generation: "2".into(),
            source_index_checksum_algorithm: "md5_base64".into(),
            source_index_checksum: "y".into(),
            retry_attempt_id: None,
            controlled_fail_once: None,
        }
    }

    #[test]
    fn manifest_requires_exact_per_contig_adjacency_and_full_coverage() {
        for chrom in (1..=22)
            .map(|number| format!("chr{number}"))
            .chain(["chrX".to_string(), "chrY".to_string()])
        {
            let length = grch38_contig_length(&chrom).unwrap();
            assert!(
                validate_manifest(&[task_for(&chrom, 0, 1, length)]).is_ok(),
                "{chrom}"
            );
            assert!(
                validate_manifest(&[task_for(&chrom, 0, 1, length - 1)]).is_err(),
                "{chrom}"
            );
            assert!(
                validate_manifest(&[task_for(&chrom, 0, 1, 10), task_for(&chrom, 1, 12, length),])
                    .is_err(),
                "{chrom}"
            );
        }
        assert!(validate_manifest(&[task_for("chrM", 0, 1, 16_569)]).is_err());
    }

    #[test]
    fn manifest_cannot_mix_contigs() {
        let mut second = task_for("chr2", 1, 11, grch38_contig_length("chr2").unwrap());
        second.run_id = "run".into();
        assert!(validate_manifest(&[task_for("chr1", 0, 1, 10), second]).is_err());
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

    fn snapshot() -> (
        Vec<PoolY1TaskSpec>,
        BTreeMap<String, String>,
        BTreeMap<(String, String), StagedCounts>,
        BTreeMap<(String, String), String>,
        Vec<PhysicalAttemptView>,
    ) {
        let length = grch38_contig_length("chr22").unwrap();
        let task = task_for("chr22", 0, 1, length);
        let accepted_id = "accepted".to_string();
        let failed_id = "failed".to_string();
        let counts = StagedCounts {
            source_records: 1,
            summaries: 1,
            alleles: 2,
            frequencies: 3,
            carriers: 0,
            rejects: 0,
        };
        let accepted = BTreeMap::from([(task.task_id.clone(), accepted_id.clone())]);
        let terminal_counts = BTreeMap::from([
            ((task.task_id.clone(), accepted_id.clone()), counts),
            ((task.task_id.clone(), failed_id.clone()), counts),
        ]);
        let terminal_states = BTreeMap::from([
            (
                (task.task_id.clone(), accepted_id.clone()),
                "accepted".to_string(),
            ),
            (
                (task.task_id.clone(), failed_id.clone()),
                "failed".to_string(),
            ),
        ]);
        let mut physical = Vec::new();
        for attempt in [&accepted_id, &failed_id] {
            for (table, rows, signature) in [
                ("summaries", 1, 11),
                ("alleles", 2, 22),
                ("frequencies", 3, 33),
                ("carriers", 0, 0),
                ("rejects", 0, 0),
            ] {
                if rows != 0 {
                    physical.push(PhysicalAttemptView {
                        table: table.to_string(),
                        task_id: task.task_id.clone(),
                        attempt_id: attempt.clone(),
                        rows,
                        unique_keys: rows,
                        identity_violations: 0,
                        min_position: 1,
                        max_position: 1,
                        signature,
                    });
                }
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
    fn independent_reconciliation_rejects_malformed_hashes_and_source_drops() {
        let task = task_for("chr22", 0, 1, grch38_contig_length("chr22").unwrap());
        let valid = independent_for(&task);
        assert!(validate_independent_reconciliation(&valid, &task).is_ok());

        let mut malformed = independent_for(&task);
        malformed.facts.source_content_sha256 = "not-a-sha256".into();
        assert!(validate_independent_reconciliation(&malformed, &task).is_err());

        let mut dropped = independent_for(&task);
        dropped.facts.source_records = 2;
        assert!(validate_independent_reconciliation(&dropped, &task).is_err());

        let mut cross_source = independent_for(&task);
        cross_source.source_generation = "other-generation".into();
        assert!(validate_independent_reconciliation(&cross_source, &task).is_err());

        let mut encoded = serde_json::to_value(independent_for(&task)).unwrap();
        encoded.as_object_mut().unwrap().insert(
            "unexpected".into(),
            serde_json::Value::String("field".into()),
        );
        assert!(serde_json::from_value::<IndependentExpectedCounts>(encoded).is_err());
    }

    #[test]
    fn physical_acceptance_accounts_for_failed_retry_and_is_deterministic() {
        let (tasks, accepted, counts, states, mut physical) = snapshot();
        let first =
            validate_physical_attempts(&tasks, &accepted, &counts, &states, &physical).unwrap();
        physical.reverse();
        let second =
            validate_physical_attempts(&tasks, &accepted, &counts, &states, &physical).unwrap();
        assert_eq!(first, second);
        assert_eq!(first[0].rows, 1);
        assert_eq!(first[0].signature, 11);
    }

    #[test]
    fn physical_acceptance_rejects_missing_duplicate_and_stale_attempts() {
        let (tasks, accepted, counts, states, physical) = snapshot();
        assert!(
            validate_physical_attempts(&tasks, &BTreeMap::new(), &counts, &states, &physical)
                .is_err()
        );

        let mut duplicate_counts = counts.clone();
        let mut duplicate_states = states.clone();
        duplicate_counts.insert(
            (tasks[0].task_id.clone(), "second-accepted".into()),
            StagedCounts::default(),
        );
        duplicate_states.insert(
            (tasks[0].task_id.clone(), "second-accepted".into()),
            "accepted".into(),
        );
        assert!(validate_physical_attempts(
            &tasks,
            &accepted,
            &duplicate_counts,
            &duplicate_states,
            &physical
        )
        .is_err());

        let mut stale = physical.clone();
        let mut orphan = stale[0].clone();
        orphan.attempt_id = "orphan".into();
        stale.push(orphan);
        assert!(validate_physical_attempts(&tasks, &accepted, &counts, &states, &stale).is_err());
    }

    #[test]
    fn physical_acceptance_rejects_cross_identity_count_and_signature_changes() {
        let (tasks, accepted, counts, states, physical) = snapshot();
        let mut cross_identity = physical.clone();
        cross_identity[0].identity_violations = 1;
        assert!(
            validate_physical_attempts(&tasks, &accepted, &counts, &states, &cross_identity)
                .is_err()
        );

        let mut wrong_count = physical.clone();
        wrong_count[0].rows = 2;
        wrong_count[0].unique_keys = 2;
        assert!(
            validate_physical_attempts(&tasks, &accepted, &counts, &states, &wrong_count).is_err()
        );

        let baseline =
            validate_physical_attempts(&tasks, &accepted, &counts, &states, &physical).unwrap();
        let mut changed = physical;
        changed[0].signature ^= 1;
        let changed =
            validate_physical_attempts(&tasks, &accepted, &counts, &states, &changed).unwrap();
        assert_ne!(baseline, changed);
    }

    #[test]
    fn local_clickhouse_physical_snapshot_queries_compile() {
        let Ok(endpoint) = std::env::var("GNOMAD_LR_Y1_TEST_ENDPOINT") else {
            return;
        };
        let database = std::env::var("GNOMAD_LR_Y1_TEST_DATABASE")
            .unwrap_or_else(|_| "gnomad_lr_y1_scratch_v4_ci".to_string());
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
        let snapshot = read_physical_attempts(&target, "absent-run", "aou", "chr22").unwrap();
        assert!(snapshot.is_empty());
    }

    #[test]
    fn future_finalization_requires_revision_bound_worker_provenance() {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let valid = serde_json::json!({
            "worker_identity": "worker-7",
            "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
            "backend_revision": revision,
        });
        assert!(validate_worker_provenance(&valid, "attempt-7").is_ok());

        for invalid in [
            serde_json::json!({
                "worker_identity": "unknown-worker",
                "worker_build_version": format!("gnomad-lr/{revision}/x86_64-linux-release"),
                "backend_revision": revision,
            }),
            serde_json::json!({
                "worker_identity": "worker-7",
                "worker_build_version": "0.1.0",
                "backend_revision": "unknown",
            }),
            serde_json::json!({
                "worker_identity": "worker-7",
                "worker_build_version": "gnomad-lr/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/release",
                "backend_revision": revision,
            }),
        ] {
            assert!(validate_worker_provenance(&invalid, "attempt-7").is_err());
        }
    }
}
