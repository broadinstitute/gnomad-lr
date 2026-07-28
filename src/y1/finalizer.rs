use super::{
    publish_staged_run, record_load_run, ClickHouseTarget, Cohort, LoadRunLedgerRow, LoadScope,
    PoolY1TaskSpec, PublicationRequest, ReferenceGenome, Release, StagedCounts, TargetKind,
    Y1_SCHEMA_VERSION,
};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const CHR22_LENGTH: u32 = 50_818_468;

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
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

struct LedgerCoverage {
    accepted: BTreeMap<String, String>,
    failed: BTreeMap<String, Vec<String>>,
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
    pub published: bool,
}

pub fn finalize_chr22_run(
    target: &ClickHouseTarget,
    manifest_path: &Path,
    expected_path: &Path,
    operator_identity: &str,
) -> anyhow::Result<FinalizationReport> {
    if target.kind() != TargetKind::Scratch {
        bail!("chr22 candidate finalization is restricted to an isolated scratch target");
    }
    if operator_identity.trim().is_empty() {
        bail!("finalization requires a non-empty operator identity");
    }
    let manifest_bytes = std::fs::read(manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let tasks: Vec<PoolY1TaskSpec> = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("invalid task manifest {}", manifest_path.display()))?;
    let run = validate_manifest(&tasks)?;

    let expected_bytes = std::fs::read(expected_path)
        .with_context(|| format!("failed to read {}", expected_path.display()))?;
    let expected: IndependentExpectedCounts = serde_json::from_slice(&expected_bytes)
        .with_context(|| format!("invalid independent counts {}", expected_path.display()))?;
    if expected.contract_version != 1
        || expected.run_id != run.run_id
        || expected.cohort != run.cohort
        || expected.chrom != "chr22"
        || expected.source_generation != run.source_generation
        || expected.source_checksum != run.source_checksum
    {
        bail!("independent reconciliation identity does not match the manifest run/source");
    }
    if expected.evidence_uri.trim().is_empty()
        || expected.producer.trim().is_empty()
        || expected.counts.source_records == 0
        || expected.facts.source_records != expected.counts.source_records
        || expected.facts.alt_alleles != expected.counts.alleles
        || expected.facts.frequency_rows != expected.counts.frequencies
        || expected.facts.carrier_alt_copies != expected.counts.carriers
        || (run.cohort == "hgsvc_hprc" && expected.facts.called_alleles == 0)
        || expected.facts.source_content_sha256.len() != 64
        || expected.facts.genotype_content_sha256.len() != 64
        || expected.facts.annotation_content_sha256.len() != 64
    {
        bail!(
            "independent reconciliation facts are incomplete or inconsistent with expected counts"
        );
    }
    if expected.counts.rejects != 0 || expected.counts.summaries != expected.counts.source_records {
        bail!("independent counts require zero rejects and one summary per source record");
    }
    if run.cohort == "aou" && expected.counts.carriers != 0 {
        bail!("AoU independent counts must contain zero carriers");
    }

    let ledger = validate_ledger_coverage(target, &run.run_id, &tasks)?;
    let cohort = parse_cohort(&run.cohort)?;
    let request = PublicationRequest {
        run_id: run.run_id.clone(),
        scope: LoadScope::FullChromosome,
        release: Release::Y1,
        cohort,
        reference_genome: ReferenceGenome::Grch38,
        chrom: "chr22".to_string(),
        interval_start: 1,
        interval_end: CHR22_LENGTH,
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
    if let Err(error) = publish_staged_run(target, &request) {
        let _ = record_state(
            target,
            &run,
            &expected,
            tasks.len(),
            revision_now()?,
            "finalization_failed",
            &format!("{operator_identity}: {error:#}"),
        );
        return Err(error.context("guarded full-chr22 publication failed"));
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
        chrom: "chr22".to_string(),
        operator_identity: operator_identity.to_string(),
        manifest_tasks: tasks.len(),
        manifest_sha256: format!("{:x}", Sha256::digest(&manifest_bytes)),
        accepted_attempts: ledger.accepted,
        failed_attempts: ledger.failed,
        expected_counts: expected.counts,
        independent_evidence_uri: expected.evidence_uri,
        independent_counts_sha256: format!("{:x}", Sha256::digest(&expected_bytes)),
        published: true,
    })
}

fn validate_manifest(tasks: &[PoolY1TaskSpec]) -> anyhow::Result<PoolY1TaskSpec> {
    let Some(first) = tasks.first() else {
        bail!("task manifest must not be empty")
    };
    let mut previous_stop = 0;
    let mut task_ids = BTreeSet::new();
    for (index, task) in tasks.iter().enumerate() {
        task.validate(&format!("custom_{index}"))?;
        if task.run_id != first.run_id
            || task.cohort != first.cohort
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
    if previous_stop != CHR22_LENGTH {
        bail!("task manifest must cover chr22:1-{CHR22_LENGTH} exactly");
    }
    Ok(first.clone())
}

fn validate_ledger_coverage(
    target: &ClickHouseTarget,
    run_id: &str,
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
    let expected: BTreeMap<&str, (u32, u32)> = tasks
        .iter()
        .map(|task| (task.task_id.as_str(), (task.start, task.stop)))
        .collect();
    let mut accepted = BTreeMap::new();
    let mut failed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in body.lines().filter(|line| !line.trim().is_empty()) {
        let row: AttemptView = serde_json::from_str(line).context("invalid attempt ledger JSON")?;
        let Some(bounds) = expected.get(row.task_id.as_str()) else {
            bail!(
                "run ledger contains task {} absent from the checked manifest",
                row.task_id
            );
        };
        if row.chrom != "chr22" || (row.interval_start, row.interval_end) != *bounds {
            bail!(
                "attempt {} has bounds inconsistent with its manifest task",
                row.attempt_id
            );
        }
        let report: serde_json::Value = serde_json::from_str(&row.report_json)
            .context("invalid durable attempt report JSON")?;
        for (field, expected_value) in [
            ("run_id", run_id),
            ("task_id", row.task_id.as_str()),
            ("attempt_id", row.attempt_id.as_str()),
        ] {
            if report.get(field).and_then(|value| value.as_str()) != Some(expected_value) {
                bail!("attempt {} report has inconsistent {field}", row.attempt_id);
            }
        }
        let manifest_task = tasks
            .iter()
            .find(|task| task.task_id == row.task_id)
            .expect("task was checked against the manifest above");
        let ledger_counts = StagedCounts {
            source_records: row.source_records,
            summaries: row.summary_rows,
            alleles: row.allele_rows,
            frequencies: row.frequency_rows,
            carriers: row.carrier_rows,
            rejects: row.rejected_records,
        };
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
            || [
                "worker_identity",
                "worker_build_version",
                "backend_revision",
            ]
            .iter()
            .any(|field| {
                report
                    .get(field)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .is_empty()
            })
        {
            bail!("attempt {} report is incomplete or inconsistent with its immutable ledger/source identity", row.attempt_id);
        }
        if row.state == "accepted" {
            let expected_inserted = row.summary_rows
                + row.allele_rows
                + row.frequency_rows
                + row.carrier_rows
                + row.rejected_records;
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
        if failures != [task.attempt_id.as_str()]
            || accepted.get(&task.task_id) != task.retry_attempt_id.as_ref()
        {
            bail!("controlled fail-once task {} does not prove failed initial and deterministically accepted retry attempts", task.task_id);
        }
    }
    Ok(LedgerCoverage { accepted, failed })
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
            chrom: "chr22".to_string(),
            interval_start: 1,
            interval_end: CHR22_LENGTH,
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

    fn task(index: usize, start: u32, stop: u32) -> PoolY1TaskSpec {
        PoolY1TaskSpec {
            coordinator_task_id: format!("custom_{index}"),
            label: "test".into(),
            run_id: "run".into(),
            task_id: format!("task-{index}"),
            attempt_id: format!("attempt-{index}"),
            release: "y1".into(),
            cohort: "aou".into(),
            reference_genome: "GRCh38".into(),
            chrom: "chr22".into(),
            start,
            stop,
            source_uri: "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.chr22.vcf.gz"
                .into(),
            source_generation: "1".into(),
            source_checksum_algorithm: "md5_base64".into(),
            source_checksum: "x".into(),
            source_size_bytes: 1,
            source_index_uri:
                "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.chr22.vcf.gz.tbi".into(),
            source_index_generation: "2".into(),
            source_index_checksum_algorithm: "md5_base64".into(),
            source_index_checksum: "y".into(),
            retry_attempt_id: None,
            controlled_fail_once: None,
        }
    }

    #[test]
    fn manifest_requires_exact_adjacency_and_full_coverage() {
        assert!(validate_manifest(&[task(0, 1, CHR22_LENGTH)]).is_ok());
        assert!(validate_manifest(&[task(0, 1, CHR22_LENGTH - 1)]).is_err());
        assert!(validate_manifest(&[task(0, 1, 10), task(1, 12, CHR22_LENGTH)]).is_err());
    }
}
