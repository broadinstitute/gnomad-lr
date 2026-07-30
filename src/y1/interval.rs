use super::contig::{canonical_y1_mirror_uri, grch38_contig_length};
use super::storage::ensure_run_accepts_primary_writes;
use super::{
    record_task_attempt, stage_attempt_tracked, AttemptContext, AttemptState, ClickHouseTarget,
    Cohort, InsertStats, StagedCounts, TaskAttemptLedgerRow, TransformationReport, Y1Header,
};
use crate::loader::vcf_reader::{read_header_text, VcfStream};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolY1TargetSpec {
    pub endpoint: String,
    pub database: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolY1JobSpec {
    pub action: String,
    pub target: PoolY1TargetSpec,
    #[serde(default = "default_batch_records")]
    pub batch_records: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PoolY1TaskSpec {
    pub coordinator_task_id: String,
    pub label: String,
    pub run_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub release: String,
    pub cohort: String,
    pub reference_genome: String,
    pub chrom: String,
    pub start: u32,
    pub stop: u32,
    pub source_uri: String,
    pub source_generation: String,
    pub source_checksum_algorithm: String,
    pub source_checksum: String,
    pub source_size_bytes: u64,
    pub source_index_uri: String,
    pub source_index_generation: String,
    pub source_index_checksum_algorithm: String,
    pub source_index_checksum: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controlled_fail_once: Option<ControlledFailOnce>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlledFailOnce {
    pub mode: String,
    pub evidence_token: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StructuredAttemptFailure {
    pub code: String,
    pub phase: String,
    pub message: String,
    pub controlled: bool,
    pub evidence_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PoolY1AttemptReport {
    pub run_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub cohort: Cohort,
    pub chrom: String,
    pub start: u32,
    pub stop: u32,
    pub source_uri: String,
    pub source_generation: String,
    pub source_size_bytes: u64,
    pub counts: StagedCounts,
    pub transformation: TransformationReport,
    pub inserted: InsertStats,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    pub elapsed_ms: u128,
    pub parse_transform_insert_ms: u128,
    pub linux_peak_rss_bytes: Option<u64>,
    pub worker_identity: String,
    pub worker_build_version: String,
    pub backend_revision: String,
    pub state: String,
    pub failure: Option<StructuredAttemptFailure>,
    pub published: bool,
}

fn default_batch_records() -> usize {
    250
}

impl PoolY1JobSpec {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.action != "load_y1_interval" {
            bail!("strict Y1 job action must be load_y1_interval");
        }
        if self.batch_records == 0 {
            bail!("batch_records must be greater than zero");
        }
        Ok(())
    }
}

impl PoolY1TaskSpec {
    pub fn validate(&self, descriptor_id: &str) -> anyhow::Result<()> {
        if self.coordinator_task_id != descriptor_id {
            bail!("descriptor ID must exactly match manifest coordinator_task_id");
        }
        if self.task_id.is_empty() || self.label.is_empty() {
            bail!("manifest task_id and label must not be empty");
        }
        if !matches!(self.cohort.as_str(), "hgsvc_hprc" | "aou") {
            bail!("cohort must be hgsvc_hprc or aou");
        }
        if self.release != "y1" || self.reference_genome != "GRCh38" {
            bail!("pool Y1 tasks are restricted to release y1 and GRCh38");
        }
        let contig_length = grch38_contig_length(&self.chrom)?;
        if self.start == 0 || self.start > self.stop || self.stop > contig_length {
            bail!(
                "task bounds must be a non-empty one-based inclusive {} interval ending by {contig_length}",
                self.chrom
            );
        }
        let expected_source_uri = canonical_y1_mirror_uri(&self.cohort, &self.chrom)?;
        let expected_index_uri = format!("{expected_source_uri}.tbi");
        if self.source_uri != expected_source_uri || self.source_index_uri != expected_index_uri {
            bail!("task source and index must exactly equal the declared immutable Y1 cohort/contig mirror identities");
        }
        if self.source_generation.is_empty()
            || self.source_checksum.is_empty()
            || self.source_index_generation.is_empty()
            || self.source_index_checksum.is_empty()
            || self.source_size_bytes == 0
        {
            bail!("task source identity must be complete and immutable");
        }
        if self.source_checksum_algorithm != "md5_base64"
            || self.source_index_checksum_algorithm != "md5_base64"
        {
            bail!("only checked md5_base64 source identities are accepted");
        }
        match (&self.controlled_fail_once, &self.retry_attempt_id) {
            (Some(injection), Some(retry_id)) => {
                if injection.mode != "after_first_staged_batch"
                    || injection.evidence_token.trim().is_empty()
                {
                    bail!("controlled fail-once requires mode after_first_staged_batch and a non-empty evidence token");
                }
                if retry_id.is_empty() || retry_id == &self.attempt_id {
                    bail!("controlled fail-once requires a distinct non-empty retry_attempt_id");
                }
            }
            (None, None) => {}
            _ => bail!("controlled_fail_once and retry_attempt_id must be supplied together"),
        }
        Ok(())
    }
}

pub fn run_pool_interval_attempt(
    target: &ClickHouseTarget,
    task: &PoolY1TaskSpec,
    batch_records: usize,
    worker_identity: &str,
    worker_build_version: &str,
    backend_revision: &str,
) -> anyhow::Result<PoolY1AttemptReport> {
    task.validate(&task.coordinator_task_id)?;
    if target.kind() != super::TargetKind::Scratch {
        bail!("pool interval attempts may write only to a scratch target");
    }
    if batch_records == 0 {
        bail!("batch_records must be greater than zero");
    }

    ensure_run_accepts_primary_writes(target, &task.run_id)?;
    let (attempt_id, inject_failure) = select_attempt(target, task)?;
    let cohort = match task.cohort.as_str() {
        "hgsvc_hprc" => Cohort::HgsvcHprc,
        "aou" => Cohort::Aou,
        _ => bail!("unsupported Y1 cohort"),
    };
    let context = AttemptContext {
        run_id: task.run_id.clone(),
        task_id: task.task_id.clone(),
        attempt_id,
        cohort,
        chrom: task.chrom.clone(),
        interval_start: task.start,
        interval_end: task.stop,
    };
    context.validate()?;
    let claim_revision = claim_attempt(
        target,
        task,
        &context,
        worker_identity,
        worker_build_version,
        backend_revision,
    )?;

    let started_at_revision = revision_now()?;
    let started = Instant::now();
    let phase_started = Instant::now();
    let mut total_counts = StagedCounts::default();
    let mut total_report = TransformationReport::default();
    let mut inserted = InsertStats::default();
    let mut injected = false;

    let execution = (|| -> anyhow::Result<()> {
        let header_text =
            read_header_text(&task.source_uri).context("failed to read pinned Y1 header")?;
        let header = Y1Header::parse(&header_text, cohort)?;
        if header.reference_genome.as_str() != task.reference_genome {
            bail!("source header reference does not match manifest reference_genome");
        }

        let mut record_offset = 0usize;
        let mut record_batch = Vec::with_capacity(batch_records);
        let records = VcfStream::open_region_required_index(
            &task.source_uri,
            &task.chrom,
            task.start,
            task.stop,
        )?
        .records();
        let mut staged_batches = 0usize;

        for record in records {
            record_batch.push(record?);
            if record_batch.len() == batch_records {
                stage_batch(
                    target,
                    &context,
                    claim_revision,
                    &header,
                    &mut record_batch,
                    &mut record_offset,
                    &mut total_counts,
                    &mut total_report,
                    &mut inserted,
                )?;
                staged_batches += 1;
                if inject_failure && staged_batches == 1 {
                    injected = true;
                    bail!("controlled fail-once after first acknowledged ClickHouse staging batch");
                }
            }
        }
        if !record_batch.is_empty() {
            stage_batch(
                target,
                &context,
                claim_revision,
                &header,
                &mut record_batch,
                &mut record_offset,
                &mut total_counts,
                &mut total_report,
                &mut inserted,
            )?;
            if inject_failure {
                injected = true;
                bail!("controlled fail-once after first acknowledged ClickHouse staging batch");
            }
        }
        if inject_failure {
            bail!("controlled fail-once task contained no source records; injection was not exercised");
        }
        if total_counts.rejects != 0 || total_counts.summaries != total_counts.source_records {
            bail!("transformation validation failed");
        }
        Ok(())
    })();

    // A concurrent claimant must never be overwritten by this execution's terminal row.
    // This also fences a worker that resumes after the coordinator has reassigned its task.
    ensure_attempt_claim(target, &context, claim_revision)
        .context("Y1 attempt lost its staging claim; refusing to record a terminal result")?;

    let failure = execution
        .as_ref()
        .err()
        .map(|error| StructuredAttemptFailure {
            code: if injected {
                "controlled_fail_once".to_string()
            } else {
                "attempt_execution_failed".to_string()
            },
            phase: if inserted.requests == 0 {
                "source_or_transform".to_string()
            } else {
                "parse_transform_insert".to_string()
            },
            message: format!("{error:#}"),
            controlled: injected,
            evidence_token: injected.then(|| {
                task.controlled_fail_once
                    .as_ref()
                    .unwrap()
                    .evidence_token
                    .clone()
            }),
        });
    let accepted = failure.is_none();
    let finished_revision = revision_now()?.max(
        claim_revision
            .checked_add(1)
            .context("Y1 attempt claim revision exhausted UInt64")?,
    );
    let report = PoolY1AttemptReport {
        run_id: task.run_id.clone(),
        task_id: task.task_id.clone(),
        attempt_id: context.attempt_id.clone(),
        cohort,
        chrom: task.chrom.clone(),
        start: task.start,
        stop: task.stop,
        source_uri: task.source_uri.clone(),
        source_generation: task.source_generation.clone(),
        source_size_bytes: task.source_size_bytes,
        counts: total_counts,
        transformation: total_report.clone(),
        inserted,
        started_at_ms: started_at_revision / 1_000_000,
        finished_at_ms: finished_revision / 1_000_000,
        elapsed_ms: started.elapsed().as_millis(),
        parse_transform_insert_ms: phase_started.elapsed().as_millis(),
        linux_peak_rss_bytes: linux_peak_rss_bytes(),
        worker_identity: worker_identity.to_string(),
        worker_build_version: worker_build_version.to_string(),
        backend_revision: backend_revision.to_string(),
        state: if accepted { "accepted" } else { "failed" }.to_string(),
        failure,
        published: false,
    };
    let error_text = report
        .failure
        .as_ref()
        .map(|failure| failure.message.as_str())
        .unwrap_or("");
    let mut ledger = TaskAttemptLedgerRow::new(
        &context,
        finished_revision,
        if accepted {
            AttemptState::Accepted
        } else {
            AttemptState::Failed
        },
        total_counts,
        &total_report,
        error_text,
    )?;
    ledger.started_at_ms = report.started_at_ms;
    ledger.updated_at_ms = report.finished_at_ms;
    ledger.report_json = serde_json::to_string(&report)?;
    record_task_attempt(target, &ledger)
        .context("failed to durably record complete Y1 attempt result")?;
    if let Err(error) = execution {
        return Err(error.context(format!(
            "Y1 attempt {} failed after its immutable result was recorded",
            context.attempt_id
        )));
    }
    Ok(report)
}

fn select_attempt(
    target: &ClickHouseTarget,
    task: &PoolY1TaskSpec,
) -> anyhow::Result<(String, bool)> {
    let Some(retry_id) = &task.retry_attempt_id else {
        if latest_attempt_state(target, &task.run_id, &task.task_id, &task.attempt_id)?.is_some() {
            bail!("attempt {} already has an immutable ledger result; retry requires a new attempt ID", task.attempt_id);
        }
        return Ok((task.attempt_id.clone(), false));
    };
    match latest_attempt_state(target, &task.run_id, &task.task_id, &task.attempt_id)?.as_deref() {
        None => Ok((task.attempt_id.clone(), true)),
        Some("failed") => {
            if latest_attempt_state(target, &task.run_id, &task.task_id, retry_id)?.is_some() {
                bail!("controlled retry attempt {retry_id} already has an immutable ledger result");
            }
            Ok((retry_id.clone(), false))
        }
        Some(state) => {
            bail!("controlled initial attempt already ended in state {state:?}; refusing retry")
        }
    }
}

#[derive(Debug, Deserialize)]
struct LatestAttempt {
    state: String,
    revision: u64,
}

fn latest_attempt(
    target: &ClickHouseTarget,
    run_id: &str,
    task_id: &str,
    attempt_id: &str,
) -> anyhow::Result<Option<LatestAttempt>> {
    let query = r#"
SELECT state, revision
FROM lr_y1_task_attempts
WHERE run_id = {run_id:String} AND task_id = {task_id:String} AND attempt_id = {attempt_id:String}
ORDER BY revision DESC
LIMIT 1
FORMAT JSONEachRow
"#;
    let body = target.query_text(
        query,
        &[
            ("run_id", run_id),
            ("task_id", task_id),
            ("attempt_id", attempt_id),
        ],
    )?;
    if body.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            serde_json::from_str(body.trim()).context("invalid latest Y1 attempt claim row")?,
        ))
    }
}

fn latest_attempt_state(
    target: &ClickHouseTarget,
    run_id: &str,
    task_id: &str,
    attempt_id: &str,
) -> anyhow::Result<Option<String>> {
    Ok(latest_attempt(target, run_id, task_id, attempt_id)?.map(|row| row.state))
}

fn staged_attempt_rows(target: &ClickHouseTarget, context: &AttemptContext) -> anyhow::Result<u64> {
    let query = r#"
SELECT sum(rows)
FROM (
    SELECT count() AS rows FROM lr_y1_summaries WHERE run_id = {run_id:String} AND task_id = {task_id:String} AND attempt_id = {attempt_id:String}
    UNION ALL
    SELECT count() AS rows FROM lr_y1_alleles WHERE run_id = {run_id:String} AND task_id = {task_id:String} AND attempt_id = {attempt_id:String}
    UNION ALL
    SELECT count() AS rows FROM lr_y1_frequencies WHERE run_id = {run_id:String} AND task_id = {task_id:String} AND attempt_id = {attempt_id:String}
    UNION ALL
    SELECT count() AS rows FROM lr_y1_carriers WHERE run_id = {run_id:String} AND task_id = {task_id:String} AND attempt_id = {attempt_id:String}
    UNION ALL
    SELECT count() AS rows FROM lr_y1_rejects_staging WHERE run_id = {run_id:String} AND task_id = {task_id:String} AND attempt_id = {attempt_id:String}
)
FORMAT TabSeparated
"#;
    let body = target.query_text(
        query,
        &[
            ("run_id", &context.run_id),
            ("task_id", &context.task_id),
            ("attempt_id", &context.attempt_id),
        ],
    )?;
    body.trim()
        .parse()
        .context("invalid Y1 staged-attempt row count")
}

fn claim_attempt(
    target: &ClickHouseTarget,
    task: &PoolY1TaskSpec,
    context: &AttemptContext,
    worker_identity: &str,
    worker_build_version: &str,
    backend_revision: &str,
) -> anyhow::Result<u64> {
    if let Some(existing) = latest_attempt(
        target,
        &context.run_id,
        &context.task_id,
        &context.attempt_id,
    )? {
        bail!(
            "attempt {} already has immutable state {:?}; generic requeue must use a new attempt ID",
            context.attempt_id,
            existing.state
        );
    }
    let existing_rows = staged_attempt_rows(target, context)?;
    if existing_rows != 0 {
        bail!(
            "attempt {} has {existing_rows} orphaned staging rows but no ledger claim; refusing a same-ID requeue",
            context.attempt_id
        );
    }

    let revision = revision_now()?;
    let claim_report = PoolY1AttemptReport {
        run_id: context.run_id.clone(),
        task_id: context.task_id.clone(),
        attempt_id: context.attempt_id.clone(),
        cohort: context.cohort,
        chrom: context.chrom.clone(),
        start: context.interval_start,
        stop: context.interval_end,
        source_uri: task.source_uri.clone(),
        source_generation: task.source_generation.clone(),
        source_size_bytes: task.source_size_bytes,
        counts: StagedCounts::default(),
        transformation: TransformationReport::default(),
        inserted: InsertStats::default(),
        started_at_ms: revision / 1_000_000,
        finished_at_ms: revision / 1_000_000,
        elapsed_ms: 0,
        parse_transform_insert_ms: 0,
        linux_peak_rss_bytes: linux_peak_rss_bytes(),
        worker_identity: worker_identity.to_string(),
        worker_build_version: worker_build_version.to_string(),
        backend_revision: backend_revision.to_string(),
        state: "running".to_string(),
        failure: None,
        published: false,
    };
    let mut claim = TaskAttemptLedgerRow::new(
        context,
        revision,
        AttemptState::Running,
        StagedCounts::default(),
        &TransformationReport::default(),
        "attempt claimed before staging",
    )?;
    claim.started_at_ms = claim_report.started_at_ms;
    claim.updated_at_ms = claim_report.finished_at_ms;
    claim.report_json = serde_json::to_string(&claim_report)?;
    record_task_attempt(target, &claim).context("failed to claim Y1 attempt before staging")?;
    ensure_attempt_claim(target, context, revision)?;
    ensure_run_accepts_primary_writes(target, &context.run_id)?;

    // Close the legacy gap where a pre-claim worker may still publish after our first check.
    let raced_rows = staged_attempt_rows(target, context)?;
    if raced_rows != 0 {
        bail!(
            "attempt {} acquired a claim but found {raced_rows} concurrently staged rows; refusing to continue",
            context.attempt_id
        );
    }
    Ok(revision)
}

fn ensure_attempt_claim(
    target: &ClickHouseTarget,
    context: &AttemptContext,
    claim_revision: u64,
) -> anyhow::Result<()> {
    let latest = latest_attempt(
        target,
        &context.run_id,
        &context.task_id,
        &context.attempt_id,
    )?
    .context("Y1 attempt claim disappeared")?;
    validate_claim_snapshot(&latest, claim_revision)
}

fn validate_claim_snapshot(latest: &LatestAttempt, claim_revision: u64) -> anyhow::Result<()> {
    if latest.state != "running" || latest.revision != claim_revision {
        bail!(
            "Y1 attempt claim is no longer current (state {:?}, revision {})",
            latest.state,
            latest.revision
        );
    }
    Ok(())
}

fn stage_batch(
    target: &ClickHouseTarget,
    context: &AttemptContext,
    claim_revision: u64,
    header: &Y1Header,
    records: &mut Vec<String>,
    record_offset: &mut usize,
    total_counts: &mut StagedCounts,
    total_report: &mut TransformationReport,
    inserted: &mut InsertStats,
) -> anyhow::Result<()> {
    ensure_attempt_claim(target, context, claim_revision)?;
    let mut batch = super::transform_records(header, records.iter().map(String::as_str));
    for reject in &mut batch.report.rejects {
        if let Some(record_number) = &mut reject.record_number {
            *record_number += *record_offset;
        }
    }
    let counts = stage_attempt_tracked(target, context, &batch, inserted)?;
    total_counts.source_records += counts.source_records;
    total_counts.summaries += counts.summaries;
    total_counts.alleles += counts.alleles;
    total_counts.frequencies += counts.frequencies;
    total_counts.carriers += counts.carriers;
    total_counts.rejects += counts.rejects;

    total_report.source_records += batch.report.source_records;
    total_report.summary_rows += batch.report.summary_rows;
    total_report.carrier_rows += batch.report.carrier_rows;
    total_report.genotype_calls += batch.report.genotype_calls;
    total_report.missing_genotypes += batch.report.missing_genotypes;
    total_report.partially_called_genotypes += batch.report.partially_called_genotypes;
    total_report.reference_genotypes += batch.report.reference_genotypes;
    total_report.rejected_records += batch.report.rejected_records;
    total_report.rejects.append(&mut batch.report.rejects);

    *record_offset += batch.report.source_records;
    records.clear();
    Ok(())
}

fn revision_now() -> anyhow::Result<u64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}

fn linux_peak_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let kb = status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?;
        return kb.checked_mul(1024);
    }
    #[cfg(not(target_os = "linux"))]
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_task() -> PoolY1TaskSpec {
        PoolY1TaskSpec {
            coordinator_task_id: "custom_0".into(),
            label: "HGSVC/HPRC chr22 canary".into(),
            run_id: "run-1".into(),
            task_id: "hgsvc-hprc-chr22-20000000-20010000".into(),
            attempt_id: "attempt-1".into(),
            release: "y1".into(),
            cohort: "hgsvc_hprc".into(),
            reference_genome: "GRCh38".into(),
            chrom: "chr22".into(),
            start: 20_000_000,
            stop: 20_010_000,
            source_uri: "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chr22.vcf.gz".into(),
            source_generation: "1".into(),
            source_checksum_algorithm: "md5_base64".into(),
            source_checksum: "abc".into(),
            source_size_bytes: 1,
            source_index_uri: "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chr22.vcf.gz.tbi".into(),
            source_index_generation: "2".into(),
            source_index_checksum_algorithm: "md5_base64".into(),
            source_index_checksum: "def".into(),
            retry_attempt_id: None,
            controlled_fail_once: None,
        }
    }

    #[test]
    fn task_contract_is_manifest_strict() {
        let task = valid_task();
        task.validate(&task.coordinator_task_id).unwrap();
        let mut value = serde_json::to_value(&task).unwrap();
        value["legacy_vcf_path"] = serde_json::json!("forbidden");
        assert!(serde_json::from_value::<PoolY1TaskSpec>(value).is_err());
    }

    #[test]
    fn descriptor_must_match_stable_task_id() {
        assert!(valid_task().validate("coordinator-renamed-task").is_err());
    }

    #[test]
    fn deceptive_mirror_paths_fail_closed() {
        let mut task = valid_task();
        task.source_uri =
            "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chr22.vcf.gz".into();
        task.source_index_uri = format!("{}.tbi", task.source_uri);
        assert!(task.validate(&task.coordinator_task_id).is_err());

        let mut task = valid_task();
        task.source_uri = "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/deceptive/gnomAD_LR_Y1.hgsvc_hprc.chr22.vcf.gz".into();
        task.source_index_uri = format!("{}.tbi", task.source_uri);
        assert!(task.validate(&task.coordinator_task_id).is_err());

        let mut task = valid_task();
        task.source_index_uri = "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chr1.vcf.gz.tbi".into();
        assert!(task.validate(&task.coordinator_task_id).is_err());
    }

    #[test]
    fn task_contract_accepts_each_primary_contig_and_rejects_mt() {
        for chrom in (1..=22)
            .map(|number| format!("chr{number}"))
            .chain(["chrX".to_string(), "chrY".to_string()])
        {
            let mut task = valid_task();
            task.chrom = chrom.clone();
            task.start = 1;
            task.stop = grch38_contig_length(&chrom).unwrap();
            task.source_uri = format!(
                "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.{chrom}.vcf.gz"
            );
            task.source_index_uri = format!("{}.tbi", task.source_uri);
            task.validate(&task.coordinator_task_id).unwrap();
            task.stop += 1;
            assert!(task.validate(&task.coordinator_task_id).is_err(), "{chrom}");
        }

        let mut mt = valid_task();
        mt.chrom = "chrM".into();
        assert!(mt.validate(&mt.coordinator_task_id).is_err());
    }

    #[test]
    fn controlled_failure_requires_a_distinct_retry_identity() {
        let mut task = valid_task();
        task.controlled_fail_once = Some(ControlledFailOnce {
            mode: "after_first_staged_batch".into(),
            evidence_token: "exercise-1".into(),
        });
        assert!(task.validate(&task.coordinator_task_id).is_err());
        task.retry_attempt_id = Some("attempt-2".into());
        task.validate(&task.coordinator_task_id).unwrap();
    }

    #[test]
    fn only_the_exact_running_claim_can_continue_staging() {
        let owned = LatestAttempt {
            state: "running".into(),
            revision: 42,
        };
        assert!(validate_claim_snapshot(&owned, 42).is_ok());

        let superseded = LatestAttempt {
            state: "running".into(),
            revision: 43,
        };
        assert!(validate_claim_snapshot(&superseded, 42).is_err());

        let terminal = LatestAttempt {
            state: "accepted".into(),
            revision: 42,
        };
        assert!(validate_claim_snapshot(&terminal, 42).is_err());
    }
}
