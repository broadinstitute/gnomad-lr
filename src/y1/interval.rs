use super::{
    record_task_attempt, stage_attempt, AttemptContext, AttemptState, ClickHouseTarget, Cohort,
    StagedCounts, TaskAttemptLedgerRow, TransformationReport, Y1Header,
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
    pub elapsed_ms: u128,
    pub parse_transform_insert_ms: u128,
    pub linux_peak_rss_bytes: Option<u64>,
    pub worker_identity: String,
    pub worker_build_version: String,
    pub backend_revision: String,
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
        if self.chrom != "chr22" {
            bail!("full-chr22 rehearsal tasks are restricted to chr22");
        }
        if self.start == 0 || self.start > self.stop || self.stop > 50_818_468 {
            bail!("task bounds must be a non-empty one-based inclusive chr22 interval");
        }
        if !self
            .source_uri
            .starts_with("gs://gnomad-lr-data/y1/sources/")
            || self.source_index_uri != format!("{}.tbi", self.source_uri)
        {
            bail!("task source must be a mirrored Y1 VCF with its exact adjacent TBI");
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

    let started = Instant::now();
    let cohort = match task.cohort.as_str() {
        "hgsvc_hprc" => Cohort::HgsvcHprc,
        "aou" => Cohort::Aou,
        _ => bail!("unsupported Y1 cohort"),
    };
    let header_text =
        read_header_text(&task.source_uri).context("failed to read pinned Y1 header")?;
    let header = Y1Header::parse(&header_text, cohort)?;
    if header.reference_genome.as_str() != task.reference_genome {
        bail!("source header reference does not match manifest reference_genome");
    }

    let context = AttemptContext {
        run_id: task.run_id.clone(),
        task_id: task.task_id.clone(),
        attempt_id: task.attempt_id.clone(),
        cohort,
        chrom: task.chrom.clone(),
        interval_start: task.start,
        interval_end: task.stop,
    };
    context.validate()?;

    let phase_started = Instant::now();
    let mut total_counts = StagedCounts::default();
    let mut total_report = TransformationReport::default();
    let mut record_offset = 0usize;
    let mut record_batch = Vec::with_capacity(batch_records);
    let records = VcfStream::open_region_required_index(
        &task.source_uri,
        &task.chrom,
        task.start,
        task.stop,
    )?
    .records();

    for record in records {
        record_batch.push(record?);
        if record_batch.len() == batch_records {
            stage_batch(
                target,
                &context,
                &header,
                &mut record_batch,
                &mut record_offset,
                &mut total_counts,
                &mut total_report,
            )?;
        }
    }
    if !record_batch.is_empty() {
        stage_batch(
            target,
            &context,
            &header,
            &mut record_batch,
            &mut record_offset,
            &mut total_counts,
            &mut total_report,
        )?;
    }

    let accepted =
        total_counts.rejects == 0 && total_counts.summaries == total_counts.source_records;
    let ledger = TaskAttemptLedgerRow::new(
        &context,
        revision_now()?,
        if accepted {
            AttemptState::Accepted
        } else {
            AttemptState::Failed
        },
        total_counts,
        &total_report,
        if accepted {
            ""
        } else {
            "transformation validation failed"
        },
    )?;
    record_task_attempt(target, &ledger)?;
    if !accepted {
        bail!("Y1 pool attempt failed transformation validation");
    }

    Ok(PoolY1AttemptReport {
        run_id: task.run_id.clone(),
        task_id: task.task_id.clone(),
        attempt_id: task.attempt_id.clone(),
        cohort,
        chrom: task.chrom.clone(),
        start: task.start,
        stop: task.stop,
        source_uri: task.source_uri.clone(),
        source_generation: task.source_generation.clone(),
        source_size_bytes: task.source_size_bytes,
        counts: total_counts,
        transformation: total_report,
        elapsed_ms: started.elapsed().as_millis(),
        parse_transform_insert_ms: phase_started.elapsed().as_millis(),
        linux_peak_rss_bytes: linux_peak_rss_bytes(),
        worker_identity: worker_identity.to_string(),
        worker_build_version: worker_build_version.to_string(),
        backend_revision: backend_revision.to_string(),
        published: false,
    })
}

fn stage_batch(
    target: &ClickHouseTarget,
    context: &AttemptContext,
    header: &Y1Header,
    records: &mut Vec<String>,
    record_offset: &mut usize,
    total_counts: &mut StagedCounts,
    total_report: &mut TransformationReport,
) -> anyhow::Result<()> {
    let mut batch = super::transform_records(header, records.iter().map(String::as_str));
    for reject in &mut batch.report.rejects {
        if let Some(record_number) = &mut reject.record_number {
            *record_number += *record_offset;
        }
    }
    let counts = stage_attempt(target, context, &batch)?;
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
}
