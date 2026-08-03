use genohype_pool::distributed::message::TaskDescriptor;
use genohype_pool::{TaskHandler, TaskResult};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::info;

use crate::loader::vcf_reader::VcfStream;
use crate::{domain, loader};

pub struct LrTaskHandler;

const MAX_DURABLE_ATTEMPT_ID_LEN: usize = 127;
const PREFIX_FINGERPRINT_HEX_LEN: usize = 64;
const ASSIGNMENT_DIGEST_HEX_LEN: usize = 32;
const MAX_LEASE_TOKEN_LEN: usize = 1024;

pub(crate) const WORKER_BUILD_IDENTITY: &str = match option_env!("GNOMAD_LR_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => concat!(
        "gnomad-lr/",
        env!("CARGO_PKG_VERSION"),
        "/development-build"
    ),
};
pub(crate) const BACKEND_REVISION: &str = match option_env!("GNOMAD_LR_GIT_SHA") {
    Some(revision) => revision,
    None => "unversioned-development-build",
};

fn non_empty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn resolve_worker_identity(
    configured: Option<String>,
    hostname: Option<String>,
    build_identity: &str,
) -> String {
    non_empty(configured)
        .or_else(|| non_empty(hostname))
        .unwrap_or_else(|| format!("build:{build_identity}"))
}

pub(crate) fn worker_identity() -> String {
    resolve_worker_identity(
        std::env::var("GNOMAD_LR_WORKER_ID").ok(),
        std::env::var("HOSTNAME").ok(),
        WORKER_BUILD_IDENTITY,
    )
}

fn required_custom_lease(descriptor: &TaskDescriptor) -> anyhow::Result<(u64, &str)> {
    let assignment_attempt = descriptor.assignment_attempt.ok_or_else(|| {
        anyhow::anyhow!(
            "custom task {} has no assignment_attempt; refusing an unfenced legacy assignment",
            descriptor.id
        )
    })?;
    if assignment_attempt == 0 {
        anyhow::bail!(
            "custom task {} has stale or invalid assignment_attempt 0",
            descriptor.id
        );
    }
    let lease_token = descriptor.lease_token.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "custom task {} has no lease_token; refusing an unfenced legacy assignment",
            descriptor.id
        )
    })?;
    if lease_token.trim().is_empty() || lease_token.len() > MAX_LEASE_TOKEN_LEN {
        anyhow::bail!(
            "custom task {} has an empty or oversized lease_token",
            descriptor.id
        );
    }
    Ok((assignment_attempt, lease_token))
}

fn manifest_prefix_fingerprint(manifest_prefix: &str) -> String {
    format!("{:x}", Sha256::digest(manifest_prefix.as_bytes()))
}

fn is_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// Verify the independently encoded fingerprint of the complete manifest
/// attempt prefix and the canonical shape of a durable assignment identity.
/// The assignment digest cannot be recomputed here because its lease token is
/// deliberately not persisted.
pub(crate) fn durable_attempt_matches_prefix(id: &str, manifest_prefix: &str) -> bool {
    if id.len() > MAX_DURABLE_ATTEMPT_ID_LEN {
        return false;
    }
    let Some(remainder) = id.strip_prefix('p') else {
        return false;
    };
    let Some((fingerprint, remainder)) = remainder.split_once("-a") else {
        return false;
    };
    let Some((assignment, assignment_digest)) = remainder.split_once("-d") else {
        return false;
    };

    is_lower_hex(fingerprint, PREFIX_FINGERPRINT_HEX_LEN)
        && fingerprint == manifest_prefix_fingerprint(manifest_prefix)
        && !assignment.is_empty()
        && !assignment.starts_with('0')
        && assignment.bytes().all(|byte| byte.is_ascii_digit())
        && assignment.parse::<u64>().is_ok_and(|attempt| attempt != 0)
        && is_lower_hex(assignment_digest, ASSIGNMENT_DIGEST_HEX_LEN)
}

fn durable_attempt_id(
    manifest_prefix: &str,
    coordinator_task_id: &str,
    assignment_attempt: u64,
    lease_token: &str,
) -> anyhow::Result<String> {
    if manifest_prefix.trim().is_empty() || coordinator_task_id.trim().is_empty() {
        anyhow::bail!("durable attempt identity requires non-empty manifest and task IDs");
    }
    if assignment_attempt == 0 || lease_token.trim().is_empty() {
        anyhow::bail!("durable attempt identity requires a current non-empty assignment lease");
    }

    let assignment = assignment_attempt.to_string();
    let mut digest = Sha256::new();
    for component in [
        manifest_prefix.as_bytes(),
        coordinator_task_id.as_bytes(),
        assignment.as_bytes(),
        lease_token.as_bytes(),
    ] {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component);
    }
    let assignment_digest = format!("{:x}", digest.finalize());
    let id = format!(
        "p{}-a{}-d{}",
        manifest_prefix_fingerprint(manifest_prefix),
        assignment,
        &assignment_digest[..ASSIGNMENT_DIGEST_HEX_LEN]
    );
    debug_assert!(id.len() <= MAX_DURABLE_ATTEMPT_ID_LEN);
    Ok(id)
}

fn bind_y1_assignment(
    task: &mut crate::y1::PoolY1TaskSpec,
    descriptor: &TaskDescriptor,
) -> anyhow::Result<()> {
    let (assignment_attempt, lease_token) = required_custom_lease(descriptor)?;
    let manifest_attempt_prefix = task.attempt_id.clone();
    let controlled_retry_prefix = task.retry_attempt_id.clone();

    // A controlled fail-once manifest deliberately fails its first coordinator
    // assignment. A subsequent fenced assignment uses the immutable retry
    // prefix but no longer injects the failure. Generic requeues continue to use
    // the original manifest prefix, with uniqueness supplied by the lease.
    let prefix = if assignment_attempt > 1 && task.controlled_fail_once.is_some() {
        controlled_retry_prefix.as_deref().ok_or_else(|| {
            anyhow::anyhow!("controlled retry has no immutable retry attempt prefix")
        })?
    } else {
        manifest_attempt_prefix.as_str()
    };
    let current_id = durable_attempt_id(prefix, &descriptor.id, assignment_attempt, lease_token)?;

    if assignment_attempt > 1 && task.controlled_fail_once.is_some() {
        task.attempt_id = current_id;
        task.retry_attempt_id = None;
        task.controlled_fail_once = None;
    } else {
        task.attempt_id = current_id;
        if let Some(retry_prefix) = controlled_retry_prefix {
            task.retry_attempt_id = Some(durable_attempt_id(
                &retry_prefix,
                &descriptor.id,
                assignment_attempt,
                lease_token,
            )?);
        }
    }
    Ok(())
}

#[genohype_pool::async_trait]
impl TaskHandler for LrTaskHandler {
    async fn handle_task(
        &self,
        payload: &Value,
        tasks: Vec<TaskDescriptor>,
    ) -> Result<TaskResult, anyhow::Error> {
        let action = payload["action"].as_str().unwrap_or("load");

        match action {
            "index" => handle_index_tasks(tasks).await,
            "load_y1_interval" => handle_y1_interval_tasks(payload, tasks).await,
            "load_y1_phased_mirror_chr22" => {
                handle_y1_phased_mirror_chr22_task(payload, tasks).await
            }
            "load_coverage" => handle_coverage_tasks(payload, tasks).await,
            "load_metadata" => handle_metadata_tasks(payload, tasks).await,
            "load_histograms" => handle_histograms_tasks(payload, tasks).await,
            "load_methylation" => handle_methylation_tasks(payload, tasks).await,
            "build_cache" => handle_build_cache_tasks(payload, tasks).await,
            "load" => handle_load_tasks(payload, tasks).await,
            unknown => {
                anyhow::bail!("unsupported pool action {unknown:?}; refusing legacy fallback")
            }
        }
    }
}

async fn handle_y1_phased_mirror_chr22_task(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let job: crate::y1::PhasedMirrorJobSpec = serde_json::from_value(payload.clone())?;
    job.validate(BACKEND_REVISION, WORKER_BUILD_IDENTITY)?;
    if tasks.len() != 1 {
        anyhow::bail!("phased mirror canary requires exactly one fenced task per completion");
    }
    let descriptor = tasks.into_iter().next().unwrap();
    if descriptor.task_type != "custom" {
        anyhow::bail!("phased mirror canary accepts only manifest-backed custom tasks");
    }
    let task: crate::y1::PhasedMirrorTaskSpec = serde_json::from_value(descriptor.payload.clone())?;
    crate::y1::validate_task_against_ledger(&task, &descriptor.id)?;
    let (assignment_attempt, lease_token) = required_custom_lease(&descriptor)?;
    let attempt_id = durable_attempt_id(
        &task.attempt_prefix,
        &descriptor.id,
        assignment_attempt,
        lease_token,
    )?;
    let target = job.target()?;
    let worker = worker_identity();
    let batch_records = job.batch_records;
    let descriptor_id = descriptor.id.clone();
    let report = tokio::task::spawn_blocking(move || {
        crate::y1::run_phased_mirror_task(
            &target,
            &task,
            &descriptor_id,
            assignment_attempt,
            &attempt_id,
            &worker,
            BACKEND_REVISION,
            WORKER_BUILD_IDENTITY,
            batch_records,
        )
    })
    .await??;
    let rows = usize::try_from(report.rows())?;
    Ok(TaskResult::success(
        rows,
        Some(serde_json::to_value(report)?),
    ))
}

async fn handle_y1_interval_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let job: crate::y1::PoolY1JobSpec = serde_json::from_value(payload.clone())?;
    job.validate()?;
    let target = crate::y1::ClickHouseTarget::new(
        &job.target.endpoint,
        &job.target.database,
        crate::y1::TargetKind::Scratch,
        crate::y1::AuthSource::PasswordlessUser {
            username: job.target.worker_principal.clone(),
        },
        true,
        false,
    )?;
    let authenticated_worker_principal =
        target.attest_current_user(&job.target.worker_principal)?;
    target.attest_synchronous_inserts()?;
    let worker_identity = worker_identity();
    let build_identity = WORKER_BUILD_IDENTITY;
    let backend_revision = BACKEND_REVISION;
    // Carry the exact currentUser() result into every immutable running and
    // terminal attempt ledger report.
    let batch_records = job.batch_records;
    let mut reports = Vec::with_capacity(tasks.len());
    let mut processed = 0usize;

    for descriptor in tasks {
        if descriptor.task_type != "custom" {
            anyhow::bail!(
                "strict Y1 action requires the manifest-backed custom task protocol, got {:?}",
                descriptor.task_type
            );
        }
        let mut task: crate::y1::PoolY1TaskSpec =
            serde_json::from_value(descriptor.payload.clone())?;
        task.validate(&descriptor.id)?;
        bind_y1_assignment(&mut task, &descriptor)?;
        task.validate(&descriptor.id)?;
        let report = tokio::task::spawn_blocking({
            let target = target.clone();
            let worker_identity = worker_identity.clone();
            let task = task.clone();
            let authenticated_worker_principal = authenticated_worker_principal.clone();
            move || {
                crate::y1::run_pool_interval_attempt(
                    &target,
                    &task,
                    batch_records,
                    &worker_identity,
                    build_identity,
                    backend_revision,
                    &authenticated_worker_principal,
                )
            }
        })
        .await??;
        processed += usize::try_from(report.counts.source_records)?;
        reports.push(report);
    }

    Ok(TaskResult::success(
        processed,
        Some(serde_json::json!({
            "action": "load_y1_interval",
            "published": false,
            "attempts": reports
        })),
    ))
}

fn task_region(payload: &Value) -> anyhow::Result<Option<loader::RegionFilter>> {
    if let Some(region) = payload["region"].as_str() {
        let (chrom, start, stop) = crate::cli::parse_region(region)?;
        return Ok(Some(loader::RegionFilter::new(chrom, start, stop)));
    }

    Ok(payload["chrom"].as_str().map(|chrom| {
        loader::RegionFilter::new(
            chrom.to_string(),
            payload["start"].as_u64().unwrap_or(0) as u32,
            payload["stop"].as_u64().unwrap_or(u32::MAX as u64) as u32,
        )
    }))
}

/// Phase-4 `gcs-cache` build: each task carries a chunk of `gene_id`s; the worker
/// materializes one `{gene_id}.json` GeneVariantsResponse blob per gene to the
/// shared `output_prefix` (a `gs://` cache prefix). Job-wide table paths come from
/// the top-level payload; the per-task `gene_ids` array is the chunk to build.
///
/// Reuses [`genohype_core::export::build_cache`] (the single source of truth for
/// the blob contract; see `genohype/core/src/export/cache_builder.rs`).
async fn handle_build_cache_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let genes_path = payload["genes_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing 'genes_path' in build_cache payload"))?
        .to_string();
    let variants_path = payload["variants_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing 'variants_path' in build_cache payload"))?
        .to_string();
    let output_prefix = payload["output_prefix"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing 'output_prefix' in build_cache payload"))?
        .to_string();

    let mut total_blobs = 0usize;
    for task in &tasks {
        let gene_ids: Vec<String> = task.payload["gene_ids"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("missing 'gene_ids' in build_cache task {}", task.id))?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        info!(
            "Task {}: building cache for {} genes",
            task.id,
            gene_ids.len()
        );

        let genes_path = genes_path.clone();
        let variants_path = variants_path.clone();
        let output_prefix = output_prefix.clone();
        let stats = tokio::task::spawn_blocking(move || {
            genohype_core::export::build_cache(
                &genes_path,
                &variants_path,
                &output_prefix,
                Some(&gene_ids),
                None,
            )
        })
        .await??;

        info!(
            "Task {} complete: {} blobs, {} variants, {} bytes",
            task.id, stats.blobs_written, stats.total_variants, stats.total_bytes
        );
        total_blobs += stats.blobs_written;
    }

    Ok(TaskResult::success(total_blobs, None))
}

async fn handle_index_tasks(tasks: Vec<TaskDescriptor>) -> Result<TaskResult, anyhow::Error> {
    let mut total = 0usize;

    for task in &tasks {
        let vcf_path = task.payload["vcf_path"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'vcf_path' in index task"))?
            .to_string();

        info!("Task {}: indexing {}", task.id, vcf_path);

        let rows = tokio::task::spawn_blocking(move || {
            let index = genohype_core::vcf::index::build_tabix_index(&vcf_path, None)?;
            let tbi_path = format!("{}.tbi", vcf_path);
            genohype_core::vcf::index::write_tabix_index(&index, &tbi_path)?;
            info!("Wrote index to {}", tbi_path);
            Ok::<_, anyhow::Error>(1usize)
        })
        .await??;

        total += rows;
        info!("Task {} complete", task.id);
    }

    Ok(TaskResult::success(total, None))
}

async fn handle_load_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let ch_url = payload["clickhouse_url"]
        .as_str()
        .unwrap_or("http://localhost:8123")
        .to_string();

    // Reject a mixed task list before any task can write partial legacy output.
    for task in &tasks {
        if let Some(vcf_path) = task.payload["vcf_path"].as_str() {
            domain::ensure_legacy_vcf_compatible(vcf_path)?;
        }
    }

    // Process tasks sequentially to limit memory usage.
    // Each region buffers all records in memory (~300MB for dense 5MB regions),
    // so running multiple in parallel OOMs 7.5GB VMs.
    let mut total_rows = 0usize;
    let mut combined_metrics = loader::IngestMetrics::default();
    let combined_start = std::time::Instant::now();

    for task in &tasks {
        let chrom = task.payload["chrom"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'chrom' in task payload"))?
            .to_string();

        let vcf_path = task.payload["vcf_path"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| domain::resolve_vcf_path(&chrom))
            .ok_or_else(|| anyhow::anyhow!("no VCF path for {}", chrom))?;
        domain::ensure_legacy_vcf_compatible(&vcf_path)?;

        let start = task.payload["start"].as_u64().unwrap_or(0) as u32;
        let stop = task.payload["stop"].as_u64().unwrap_or(u32::MAX as u64) as u32;
        let limit = task.payload["limit"]
            .as_u64()
            .or_else(|| payload["limit"].as_u64())
            .map(|value| value as usize);
        let task_id = task.id.clone();

        info!(
            "Task {}: loading {}:{}-{} from {}",
            task_id, chrom, start, stop, vcf_path
        );

        let ch_url = ch_url.clone();
        let metrics = tokio::task::spawn_blocking(move || {
            let mut metrics = loader::IngestMetrics::default();
            let task_start = std::time::Instant::now();

            // Read the VCF region once into memory
            info!(
                "Reading VCF region into memory: {}:{}-{}",
                chrom, start, stop
            );
            let stream = VcfStream::open_region(&vcf_path, &chrom, start, stop)?;
            let sample_names = stream.sample_names.clone();
            let records = stream
                .records()
                .take(limit.unwrap_or(usize::MAX))
                .collect::<anyhow::Result<Vec<_>>>()?;
            info!(
                "Buffered {} records, {} samples",
                records.len(),
                sample_names.len()
            );

            loader::variants::load_variants(
                &ch_url,
                &records,
                &sample_names,
                &chrom,
                start,
                stop,
                &mut metrics,
            )?;
            loader::haplotypes::load_haplotypes(
                &ch_url,
                &records,
                &sample_names,
                &chrom,
                start,
                stop,
                &mut metrics,
            )?;

            metrics.total_ms = task_start.elapsed().as_millis() as u64;
            info!(
                "Task {} complete ({}ms total, {}ms CH insert, {} rows)",
                task_id, metrics.total_ms, metrics.ch_insert_ms, metrics.ch_rows_inserted
            );
            Ok::<_, anyhow::Error>(metrics)
        })
        .await??;

        total_rows += metrics.ch_rows_inserted;
        combined_metrics.prescan_ms += metrics.prescan_ms;
        combined_metrics.ch_insert_ms += metrics.ch_insert_ms;
        combined_metrics.ch_insert_count += metrics.ch_insert_count;
        combined_metrics.ch_rows_inserted += metrics.ch_rows_inserted;
    }
    combined_metrics.total_ms = combined_start.elapsed().as_millis() as u64;

    let metrics_json = serde_json::to_value(&combined_metrics)?;
    Ok(TaskResult::success(total_rows, Some(metrics_json)))
}

fn task_clickhouse_url(
    job_payload: &Value,
    task_payload: &Value,
    action: &str,
) -> anyhow::Result<String> {
    for (scope, payload) in [("task", task_payload), ("job", job_payload)] {
        if let Some(value) = payload.get("clickhouse_url") {
            return value
                .as_str()
                .filter(|url| !url.trim().is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{action} {scope}-level 'clickhouse_url' must be a nonempty string"
                    )
                });
        }
    }
    anyhow::bail!(
        "{action} requires a task-level 'clickhouse_url' or compatible job-level fallback"
    )
}

async fn handle_coverage_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let task_urls = tasks
        .iter()
        .map(|task| task_clickhouse_url(payload, &task.payload, "load_coverage"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut total_rows = 0usize;

    for (task, ch_url) in tasks.iter().zip(task_urls) {
        let gcs_path = task.payload["gcs_path"]
            .as_str()
            .unwrap_or("gs://gnomad-v4-data-pipeline/inputs/secondary-analyses/gnomAD-LR/v2/hgsvc_hprc.coverage.tsv.gz")
            .to_string();
        let downsample = task.payload["downsample"].as_u64().unwrap_or(1) as u32;
        let region = task_region(&task.payload)?;
        let limit = task.payload["limit"].as_u64().map(|value| value as usize);

        info!("Task {}: loading coverage from {}", task.id, gcs_path);
        let rows = tokio::task::spawn_blocking(move || {
            loader::coverage::load_coverage(&ch_url, &gcs_path, downsample, region.as_ref(), limit)
        })
        .await??;
        total_rows += rows;
    }

    Ok(TaskResult::success(total_rows, None))
}

async fn handle_metadata_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let ch_url = payload["clickhouse_url"]
        .as_str()
        .unwrap_or("http://localhost:8123")
        .to_string();

    let mut total_rows = 0usize;

    for task in &tasks {
        let csv_url = task.payload["csv_url"]
            .as_str()
            .unwrap_or(loader::metadata::HPRC_METADATA_URL)
            .to_string();
        let limit = task.payload["limit"].as_u64().map(|value| value as usize);
        let ch_url = ch_url.clone();

        info!("Task {}: loading sample metadata from {}", task.id, csv_url);
        let rows = tokio::task::spawn_blocking(move || {
            loader::metadata::load_sample_metadata(&ch_url, &csv_url, limit)
        })
        .await??;
        total_rows += rows;
    }

    Ok(TaskResult::success(total_rows, None))
}

async fn handle_histograms_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let task_urls = tasks
        .iter()
        .map(|task| task_clickhouse_url(payload, &task.payload, "load_histograms"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut total_rows = 0usize;

    for (task, ch_url) in tasks.iter().zip(task_urls) {
        let gcs_path = task.payload["gcs_path"]
            .as_str()
            .unwrap_or("gs://gnomad-v4-data-pipeline/inputs/secondary-analyses/gnomAD-LR/v2/hgsvc_hprc.af_histograms.tsv")
            .to_string();
        let region = task_region(&task.payload)?;
        let limit = task.payload["limit"].as_u64().map(|value| value as usize);

        info!("Task {}: loading STR histograms from {}", task.id, gcs_path);
        let rows = tokio::task::spawn_blocking(move || {
            loader::histograms::load_str_histograms(&ch_url, &gcs_path, region.as_ref(), limit)
        })
        .await??;
        total_rows += rows;
    }

    Ok(TaskResult::success(total_rows, None))
}

async fn handle_methylation_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let ch_url = payload["clickhouse_url"]
        .as_str()
        .unwrap_or("http://localhost:8123")
        .to_string();

    let mut total_rows = 0usize;

    for task in &tasks {
        let bed_path = task.payload["bed_path"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'bed_path' in methylation task"))?
            .to_string();
        let sample_id = task.payload["sample_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'sample_id' in methylation task"))?
            .to_string();
        let chrom = task.payload["chrom"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing 'chrom' in methylation task"))?
            .to_string();
        let start = task.payload["start"].as_u64().unwrap_or(0) as u32;
        let stop = task.payload["stop"].as_u64().unwrap_or(400_000_000) as u32;
        let limit = task.payload["limit"].as_u64().map(|value| value as usize);
        let ch_url = ch_url.clone();

        info!(
            "Task {}: loading methylation for {} {}:{}-{}",
            task.id, sample_id, chrom, start, stop
        );
        let rows = tokio::task::spawn_blocking(move || {
            loader::methylation::load_methylation(
                &ch_url, &bed_path, &sample_id, &chrom, start, stop, limit,
            )
        })
        .await??;
        total_rows += rows;
    }

    Ok(TaskResult::success(total_rows, None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ancillary_tasks_can_target_separate_cohort_databases() {
        let job = serde_json::json!({
            "clickhouse_url": "http://localhost:8123/?database=compat"
        });
        let hgsvc = serde_json::json!({
            "cohort": "hgsvc_hprc",
            "clickhouse_url": "http://clickhouse:8123/?database=coverage_hgsvc"
        });
        let aou = serde_json::json!({
            "cohort": "aou",
            "clickhouse_url": "http://clickhouse:8123/?database=coverage_aou"
        });

        let hgsvc_url = task_clickhouse_url(&job, &hgsvc, "load_coverage").unwrap();
        let aou_url = task_clickhouse_url(&job, &aou, "load_coverage").unwrap();
        assert_eq!(hgsvc_url, "http://clickhouse:8123/?database=coverage_hgsvc");
        assert_eq!(aou_url, "http://clickhouse:8123/?database=coverage_aou");
        assert_ne!(hgsvc_url, aou_url);
    }

    #[test]
    fn ancillary_task_target_keeps_job_level_compatibility_and_fails_closed() {
        let compatible = serde_json::json!({
            "clickhouse_url": "http://clickhouse:8123/?database=legacy_job_target"
        });
        let ordinary_task = serde_json::json!({"gcs_path": "gs://bucket/source.tsv"});
        for action in ["load_coverage", "load_histograms"] {
            assert_eq!(
                task_clickhouse_url(&compatible, &ordinary_task, action).unwrap(),
                "http://clickhouse:8123/?database=legacy_job_target"
            );
            assert!(task_clickhouse_url(&serde_json::json!({}), &ordinary_task, action).is_err());
        }

        let malformed_task = serde_json::json!({"clickhouse_url": ""});
        assert!(task_clickhouse_url(&compatible, &malformed_task, "load_coverage").is_err());
    }

    #[test]
    fn worker_identity_prefers_explicit_then_hostname() {
        assert_eq!(
            resolve_worker_identity(Some(" worker-7 ".into()), Some("host-2".into()), "build"),
            "worker-7"
        );
        assert_eq!(
            resolve_worker_identity(Some(" ".into()), Some(" host-2 ".into()), "build"),
            "host-2"
        );
    }

    #[test]
    fn worker_identity_has_deterministic_build_fallback() {
        assert_eq!(
            resolve_worker_identity(None, Some("".into()), "gnomad-lr/git-abc"),
            "build:gnomad-lr/git-abc"
        );
        assert_ne!(
            resolve_worker_identity(None, None, WORKER_BUILD_IDENTITY),
            "unknown-worker"
        );
    }

    fn descriptor(attempt: Option<u64>, token: Option<&str>) -> TaskDescriptor {
        TaskDescriptor {
            id: "custom_0".into(),
            task_type: "custom".into(),
            label: None,
            index: Some(0),
            total: Some(1),
            payload: serde_json::Value::Null,
            assignment_attempt: attempt,
            lease_token: token.map(str::to_string),
        }
    }

    #[test]
    fn fresh_requeues_get_distinct_durable_attempt_identities() {
        let first = durable_attempt_id("attempt-1-0000", "custom_0", 1, "lease-one").unwrap();
        let requeue = durable_attempt_id("attempt-1-0000", "custom_0", 2, "lease-two").unwrap();
        assert_ne!(first, requeue);
        assert!(first.contains("-a1-d"));
        assert!(requeue.contains("-a2-d"));
        assert!(durable_attempt_matches_prefix(&first, "attempt-1-0000"));
        assert!(durable_attempt_matches_prefix(&requeue, "attempt-1-0000"));
    }

    #[test]
    fn missing_or_invalid_custom_leases_fail_closed() {
        for invalid in [
            descriptor(None, Some("token")),
            descriptor(Some(1), None),
            descriptor(Some(0), Some("stale-token")),
            descriptor(Some(1), Some("  ")),
        ] {
            assert!(required_custom_lease(&invalid).is_err());
        }
        assert!(required_custom_lease(&descriptor(Some(1), Some("token"))).is_ok());
    }

    #[test]
    fn durable_attempt_encoding_is_deterministic_safe_and_bounded() {
        let prefix = format!("unsafe prefix/{}", "x".repeat(500));
        let first = durable_attempt_id(&prefix, "custom/0", u64::MAX, "capability").unwrap();
        let second = durable_attempt_id(&prefix, "custom/0", u64::MAX, "capability").unwrap();
        assert_eq!(first, second);
        assert!(first.len() <= MAX_DURABLE_ATTEMPT_ID_LEN);
        assert!(first
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')));
        assert!(!first.contains("capability"));
        assert!(durable_attempt_matches_prefix(&first, &prefix));
        assert!(!durable_attempt_matches_prefix(&first, "other-prefix"));
    }

    #[test]
    fn full_prefix_fingerprint_rejects_sanitized_and_truncated_collisions() {
        let shared = "x".repeat(80);
        let slash = format!("{shared}/suffix");
        let question = format!("{shared}?suffix");
        let changed_tail = format!("{shared}/different");
        let id = durable_attempt_id(&slash, "custom_0", 7, "lease").unwrap();

        assert!(durable_attempt_matches_prefix(&id, &slash));
        assert!(!durable_attempt_matches_prefix(&id, &question));
        assert!(!durable_attempt_matches_prefix(&id, &changed_tail));
    }

    #[test]
    fn durable_attempt_matcher_rejects_noncanonical_and_malformed_forms() {
        let manifest_prefix = "attempt/full-prefix";
        let valid = durable_attempt_id(manifest_prefix, "custom_0", 7, "lease").unwrap();
        let (fingerprint, digest) = valid
            .strip_prefix('p')
            .unwrap()
            .split_once("-a7-d")
            .unwrap();

        let uppercase_fingerprint = format!("p{}-a7-d{digest}", fingerprint.to_uppercase());
        let uppercase_digest = format!("p{fingerprint}-a7-d{}", digest.to_uppercase());
        for invalid in [
            format!("p{fingerprint}-a0-d{digest}"),
            format!("p{fingerprint}-a07-d{digest}"),
            format!("p{fingerprint}-a18446744073709551616-d{digest}"),
            format!("p{fingerprint}-a+7-d{digest}"),
            format!("p{fingerprint}-a7-d{}", &digest[..digest.len() - 1]),
            format!("p{fingerprint}-a7-d{}g", &digest[..digest.len() - 1]),
            format!("{valid}-trailing"),
            uppercase_fingerprint,
            uppercase_digest,
        ] {
            assert!(
                !durable_attempt_matches_prefix(&invalid, manifest_prefix),
                "{invalid}"
            );
        }
    }

    #[test]
    fn bound_attempt_keeps_domain_identity_and_report_compatible_attempt_id() {
        let manifest = include_str!("../manifests/y1/hgsvc-hprc-chr22-1mb.json");
        let mut tasks: Vec<crate::y1::PoolY1TaskSpec> = serde_json::from_str(manifest).unwrap();
        let mut task = tasks.remove(0);
        let run_id = task.run_id.clone();
        let task_id = task.task_id.clone();
        let assignment = descriptor(Some(1), Some("lease-one"));

        bind_y1_assignment(&mut task, &assignment).unwrap();

        assert_eq!(task.run_id, run_id);
        assert_eq!(task.task_id, task_id);
        task.validate(&assignment.id).unwrap();
        let report = serde_json::json!({
            "run_id": task.run_id,
            "task_id": task.task_id,
            "attempt_id": task.attempt_id,
        });
        assert_eq!(report["run_id"], run_id);
        assert_eq!(report["task_id"], task_id);
        assert_eq!(report["attempt_id"], task.attempt_id);
    }

    #[test]
    fn controlled_requeue_uses_retry_prefix_without_reinjecting_failure() {
        let manifest = include_str!("../manifests/y1/hgsvc-hprc-chr22-1mb.json");
        let tasks: Vec<crate::y1::PoolY1TaskSpec> = serde_json::from_str(manifest).unwrap();
        let original = tasks
            .into_iter()
            .find(|task| task.controlled_fail_once.is_some())
            .unwrap();
        let retry_prefix = original.retry_attempt_id.clone().unwrap();
        let mut retry = original.clone();

        bind_y1_assignment(&mut retry, &descriptor(Some(2), Some("fresh-requeue"))).unwrap();

        assert!(retry.attempt_id.contains("-a2-d"));
        assert!(durable_attempt_matches_prefix(
            &retry.attempt_id,
            &retry_prefix
        ));
        assert!(retry.controlled_fail_once.is_none());
        assert!(retry.retry_attempt_id.is_none());
        assert_eq!(retry.run_id, original.run_id);
        assert_eq!(retry.task_id, original.task_id);
    }
}
