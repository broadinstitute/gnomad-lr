use genohype_pool::distributed::message::TaskDescriptor;
use genohype_pool::{TaskHandler, TaskResult};
use serde_json::Value;
use tracing::info;

use crate::loader::vcf_reader::VcfStream;
use crate::{domain, loader};

pub struct LrTaskHandler;

const WORKER_BUILD_IDENTITY: &str = match option_env!("GNOMAD_LR_BUILD_IDENTITY") {
    Some(identity) => identity,
    None => concat!(
        "gnomad-lr/",
        env!("CARGO_PKG_VERSION"),
        "/development-build"
    ),
};
const BACKEND_REVISION: &str = match option_env!("GNOMAD_LR_GIT_SHA") {
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

fn worker_identity() -> String {
    resolve_worker_identity(
        std::env::var("GNOMAD_LR_WORKER_ID").ok(),
        std::env::var("HOSTNAME").ok(),
        WORKER_BUILD_IDENTITY,
    )
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
        crate::y1::AuthSource::PrivateNetwork,
        true,
        false,
    )?;
    let worker_identity = worker_identity();
    let build_identity = WORKER_BUILD_IDENTITY;
    let backend_revision = BACKEND_REVISION;
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
        let task: crate::y1::PoolY1TaskSpec = serde_json::from_value(descriptor.payload)?;
        task.validate(&descriptor.id)?;
        let report = tokio::task::spawn_blocking({
            let target = target.clone();
            let worker_identity = worker_identity.clone();
            let task = task.clone();
            move || {
                crate::y1::run_pool_interval_attempt(
                    &target,
                    &task,
                    batch_records,
                    &worker_identity,
                    build_identity,
                    backend_revision,
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

async fn handle_coverage_tasks(
    payload: &Value,
    tasks: Vec<TaskDescriptor>,
) -> Result<TaskResult, anyhow::Error> {
    let ch_url = payload["clickhouse_url"]
        .as_str()
        .unwrap_or("http://localhost:8123")
        .to_string();

    let mut total_rows = 0usize;

    for task in &tasks {
        let gcs_path = task.payload["gcs_path"]
            .as_str()
            .unwrap_or("gs://gnomad-v4-data-pipeline/inputs/secondary-analyses/gnomAD-LR/v2/hgsvc_hprc.coverage.tsv.gz")
            .to_string();
        let downsample = task.payload["downsample"].as_u64().unwrap_or(1) as u32;
        let region = task_region(&task.payload)?;
        let limit = task.payload["limit"].as_u64().map(|value| value as usize);
        let ch_url = ch_url.clone();

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
    let ch_url = payload["clickhouse_url"]
        .as_str()
        .unwrap_or("http://localhost:8123")
        .to_string();

    let mut total_rows = 0usize;

    for task in &tasks {
        let gcs_path = task.payload["gcs_path"]
            .as_str()
            .unwrap_or("gs://gnomad-v4-data-pipeline/inputs/secondary-analyses/gnomAD-LR/v2/hgsvc_hprc.af_histograms.tsv")
            .to_string();
        let region = task_region(&task.payload)?;
        let limit = task.payload["limit"].as_u64().map(|value| value as usize);
        let ch_url = ch_url.clone();

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
}
