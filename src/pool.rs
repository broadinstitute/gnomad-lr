use genohype_pool::distributed::message::TaskDescriptor;
use genohype_pool::{TaskHandler, TaskResult};
use serde_json::Value;
use tracing::info;

use crate::{domain, loader};
use crate::loader::vcf_reader::VcfStream;

pub struct LrTaskHandler;

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
            "load_coverage" => handle_coverage_tasks(payload, tasks).await,
            "load_metadata" => handle_metadata_tasks(payload, tasks).await,
            "load_histograms" => handle_histograms_tasks(payload, tasks).await,
            "load_methylation" => handle_methylation_tasks(payload, tasks).await,
            "build_cache" => handle_build_cache_tasks(payload, tasks).await,
            "load" | _ => handle_load_tasks(payload, tasks).await,
        }
    }
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

        info!("Task {}: building cache for {} genes", task.id, gene_ids.len());

        let genes_path = genes_path.clone();
        let variants_path = variants_path.clone();
        let output_prefix = output_prefix.clone();
        let stats = tokio::task::spawn_blocking(move || {
            genohype_core::export::build_cache(
                &genes_path,
                &variants_path,
                &output_prefix,
                Some(&gene_ids),
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

        let start = task.payload["start"].as_u64().unwrap_or(0) as u32;
        let stop = task.payload["stop"].as_u64().unwrap_or(u32::MAX as u64) as u32;
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
            info!("Reading VCF region into memory: {}:{}-{}", chrom, start, stop);
            let stream = VcfStream::open_region(&vcf_path, &chrom, start, stop)?;
            let sample_names = stream.sample_names.clone();
            let records: Vec<String> = stream.records().collect();
            info!("Buffered {} records, {} samples", records.len(), sample_names.len());

            loader::variants::load_variants(&ch_url, &records, &sample_names, &chrom, start, stop, &mut metrics)?;
            loader::haplotypes::load_haplotypes(&ch_url, &records, &sample_names, &chrom, start, stop, &mut metrics)?;

            metrics.total_ms = task_start.elapsed().as_millis() as u64;
            info!("Task {} complete ({}ms total, {}ms CH insert, {} rows)", task_id, metrics.total_ms, metrics.ch_insert_ms, metrics.ch_rows_inserted);
            Ok::<_, anyhow::Error>(metrics)
        }).await??;

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
        let ch_url = ch_url.clone();

        info!("Task {}: loading coverage from {}", task.id, gcs_path);
        let rows = tokio::task::spawn_blocking(move || {
            loader::coverage::load_coverage(&ch_url, &gcs_path, downsample)
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
        let ch_url = ch_url.clone();

        info!("Task {}: loading sample metadata from {}", task.id, csv_url);
        let rows = tokio::task::spawn_blocking(move || {
            loader::metadata::load_sample_metadata(&ch_url, &csv_url)
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
        let ch_url = ch_url.clone();

        info!("Task {}: loading STR histograms from {}", task.id, gcs_path);
        let rows = tokio::task::spawn_blocking(move || {
            loader::histograms::load_str_histograms(&ch_url, &gcs_path)
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
        let ch_url = ch_url.clone();

        info!(
            "Task {}: loading methylation for {} {}:{}-{}",
            task.id, sample_id, chrom, start, stop
        );
        let rows = tokio::task::spawn_blocking(move || {
            loader::methylation::load_methylation(&ch_url, &bed_path, &sample_id, &chrom, start, stop)
        })
        .await??;
        total_rows += rows;
    }

    Ok(TaskResult::success(total_rows, None))
}
