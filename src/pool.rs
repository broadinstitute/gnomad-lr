use genohype_pool::distributed::message::TaskDescriptor;
use genohype_pool::{TaskHandler, TaskResult};
use serde_json::Value;
use tracing::info;

use crate::{domain, loader};

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
            "load" | _ => handle_load_tasks(payload, tasks).await,
        }
    }
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

    // Process all tasks in parallel across threads
    let mut handles = Vec::new();

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
        let handle = tokio::task::spawn_blocking(move || {
            let mut metrics = loader::IngestMetrics::default();
            let task_start = std::time::Instant::now();

            loader::variants::load_variants(&ch_url, &vcf_path, &chrom, start, stop, &mut metrics)?;
            loader::haplotypes::load_haplotypes(&ch_url, &vcf_path, &chrom, start, stop, &mut metrics)?;

            metrics.total_ms = task_start.elapsed().as_millis() as u64;
            info!("Task {} complete ({}ms total, {}ms CH insert, {} rows)", task_id, metrics.total_ms, metrics.ch_insert_ms, metrics.ch_rows_inserted);
            Ok::<_, anyhow::Error>(metrics)
        });
        handles.push(handle);
    }

    // Wait for all tasks and aggregate metrics
    let mut total_rows = 0usize;
    let mut combined_metrics = loader::IngestMetrics::default();
    let combined_start = std::time::Instant::now();

    for handle in handles {
        let metrics = handle.await??;
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
