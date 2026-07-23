mod cli;
mod clickhouse;
mod domain;
mod loader;
mod models;
mod orchestrate;
mod pool;

#[cfg(not(feature = "clickhouse"))]
compile_error!("gnomad-lr requires the default `clickhouse` feature");

use clap::Parser;
use cli::{Cli, Commands, LoadTarget, ServiceAction, parse_region};
use genohype_pool::distributed::worker::{run_worker, WorkerConfig};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gnomad_lr=info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Load { target } => {
            tokio::task::spawn_blocking(move || run_load(target)).await??;
        }
        Commands::Init(args) => {
            tokio::task::spawn_blocking(move || clickhouse::init_tables(&args.clickhouse_url))
                .await??;
        }
        Commands::Run(args) => {
            orchestrate::run(&args)?;
        }
        Commands::Service { action } => match action {
            ServiceAction::StartWorker {
                url,
                worker_id,
                poll_interval,
            } => {
                let handler = Arc::new(pool::LrTaskHandler);
                let config = WorkerConfig {
                    worker_id,
                    coordinator_url: url,
                    poll_interval_ms: poll_interval,
                    build_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    ..Default::default()
                };
                run_worker(config, handler).await?;
            }
            ServiceAction::StartCoordinator { .. } => {
                eprintln!("This binary is a worker-only implementation.");
                eprintln!("Use the standard genohype coordinator (pool create).");
                std::process::exit(1);
            }
        },
    }

    Ok(())
}

fn run_load(target: LoadTarget) -> anyhow::Result<()> {
    use loader::vcf_reader::VcfStream;

    let mut metrics = loader::IngestMetrics::default();
    let task_start = std::time::Instant::now();

    match target {
        LoadTarget::Haplotypes(args) => {
            let (chrom, start, stop) = parse_region(&args.region)?;
            let vcf_path = args.vcf_path.or_else(|| domain::resolve_vcf_path(&chrom));
            let vcf_path = vcf_path.ok_or_else(|| anyhow::anyhow!("No VCF path for {}", chrom))?;
            info!("Loading haplotypes from {} for {}:{}-{}", vcf_path, chrom, start, stop);
            let stream = VcfStream::open_region(&vcf_path, &chrom, start, stop)?;
            let sample_names = stream.sample_names.clone();
            let records: Vec<String> = stream.records().collect();
            info!("Buffered {} records", records.len());
            loader::haplotypes::load_haplotypes(&args.clickhouse_url, &records, &sample_names, &chrom, start, stop, &mut metrics)?;
        }
        LoadTarget::Variants(args) => {
            let (chrom, start, stop) = parse_region(&args.region)?;
            let vcf_path = args.vcf_path.or_else(|| domain::resolve_vcf_path(&chrom));
            let vcf_path = vcf_path.ok_or_else(|| anyhow::anyhow!("No VCF path for {}", chrom))?;
            info!("Loading variants from {} for {}:{}-{}", vcf_path, chrom, start, stop);
            let stream = VcfStream::open_region(&vcf_path, &chrom, start, stop)?;
            let sample_names = stream.sample_names.clone();
            let records: Vec<String> = stream.records().collect();
            info!("Buffered {} records", records.len());
            loader::variants::load_variants(&args.clickhouse_url, &records, &sample_names, &chrom, start, stop, &mut metrics)?;
        }
        LoadTarget::All(args) => {
            let (chrom, start, stop) = parse_region(&args.region)?;
            let vcf_path = args.vcf_path.or_else(|| domain::resolve_vcf_path(&chrom));
            let vcf_path = vcf_path.ok_or_else(|| anyhow::anyhow!("No VCF path for {}", chrom))?;
            info!("Loading all from {} for {}:{}-{}", vcf_path, chrom, start, stop);

            // Read VCF region once into memory
            let stream = VcfStream::open_region(&vcf_path, &chrom, start, stop)?;
            let sample_names = stream.sample_names.clone();
            let records: Vec<String> = stream.records().collect();
            info!("Buffered {} records", records.len());

            // Load variants first (includes prescan)
            loader::variants::load_variants(&args.clickhouse_url, &records, &sample_names, &chrom, start, stop, &mut metrics)?;

            // Load haplotypes
            loader::haplotypes::load_haplotypes(&args.clickhouse_url, &records, &sample_names, &chrom, start, stop, &mut metrics)?;
        }
        LoadTarget::Coverage(args) => {
            let count = loader::coverage::load_coverage(&args.clickhouse_url, &args.gcs_path, args.downsample)?;
            info!("Coverage: {} rows loaded", count);
            return Ok(());
        }
        LoadTarget::Metadata(args) => {
            let count = loader::metadata::load_sample_metadata(&args.clickhouse_url, &args.csv_url)?;
            info!("Metadata: {} rows loaded", count);
            return Ok(());
        }
        LoadTarget::Histograms(args) => {
            let count = loader::histograms::load_str_histograms(&args.clickhouse_url, &args.gcs_path)?;
            info!("Histograms: {} rows loaded", count);
            return Ok(());
        }
        LoadTarget::Methylation(args) => {
            let count = loader::methylation::load_methylation(
                &args.clickhouse_url,
                &args.bed_path,
                &args.sample_id,
                &args.chrom,
                args.start,
                args.stop,
            )?;
            info!("Methylation: {} rows loaded", count);
            return Ok(());
        }
    }

    metrics.total_ms = task_start.elapsed().as_millis() as u64;
    info!("Load complete: {}ms total, {}ms prescan, {}ms CH insert ({} flushes), {} rows",
        metrics.total_ms, metrics.prescan_ms, metrics.ch_insert_ms, metrics.ch_insert_count, metrics.ch_rows_inserted);

    Ok(())
}
