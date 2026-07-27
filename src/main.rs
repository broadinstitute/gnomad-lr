mod cli;
mod clickhouse;
mod domain;
mod loader;
mod models;
mod orchestrate;
mod pool;
pub mod y1;

#[cfg(not(feature = "clickhouse"))]
compile_error!("gnomad-lr requires the default `clickhouse` feature");

use clap::Parser;
use cli::{
    parse_region, Cli, Commands, LoadTarget, ServiceAction, Y1AuthSourceArg, Y1CohortArg,
    Y1InitArgs, Y1IntervalArgs, Y1TargetKindArg,
};
use genohype_pool::distributed::worker::{run_worker, WorkerConfig};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
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
        Commands::InitY1(args) => {
            tokio::task::spawn_blocking(move || {
                let target = y1_target(&args)?;
                info!("Initializing Y1 schema in {}", target.display_name());
                y1::init_schema(&target)
            })
            .await??;
        }
        Commands::LoadY1Interval(args) => {
            tokio::task::spawn_blocking(move || run_y1_interval(args)).await??;
        }
        Commands::ReconcileY1Metadata(args) => {
            tokio::task::spawn_blocking(move || {
                let target = y1_target(&args.target)?;
                let joins = y1::metadata::reconcile_and_publish(
                    &target,
                    &args.metadata_run_id,
                    &args.source_manifest,
                    &args.report,
                    &args.publisher_identity,
                    &args.carrier_run_id,
                )?;
                info!(
                    "accepted metadata run {} with {} carrier join validations",
                    args.metadata_run_id,
                    joins.len()
                );
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        }
        Commands::ActivateY1Metadata(args) | Commands::RollbackY1Metadata(args) => {
            tokio::task::spawn_blocking(move || {
                let target = y1_target(&args.target)?;
                let previous = y1::metadata::activate_metadata(
                    &target,
                    &args.metadata_run_id,
                    &args.activated_by,
                )?;
                info!(
                    "active metadata run is {}; previous run was {}",
                    args.metadata_run_id, previous
                );
                Ok::<_, anyhow::Error>(())
            })
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

fn y1_target(args: &Y1InitArgs) -> anyhow::Result<y1::ClickHouseTarget> {
    let kind = match args.target_kind {
        Y1TargetKindArg::Scratch => y1::TargetKind::Scratch,
        Y1TargetKindArg::Serving => y1::TargetKind::Serving,
    };
    let auth = match args.auth_source {
        Y1AuthSourceArg::None => y1::AuthSource::None,
        Y1AuthSourceArg::Environment => y1::AuthSource::Environment {
            username_variable: args.username_env.clone(),
            password_variable: args.password_env.clone(),
        },
    };
    y1::ClickHouseTarget::new(
        &args.endpoint,
        &args.database,
        kind,
        auth,
        args.allow_remote,
        args.allow_serving,
    )
}

fn run_y1_interval(args: Y1IntervalArgs) -> anyhow::Result<()> {
    let started = Instant::now();
    if args.batch_records == 0 {
        anyhow::bail!("--batch-records must be greater than zero");
    }
    let target = y1_target(&args.target)?;
    if target.kind() != y1::TargetKind::Scratch {
        anyhow::bail!("bounded Y1 source loads are restricted to scratch targets");
    }

    let cohort = match args.cohort {
        Y1CohortArg::HgsvcHprc => y1::Cohort::HgsvcHprc,
        Y1CohortArg::Aou => y1::Cohort::Aou,
    };
    let (chrom, start, stop) = parse_region(&args.region)?;
    let header_text = loader::vcf_reader::read_header_text(&args.vcf)?;
    let header = y1::Y1Header::parse(&header_text, cohort)?;

    let revision = y1_revision()?;
    let run_id = args.run_id.unwrap_or_else(|| {
        format!(
            "y1-{}-{}-{}-{}-{}",
            cohort.as_str(),
            chrom,
            start,
            stop,
            revision
        )
    });
    let context = y1::AttemptContext {
        run_id: run_id.clone(),
        task_id: format!("{chrom}-{start}-{stop}"),
        attempt_id: format!("attempt-{revision}"),
        cohort,
        chrom: chrom.clone(),
        interval_start: start,
        interval_end: stop,
    };

    let mut total_counts = y1::StagedCounts::default();
    let mut total_report = y1::TransformationReport::default();
    let mut record_offset = 0usize;
    let mut record_batch = Vec::with_capacity(args.batch_records);
    let records =
        loader::vcf_reader::VcfStream::open_region_required_index(&args.vcf, &chrom, start, stop)?
            .records();

    for record in records {
        record_batch.push(record?);
        if record_batch.len() == args.batch_records {
            stage_y1_record_batch(
                &target,
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
        stage_y1_record_batch(
            &target,
            &context,
            &header,
            &mut record_batch,
            &mut record_offset,
            &mut total_counts,
            &mut total_report,
        )?;
    }

    let counts = total_counts;
    let accepted = counts.rejects == 0 && counts.summaries == counts.source_records;
    let attempt = y1::TaskAttemptLedgerRow::new(
        &context,
        revision,
        if accepted {
            y1::AttemptState::Accepted
        } else {
            y1::AttemptState::Failed
        },
        counts,
        &total_report,
        if accepted {
            ""
        } else {
            "transformation validation failed"
        },
    )?;
    y1::record_task_attempt(&target, &attempt)?;

    let run = y1::LoadRunLedgerRow {
        run_id: run_id.clone(),
        revision,
        state: if accepted { "validated" } else { "rejected" }.to_string(),
        load_scope: y1::LoadScope::Interval.as_str().to_string(),
        release: y1::Release::Y1.as_str().to_string(),
        cohort: cohort.as_str().to_string(),
        reference_genome: header.reference_genome.as_str().to_string(),
        chrom: chrom.clone(),
        interval_start: start,
        interval_end: stop,
        source_uri: args.vcf.clone(),
        source_generation: args.source_generation.clone(),
        source_checksum_algorithm: "md5_base64".to_string(),
        source_checksum: args.source_checksum.clone(),
        source_index_uri: format!("{}.tbi", args.vcf),
        source_index_generation: args.index_generation.clone(),
        source_index_checksum: args.index_checksum.clone(),
        schema_version: y1::Y1_SCHEMA_VERSION,
        loader_version: env!("CARGO_PKG_VERSION").to_string(),
        expected_tasks: 1,
        expected_source_records: counts.source_records,
        summary_rows: counts.summaries,
        allele_rows: counts.alleles,
        frequency_rows: counts.frequencies,
        carrier_rows: counts.carriers,
        rejected_records: counts.rejects,
        created_at_ms: revision / 1_000_000,
        updated_at_ms: revision / 1_000_000,
        message: "strict bounded scratch load".to_string(),
    };
    y1::record_load_run(&target, &run)?;

    if accepted {
        let request = y1::PublicationRequest {
            run_id: run_id.clone(),
            scope: y1::LoadScope::Interval,
            release: y1::Release::Y1,
            cohort,
            reference_genome: header.reference_genome,
            chrom: chrom.clone(),
            interval_start: start,
            interval_end: stop,
            expected_tasks: 1,
            expected_counts: counts,
            source_uri: args.vcf.clone(),
            source_generation: args.source_generation.clone(),
            source_checksum: args.source_checksum.clone(),
        };
        y1::publish_staged_run(&target, &request)?;
    }

    let report = serde_json::json!({
        "run_id": run_id,
        "database": target.database(),
        "cohort": cohort,
        "region": { "chrom": chrom, "start": start, "stop": stop },
        "source": {
            "uri": args.vcf,
            "generation": args.source_generation,
            "checksum_algorithm": "md5_base64",
            "checksum": args.source_checksum,
            "index_generation": args.index_generation,
            "index_checksum": args.index_checksum
        },
        "accepted": accepted,
        "batch_records": args.batch_records,
        "counts": counts,
        "transformation": total_report,
        "elapsed_ms": started.elapsed().as_millis()
    });
    std::fs::write(&args.report_path, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);

    if !accepted {
        anyhow::bail!("Y1 interval rejected; see {}", args.report_path.display());
    }
    Ok(())
}

fn stage_y1_record_batch(
    target: &y1::ClickHouseTarget,
    context: &y1::AttemptContext,
    header: &y1::Y1Header,
    records: &mut Vec<String>,
    record_offset: &mut usize,
    total_counts: &mut y1::StagedCounts,
    total_report: &mut y1::TransformationReport,
) -> anyhow::Result<()> {
    let mut batch = y1::transform_records(header, records.iter().map(String::as_str));
    for reject in &mut batch.report.rejects {
        if let Some(record_number) = &mut reject.record_number {
            *record_number += *record_offset;
        }
    }

    let batch_counts = y1::stage_attempt(target, context, &batch)?;
    total_counts.source_records += batch_counts.source_records;
    total_counts.summaries += batch_counts.summaries;
    total_counts.alleles += batch_counts.alleles;
    total_counts.frequencies += batch_counts.frequencies;
    total_counts.carriers += batch_counts.carriers;
    total_counts.rejects += batch_counts.rejects;

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

fn y1_revision() -> anyhow::Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}

fn read_vcf_records(
    vcf_path: &str,
    chrom: &str,
    start: u32,
    stop: u32,
    limit: Option<usize>,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    domain::ensure_legacy_vcf_compatible(vcf_path)?;
    let stream = loader::vcf_reader::VcfStream::open_region(vcf_path, chrom, start, stop)?;
    let sample_names = stream.sample_names.clone();
    let records = stream
        .records()
        .take(limit.unwrap_or(usize::MAX))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((sample_names, records))
}

fn parse_optional_region(region: Option<&str>) -> anyhow::Result<Option<loader::RegionFilter>> {
    region.map(parse_region).transpose().map(|parsed| {
        parsed.map(|(chrom, start, stop)| loader::RegionFilter::new(chrom, start, stop))
    })
}

fn run_load(target: LoadTarget) -> anyhow::Result<()> {
    let mut metrics = loader::IngestMetrics::default();
    let task_start = std::time::Instant::now();

    match target {
        LoadTarget::Haplotypes(args) => {
            let (chrom, start, stop) = parse_region(&args.region)?;
            let vcf_path = args
                .vcf_path
                .or_else(|| domain::resolve_vcf_path(&chrom))
                .ok_or_else(|| anyhow::anyhow!("No VCF path for {}", chrom))?;
            info!(
                "Loading haplotypes from {} for {}:{}-{}",
                vcf_path, chrom, start, stop
            );
            let (sample_names, records) =
                read_vcf_records(&vcf_path, &chrom, start, stop, args.limit)?;
            info!("Buffered {} records", records.len());
            loader::haplotypes::load_haplotypes(
                &args.clickhouse_url,
                &records,
                &sample_names,
                &chrom,
                start,
                stop,
                &mut metrics,
            )?;
        }
        LoadTarget::Variants(args) => {
            let (chrom, start, stop) = parse_region(&args.region)?;
            let vcf_path = args
                .vcf_path
                .or_else(|| domain::resolve_vcf_path(&chrom))
                .ok_or_else(|| anyhow::anyhow!("No VCF path for {}", chrom))?;
            info!(
                "Loading variants from {} for {}:{}-{}",
                vcf_path, chrom, start, stop
            );
            let (sample_names, records) =
                read_vcf_records(&vcf_path, &chrom, start, stop, args.limit)?;
            info!("Buffered {} records", records.len());
            loader::variants::load_variants(
                &args.clickhouse_url,
                &records,
                &sample_names,
                &chrom,
                start,
                stop,
                &mut metrics,
            )?;
        }
        LoadTarget::All(args) => {
            let (chrom, start, stop) = parse_region(&args.region)?;
            let vcf_path = args
                .vcf_path
                .or_else(|| domain::resolve_vcf_path(&chrom))
                .ok_or_else(|| anyhow::anyhow!("No VCF path for {}", chrom))?;
            info!(
                "Loading all from {} for {}:{}-{}",
                vcf_path, chrom, start, stop
            );

            // Read the bounded VCF region once into memory.
            let (sample_names, records) =
                read_vcf_records(&vcf_path, &chrom, start, stop, args.limit)?;
            info!("Buffered {} records", records.len());

            loader::variants::load_variants(
                &args.clickhouse_url,
                &records,
                &sample_names,
                &chrom,
                start,
                stop,
                &mut metrics,
            )?;
            loader::haplotypes::load_haplotypes(
                &args.clickhouse_url,
                &records,
                &sample_names,
                &chrom,
                start,
                stop,
                &mut metrics,
            )?;
        }
        LoadTarget::Coverage(args) => {
            let region = parse_optional_region(args.region.as_deref())?;
            let count = loader::coverage::load_coverage(
                &args.clickhouse_url,
                &args.gcs_path,
                args.downsample,
                region.as_ref(),
                args.limit,
            )?;
            info!("Coverage: {} rows loaded", count);
            return Ok(());
        }
        LoadTarget::Metadata(args) => {
            let count = loader::metadata::load_sample_metadata(
                &args.clickhouse_url,
                &args.csv_url,
                args.limit,
            )?;
            info!("Metadata: {} rows loaded", count);
            return Ok(());
        }
        LoadTarget::Histograms(args) => {
            let region = parse_optional_region(args.region.as_deref())?;
            let count = loader::histograms::load_str_histograms(
                &args.clickhouse_url,
                &args.gcs_path,
                region.as_ref(),
                args.limit,
            )?;
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
                args.limit,
            )?;
            info!("Methylation: {} rows loaded", count);
            return Ok(());
        }
    }

    metrics.total_ms = task_start.elapsed().as_millis() as u64;
    info!(
        "Load complete: {}ms total, {}ms prescan, {}ms CH insert ({} flushes), {} rows",
        metrics.total_ms,
        metrics.prescan_ms,
        metrics.ch_insert_ms,
        metrics.ch_insert_count,
        metrics.ch_rows_inserted
    );

    Ok(())
}
