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
    Y1FinalizeArgs, Y1InitArgs, Y1IntervalArgs, Y1PhasedMethylationEvaluationArgs,
    Y1PhasedMethylationSmokeArgs, Y1TargetKindArg,
};
use genohype_pool::distributed::worker::{run_worker, WorkerConfig};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
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
        Commands::SmokeY1PhasedMethylation(args) => {
            tokio::task::spawn_blocking(move || run_y1_phased_methylation_smoke(args)).await??;
        }
        Commands::EvaluateY1PhasedMethylation(args) => {
            tokio::task::spawn_blocking(move || run_y1_phased_methylation_evaluation(args))
                .await??;
        }
        Commands::FinalizeY1Contig(args) => {
            tokio::task::spawn_blocking(move || run_y1_finalization(args, false)).await??;
        }
        Commands::FinalizeY1Chr22(args) => {
            tokio::task::spawn_blocking(move || run_y1_finalization(args, true)).await??;
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
                    // Genohype persists this exact assignment-time identity in
                    // terminal custom receipts; package version alone is not
                    // sufficient to accept a canary attempt.
                    build_version: Some(pool::WORKER_BUILD_IDENTITY.to_string()),
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
        Y1AuthSourceArg::PrivateNetwork => y1::AuthSource::PrivateNetwork,
        Y1AuthSourceArg::PasswordlessUser => anyhow::bail!(
            "passwordless-user requires a named worker principal; for finalization use an operator --auth-source plus --worker-auth-source passwordless-user"
        ),
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

fn worker_target(
    args: &Y1InitArgs,
    worker_auth_source: Option<Y1AuthSourceArg>,
    worker_principal: &str,
    username_env: &str,
    password_env: &str,
) -> anyhow::Result<y1::ClickHouseTarget> {
    let kind = match args.target_kind {
        Y1TargetKindArg::Scratch => y1::TargetKind::Scratch,
        Y1TargetKindArg::Serving => y1::TargetKind::Serving,
    };
    let auth = match worker_auth_source.unwrap_or(args.auth_source) {
        Y1AuthSourceArg::None => y1::AuthSource::None,
        Y1AuthSourceArg::PrivateNetwork => y1::AuthSource::PrivateNetwork,
        Y1AuthSourceArg::PasswordlessUser => y1::AuthSource::PasswordlessUser {
            username: worker_principal.to_string(),
        },
        Y1AuthSourceArg::Environment => y1::AuthSource::Environment {
            username_variable: username_env.to_string(),
            password_variable: password_env.to_string(),
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

fn run_y1_finalization(args: Y1FinalizeArgs, chr22_compatibility: bool) -> anyhow::Result<()> {
    let target = y1_target(&args.target)?;
    let worker = worker_target(
        &args.target,
        args.worker_auth_source,
        &args.worker_principal,
        &args.worker_username_env,
        &args.worker_password_env,
    )?;
    let fence = y1::WorkerWriteFence::new(&target, worker, &args.worker_principal)?;
    let report = if chr22_compatibility {
        y1::finalizer::finalize_chr22_run(
            &target,
            &fence,
            &args.manifest,
            &args.independent_counts,
            &args.operator_identity,
        )?
    } else {
        y1::finalizer::finalize_contig_run(
            &target,
            &fence,
            &args.manifest,
            &args.independent_counts,
            &args.operator_identity,
        )?
    };
    write_json_report(&args.report, &report)
}

fn write_json_report<T: serde::Serialize>(
    path: &std::path::Path,
    report: &T,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(report)?)?;
    println!("{}", serde_json::to_string_pretty(report)?);
    Ok(())
}

fn run_y1_phased_methylation_evaluation(
    args: Y1PhasedMethylationEvaluationArgs,
) -> anyhow::Result<()> {
    let target = y1::ClickHouseTarget::new(
        &args.endpoint,
        y1::PHASED_METHYLATION_EVALUATION_DATABASE,
        y1::TargetKind::Scratch,
        y1::AuthSource::Environment {
            username_variable: y1::Y1_WORKER_USERNAME_ENV.to_string(),
            password_variable: y1::Y1_WORKER_PASSWORD_ENV.to_string(),
        },
        args.allow_remote,
        false,
    )?;
    let mut report_file = reserve_new_json_report(&args.report_path)?;
    let receipt = match y1::run_phased_methylation_evaluation(&target) {
        Ok(receipt) => receipt,
        Err(error) => {
            drop(report_file);
            let _ = std::fs::remove_file(&args.report_path);
            return Err(error);
        }
    };
    write_reserved_json_report(&mut report_file, &receipt)
}

fn run_y1_phased_methylation_smoke(args: Y1PhasedMethylationSmokeArgs) -> anyhow::Result<()> {
    let target = y1::ClickHouseTarget::new(
        &args.endpoint,
        &args.database,
        y1::TargetKind::Scratch,
        y1::AuthSource::Environment {
            username_variable: y1::Y1_WORKER_USERNAME_ENV.to_string(),
            password_variable: y1::Y1_WORKER_PASSWORD_ENV.to_string(),
        },
        args.allow_remote,
        false,
    )?;
    let mut report_file = reserve_new_json_report(&args.report_path)?;
    let receipt = match y1::run_phased_methylation_smoke(&target) {
        Ok(receipt) => receipt,
        Err(error) => {
            drop(report_file);
            let _ = std::fs::remove_file(&args.report_path);
            return Err(error);
        }
    };
    write_reserved_json_report(&mut report_file, &receipt)
}

fn reserve_new_json_report(path: &std::path::Path) -> anyhow::Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            anyhow::anyhow!(
                "refusing to overwrite smoke receipt {}: {error}",
                path.display()
            )
        })
}

fn write_reserved_json_report<T: serde::Serialize>(
    file: &mut std::fs::File,
    report: &T,
) -> anyhow::Result<()> {
    use std::io::Write;

    let body = serde_json::to_vec_pretty(report)?;
    file.write_all(&body)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    println!("{}", String::from_utf8(body)?);
    Ok(())
}

fn run_y1_interval(args: Y1IntervalArgs) -> anyhow::Result<()> {
    if args.batch_records == 0 {
        anyhow::bail!("--batch-records must be greater than zero");
    }
    let target = worker_target(
        &args.target,
        args.worker_auth_source,
        &args.worker_principal,
        &args.worker_username_env,
        &args.worker_password_env,
    )?;
    if target.kind() != y1::TargetKind::Scratch {
        anyhow::bail!("bounded Y1 source loads are restricted to scratch targets");
    }
    let authenticated_worker_principal = target.attest_current_user(&args.worker_principal)?;
    target.attest_synchronous_inserts()?;

    let cohort = match args.cohort {
        Y1CohortArg::HgsvcHprc => y1::Cohort::HgsvcHprc,
        Y1CohortArg::Aou => y1::Cohort::Aou,
    };
    let (chrom, start, stop) = parse_region(&args.region)?;
    let revision = y1_revision()?;
    let run_id = args.run_id.clone().unwrap_or_else(|| {
        format!(
            "y1-{}-{}-{}-{}-{}",
            cohort.as_str(),
            chrom,
            start,
            stop,
            revision
        )
    });
    let task_id = format!("{chrom}-{start}-{stop}");
    let source_index_uri = format!("{}.tbi", args.vcf);
    let task = y1::PoolY1TaskSpec {
        coordinator_task_id: task_id.clone(),
        label: format!("direct {} {}:{}-{}", cohort.as_str(), chrom, start, stop),
        run_id: run_id.clone(),
        task_id,
        attempt_id: format!("attempt-{revision}"),
        release: y1::Release::Y1.as_str().to_string(),
        cohort: cohort.as_str().to_string(),
        reference_genome: y1::ReferenceGenome::Grch38.as_str().to_string(),
        chrom: chrom.clone(),
        start,
        stop,
        source_uri: args.vcf.clone(),
        source_generation: args.source_generation.clone(),
        source_checksum_algorithm: "md5_base64".to_string(),
        source_checksum: args.source_checksum.clone(),
        source_size_bytes: args.source_size_bytes,
        source_index_uri: source_index_uri.clone(),
        source_index_generation: args.index_generation.clone(),
        source_index_checksum_algorithm: "md5_base64".to_string(),
        source_index_checksum: args.index_checksum.clone(),
        source_index_size_bytes: args.index_size_bytes,
        retry_attempt_id: None,
        controlled_fail_once: None,
    };

    // Use the pool loader's claim, re-attestation, staging, and terminal-report
    // path so every durable direct-loader row has identical principal provenance.
    let attempt = y1::run_pool_interval_attempt(
        &target,
        &task,
        args.batch_records,
        &pool::worker_identity(),
        pool::WORKER_BUILD_IDENTITY,
        pool::BACKEND_REVISION,
        &authenticated_worker_principal,
    )?;
    let counts = attempt.counts;
    let run = y1::LoadRunLedgerRow {
        run_id: run_id.clone(),
        revision,
        state: "validated".to_string(),
        load_scope: y1::LoadScope::Interval.as_str().to_string(),
        release: y1::Release::Y1.as_str().to_string(),
        cohort: cohort.as_str().to_string(),
        reference_genome: y1::ReferenceGenome::Grch38.as_str().to_string(),
        chrom: chrom.clone(),
        interval_start: start,
        interval_end: stop,
        source_uri: args.vcf.clone(),
        source_generation: args.source_generation.clone(),
        source_checksum_algorithm: "md5_base64".to_string(),
        source_checksum: args.source_checksum.clone(),
        source_index_uri: source_index_uri.clone(),
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

    let report = direct_y1_report(
        &attempt,
        target.database(),
        args.batch_records,
        &args.source_checksum,
        &source_index_uri,
        &args.index_generation,
        &args.index_checksum,
        args.index_size_bytes,
    )?;
    write_json_report(&args.report_path, &report)
}

fn direct_y1_report(
    attempt: &y1::PoolY1AttemptReport,
    database: &str,
    batch_records: usize,
    source_checksum: &str,
    source_index_uri: &str,
    source_index_generation: &str,
    source_index_checksum: &str,
    source_index_size_bytes: u64,
) -> anyhow::Result<serde_json::Value> {
    let mut report = serde_json::to_value(attempt)?;
    let object = report
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("direct Y1 attempt report must serialize as an object"))?;
    object.insert("database".to_string(), serde_json::json!(database));
    object.insert(
        "accepted".to_string(),
        serde_json::json!(attempt.state == "accepted"),
    );
    object.insert(
        "batch_records".to_string(),
        serde_json::json!(batch_records),
    );
    object.insert(
        "region".to_string(),
        serde_json::json!({ "chrom": attempt.chrom, "start": attempt.start, "stop": attempt.stop }),
    );
    object.insert(
        "source".to_string(),
        serde_json::json!({
            "uri": attempt.source_uri,
            "generation": attempt.source_generation,
            "size_bytes": attempt.source_size_bytes,
            "checksum_algorithm": "md5_base64",
            "checksum": source_checksum,
            "index_uri": source_index_uri,
            "index_generation": source_index_generation,
            "index_checksum_algorithm": "md5_base64",
            "index_checksum": source_index_checksum,
            "index_size_bytes": source_index_size_bytes
        }),
    );
    Ok(report)
}

#[cfg(test)]
mod direct_y1_tests {
    use super::*;

    #[test]
    fn worker_target_exposes_named_passwordless_auth() {
        let target_args = Y1InitArgs {
            endpoint: "http://10.0.0.2:8123".into(),
            database: "gnomad_lr_y1_scratch_v5_unit".into(),
            target_kind: Y1TargetKindArg::Scratch,
            auth_source: Y1AuthSourceArg::PasswordlessUser,
            username_env: "IGNORED_USER".into(),
            password_env: "IGNORED_PASSWORD".into(),
            allow_remote: true,
            allow_serving: false,
        };
        worker_target(
            &target_args,
            None,
            "gnomad_lr_y1_pool_writer",
            "IGNORED_WORKER_USER",
            "IGNORED_WORKER_PASSWORD",
        )
        .unwrap();
        let error = worker_target(
            &target_args,
            None,
            "default",
            "IGNORED_WORKER_USER",
            "IGNORED_WORKER_PASSWORD",
        )
        .unwrap_err();
        assert!(error.to_string().contains("default ClickHouse principal"));
    }

    #[test]
    fn finalizer_can_separate_operator_and_passwordless_worker_auth() {
        let target_args = Y1InitArgs {
            endpoint: "http://127.0.0.1:8123".into(),
            database: "gnomad_lr_y1_scratch_v5_unit".into(),
            target_kind: Y1TargetKindArg::Scratch,
            auth_source: Y1AuthSourceArg::None,
            username_env: "IGNORED_USER".into(),
            password_env: "IGNORED_PASSWORD".into(),
            allow_remote: false,
            allow_serving: false,
        };
        y1_target(&target_args).unwrap();
        worker_target(
            &target_args,
            Some(Y1AuthSourceArg::PasswordlessUser),
            "gnomad_lr_y1_pool_writer",
            "IGNORED_WORKER_USER",
            "IGNORED_WORKER_PASSWORD",
        )
        .unwrap();
    }

    #[test]
    fn direct_loader_report_preserves_principal_bound_attempt_shape() {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        let attempt = y1::PoolY1AttemptReport {
            run_id: "run-1".into(),
            task_id: "chr22-1-10".into(),
            attempt_id: "attempt-1".into(),
            cohort: y1::Cohort::HgsvcHprc,
            chrom: "chr22".into(),
            start: 1,
            stop: 10,
            source_uri: "gs://gnomad-lr-data/y1/sources/hgsvc_hprc/vcfs/gnomAD_LR_Y1.hgsvc_hprc.chr22.vcf.gz".into(),
            source_generation: "123".into(),
            source_size_bytes: 456,
            source_checksum_algorithm: "md5_base64".into(),
            source_checksum: "source-md5".into(),
            source_index_uri: "gs://source.vcf.gz.tbi".into(),
            source_index_generation: "124".into(),
            source_index_size_bytes: 789,
            source_index_checksum_algorithm: "md5_base64".into(),
            source_index_checksum: "index-md5".into(),
            counts: y1::StagedCounts::default(),
            transformation: y1::TransformationReport::default(),
            inserted: y1::InsertStats::default(),
            started_at_ms: 1,
            finished_at_ms: 2,
            elapsed_ms: 1,
            parse_transform_insert_ms: 1,
            linux_peak_rss_bytes: None,
            worker_identity: "direct-worker".into(),
            worker_build_version: format!("gnomad-lr/{revision}/host-release"),
            backend_revision: revision.into(),
            worker_principal: "writer_a".into(),
            state: "accepted".into(),
            failure: None,
            published: false,
        };
        let report = direct_y1_report(
            &attempt,
            "gnomad_lr_y1_scratch_v5_test",
            250,
            "source-md5",
            "gs://source.vcf.gz.tbi",
            "124",
            "index-md5",
            789,
        )
        .unwrap();

        for field in [
            "worker_identity",
            "worker_build_version",
            "backend_revision",
            "worker_principal",
        ] {
            assert!(report.get(field).and_then(|value| value.as_str()).is_some());
        }
        assert_eq!(report["worker_principal"], "writer_a");
        assert_eq!(report["source"]["size_bytes"], 456);
        assert_eq!(report["source"]["index_generation"], "124");
        assert_eq!(report["source"]["index_size_bytes"], 789);
        assert_eq!(report["accepted"], true);
    }
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
