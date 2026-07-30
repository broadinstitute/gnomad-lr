use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "gnomad-lr",
    version,
    about = "gnomAD Long Read VCF loader for ClickHouse"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Load data into ClickHouse
    Load {
        #[command(subcommand)]
        target: LoadTarget,
    },
    /// Pool service commands (worker/coordinator)
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    /// Initialize the current legacy-contract schema (not the Y1 v4 schema)
    Init(InitArgs),
    /// Strictly create fresh isolated Y1 v4 tables or verify an exact attested schema
    InitY1(Y1InitArgs),
    /// Strict bounded Y1 source load into an isolated scratch database
    LoadY1Interval(Y1IntervalArgs),
    /// Fence, verify, digest, and freeze one canonical Y1 GRCh38 chr1-22/X/Y candidate in place
    FinalizeY1Contig(Y1FinalizeArgs),
    /// Backward-compatible chr22-only finalization command
    FinalizeY1Chr22(Y1FinalizeArgs),
    /// Materialize and signature-verify one published scratch GRCh38 contig
    MaterializeY1Contig(Y1MaterializeArgs),
    /// Backward-compatible chr22-only materialization command
    MaterializeY1Chr22(Y1MaterializeArgs),
    /// Activate one validated, published full-contig Y1 primary run
    ActivateY1Contig(Y1PrimaryPointerArgs),
    /// Backward-compatible chr22-only activation command
    ActivateY1Chr22(Y1PrimaryPointerArgs),
    /// Roll back one Y1 contig pointer to its recorded previous run
    RollbackY1Contig(Y1PrimaryPointerArgs),
    /// Backward-compatible chr22-only rollback command
    RollbackY1Chr22(Y1PrimaryPointerArgs),
    /// Reconcile and publish an immutable HGSVC/HPRC Y1 metadata candidate
    ReconcileY1Metadata(Y1MetadataArgs),
    /// Activate an accepted Y1 metadata run on a serving target
    ActivateY1Metadata(Y1MetadataPointerArgs),
    /// Roll back the active pointer to a previously accepted Y1 metadata run
    RollbackY1Metadata(Y1MetadataPointerArgs),
    /// Run the legacy distributed VCF pipeline (not compatible with Y1 inputs)
    Run(RunArgs),
}

#[derive(Args, Clone)]
pub struct InitArgs {
    /// ClickHouse HTTP URL. A `database` query parameter selects an isolated database.
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Y1TargetKindArg {
    Scratch,
    Serving,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Y1AuthSourceArg {
    None,
    PrivateNetwork,
    Environment,
}

#[derive(Args, Clone)]
pub struct Y1InitArgs {
    /// ClickHouse HTTP endpoint without credentials, path, or query parameters
    #[arg(long)]
    pub endpoint: String,

    /// Explicit isolated Y1 database (never `default`)
    #[arg(long)]
    pub database: String,

    /// Safety class for database-name validation
    #[arg(long, value_enum)]
    pub target_kind: Y1TargetKindArg,

    /// Credential source; environment credentials are resolved only per request
    #[arg(long, value_enum)]
    pub auth_source: Y1AuthSourceArg,

    /// Environment variable containing the ClickHouse username
    #[arg(long, default_value = "Y1_CLICKHOUSE_USER")]
    pub username_env: String,

    /// Environment variable containing the ClickHouse password
    #[arg(long, default_value = "Y1_CLICKHOUSE_PASSWORD")]
    pub password_env: String,

    /// Acknowledge that the endpoint is not loopback
    #[arg(long)]
    pub allow_remote: bool,

    /// Acknowledge schema operations against a serving-class Y1 database
    #[arg(long)]
    pub allow_serving: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Y1CohortArg {
    HgsvcHprc,
    Aou,
}

#[derive(Args, Clone)]
pub struct Y1IntervalArgs {
    #[command(flatten)]
    pub target: Y1InitArgs,

    /// Source cohort; AoU is enforced as summary-only by the transformer
    #[arg(long, value_enum)]
    pub cohort: Y1CohortArg,

    /// Generation-pinned VCF URI with an adjacent .tbi object
    #[arg(long)]
    pub vcf: String,

    #[arg(long)]
    pub source_generation: String,

    /// Base64 MD5 of the source VCF
    #[arg(long)]
    pub source_checksum: String,

    #[arg(long)]
    pub index_generation: String,

    /// Base64 MD5 of the adjacent TBI
    #[arg(long)]
    pub index_checksum: String,

    /// Inclusive one-based interval, for example chr22:20000000-20010000
    #[arg(long)]
    pub region: String,

    /// Maximum source records transformed and staged per bounded batch
    #[arg(long, default_value_t = 250)]
    pub batch_records: usize,

    /// Optional explicit run ID; generated when omitted
    #[arg(long)]
    pub run_id: Option<String>,

    /// Machine-readable validation report destination
    #[arg(long)]
    pub report_path: std::path::PathBuf,
}

#[derive(Args, Clone)]
pub struct Y1FinalizeArgs {
    #[command(flatten)]
    pub target: Y1InitArgs,

    /// Checked, deterministic single-contig Genohype task manifest
    #[arg(long)]
    pub manifest: std::path::PathBuf,

    /// Independently derived expected-count JSON (not loader output)
    #[arg(long)]
    pub independent_counts: std::path::PathBuf,

    /// Human or service identity recorded in the immutable run ledger
    #[arg(long)]
    pub operator_identity: String,

    /// Machine-readable finalization report destination
    #[arg(long)]
    pub report: std::path::PathBuf,
}

#[derive(Args, Clone)]
pub struct Y1MaterializeArgs {
    #[command(flatten)]
    pub target: Y1InitArgs,

    /// Isolated scratch database on the same ClickHouse server
    #[arg(long)]
    pub scratch_database: String,

    #[arg(long)]
    pub run_id: String,

    #[arg(long, value_enum)]
    pub cohort: Y1CohortArg,

    /// Canonical GRCh38 contig (required by the generic command; legacy command fixes chr22)
    #[arg(long)]
    pub chrom: Option<String>,

    #[arg(long)]
    pub operator_identity: String,

    /// Machine-readable, activation-consumable acceptance report
    #[arg(long)]
    pub report: std::path::PathBuf,
}

#[derive(Args, Clone)]
pub struct Y1PrimaryPointerArgs {
    #[command(flatten)]
    pub target: Y1InitArgs,

    /// Published full-contig run to activate/restore; with --restore-absence, the current run whose acceptance authorizes the tombstone
    #[arg(long)]
    pub run_id: String,

    /// Roll back an initial activation to its recorded prior absent state
    #[arg(
        long,
        requires = "expected_current_run_id",
        conflicts_with = "expect_no_current"
    )]
    pub restore_absence: bool,

    /// Cohort partition whose primary pointer will change
    #[arg(long, value_enum)]
    pub cohort: Y1CohortArg,

    /// Canonical GRCh38 contig (required by generic commands; legacy commands fix chr22)
    #[arg(long)]
    pub chrom: Option<String>,

    /// Acceptance report emitted by the matching materialization command for this serving database/run/cohort/contig
    #[arg(long)]
    pub acceptance: std::path::PathBuf,

    /// Expected current run. Omit only together with --expect-no-current.
    #[arg(long, conflicts_with = "expect_no_current")]
    pub expected_current_run_id: Option<String>,

    /// Expected current pointer revision. Required with --expected-current-run-id.
    #[arg(long, requires = "expected_current_run_id")]
    pub expected_current_revision: Option<u64>,

    /// Explicitly assert that no current pointer exists
    #[arg(long, conflicts_with_all = ["expected_current_run_id", "expected_current_revision"])]
    pub expect_no_current: bool,

    /// Human or service identity recorded in the append-only pointer ledger
    #[arg(long)]
    pub operator_identity: String,

    /// Validate and emit the proposed change without appending a pointer revision
    #[arg(long)]
    pub dry_run: bool,

    /// Machine-readable activation or rollback report destination
    #[arg(long)]
    pub report: std::path::PathBuf,
}

#[derive(Args, Clone)]
pub struct Y1MetadataArgs {
    #[command(flatten)]
    pub target: Y1InitArgs,

    #[arg(long)]
    pub metadata_run_id: String,

    /// Checked JSON manifest containing all immutable source identities and checksums
    #[arg(long)]
    pub source_manifest: std::path::PathBuf,

    /// Full stable JSON report path; compact JSON and audit JSONL are written alongside it
    #[arg(long)]
    pub report: std::path::PathBuf,

    /// Human or service identity recorded in the immutable run ledger
    #[arg(long)]
    pub publisher_identity: String,

    /// Accepted carrier run IDs to validate; repeat for 10 kb, 1 Mb, and chr22 runs
    #[arg(long)]
    pub carrier_run_id: Vec<String>,
}

#[derive(Args, Clone)]
pub struct Y1MetadataPointerArgs {
    #[command(flatten)]
    pub target: Y1InitArgs,

    /// Accepted metadata run to make active (for rollback, the prior run ID)
    #[arg(long)]
    pub metadata_run_id: String,

    #[arg(long)]
    pub activated_by: String,
}

#[derive(Args, Clone)]
pub struct RunArgs {
    /// ClickHouse HTTP URL reachable from pool workers (must be explicit)
    #[arg(long)]
    pub clickhouse_url: String,

    /// Pool name
    #[arg(long, default_value = "lr")]
    pub pool: String,

    /// GCS base path for VCFs
    #[arg(long, default_value = "gs://gnomad-lr-data/vcf/v3")]
    pub vcf_base: String,

    /// Region size in base pairs for splitting chromosomes into tasks
    #[arg(long, default_value = "2000000")]
    pub region_size: u32,

    /// Only run on specific chromosomes (e.g. chr22,chrX)
    #[arg(long, value_delimiter = ',')]
    pub chroms: Option<Vec<String>>,

    /// Skip the indexing phase (assume .tbi files exist)
    #[arg(long)]
    pub skip_index: bool,

    /// Only run the indexing phase
    #[arg(long)]
    pub index_only: bool,
}

#[derive(Subcommand)]
pub enum ServiceAction {
    /// Start as a distributed worker
    StartWorker {
        /// Coordinator URL
        #[arg(long)]
        url: String,
        /// Unique worker ID
        #[arg(long)]
        worker_id: String,
        /// Poll interval in milliseconds
        #[arg(long, default_value = "2000")]
        poll_interval: u64,
    },
    /// Start as a coordinator (not supported — use genohype coordinator)
    StartCoordinator {
        #[arg(long, default_value = "3000")]
        port: u16,
        #[arg(long)]
        db_path: Option<String>,
        #[arg(long)]
        backup_path: Option<String>,
        #[arg(long)]
        input: Option<String>,
        #[arg(long)]
        output: Option<String>,
        #[arg(long)]
        total_partitions: Option<usize>,
        #[arg(long)]
        batch_size: Option<usize>,
        #[arg(long)]
        timeout: Option<u64>,
        #[arg(long)]
        pool_name: Option<String>,
        #[arg(long)]
        gcp_project: Option<String>,
        #[arg(long)]
        gcp_zone: Option<String>,
        #[arg(long)]
        cluster_machine_type: Option<String>,
        #[arg(long)]
        cluster_spot: Option<bool>,
        #[arg(long)]
        cluster_network: Option<String>,
        #[arg(long)]
        cluster_subnet: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum LoadTarget {
    /// Load legacy per-sample haplotype rows (not compatible with Y1 inputs)
    Haplotypes(LoadArgs),
    /// Load legacy site-level variant rows (not compatible with Y1 inputs)
    Variants(LoadArgs),
    /// Load both legacy primary tables (not compatible with Y1 inputs)
    All(LoadArgs),
    /// Load coverage data from GCS TSV.gz
    Coverage(CoverageArgs),
    /// Load sample metadata from HPRC CSV
    Metadata(MetadataArgs),
    /// Load STR allele frequency histograms from GCS TSV
    Histograms(HistogramsArgs),
    /// Load methylation data from tabix-indexed BED
    Methylation(MethylationArgs),
}

#[derive(Args, Clone)]
pub struct CoverageArgs {
    /// GCS path to coverage TSV.gz
    #[arg(
        long,
        default_value = "gs://gnomad-v4-data-pipeline/inputs/secondary-analyses/gnomAD-LR/v2/hgsvc_hprc.coverage.tsv.gz"
    )]
    pub gcs_path: String,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,

    /// Downsample step (minimum bp spacing between retained rows)
    #[arg(long, default_value = "1")]
    pub downsample: u32,

    /// Optional genomic region to retain (for example chr22:20000000-21000000)
    #[arg(long)]
    pub region: Option<String>,

    /// Stop after inserting this many source rows (intended for smoke tests)
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Args, Clone)]
pub struct MetadataArgs {
    /// URL for the HPRC sample metadata CSV
    #[arg(
        long,
        default_value = "https://raw.githubusercontent.com/human-pangenomics/hprc_intermediate_assembly/main/data_tables/sample/hprc_release2_sample_metadata.csv"
    )]
    pub csv_url: String,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,

    /// Stop after inserting this many metadata rows (intended for smoke tests)
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Args, Clone)]
pub struct HistogramsArgs {
    /// GCS path to STR histograms TSV
    #[arg(
        long,
        default_value = "gs://gnomad-v4-data-pipeline/inputs/secondary-analyses/gnomAD-LR/v2/hgsvc_hprc.af_histograms.tsv"
    )]
    pub gcs_path: String,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,

    /// Optional genomic region to retain (for example chr22:20000000-21000000)
    #[arg(long)]
    pub region: Option<String>,

    /// Stop after inserting this many source rows (intended for smoke tests)
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Args, Clone)]
pub struct MethylationArgs {
    /// GCS path to tabix-indexed BED file
    #[arg(long)]
    pub bed_path: String,

    /// Sample ID
    #[arg(long)]
    pub sample_id: String,

    /// Chromosome (e.g. chr22)
    #[arg(long)]
    pub chrom: String,

    /// Start position
    #[arg(long, default_value = "0")]
    pub start: u32,

    /// Stop position
    #[arg(long, default_value = "400000000")]
    pub stop: u32,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,

    /// Stop after inserting this many BED rows (intended for smoke tests)
    #[arg(long)]
    pub limit: Option<usize>,
}

#[derive(Args, Clone)]
pub struct LoadArgs {
    /// Genomic region (e.g. chr22:20000000-21000000)
    #[arg(long)]
    pub region: String,

    /// VCF path (GCS URL). Defaults to auto-resolve from region chromosome.
    #[arg(long)]
    pub vcf_path: Option<String>,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://127.0.0.1:8123")]
    pub clickhouse_url: String,

    /// Stop after reading this many VCF records (intended for smoke tests)
    #[arg(long)]
    pub limit: Option<usize>,
}

/// Parse a region string like "chr22:20000000-21000000" into (chrom, start, stop).
/// Returns the full chrom (e.g. "chr22"), start position, and stop position.
pub fn parse_region(region: &str) -> anyhow::Result<(String, u32, u32)> {
    let region = region.replace(",", "");
    let parts: Vec<&str> = region.split(':').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid region format: expected chr:start-end");
    }
    let chrom = if parts[0].starts_with("chr") {
        parts[0].to_string()
    } else {
        format!("chr{}", parts[0])
    };
    let range_parts: Vec<&str> = parts[1].split('-').collect();
    if range_parts.len() != 2 {
        anyhow::bail!("Invalid region format: expected chr:start-end");
    }
    let start: u32 = range_parts[0]
        .replace("M", "000000")
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid start position: {}", e))?;
    let stop: u32 = range_parts[1]
        .replace("M", "000000")
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid stop position: {}", e))?;
    if start > stop {
        anyhow::bail!(
            "Invalid region: start ({}) is greater than stop ({})",
            start,
            stop
        );
    }
    Ok((chrom, start, stop))
}

#[cfg(test)]
mod tests {
    use super::{parse_region, Cli, Commands};
    use clap::Parser;

    #[test]
    fn parses_and_normalizes_region() {
        assert_eq!(
            parse_region("22:20,000,000-21,000,000").unwrap(),
            ("chr22".to_string(), 20_000_000, 21_000_000)
        );
    }

    #[test]
    fn rejects_reversed_region() {
        assert!(parse_region("chr22:21-20").is_err());
    }

    #[test]
    fn parses_generic_per_contig_finalization() {
        let cli = Cli::try_parse_from([
            "gnomad-lr",
            "finalize-y1-contig",
            "--endpoint",
            "http://127.0.0.1:8123",
            "--database",
            "gnomad_lr_y1_scratch_unit",
            "--target-kind",
            "scratch",
            "--auth-source",
            "none",
            "--manifest",
            "chr1.json",
            "--independent-counts",
            "chr1-counts.json",
            "--operator-identity",
            "unit-test",
            "--report",
            "chr1-report.json",
        ])
        .unwrap();
        assert!(matches!(cli.command, Commands::FinalizeY1Contig(_)));
    }

    #[test]
    fn parses_guarded_primary_activation_plan() {
        let cli = Cli::try_parse_from([
            "gnomad-lr",
            "activate-y1-chr22",
            "--endpoint",
            "http://127.0.0.1:8123",
            "--database",
            "gnomad_lr_y1_serving_unit",
            "--target-kind",
            "serving",
            "--auth-source",
            "none",
            "--allow-serving",
            "--run-id",
            "run-r2",
            "--cohort",
            "aou",
            "--acceptance",
            "acceptance.json",
            "--expect-no-current",
            "--operator-identity",
            "unit-test",
            "--dry-run",
            "--report",
            "activation.json",
        ])
        .unwrap();
        let Commands::ActivateY1Chr22(args) = cli.command else {
            panic!("expected primary activation command");
        };
        assert!(args.dry_run);
        assert_eq!(args.run_id, "run-r2");
        assert!(args.expect_no_current);
        assert_eq!(args.acceptance, std::path::PathBuf::from("acceptance.json"));
    }

    #[test]
    fn absence_rollback_requires_a_guarded_current_token() {
        let result = Cli::try_parse_from([
            "gnomad-lr",
            "rollback-y1-chr22",
            "--endpoint",
            "http://127.0.0.1:8123",
            "--database",
            "gnomad_lr_y1_serving_unit",
            "--target-kind",
            "serving",
            "--auth-source",
            "none",
            "--allow-serving",
            "--run-id",
            "run-r1",
            "--restore-absence",
            "--cohort",
            "aou",
            "--acceptance",
            "acceptance.json",
            "--operator-identity",
            "unit-test",
            "--report",
            "rollback.json",
        ]);
        assert!(result.is_err());
    }
}
