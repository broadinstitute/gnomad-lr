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
    /// Initialize the current legacy-contract schema (not the Y1 v2 schema)
    Init(InitArgs),
    /// Initialize isolated cohort-aware Y1 v2 tables
    InitY1(Y1InitArgs),
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
    use super::parse_region;

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
}
