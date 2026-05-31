use clap::{Args, Parser, Subcommand};

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
    /// Run the full distributed pipeline (index → load) on a pool
    Run(RunArgs),
}

#[derive(Args, Clone)]
pub struct RunArgs {
    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://192.168.0.6:8123")]
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
    /// Load per-sample haplotype rows from VCF
    Haplotypes(LoadArgs),
    /// Load site-level variant rows from VCF
    Variants(LoadArgs),
    /// Load both haplotypes and variants
    All(LoadArgs),
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
    #[arg(long, default_value = "http://localhost:8123")]
    pub clickhouse_url: String,
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
    Ok((chrom, start, stop))
}
