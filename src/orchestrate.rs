//! Orchestration logic for the `gnomad-lr run` command.
//!
//! Coordinates a two-phase distributed pipeline:
//! 1. Index: build .tbi files for any VCFs missing them
//! 2. Load: split chromosomes into region tasks and load into ClickHouse

use crate::cli::RunArgs;
use serde_json::json;
use std::process::Command;
use tracing::info;

const CHROMS: &[&str] = &[
    "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
    "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21",
    "chr22", "chrX", "chrY",
];

/// Approximate chromosome lengths (GRCh38) for region splitting.
fn chrom_length(chrom: &str) -> u32 {
    match chrom {
        "chr1" => 248_956_422,
        "chr2" => 242_193_529,
        "chr3" => 198_295_559,
        "chr4" => 190_214_555,
        "chr5" => 181_538_259,
        "chr6" => 170_805_979,
        "chr7" => 159_345_973,
        "chr8" => 145_138_636,
        "chr9" => 138_394_717,
        "chr10" => 133_797_422,
        "chr11" => 135_086_622,
        "chr12" => 133_275_309,
        "chr13" => 114_364_328,
        "chr14" => 107_043_718,
        "chr15" => 101_991_189,
        "chr16" => 90_338_345,
        "chr17" => 83_257_441,
        "chr18" => 80_373_285,
        "chr19" => 58_617_616,
        "chr20" => 64_444_167,
        "chr21" => 46_709_983,
        "chr22" => 50_818_468,
        "chrX" => 156_040_895,
        "chrY" => 57_227_415,
        _ => 250_000_000,
    }
}

fn vcf_path(base: &str, chrom: &str) -> String {
    format!("{}/{}.renamed.vcf.gz", base, chrom)
}

fn tbi_path(base: &str, chrom: &str) -> String {
    format!("{}/{}.renamed.vcf.gz.tbi", base, chrom)
}

/// Check which chromosomes are missing .tbi index files.
fn find_missing_indexes(base: &str, chroms: &[&str]) -> Vec<String> {
    let mut missing = Vec::new();
    for chrom in chroms {
        let tbi = tbi_path(base, chrom);
        let output = Command::new("gcloud")
            .args(["storage", "ls", &tbi])
            .output();
        match output {
            Ok(o) if o.status.success() => {
                info!("  {} .tbi exists", chrom);
            }
            _ => {
                info!("  {} .tbi MISSING", chrom);
                missing.push(chrom.to_string());
            }
        }
    }
    missing
}

/// Submit a job to the pool and wait for completion.
fn submit_and_wait(pool: &str, payload: &serde_json::Value, manifest_path: &str) -> anyhow::Result<()> {
    let payload_str = serde_json::to_string(payload)?;

    info!("Submitting job to pool '{}'...", pool);

    let status = Command::new("genohype")
        .args([
            "pool", "submit", pool, "--",
            "custom",
            "--payload", &payload_str,
            "--manifest", manifest_path,
        ])
        .status()?;

    if !status.success() {
        anyhow::bail!("Job submission failed");
    }

    Ok(())
}

pub fn run(args: &RunArgs) -> anyhow::Result<()> {
    let chroms: Vec<&str> = match &args.chroms {
        Some(selected) => {
            selected.iter().map(|s| {
                CHROMS.iter().find(|&&c| c == s.as_str())
                    .copied()
                    .unwrap_or_else(|| panic!("Unknown chromosome: {}", s))
            }).collect()
        }
        None => CHROMS.to_vec(),
    };

    info!("gnomad-lr distributed pipeline");
    info!("  Pool: {}", args.pool);
    info!("  VCF base: {}", args.vcf_base);
    info!("  Chromosomes: {}", chroms.len());
    info!("  Region size: {}bp", args.region_size);

    // Phase 1: Indexing
    if !args.skip_index {
        info!("");
        info!("Phase 1: Checking for tabix indexes...");
        let missing = find_missing_indexes(&args.vcf_base, &chroms);

        if missing.is_empty() {
            info!("All {} indexes present, skipping indexing phase.", chroms.len());
        } else {
            info!("{} VCFs need indexing: {:?}", missing.len(), missing);

            // Build index manifest
            let manifest: Vec<serde_json::Value> = missing
                .iter()
                .map(|chrom| {
                    json!({
                        "action": "index",
                        "vcf_path": vcf_path(&args.vcf_base, chrom),
                        "label": format!("index {}", chrom),
                    })
                })
                .collect();

            let manifest_path = "/tmp/gnomad-lr-index-manifest.json";
            std::fs::write(manifest_path, serde_json::to_string_pretty(&manifest)?)?;

            let payload = json!({ "action": "index" });
            submit_and_wait(&args.pool, &payload, manifest_path)?;

            info!("Indexing phase complete.");
        }
    }

    if args.index_only {
        info!("--index-only specified, stopping after indexing.");
        return Ok(());
    }

    // Phase 2: Load
    info!("");
    info!("Phase 2: Building region-based load manifest...");

    let mut tasks: Vec<serde_json::Value> = Vec::new();

    for chrom in &chroms {
        let len = chrom_length(chrom);
        let mut start: u32 = 0;
        while start < len {
            let stop = (start + args.region_size).min(len);
            tasks.push(json!({
                "chrom": chrom,
                "vcf_path": vcf_path(&args.vcf_base, chrom),
                "start": start,
                "stop": stop,
                "label": format!("{}:{}-{}", chrom, start / 1_000_000, stop / 1_000_000),
            }));
            start = stop;
        }
    }

    info!("Generated {} region tasks across {} chromosomes", tasks.len(), chroms.len());

    let manifest_path = "/tmp/gnomad-lr-load-manifest.json";
    std::fs::write(manifest_path, serde_json::to_string_pretty(&tasks)?)?;

    let payload = json!({
        "action": "load",
        "clickhouse_url": args.clickhouse_url,
    });
    submit_and_wait(&args.pool, &payload, manifest_path)?;

    info!("Load phase complete.");
    Ok(())
}
