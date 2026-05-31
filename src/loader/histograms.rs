//! STR histogram loader — streams TSV from GCS into lr_str_histograms.

use crate::clickhouse::ClickHouseInserter;
use crate::models::StrHistogramRow;
use genohype_core::io::get_reader;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use tracing::info;

/// Load STR allele frequency histograms from a TSV on GCS into ClickHouse.
///
/// The TSV has a header line. LocusId format: `{chrom_num}-{start}-{end}-{motif}`.
/// Population histogram columns start at index 15.
pub fn load_str_histograms(ch_url: &str, gcs_path: &str) -> anyhow::Result<usize> {
    info!("Loading STR histograms from {}", gcs_path);

    let raw_reader = get_reader(gcs_path)?;
    let reader = BufReader::new(raw_reader);

    let mut inserter = ClickHouseInserter::new(ch_url, "lr_str_histograms", 50_000);
    let mut count = 0usize;
    let mut header_cols: Option<Vec<String>> = None;
    let pop_start_idx: usize = 15;

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split('\t').collect();

        // First non-empty line is the header
        if header_cols.is_none() {
            header_cols = Some(parts.iter().map(|s| s.to_string()).collect());
            info!("STR histograms header: {} columns", parts.len());
            continue;
        }

        if parts.len() < 15 {
            continue;
        }

        // Parse LocusId: {chrom_num}-{start}-{end}-{motif}
        let locus_id = parts[0];
        let locus_parts: Vec<&str> = locus_id.splitn(4, '-').collect();
        if locus_parts.len() < 4 {
            continue;
        }

        let chrom = format!("chr{}", locus_parts[0]);
        let position: u32 = match locus_parts[1].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let end_position: u32 = match locus_parts[2].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let motif = locus_parts[3].to_string();

        // Build populations map from dynamic columns
        let mut populations = HashMap::new();
        if let Some(ref headers) = header_cols {
            for col_idx in pop_start_idx..parts.len() {
                if col_idx < headers.len() {
                    let pop_val = parts[col_idx];
                    if !pop_val.is_empty() && pop_val != "." {
                        populations.insert(headers[col_idx].clone(), pop_val.to_string());
                    }
                }
            }
        }

        let row = StrHistogramRow {
            chrom,
            position,
            end_position,
            motif,
            allele_size_histogram: parts.get(1).unwrap_or(&"").to_string(),
            biallelic_histogram: parts.get(2).unwrap_or(&"").to_string(),
            min_repeats: safe_float(parts.get(3).unwrap_or(&"")),
            mode_repeats: safe_float(parts.get(4).unwrap_or(&"")),
            mean_repeats: safe_float(parts.get(5).unwrap_or(&"")),
            stdev_repeats: safe_float(parts.get(6).unwrap_or(&"")),
            median_repeats: safe_float(parts.get(7).unwrap_or(&"")),
            p99_repeats: safe_float(parts.get(8).unwrap_or(&"")),
            max_repeats: safe_float(parts.get(9).unwrap_or(&"")),
            unique_allele_lengths: safe_u32(parts.get(10).unwrap_or(&"")),
            num_called_alleles: safe_u32(parts.get(11).unwrap_or(&"")),
            populations,
        };

        inserter.insert(&row)?;
        count += 1;

        if count % 100_000 == 0 {
            info!("STR histograms progress: {} rows inserted", count);
        }
    }

    inserter.finish()?;
    info!("STR histograms load complete: {} rows", count);
    Ok(count)
}

fn safe_float(val: &str) -> f32 {
    if val.is_empty() || val == "." {
        return 0.0;
    }
    val.parse().unwrap_or(0.0)
}

fn safe_u32(val: &str) -> u32 {
    if val.is_empty() || val == "." {
        return 0;
    }
    val.parse().unwrap_or(0)
}
