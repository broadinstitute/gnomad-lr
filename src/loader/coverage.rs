//! Coverage loader — streams gzipped TSV from GCS into lr_coverage.

use crate::clickhouse::ClickHouseInserter;
use crate::loader::RegionFilter;
use crate::models::CoverageRow;
use flate2::read::MultiGzDecoder;
use genohype_core::io::get_reader;
use std::io::{BufRead, BufReader};
use tracing::info;

/// Load coverage data from a gzipped TSV on GCS into ClickHouse.
///
/// The TSV is headerless with columns:
/// chrom, pos, pos2, mean, median, total_bases, over_1, over_5, over_10,
/// over_15, over_20, over_25, over_30, over_50, over_100
///
/// `downsample` controls the minimum base-pair spacing between retained rows.
/// Set to 1 to keep all rows. `region` and `limit` provide deterministic,
/// bounded reads for development smoke tests.
pub fn load_coverage(
    ch_url: &str,
    gcs_path: &str,
    downsample: u32,
    region: Option<&RegionFilter>,
    limit: Option<usize>,
) -> anyhow::Result<usize> {
    let downsample = downsample.max(1);
    info!(
        "Loading coverage from {} (downsample={}, region={:?}, limit={:?})",
        gcs_path, downsample, region, limit
    );

    let raw_reader = get_reader(gcs_path)?;
    let gz_decoder = MultiGzDecoder::new(raw_reader);
    let reader = BufReader::new(gz_decoder);

    let mut inserter = ClickHouseInserter::new(ch_url, "lr_coverage", 50_000);
    let mut count = 0usize;
    let mut last_pos: i64 = -(downsample as i64);
    let mut last_chrom = String::new();

    for line_result in reader.lines() {
        if limit.is_some_and(|max| count >= max) {
            break;
        }

        let line = line_result?;
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 15 {
            continue;
        }

        let chrom = parts[0];
        let pos: u32 = match parts[1].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };

        if region.is_some_and(|filter| !filter.contains(chrom, pos)) {
            continue;
        }

        // Reset downsample tracking on chromosome change
        if chrom != last_chrom {
            last_chrom = chrom.to_string();
            last_pos = -(downsample as i64);
        }

        // Downsample: only keep rows spaced at least `downsample` bp apart
        if (pos as i64) - last_pos < downsample as i64 {
            continue;
        }
        last_pos = pos as i64;

        let row = CoverageRow {
            chrom: chrom.to_string(),
            pos,
            mean: parts[3].parse().unwrap_or(0.0),
            median: parts[4].parse().unwrap_or(0.0),
            over_1: parts[6].parse().unwrap_or(0.0),
            over_5: parts[7].parse().unwrap_or(0.0),
            over_10: parts[8].parse().unwrap_or(0.0),
            over_15: parts[9].parse().unwrap_or(0.0),
            over_20: parts[10].parse().unwrap_or(0.0),
            over_25: parts[11].parse().unwrap_or(0.0),
            over_30: parts[12].parse().unwrap_or(0.0),
            over_50: parts[13].parse().unwrap_or(0.0),
            over_100: parts[14].parse().unwrap_or(0.0),
        };

        inserter.insert(&row)?;
        count += 1;

        if count % 500_000 == 0 {
            info!("Coverage progress: {} rows inserted", count);
        }
    }

    inserter.finish()?;
    info!("Coverage load complete: {} rows", count);
    Ok(count)
}
