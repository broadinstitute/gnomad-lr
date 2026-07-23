//! Methylation loader — streams tabix-indexed BED files into lr_methylation.

use crate::clickhouse::ClickHouseInserter;
use crate::loader::bed_reader::BedStream;
use crate::models::MethylationRow;
use tracing::info;

/// Load methylation data from a tabix-indexed BED file for a given region.
///
/// BED columns (pb-cpg-tools output):
///   0: chrom
///   1: pos1 (start, 0-based)
///   2: pos2 (end)
///   3: mod_score (methylation percentage, e.g. 85.0)
///   4: type ("Total")
///   5: coverage (total reads)
///   6: modified count
///   7: unmodified count
///   8: pct_modified
pub fn load_methylation(
    ch_url: &str,
    bed_path: &str,
    sample_id: &str,
    chrom: &str,
    start: u32,
    stop: u32,
    limit: Option<usize>,
) -> anyhow::Result<usize> {
    info!(
        "Loading methylation for sample {} region {}:{}-{} from {}",
        sample_id, chrom, start, stop, bed_path
    );

    let stream = BedStream::open_region(bed_path, chrom, start, stop)?;
    let mut inserter = ClickHouseInserter::new(ch_url, "lr_methylation", 50_000);
    let mut count = 0usize;

    for line in stream.lines() {
        if limit.is_some_and(|max| count >= max) {
            break;
        }

        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 6 {
            continue;
        }

        let pos1: u32 = match parts[1].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let pos2: u32 = match parts[2].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let methylation: f32 = match parts[3].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let coverage: u16 = match parts[5].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };

        let row = MethylationRow {
            chrom: parts[0].to_string(),
            pos1,
            pos2,
            sample_id: sample_id.to_string(),
            methylation,
            coverage,
        };

        inserter.insert(&row)?;
        count += 1;
    }

    inserter.finish()?;
    info!(
        "Methylation load complete for {} {}:{}-{}: {} rows",
        sample_id, chrom, start, stop, count
    );
    Ok(count)
}
