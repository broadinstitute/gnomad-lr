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

        // The noodles chunk iterator can expose an empty separator at a BGZF
        // chunk boundary. It is not a BED record and must not become a reject.
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        // This loader is also used for the isolated Y1 chr22 prototype.  Fail
        // closed rather than silently turning malformed source records into
        // unexplained count gaps.
        if parts.len() != 9 {
            anyhow::bail!(
                "methylation source schema mismatch for {sample_id}: expected 9 columns, got {}",
                parts.len()
            );
        }
        if parts[0] != chrom || parts[4] != "Total" {
            anyhow::bail!(
                "methylation source schema mismatch for {sample_id}: chrom/type are {:?}/{:?}",
                parts[0],
                parts[4]
            );
        }

        let parse_error = |column: &str, value: &str| {
            anyhow::anyhow!(
                "methylation source schema mismatch for {sample_id}: invalid {column} value {value:?}"
            )
        };
        let pos1: u32 = parts[1]
            .parse()
            .map_err(|_| parse_error("pos1", parts[1]))?;
        let pos2: u32 = parts[2]
            .parse()
            .map_err(|_| parse_error("pos2", parts[2]))?;
        let methylation: f32 = parts[3]
            .parse()
            .map_err(|_| parse_error("methylation", parts[3]))?;
        let coverage: u16 = parts[5]
            .parse()
            .map_err(|_| parse_error("coverage", parts[5]))?;
        let modified: u32 = parts[6]
            .parse()
            .map_err(|_| parse_error("modified_count", parts[6]))?;
        let unmodified: u32 = parts[7]
            .parse()
            .map_err(|_| parse_error("unmodified_count", parts[7]))?;
        let pct_modified: f32 = parts[8]
            .parse()
            .map_err(|_| parse_error("pct_modified", parts[8]))?;
        if pos2 != pos1.saturating_add(1)
            || pos1 < start
            || pos1 > stop
            || !methylation.is_finite()
            || !(0.0..=100.0).contains(&methylation)
            || !pct_modified.is_finite()
            || !(0.0..=100.0).contains(&pct_modified)
            || modified + unmodified != u32::from(coverage)
        {
            anyhow::bail!(
                "methylation source invariant mismatch for {sample_id} at {chrom}:{pos1}-{pos2}"
            );
        }

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
