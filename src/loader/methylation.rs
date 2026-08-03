//! Methylation loader — streams tabix-indexed BED files into lr_methylation.

use crate::clickhouse::ClickHouseInserter;
use crate::loader::immutable_gcs::{HttpGcsBackend, ImmutableGcsObject};
use crate::loader::strict_bed_reader::{StrictBedLines, StrictBedStream, ValidatedBedRecord};
use crate::models::MethylationRow;
use crate::y1::methylation::methylation_source_coordinates;
use crate::y1::{parse_methylation_record, MethylationSourceType};
use anyhow::Context;
use std::sync::Arc;
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
    let index_path = format!("{bed_path}.tbi");
    let lines = open_strict_records(bed_path, &index_path, chrom, start, stop)?;
    load_strict_records(ch_url, sample_id, chrom, lines, limit)
}

/// Generation-bound pool entry point. Both the BED and TBI identities are
/// revalidated before any source row is read or inserted.
pub fn load_immutable_methylation(
    ch_url: &str,
    bed: &ImmutableGcsObject,
    index: &ImmutableGcsObject,
    sample_id: &str,
    chrom: &str,
    start: u32,
    stop: u32,
    limit: Option<usize>,
) -> anyhow::Result<usize> {
    let (query_start, query_stop) = strict_query_interval(start, stop)?;
    let stream = StrictBedStream::open_immutable_region(
        Arc::new(HttpGcsBackend::new().context("failed to initialize immutable GCS backend")?),
        bed,
        index,
        chrom,
        query_start,
        query_stop,
        total_record_coordinates,
    )?;
    load_strict_records(ch_url, sample_id, chrom, stream.records(), limit)
}

fn open_strict_records(
    bed_path: &str,
    index_path: &str,
    chrom: &str,
    start: u32,
    stop: u32,
) -> anyhow::Result<StrictBedLines> {
    let (query_start, query_stop) = strict_query_interval(start, stop)?;
    Ok(StrictBedStream::open_region(
        bed_path,
        index_path,
        chrom,
        query_start,
        query_stop,
        total_record_coordinates,
    )?
    .records())
}

fn strict_query_interval(start0: u32, end0: u32) -> anyhow::Result<(u32, u32)> {
    if start0 >= end0 {
        anyhow::bail!("methylation BED interval must be nonempty and half-open");
    }
    let start1 = start0
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("methylation BED interval start overflows UInt32"))?;
    Ok((start1, end0))
}

fn total_record_coordinates(line: &str) -> anyhow::Result<ValidatedBedRecord> {
    methylation_source_coordinates(line, MethylationSourceType::Total)
}

fn parse_total_row(line: &str, sample_id: &str, chrom: &str) -> anyhow::Result<MethylationRow> {
    let record = parse_methylation_record(line, chrom, MethylationSourceType::Total)
        .with_context(|| format!("methylation source schema mismatch for {sample_id}"))?;
    Ok(MethylationRow {
        chrom: record.chrom,
        pos1: record.source_start0,
        pos2: record.source_end0,
        sample_id: sample_id.to_string(),
        methylation: record.methylation,
        coverage: record.coverage,
    })
}

fn load_strict_records(
    ch_url: &str,
    sample_id: &str,
    chrom: &str,
    lines: StrictBedLines,
    limit: Option<usize>,
) -> anyhow::Result<usize> {
    info!("Loading methylation for sample {sample_id} chromosome {chrom}");
    let mut inserter = ClickHouseInserter::new(ch_url, "lr_methylation", 50_000);
    let mut count = 0usize;

    for line in lines {
        if limit.is_some_and(|max| count >= max) {
            break;
        }
        let line = line.context("strict indexed methylation read failed")?;
        let row = parse_total_row(&line, sample_id, chrom)?;
        inserter.insert(&row)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("methylation items_processed overflow"))?;
    }

    inserter.finish()?;
    info!("Methylation load complete for {sample_id} {chrom}: {count} rows");
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_65690_is_preserved_and_uint32_overflow_is_rejected() {
        let row = parse_total_row(
            "chr1\t10\t11\t50\tTotal\t65690\t32845\t32845\t50",
            "HG002",
            "chr1",
        )
        .unwrap();
        assert_eq!(row.coverage, 65_690);

        let error = parse_total_row(
            "chr1\t10\t11\t50\tTotal\t4294967296\t1\t1\t50",
            "HG002",
            "chr1",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("coverage is not a UInt32"));
    }

    #[test]
    fn production_validator_filters_membership_only_after_source_validation() {
        let spill = "chr2\t10\t11\t50\tTotal\t93956\t46978\t46978\t50";
        let coordinates = total_record_coordinates(spill).unwrap();
        assert_eq!(
            (
                coordinates.chrom.as_str(),
                coordinates.start0,
                coordinates.end0
            ),
            ("chr2", 10, 11)
        );

        let membership_error = parse_total_row(spill, "HG002", "chr1").unwrap_err();
        assert!(format!("{membership_error:#}").contains("chromosome mismatch"));

        for malformed in [
            "chr2",
            "chr2\t10\t11\t50\thap1\t2\t1\t1\t50",
            "chr2\tnot-a-position\t11\t50\tTotal\t2\t1\t1\t50",
        ] {
            assert!(total_record_coordinates(malformed).is_err(), "{malformed}");
        }
    }

    #[test]
    fn strict_open_errors_are_not_converted_to_empty_input() {
        let missing = std::env::temp_dir().join(format!(
            "gnomad-lr-missing-methylation-{}",
            std::process::id()
        ));
        let error = open_strict_records(
            missing.to_str().unwrap(),
            missing.with_extension("tbi").to_str().unwrap(),
            "chr1",
            0,
            1,
        )
        .err()
        .expect("missing strict source/index unexpectedly opened");
        assert!(format!("{error:#}").contains("failed to load required tabix index"));
    }

    #[test]
    fn observed_partial_chr_and_arbitrary_one_column_records_are_not_ignorable() {
        for line in ["c", "ch", "chr", "chr2", "arbitrary-sentinel"] {
            let error = parse_total_row(line, "HG00235", "chr1").unwrap_err();
            assert!(format!("{error:#}").contains("exactly nine"));
        }
    }

    #[test]
    fn ordinary_total_remains_strict_and_exact() {
        let row =
            parse_total_row("chr22\t99\t100\t80\tTotal\t4\t3\t1\t75", "HG00097", "chr22").unwrap();
        assert_eq!((row.pos1, row.pos2, row.coverage), (99, 100, 4));
        assert_eq!(row.methylation, 80.0);

        assert!(
            parse_total_row("chr22\t99\t100\t80\thap1\t4\t3\t1\t75", "HG00097", "chr22").is_err()
        );
    }
}
