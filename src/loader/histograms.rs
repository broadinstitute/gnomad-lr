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

    for line_result in reader.lines() {
        let line = line_result?;
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

        let Some(row) = parse_histogram_row(&parts, header_cols.as_ref().unwrap()) else {
            continue;
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

fn parse_histogram_row(parts: &[&str], headers: &[String]) -> Option<StrHistogramRow> {
    if parts.len() < 15 {
        return None;
    }

    // Parse LocusId: {chrom_num}-{start}-{end}-{motif}.
    let locus_parts: Vec<&str> = parts[0].splitn(4, '-').collect();
    if locus_parts.len() < 4 {
        return None;
    }

    let mut populations = HashMap::new();
    for col_idx in 15..parts.len().min(headers.len()) {
        let value = parts[col_idx];
        if !value.is_empty() && value != "." {
            populations.insert(headers[col_idx].clone(), value.to_string());
        }
    }

    Some(StrHistogramRow {
        chrom: format!("chr{}", locus_parts[0]),
        position: locus_parts[1].parse().ok()?,
        end_position: locus_parts[2].parse().ok()?,
        motif: locus_parts[3].to_string(),
        // Column 1 is the source Motif column. The canonical motif is also
        // encoded in LocusId, so histogram values begin at column 2.
        allele_size_histogram: parts[2].to_string(),
        biallelic_histogram: parts[3].to_string(),
        min_repeats: safe_float(parts[4]),
        mode_repeats: safe_float(parts[5]),
        mean_repeats: safe_float(parts[6]),
        stdev_repeats: safe_float(parts[7]),
        median_repeats: safe_float(parts[8]),
        p99_repeats: safe_float(parts[9]),
        max_repeats: safe_float(parts[10]),
        // Columns 11 and 12 are short-allele summaries not represented in CH.
        unique_allele_lengths: safe_u32(parts[13]),
        num_called_alleles: safe_u32(parts[14]),
        populations,
    })
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

#[cfg(test)]
mod tests {
    use super::parse_histogram_row;

    #[test]
    fn maps_current_source_columns_without_off_by_one_shift() {
        let header = "LocusId\tMotif\tAlleleSizeHistogram\tBiallelicHistogram\tMin\tMode\tMean\tStdev\tMedian\t99thPercentile\tMax\tShortAllele99thPercentile\tShortAlleleMax\tUniqueAlleleLengths\tNumCalledAlleles\tAlleleSizeHistogram:afr:female";
        let headers: Vec<String> = header.split('\t').map(str::to_string).collect();
        let line = "1-16712-16744-GTG\tGTG\t2x:11,3x:573\t2/3:11,3/3:281\t2\t3\t2.98\t0.14\t3\t3\t3\t3\t3\t2\t584\t2x:3,3x:111";
        let parts: Vec<&str> = line.split('\t').collect();

        let row = parse_histogram_row(&parts, &headers).unwrap();
        assert_eq!(row.chrom, "chr1");
        assert_eq!(row.position, 16_712);
        assert_eq!(row.motif, "GTG");
        assert_eq!(row.allele_size_histogram, "2x:11,3x:573");
        assert_eq!(row.biallelic_histogram, "2/3:11,3/3:281");
        assert_eq!(row.min_repeats, 2.0);
        assert_eq!(row.mean_repeats, 2.98);
        assert_eq!(row.unique_allele_lengths, 2);
        assert_eq!(row.num_called_alleles, 584);
        assert_eq!(
            row.populations
                .get("AlleleSizeHistogram:afr:female")
                .map(String::as_str),
            Some("2x:3,3x:111")
        );
    }
}
