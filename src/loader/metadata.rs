//! Sample metadata loader — fetches HPRC CSV and loads into lr_sample_metadata.

use crate::clickhouse::ClickHouseInserter;
use crate::domain::SUBPOP_TO_SUPERPOP;
use crate::models::SampleMetadataRow;
use std::collections::HashSet;
use tracing::info;

/// Default URL for the HPRC release 2 sample metadata CSV.
pub const HPRC_METADATA_URL: &str = "https://raw.githubusercontent.com/human-pangenomics/hprc_intermediate_assembly/main/data_tables/sample/hprc_release2_sample_metadata.csv";

/// Load sample metadata from the HPRC CSV into ClickHouse.
///
/// Only loads metadata for samples that already exist in lr_haplotypes.
/// If lr_haplotypes is empty or unreachable, loads all samples from the CSV.
pub fn load_sample_metadata(ch_url: &str, csv_url: &str) -> anyhow::Result<usize> {
    info!("Loading sample metadata from {}", csv_url);

    let client = reqwest::blocking::Client::new();

    // Query ClickHouse for existing sample IDs in lr_haplotypes
    let our_samples: Option<HashSet<String>> = {
        let query = "SELECT DISTINCT sample_id FROM lr_haplotypes FORMAT TabSeparated";
        let url = format!("{}/?query={}", ch_url, urlencoding::encode(query));
        match client.get(&url).send() {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.text().unwrap_or_default();
                let samples: HashSet<String> = body
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| l.to_string())
                    .collect();
                info!("Found {} samples in lr_haplotypes", samples.len());
                if samples.is_empty() {
                    None
                } else {
                    Some(samples)
                }
            }
            _ => {
                info!("Could not query lr_haplotypes, will load all HPRC samples");
                None
            }
        }
    };

    // Fetch the CSV
    let resp = client.get(csv_url).send()?;
    if !resp.status().is_success() {
        anyhow::bail!("Failed to fetch metadata CSV: {}", resp.status());
    }
    let content = resp.text()?;

    let mut inserter = ClickHouseInserter::new(ch_url, "lr_sample_metadata", 50_000);
    let mut count = 0usize;

    let mut lines = content.lines();
    let header_line = lines.next().ok_or_else(|| anyhow::anyhow!("Empty CSV"))?;

    // Parse header to find column indices
    let headers: Vec<&str> = header_line.split(',').collect();
    let idx_sample_id = headers.iter().position(|&h| h == "sample_id")
        .ok_or_else(|| anyhow::anyhow!("Missing sample_id column in CSV"))?;
    let idx_pop_abbr = headers.iter().position(|&h| h == "population_abbreviation");
    let idx_pop_desc = headers.iter().position(|&h| h == "population_descriptor");
    let idx_sex = headers.iter().position(|&h| h == "sex");
    let idx_collection = headers.iter().position(|&h| h == "collection");

    for line in lines {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() <= idx_sample_id {
            continue;
        }

        let sample_id = parts[idx_sample_id].trim().to_string();
        if sample_id.is_empty() {
            continue;
        }

        // Filter to only samples in our haplotype table
        if let Some(ref samples) = our_samples {
            if !samples.contains(&sample_id) {
                continue;
            }
        }

        let subpop = idx_pop_abbr
            .and_then(|i| parts.get(i))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("N/A")
            .to_string();

        let superpop = SUBPOP_TO_SUPERPOP
            .get(subpop.as_str())
            .copied()
            .unwrap_or("N/A")
            .to_string();

        let pop_desc = idx_pop_desc
            .and_then(|i| parts.get(i))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("N/A")
            .to_string();

        let sex = idx_sex
            .and_then(|i| parts.get(i))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("N/A")
            .to_string();

        let collection = idx_collection
            .and_then(|i| parts.get(i))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("N/A")
            .to_string();

        let row = SampleMetadataRow {
            sample_id,
            subpopulation: subpop,
            superpopulation: superpop,
            population_descriptor: pop_desc,
            sex,
            collection,
        };

        inserter.insert(&row)?;
        count += 1;
    }

    inserter.finish()?;
    info!("Sample metadata load complete: {} rows", count);
    Ok(count)
}
