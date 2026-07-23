//! Sample metadata loader — fetches HPRC CSV and loads into lr_sample_metadata.

use crate::clickhouse::ClickHouseInserter;
use crate::domain::SUBPOP_TO_SUPERPOP;
use crate::models::SampleMetadataRow;
use csv::StringRecord;
use std::collections::HashSet;
use tracing::info;

/// Default URL for the HPRC release 2 sample metadata CSV.
pub const HPRC_METADATA_URL: &str = "https://raw.githubusercontent.com/human-pangenomics/hprc_intermediate_assembly/main/data_tables/sample/hprc_release2_sample_metadata.csv";

/// Load sample metadata from the HPRC CSV into ClickHouse.
///
/// Only loads metadata for samples that already exist in lr_haplotypes. If
/// lr_haplotypes is empty or unreachable, loads all samples from the CSV.
pub fn load_sample_metadata(ch_url: &str, csv_url: &str) -> anyhow::Result<usize> {
    info!("Loading sample metadata from {}", csv_url);

    let client = reqwest::blocking::Client::new();

    // Query ClickHouse for existing sample IDs in lr_haplotypes. Use RequestBuilder::query
    // so an existing `database=...` URL parameter remains intact for isolated smoke DBs.
    let our_samples: Option<HashSet<String>> = {
        let query = "SELECT DISTINCT sample_id FROM lr_haplotypes FORMAT TabSeparated";
        match client.get(ch_url).query(&[("query", query)]).send() {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.text().unwrap_or_default();
                let samples: HashSet<String> = body
                    .lines()
                    .filter(|line| !line.is_empty())
                    .map(str::to_string)
                    .collect();
                info!("Found {} samples in lr_haplotypes", samples.len());
                (!samples.is_empty()).then_some(samples)
            }
            _ => {
                info!("Could not query lr_haplotypes, will load all HPRC samples");
                None
            }
        }
    };

    let resp = client.get(csv_url).send()?;
    if !resp.status().is_success() {
        anyhow::bail!("Failed to fetch metadata CSV: {}", resp.status());
    }
    let content = resp.text()?;
    let rows = parse_metadata_rows(&content, our_samples.as_ref())?;

    let mut inserter = ClickHouseInserter::new(ch_url, "lr_sample_metadata", 50_000);
    for row in &rows {
        inserter.insert(row)?;
    }
    inserter.finish()?;

    info!("Sample metadata load complete: {} rows", rows.len());
    Ok(rows.len())
}

fn parse_metadata_rows(
    content: &str,
    our_samples: Option<&HashSet<String>>,
) -> anyhow::Result<Vec<SampleMetadataRow>> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(content.as_bytes());
    let headers = reader.headers()?.clone();

    let sample_id_idx = required_column(&headers, "sample_id")?;
    let pop_abbr_idx = optional_column(&headers, "population_abbreviation");
    let pop_desc_idx = optional_column(&headers, "population_descriptor");
    let sex_idx = optional_column(&headers, "sex");
    let collection_idx = optional_column(&headers, "collection");

    let mut rows = Vec::new();
    for record_result in reader.records() {
        // Unlike the previous split(',') implementation, csv::Reader handles quoted
        // population descriptors such as "St. Louis, Missouri" without shifting fields.
        let record = record_result?;
        let sample_id = field(&record, Some(sample_id_idx), "");
        if sample_id.is_empty() {
            continue;
        }
        if our_samples.is_some_and(|samples| !samples.contains(sample_id)) {
            continue;
        }

        let subpopulation = field(&record, pop_abbr_idx, "N/A").to_string();
        let superpopulation = SUBPOP_TO_SUPERPOP
            .get(subpopulation.as_str())
            .copied()
            .unwrap_or("N/A")
            .to_string();

        rows.push(SampleMetadataRow {
            sample_id: sample_id.to_string(),
            subpopulation,
            superpopulation,
            population_descriptor: field(&record, pop_desc_idx, "N/A").to_string(),
            sex: field(&record, sex_idx, "N/A").to_string(),
            collection: field(&record, collection_idx, "N/A").to_string(),
        });
    }

    Ok(rows)
}

fn required_column(headers: &StringRecord, name: &str) -> anyhow::Result<usize> {
    optional_column(headers, name)
        .ok_or_else(|| anyhow::anyhow!("Missing {} column in metadata CSV", name))
}

fn optional_column(headers: &StringRecord, name: &str) -> Option<usize> {
    headers.iter().position(|header| header == name)
}

fn field<'a>(record: &'a StringRecord, index: Option<usize>, fallback: &'a str) -> &'a str {
    index
        .and_then(|i| record.get(i))
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_quoted_commas_without_shifting_columns() {
        let csv = concat!(
            "sample_id,population_descriptor,population_abbreviation,sex,collection\n",
            "HG06807,\"African Americans living in St. Louis, Missouri\",ASL,female,HPRC\n",
        );

        let rows = parse_metadata_rows(csv, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sample_id, "HG06807");
        assert_eq!(
            rows[0].population_descriptor,
            "African Americans living in St. Louis, Missouri"
        );
        assert_eq!(rows[0].subpopulation, "ASL");
        assert_eq!(rows[0].superpopulation, "AFR");
        assert_eq!(rows[0].sex, "female");
        assert_eq!(rows[0].collection, "HPRC");
    }

    #[test]
    fn filters_to_loaded_samples() {
        let csv = concat!(
            "sample_id,population_descriptor,population_abbreviation,sex,collection\n",
            "A,one,GBR,male,HPRC\n",
            "B,two,JPT,female,HPRC\n",
        );
        let samples = HashSet::from(["B".to_string()]);

        let rows = parse_metadata_rows(csv, Some(&samples)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sample_id, "B");
        assert_eq!(rows[0].superpopulation, "EAS");
    }
}
