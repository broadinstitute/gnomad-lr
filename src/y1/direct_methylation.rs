//! One-shot, fail-from-zero presentation loader for source hap1/hap2 methylation.
//!
//! This path is intentionally separate from accepted Y1 ancillary loading. It
//! writes one isolated raw/canonical presentation table, creates no ledger or
//! pointer, and makes no VCF-orientation claim. The campaign owns database
//! freshness and runs with coordinator retries disabled.

use super::contig::grch38_contig_length;
use super::methylation::methylation_source_coordinates;
use super::{parse_methylation_record, MethylationSourceType};
use crate::clickhouse::ClickHouseInserter;
use crate::loader::immutable_gcs::{
    validate_source_index_pair, HttpGcsBackend, ImmutableGcsObject,
};
use crate::loader::strict_bed_reader::{StrictBedStream, ValidatedBedRecord};
use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const DIRECT_METHYLATION_TABLE: &str = "lr_y1_methylation_source_haplotype_presentation";
const KEY_DOMAIN: &[u8] = b"y1-direct-source-haplotype-methylation-key-v1";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectMethylationTaskSpec {
    pub coordinator_task_id: String,
    pub bed_path: String,
    pub bed_generation: String,
    pub bed_byte_size: u64,
    pub bed_md5_base64: String,
    pub bed_index_path: String,
    pub bed_index_generation: String,
    pub bed_index_byte_size: u64,
    pub bed_index_md5_base64: String,
    pub sample_id: String,
    pub chrom: String,
    /// One-based inclusive full-contig start; exactly 1 for this campaign.
    pub start: u32,
    /// One-based inclusive full-contig stop; exactly the GRCh38 contig length.
    pub stop: u32,
    /// Source BED label only. This is never interpreted as VCF orientation.
    pub source_haplotype: u8,
    /// Explicit per-task destination. Job-level fallback is forbidden.
    pub clickhouse_url: String,
}

impl DirectMethylationTaskSpec {
    fn source_type(&self) -> anyhow::Result<MethylationSourceType> {
        match self.source_haplotype {
            1 => Ok(MethylationSourceType::Hap1),
            2 => Ok(MethylationSourceType::Hap2),
            value => bail!("source_haplotype must be exactly 1 or 2, got {value}"),
        }
    }

    fn source_object(&self) -> ImmutableGcsObject {
        ImmutableGcsObject {
            uri: self.bed_path.clone(),
            generation: self.bed_generation.clone(),
            byte_size: self.bed_byte_size,
            checksum_algorithm: "md5_base64".into(),
            checksum: self.bed_md5_base64.clone(),
            immutable_read_uri: format!("{}?generation={}", self.bed_path, self.bed_generation),
        }
    }

    fn index_object(&self) -> ImmutableGcsObject {
        ImmutableGcsObject {
            uri: self.bed_index_path.clone(),
            generation: self.bed_index_generation.clone(),
            byte_size: self.bed_index_byte_size,
            checksum_algorithm: "md5_base64".into(),
            checksum: self.bed_index_md5_base64.clone(),
            immutable_read_uri: format!(
                "{}?generation={}",
                self.bed_index_path, self.bed_index_generation
            ),
        }
    }

    pub fn validate(&self, descriptor_id: &str) -> anyhow::Result<()> {
        if self.coordinator_task_id != descriptor_id {
            bail!("descriptor ID must exactly match coordinator_task_id");
        }
        if self.sample_id.is_empty()
            || self.sample_id.len() > 128
            || !self
                .sample_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        {
            bail!("sample_id must be a nonempty canonical identifier");
        }
        self.source_type()?;
        let contig_length = grch38_contig_length(&self.chrom)?;
        if self.start != 1 || self.stop != contig_length {
            bail!("direct methylation tasks must cover one exact full canonical GRCh38 contig");
        }
        validate_source_index_pair(&self.source_object(), &self.index_object())?;

        let url =
            reqwest::Url::parse(&self.clickhouse_url).context("invalid per-task clickhouse_url")?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            bail!("per-task clickhouse_url must use http(s) and include a host");
        }
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            bail!("per-task clickhouse_url must not contain credentials or a fragment");
        }
        let databases = url
            .query_pairs()
            .filter(|(name, _)| name == "database")
            .map(|(_, value)| value.into_owned())
            .collect::<Vec<_>>();
        if databases.len() != 1 || databases[0].is_empty() || databases[0] == "default" {
            bail!("per-task clickhouse_url must select exactly one non-default database");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
struct DirectMethylationRow {
    stable_key: String,
    chrom: String,
    pos1: u32,
    pos2: u32,
    sample_id: String,
    source_haplotype: u8,
    methylation: f32,
    coverage: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirectMethylationTaskReceipt {
    pub coordinator_task_id: String,
    pub sample_id: String,
    pub chrom: String,
    pub source_haplotype: u8,
    pub items_processed: u64,
    pub table: &'static str,
    pub presentation_only: bool,
    pub vcf_orientation_joined: bool,
}

fn stable_key(row: &DirectMethylationRow) -> String {
    let mut hash = Sha256::new();
    hash.update(KEY_DOMAIN);
    for value in [
        row.chrom.as_bytes(),
        &row.pos1.to_be_bytes(),
        &row.pos2.to_be_bytes(),
        row.sample_id.as_bytes(),
        &[row.source_haplotype],
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    format!("{:x}", hash.finalize())
}

fn parse_direct_row(
    line: &str,
    chrom: &str,
    sample_id: &str,
    source_haplotype: u8,
    expected_type: MethylationSourceType,
) -> anyhow::Result<DirectMethylationRow> {
    let record = parse_methylation_record(line, chrom, expected_type)?;
    let mut row = DirectMethylationRow {
        stable_key: String::new(),
        chrom: record.chrom,
        pos1: record.source_start0,
        pos2: record.source_end0,
        sample_id: sample_id.to_string(),
        source_haplotype,
        methylation: record.methylation,
        coverage: record.coverage,
    };
    row.stable_key = stable_key(&row);
    Ok(row)
}

fn direct_record_coordinates(
    line: &str,
    expected_type: MethylationSourceType,
) -> anyhow::Result<ValidatedBedRecord> {
    methylation_source_coordinates(line, expected_type)
}

fn direct_row_from_strict_item(
    line: anyhow::Result<String>,
    chrom: &str,
    sample_id: &str,
    source_haplotype: u8,
    expected_type: MethylationSourceType,
) -> anyhow::Result<DirectMethylationRow> {
    let line = line.context("strict indexed methylation read failed")?;
    parse_direct_row(&line, chrom, sample_id, source_haplotype, expected_type)
}

pub fn load_direct_methylation_task(
    task: &DirectMethylationTaskSpec,
    batch_records: usize,
) -> anyhow::Result<DirectMethylationTaskReceipt> {
    task.validate(&task.coordinator_task_id)?;
    if batch_records == 0 || batch_records > 100_000 {
        bail!("batch_records must be in 1..=100000");
    }
    let expected_type = task.source_type()?;
    let stream = StrictBedStream::open_immutable_region(
        Arc::new(HttpGcsBackend::new().context("failed to initialize immutable GCS backend")?),
        &task.source_object(),
        &task.index_object(),
        &task.chrom,
        task.start,
        task.stop,
        move |line: &str| direct_record_coordinates(line, expected_type),
    )?;

    let mut inserter = ClickHouseInserter::new(
        &task.clickhouse_url,
        DIRECT_METHYLATION_TABLE,
        batch_records,
    );
    let mut count = 0u64;
    for line in stream.records() {
        let row = direct_row_from_strict_item(
            line,
            &task.chrom,
            &task.sample_id,
            task.source_haplotype,
            expected_type,
        )?;
        inserter.insert(&row)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("methylation row count overflow"))?;
    }
    inserter.finish()?;

    Ok(DirectMethylationTaskReceipt {
        coordinator_task_id: task.coordinator_task_id.clone(),
        sample_id: task.sample_id.clone(),
        chrom: task.chrom.clone(),
        source_haplotype: task.source_haplotype,
        items_processed: count,
        table: DIRECT_METHYLATION_TABLE,
        presentation_only: true,
        vcf_orientation_joined: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(haplotype: u8) -> DirectMethylationTaskSpec {
        DirectMethylationTaskSpec {
            coordinator_task_id: "custom_0".into(),
            bed_path: "gs://bucket/sample.hap1.bed.gz".into(),
            bed_generation: "123".into(),
            bed_byte_size: 100,
            bed_md5_base64: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            bed_index_path: "gs://bucket/sample.hap1.bed.gz.tbi".into(),
            bed_index_generation: "124".into(),
            bed_index_byte_size: 50,
            bed_index_md5_base64: "AAAAAAAAAAAAAAAAAAAAAA==".into(),
            sample_id: "HG00097".into(),
            chrom: "chr22".into(),
            start: 1,
            stop: 50_818_468,
            source_haplotype: haplotype,
            clickhouse_url:
                "http://clickhouse:8123/?database=gnomad_lr_y1_methylation_presentation".into(),
        }
    }

    #[test]
    fn accepts_only_exact_hap1_hap2_values() {
        assert!(task(1).validate("custom_0").is_ok());
        assert!(task(2).validate("custom_0").is_ok());
        for value in [0, 3, u8::MAX] {
            assert!(task(value).validate("custom_0").is_err(), "{value}");
        }
    }

    #[test]
    fn rejects_noncanonical_contigs_and_partial_or_overflow_ranges() {
        let mut value = task(1);
        value.chrom = "22".into();
        assert!(value.validate("custom_0").is_err());
        let mut value = task(1);
        value.start = 0;
        assert!(value.validate("custom_0").is_err());
        let mut value = task(1);
        value.stop += 1;
        assert!(value.validate("custom_0").is_err());
    }

    #[test]
    fn per_task_urls_are_mandatory_and_isolated() {
        let mut first = task(1);
        let mut second = task(2);
        second.clickhouse_url = "http://clickhouse:8123/?database=other_fresh_presentation".into();
        assert!(first.validate("custom_0").is_ok());
        second.coordinator_task_id = "custom_1".into();
        assert!(second.validate("custom_1").is_ok());
        assert_ne!(first.clickhouse_url, second.clickhouse_url);
        first.clickhouse_url = "not a URL".into();
        assert!(first.validate("custom_0").is_err());
    }

    #[test]
    fn strict_source_and_parser_errors_are_not_silenced() {
        let read_error = direct_row_from_strict_item(
            Err(anyhow::anyhow!("synthetic BGZF read failure")),
            "chr22",
            "HG00097",
            1,
            MethylationSourceType::Hap1,
        )
        .unwrap_err();
        assert!(format!("{read_error:#}").contains("synthetic BGZF read failure"));

        let parse_error = parse_direct_row(
            "chr22\t10\t11\t50\thap2\t2\t1\t1\t50",
            "chr22",
            "HG00097",
            1,
            MethylationSourceType::Hap1,
        )
        .unwrap_err();
        assert!(format!("{parse_error:#}").contains("source type mismatch"));
    }

    #[test]
    fn production_haplotype_validators_filter_membership_after_shape_and_type() {
        for (haplotype, expected_type, label, wrong_label) in [
            (1, MethylationSourceType::Hap1, "hap1", "hap2"),
            (2, MethylationSourceType::Hap2, "hap2", "hap1"),
        ] {
            let spill = format!("chr2\t10\t11\t50\t{label}\t93956\t46978\t46978\t50");
            let coordinates = direct_record_coordinates(&spill, expected_type).unwrap();
            assert_eq!(
                (
                    coordinates.chrom.as_str(),
                    coordinates.start0,
                    coordinates.end0
                ),
                ("chr2", 10, 11)
            );

            let membership_error =
                parse_direct_row(&spill, "chr1", "HG00097", haplotype, expected_type).unwrap_err();
            assert!(format!("{membership_error:#}").contains("chromosome mismatch"));

            let wrong_type = format!("chr1\t10\t11\t50\t{wrong_label}\t2\t1\t1\t50");
            assert!(direct_record_coordinates(&wrong_type, expected_type).is_err());
            assert!(direct_record_coordinates("chr2", expected_type).is_err());
        }
    }

    #[test]
    fn direct_haplotype_rows_preserve_uint32_coverage() {
        let row = parse_direct_row(
            "chr22\t10\t11\t50\thap1\t65690\t32845\t32845\t50",
            "chr22",
            "HG00097",
            1,
            MethylationSourceType::Hap1,
        )
        .unwrap();
        assert_eq!(row.coverage, 65_690);

        let error = parse_direct_row(
            "chr22\t10\t11\t50\thap2\t4294967296\t1\t1\t50",
            "chr22",
            "HG00097",
            2,
            MethylationSourceType::Hap2,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("coverage is not a UInt32"));
    }

    #[test]
    fn stable_key_is_repeatable_and_haplotype_specific() {
        let line = "chr22\t10\t11\t50\thap1\t2\t1\t1\t50";
        let first =
            parse_direct_row(line, "chr22", "HG00097", 1, MethylationSourceType::Hap1).unwrap();
        let repeat =
            parse_direct_row(line, "chr22", "HG00097", 1, MethylationSourceType::Hap1).unwrap();
        let hap2_line = line.replace("hap1", "hap2");
        let second = parse_direct_row(
            &hap2_line,
            "chr22",
            "HG00097",
            2,
            MethylationSourceType::Hap2,
        )
        .unwrap();
        assert_eq!(first.stable_key, repeat.stable_key);
        assert_ne!(first.stable_key, second.stable_key);
    }
}
