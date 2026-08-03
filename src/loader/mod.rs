pub mod coverage;
pub mod haplotypes;
pub mod histograms;
pub mod immutable_gcs;
pub mod metadata;
pub mod methylation;
pub mod prescan;
pub mod strict_bed_reader;
pub mod variants;
pub mod vcf_reader;

/// Inclusive genomic interval used to bound sequential ancillary inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionFilter {
    pub chrom: String,
    pub start: u32,
    pub stop: u32,
}

impl RegionFilter {
    pub fn new(chrom: String, start: u32, stop: u32) -> Self {
        Self { chrom, start, stop }
    }

    pub fn contains(&self, chrom: &str, position: u32) -> bool {
        chrom == self.chrom && position >= self.start && position <= self.stop
    }
}

/// Per-task pipeline timing metrics, reported back to the coordinator.
#[derive(Default, serde::Serialize)]
pub struct IngestMetrics {
    pub prescan_ms: u64,
    pub gcs_index_load_ms: u64,
    pub gcs_vcf_seek_ms: u64,
    pub vcf_parse_ms: u64,
    pub ch_insert_ms: u64,
    pub ch_insert_count: usize,
    pub ch_rows_inserted: usize,
    pub total_ms: u64,
}
