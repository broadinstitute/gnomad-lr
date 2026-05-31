pub mod haplotypes;
pub mod prescan;
pub mod variants;
pub mod vcf_reader;

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
