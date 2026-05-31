use serde::Serialize;
use std::collections::HashMap;

/// Per-sample haplotype row matching lr_haplotypes ClickHouse schema.
#[derive(Debug, Serialize)]
pub struct HaplotypeRow {
    pub chrom: String,
    pub position: u32,
    pub sample_id: String,
    pub strand: u8,
    pub ref_allele: String,
    pub alt: String,
    pub rsid: String,
    pub qual: f32,
    pub filters: Vec<String>,
    #[serde(rename = "info_AF")]
    pub info_af: f32,
    #[serde(rename = "info_AC")]
    pub info_ac: u32,
    #[serde(rename = "info_AN")]
    pub info_an: u32,
    pub allele_type: String,
    pub allele_length: i32,
    pub gnomad_v4_match_type: String,
    #[serde(rename = "info_AF_afr")]
    pub info_af_afr: Option<f32>,
    #[serde(rename = "info_AF_amr")]
    pub info_af_amr: Option<f32>,
    #[serde(rename = "info_AF_eas")]
    pub info_af_eas: Option<f32>,
    #[serde(rename = "info_AF_nfe")]
    pub info_af_nfe: Option<f32>,
    #[serde(rename = "info_AF_sas")]
    pub info_af_sas: Option<f32>,
    pub gt_alleles: Vec<u8>,
    pub gt_phased: u8,
    pub depth: Option<u16>,
    pub genotype_quality: Option<u16>,
    pub cadd_phred: Option<f32>,
    pub phylop: Option<f32>,
    pub sv_consequences: Vec<String>,
    pub dbsnp_id: String,
    pub tr_id: String,
    pub tr_motifs: String,
    pub tr_struc: String,
    pub allele_methylation: Option<f32>,
    pub motif_counts: Vec<u16>,
    pub allele_purity: Option<f32>,
}

/// Rename "ref_allele" to "ref" in JSON output
impl HaplotypeRow {
    pub fn to_json(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(self).unwrap();
        if let Some(obj) = v.as_object_mut() {
            if let Some(r) = obj.remove("ref_allele") {
                obj.insert("ref".to_string(), r);
            }
        }
        v
    }
}

/// Site-level variant row matching lr_variants ClickHouse schema.
#[derive(Debug, Serialize)]
pub struct VariantRow {
    pub chrom: String,
    pub position: u32,
    #[serde(rename = "ref")]
    pub ref_allele: String,
    pub alt: String,
    pub variant_id: String,
    pub xpos: f64,
    pub rsids: Vec<String>,
    pub allele_type: String,
    pub filters: Vec<String>,
    pub intergenic: u8,
    pub gene_region: String,
    pub major_consequence: String,
    pub end: Option<u32>,
    pub length: Option<i32>,
    pub cadd_phred: Option<f32>,
    pub phylop: Option<f32>,
    pub short_read_match_id: String,
    pub short_read_match_type: String,
    pub short_read_match_source: String,
    pub enveloping_tr_id: String,
    pub enveloped_ids: Vec<String>,
    pub motifs: Vec<String>,
    pub is_likely_tr: u8,
    pub gnomad_str: String,
    #[serde(rename = "info_AF")]
    pub info_af: f32,
    pub freq_json: String,
    pub transcript_consequences_json: String,
    pub genes_json: String,
    pub main_reference_region_json: String,
}

/// Per-base coverage statistics matching lr_coverage ClickHouse schema.
#[derive(Debug, Serialize)]
pub struct CoverageRow {
    pub chrom: String,
    pub pos: u32,
    pub mean: f32,
    pub median: f32,
    pub over_1: f32,
    pub over_5: f32,
    pub over_10: f32,
    pub over_15: f32,
    pub over_20: f32,
    pub over_25: f32,
    pub over_30: f32,
    pub over_50: f32,
    pub over_100: f32,
}

/// Sample metadata matching lr_sample_metadata ClickHouse schema.
#[derive(Debug, Serialize)]
pub struct SampleMetadataRow {
    pub sample_id: String,
    pub subpopulation: String,
    pub superpopulation: String,
    pub population_descriptor: String,
    pub sex: String,
    pub collection: String,
}

/// STR allele frequency histogram matching lr_str_histograms ClickHouse schema.
#[derive(Debug, Serialize)]
pub struct StrHistogramRow {
    pub chrom: String,
    pub position: u32,
    pub end_position: u32,
    pub motif: String,
    pub allele_size_histogram: String,
    pub biallelic_histogram: String,
    pub min_repeats: f32,
    pub mode_repeats: f32,
    pub mean_repeats: f32,
    pub stdev_repeats: f32,
    pub median_repeats: f32,
    pub p99_repeats: f32,
    pub max_repeats: f32,
    pub unique_allele_lengths: u32,
    pub num_called_alleles: u32,
    pub populations: HashMap<String, String>,
}

/// Per-sample methylation row matching lr_methylation ClickHouse schema.
#[derive(Debug, Serialize)]
pub struct MethylationRow {
    pub chrom: String,
    pub pos1: u32,
    pub pos2: u32,
    pub sample_id: String,
    pub methylation: f32,
    pub coverage: u16,
}
