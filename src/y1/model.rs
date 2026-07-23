use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Release {
    Y1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cohort {
    HgsvcHprc,
    Aou,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum ReferenceGenome {
    #[serde(rename = "GRCh38")]
    Grch38,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct SourceIdentity {
    pub release: Release,
    pub cohort: Cohort,
    pub source_variant_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Frequency {
    /// `all` for the cohort-wide values, otherwise the exact source division suffix.
    pub division: String,
    pub ac: Option<Vec<u32>>,
    pub an: Option<u32>,
    pub af: Option<Vec<f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LengthProvenance {
    InfoAlleleLength,
    InfoSvlen,
    SequenceDerived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AlleleLength {
    pub value: i32,
    pub provenance: LengthProvenance,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SummaryRecord {
    pub identity: SourceIdentity,
    pub reference_genome: ReferenceGenome,
    pub chrom: String,
    pub position: u32,
    pub ref_allele: String,
    pub alts: Vec<String>,
    pub allele_type: Option<String>,
    pub qual: Option<f64>,
    pub filters: Vec<String>,
    pub ac: Vec<u32>,
    pub an: u32,
    pub af: Vec<f64>,
    pub allele_lengths: Vec<AlleleLength>,
    /// Preserve the cohort-specific source shape alongside the aligned representation.
    pub source_allele_length: Option<i32>,
    pub source_svlen: Option<Vec<i32>>,
    pub frequencies: Vec<Frequency>,
    /// Complete INFO values from the input line. Flags have a `None` value.
    pub source_info: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CarrierRecord {
    pub identity: SourceIdentity,
    pub reference_genome: ReferenceGenome,
    pub chrom: String,
    pub position: u32,
    /// One-based VCF ALT index. UInt16 is required by observed Y1 records.
    pub alt_index: u16,
    pub alt: String,
    pub sample_id: String,
    /// Zero-based position in the GT call. This does not imply biological phase.
    pub genotype_position: u16,
    pub gt_alleles: Vec<u16>,
    pub gt_phased: bool,
    /// Scalar/non-position-specific FORMAT values, excluding GT.
    pub genotype_fields: BTreeMap<String, Option<String>>,
    /// FORMAT values aligned to `genotype_position` (AL, ALLR, SD, MC, MS, AP, AM).
    pub position_fields: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RecordStats {
    pub genotype_calls: usize,
    pub missing_genotypes: usize,
    pub reference_genotypes: usize,
    pub carrier_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransformedRecord {
    pub summary: SummaryRecord,
    pub carriers: Vec<CarrierRecord>,
    pub stats: RecordStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectCode {
    HeaderShape,
    MalformedColumns,
    MissingSourceId,
    InvalidPosition,
    InvalidValue,
    MissingInfo,
    CardinalityMismatch,
    FrequencyMismatch,
    SampleCountMismatch,
    InvalidGenotype,
    AltIndexOutOfRange,
    AlleleCountMismatch,
    Io,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransformReject {
    pub code: RejectCode,
    pub record_number: Option<usize>,
    pub source_variant_id: Option<String>,
    pub message: String,
}

impl TransformReject {
    pub fn new(code: RejectCode, message: impl Into<String>) -> Self {
        Self {
            code,
            record_number: None,
            source_variant_id: None,
            message: message.into(),
        }
    }

    pub fn with_source_id(mut self, source_variant_id: impl Into<String>) -> Self {
        self.source_variant_id = Some(source_variant_id.into());
        self
    }
}

impl std::fmt::Display for TransformReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(source_id) = &self.source_variant_id {
            write!(f, "{} ({source_id})", self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}

impl std::error::Error for TransformReject {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TransformationReport {
    pub source_records: usize,
    pub summary_rows: usize,
    pub carrier_rows: usize,
    pub genotype_calls: usize,
    pub missing_genotypes: usize,
    pub reference_genotypes: usize,
    pub rejected_records: usize,
    pub rejects: Vec<TransformReject>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct TransformationBatch {
    pub summaries: Vec<SummaryRecord>,
    pub carriers: Vec<CarrierRecord>,
    pub report: TransformationReport,
}
