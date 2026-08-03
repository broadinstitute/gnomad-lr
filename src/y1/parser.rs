use super::model::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};

const FREQUENCY_TOLERANCE: f64 = 5e-6;
const POSITION_FORMAT_FIELDS: &[&str] = &["AL", "ALLR", "SD", "MC", "MS", "AP", "AM"];
const HGSVC_POPULATIONS: &[&str] = &["afr", "amr", "asj", "eas", "nfe", "sas"];
const AOU_POPULATIONS: &[&str] = &["afr"];
const REQUIRED_HGSVC_FORMAT_FIELDS: &[&str] = &[
    "GT", "RNC", "DP", "GQ", "AL", "APOS", "PS", "PF", "ALLR", "SD", "MC", "MS", "AP", "AM", "AD",
    "PL", "EV", "BEV",
];

type FormatFields = BTreeMap<String, Option<String>>;
type PositionFormatFields = Vec<FormatFields>;

struct CarrierContext<'a> {
    identity: &'a SourceIdentity,
    chrom: &'a str,
    position: u32,
    alts: &'a [String],
    expected_ac: &'a [u32],
    expected_an: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDefinition {
    pub number: String,
    pub value_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Y1Header {
    pub cohort: Cohort,
    pub reference_genome: ReferenceGenome,
    pub sample_names: Vec<String>,
    pub frequency_divisions: Vec<String>,
    pub info_fields: BTreeMap<String, FieldDefinition>,
    pub format_fields: BTreeMap<String, FieldDefinition>,
}

impl Y1Header {
    pub fn parse(text: &str, cohort: Cohort) -> Result<Self, TransformReject> {
        let mut info_fields = BTreeMap::new();
        let mut format_fields = BTreeMap::new();
        let mut reference = None;
        let mut column_header = None;

        for line in text.lines() {
            if let Some(value) = line.strip_prefix("##reference=") {
                if reference.replace(value.to_string()).is_some() {
                    return Err(header_error("multiple ##reference declarations"));
                }
            } else if let Some(raw) = line.strip_prefix("##INFO=<") {
                let (id, definition) = parse_field_definition(raw, "INFO")?;
                if info_fields.insert(id.clone(), definition).is_some() {
                    return Err(header_error(format!("duplicate INFO definition {id}")));
                }
            } else if let Some(raw) = line.strip_prefix("##FORMAT=<") {
                let (id, definition) = parse_field_definition(raw, "FORMAT")?;
                if format_fields.insert(id.clone(), definition).is_some() {
                    return Err(header_error(format!("duplicate FORMAT definition {id}")));
                }
            } else if line.starts_with("#CHROM")
                && column_header.replace(line.to_string()).is_some()
            {
                return Err(header_error("multiple #CHROM header lines"));
            }
        }

        let reference = reference.ok_or_else(|| header_error("missing ##reference declaration"))?;
        let normalized_reference = reference.to_ascii_lowercase();
        let reference_genome =
            if normalized_reference.contains("grch38") || normalized_reference.contains("hg38") {
                ReferenceGenome::Grch38
            } else {
                return Err(header_error(format!(
                    "unsupported reference genome {reference:?}; expected GRCh38/hg38"
                )));
            };

        let column_header = column_header.ok_or_else(|| header_error("missing #CHROM header"))?;
        let columns: Vec<&str> = column_header.split('\t').collect();
        if columns.len() < 8
            || columns[..8]
                != [
                    "#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO",
                ]
        {
            return Err(header_error("invalid fixed VCF columns in #CHROM header"));
        }

        let sample_names = match cohort {
            Cohort::HgsvcHprc => {
                if columns.get(8) != Some(&"FORMAT") {
                    return Err(header_error("HGSVC/HPRC header is missing FORMAT"));
                }
                let names: Vec<String> = columns[9..]
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect();
                if names.len() != 292 {
                    return Err(header_error(format!(
                        "HGSVC/HPRC header has {} samples; expected 292",
                        names.len()
                    )));
                }
                let unique: HashSet<&str> = names.iter().map(String::as_str).collect();
                if unique.len() != names.len() {
                    return Err(header_error("HGSVC/HPRC header has duplicate sample IDs"));
                }
                names
            }
            Cohort::Aou => {
                if columns.len() != 8 {
                    return Err(header_error(
                        "AoU header must be summary-only with no FORMAT or sample columns",
                    ));
                }
                Vec::new()
            }
        };

        require_field(&info_fields, "AN", "1", "Integer", "INFO")?;
        require_field(&info_fields, "AC", "A", "Integer", "INFO")?;
        require_field(&info_fields, "AF", "A", "Float", "INFO")?;
        require_field(&info_fields, "allele_length", "1", "Integer", "INFO")?;
        require_field(&info_fields, "allele_type", "1", "String", "INFO")?;
        match cohort {
            Cohort::HgsvcHprc => {
                require_field(&info_fields, "SVLEN", "1", "Integer", "INFO")?;
                for field in REQUIRED_HGSVC_FORMAT_FIELDS {
                    if !format_fields.contains_key(*field) {
                        return Err(header_error(format!(
                            "HGSVC/HPRC header is missing FORMAT/{field}"
                        )));
                    }
                }
            }
            Cohort::Aou => {
                require_field(&info_fields, "SVLEN", ".", "Integer", "INFO")?;
                if !format_fields.is_empty() {
                    return Err(header_error(
                        "AoU header unexpectedly declares FORMAT fields",
                    ));
                }
            }
        }

        let frequency_divisions = discover_frequency_divisions(&info_fields);
        let expected_divisions = expected_frequency_divisions(cohort);
        let actual_divisions: BTreeSet<String> = frequency_divisions.iter().cloned().collect();
        if actual_divisions != expected_divisions {
            let missing: Vec<_> = expected_divisions
                .difference(&actual_divisions)
                .cloned()
                .collect();
            let unexpected: Vec<_> = actual_divisions
                .difference(&expected_divisions)
                .cloned()
                .collect();
            return Err(header_error(format!(
                "frequency division mismatch; missing={missing:?}, unexpected={unexpected:?}"
            )));
        }

        Ok(Self {
            cohort,
            reference_genome,
            sample_names,
            frequency_divisions,
            info_fields,
            format_fields,
        })
    }
}

pub fn transform_record(
    header: &Y1Header,
    line: &str,
) -> Result<TransformedRecord, TransformReject> {
    transform_record_with_mode(header, line, None)
}

pub fn transform_record_with_mode(
    header: &Y1Header,
    line: &str,
    primary_load_mode: Option<PrimaryLoadMode>,
) -> Result<TransformedRecord, TransformReject> {
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() < 8 {
        return Err(TransformReject::new(
            RejectCode::MalformedColumns,
            format!(
                "VCF record has {} columns; expected at least 8",
                parts.len()
            ),
        ));
    }

    let source_id = parts[2];
    if source_id.is_empty() || source_id == "." {
        return Err(TransformReject::new(
            RejectCode::MissingSourceId,
            "VCF record has no source ID",
        ));
    }

    transform_record_with_id(header, &parts, source_id, primary_load_mode)
        .map_err(|reject| reject.with_source_id(source_id))
}

pub fn transform_records<'a, I>(header: &Y1Header, records: I) -> TransformationBatch
where
    I: IntoIterator<Item = &'a str>,
{
    transform_records_with_mode(header, records, None)
}

pub fn transform_records_with_mode<'a, I>(
    header: &Y1Header,
    records: I,
    primary_load_mode: Option<PrimaryLoadMode>,
) -> TransformationBatch
where
    I: IntoIterator<Item = &'a str>,
{
    let mut batch = TransformationBatch::default();

    for (index, line) in records.into_iter().enumerate() {
        batch.report.source_records += 1;
        match transform_record_with_mode(header, line, primary_load_mode) {
            Ok(transformed) => {
                batch.report.summary_rows += 1;
                batch.report.carrier_rows += transformed.carriers.len();
                batch.report.genotype_calls += transformed.stats.genotype_calls;
                batch.report.missing_genotypes += transformed.stats.missing_genotypes;
                batch.report.partially_called_genotypes +=
                    transformed.stats.partially_called_genotypes;
                batch.report.reference_genotypes += transformed.stats.reference_genotypes;
                batch.summaries.push(transformed.summary);
                batch.carriers.extend(transformed.carriers);
            }
            Err(mut reject) => {
                reject.record_number = Some(index + 1);
                batch.report.rejected_records += 1;
                batch.report.rejects.push(reject);
            }
        }
    }

    batch
}

fn transform_record_with_id(
    header: &Y1Header,
    parts: &[&str],
    source_id: &str,
    primary_load_mode: Option<PrimaryLoadMode>,
) -> Result<TransformedRecord, TransformReject> {
    let chrom = required_text(parts[0], "CHROM")?;
    let position = parts[1].parse::<u32>().map_err(|error| {
        TransformReject::new(
            RejectCode::InvalidPosition,
            format!("invalid POS {:?}: {error}", parts[1]),
        )
    })?;
    if position == 0 {
        return Err(TransformReject::new(
            RejectCode::InvalidPosition,
            "VCF POS must be one-based",
        ));
    }

    let ref_allele = required_text(parts[3], "REF")?.to_string();
    let alts: Vec<String> = parts[4]
        .split(',')
        .map(|alt| required_text(alt, "ALT").map(str::to_string))
        .collect::<Result<_, _>>()?;
    if alts.is_empty() {
        return Err(TransformReject::new(
            RejectCode::CardinalityMismatch,
            "record has no ALT alleles",
        ));
    }
    if alts.len() > u16::MAX as usize {
        return Err(TransformReject::new(
            RejectCode::CardinalityMismatch,
            format!("{} ALT alleles exceed UInt16 capacity", alts.len()),
        ));
    }

    let qual = parse_optional_float(parts[5], "QUAL")?;
    let filters = if parts[6] == "." || parts[6] == "PASS" {
        Vec::new()
    } else {
        parts[6].split(';').map(str::to_string).collect()
    };
    let source_info = parse_info(parts[7])?;

    let ac = parse_required_u32_list(&source_info, "AC")?;
    let an = parse_required_u32(&source_info, "AN")?;
    let af = parse_required_float_list(&source_info, "AF")?;
    validate_frequency("all", &ac, an, &af, alts.len())?;

    let source_allele_length = parse_optional_i32_scalar(&source_info, "allele_length")?;
    let source_svlen = parse_optional_i32_list(&source_info, "SVLEN")?;
    if let Some(values) = &source_svlen {
        if values.len() != 1 && values.len() != alts.len() {
            return Err(TransformReject::new(
                RejectCode::CardinalityMismatch,
                format!(
                    "SVLEN has {} values for {} ALT alleles",
                    values.len(),
                    alts.len()
                ),
            ));
        }
    }
    let allele_lengths = align_allele_lengths(
        &ref_allele,
        &alts,
        source_allele_length,
        source_svlen.as_deref(),
    )?;

    let mut frequencies = vec![Frequency {
        division: "all".to_string(),
        ac: Some(ac.clone()),
        an: Some(an),
        af: Some(af.clone()),
    }];
    for division in &header.frequency_divisions {
        let division_ac = parse_optional_u32_list(&source_info, &format!("AC_{division}"))?;
        let division_an = parse_optional_u32_scalar(&source_info, &format!("AN_{division}"))?;
        let division_af = parse_optional_float_list(&source_info, &format!("AF_{division}"))?;

        if let Some(values) = &division_ac {
            validate_cardinality(&format!("AC_{division}"), values.len(), alts.len())?;
        }
        if let Some(values) = &division_af {
            validate_cardinality(&format!("AF_{division}"), values.len(), alts.len())?;
        }
        if let (Some(division_ac), Some(division_an), Some(division_af)) =
            (&division_ac, division_an, &division_af)
        {
            validate_frequency(division, division_ac, division_an, division_af, alts.len())?;
        }

        frequencies.push(Frequency {
            division: division.clone(),
            ac: division_ac,
            an: division_an,
            af: division_af,
        });
    }

    let identity = SourceIdentity {
        release: Release::Y1,
        cohort: header.cohort,
        source_variant_id: source_id.to_string(),
    };
    let carrier_context = CarrierContext {
        identity: &identity,
        chrom,
        position,
        alts: &alts,
        expected_ac: &ac,
        expected_an: an,
    };
    let (carriers, stats) = match primary_load_mode {
        Some(PrimaryLoadMode::AggregateOnlyNoCarriers) => {
            if header.cohort != Cohort::HgsvcHprc || !matches!(chrom, "chrX" | "chrY") {
                return Err(TransformReject::new(
                    RejectCode::InvalidValue,
                    "aggregate_only_no_carriers is restricted to HGSVC/HPRC chrX/chrY",
                ));
            }
            // Intentionally do not inspect FORMAT or any sample value. The source
            // aggregate is authoritative and the per-sample inclusion contract is
            // unavailable, so parsing GT/ALLR here would create misleading carriers.
            (Vec::new(), RecordStats::default())
        }
        None => parse_carriers(header, parts, &carrier_context)?,
    };

    let summary = SummaryRecord {
        identity,
        reference_genome: header.reference_genome,
        chrom: chrom.to_string(),
        position,
        ref_allele,
        alts,
        allele_type: source_info.get("allele_type").and_then(Clone::clone),
        qual,
        filters,
        ac,
        an,
        af,
        allele_lengths,
        source_allele_length,
        source_svlen,
        frequencies,
        source_info,
    };

    Ok(TransformedRecord {
        summary,
        carriers,
        stats,
    })
}

fn parse_carriers(
    header: &Y1Header,
    parts: &[&str],
    context: &CarrierContext<'_>,
) -> Result<(Vec<CarrierRecord>, RecordStats), TransformReject> {
    if header.cohort == Cohort::Aou {
        if parts.len() != 8 {
            return Err(TransformReject::new(
                RejectCode::SampleCountMismatch,
                "AoU record unexpectedly has FORMAT or sample columns",
            ));
        }
        return Ok((Vec::new(), RecordStats::default()));
    }

    let expected_columns = 9 + header.sample_names.len();
    if parts.len() != expected_columns {
        return Err(TransformReject::new(
            RejectCode::SampleCountMismatch,
            format!(
                "record has {} columns; expected {expected_columns} for {} samples",
                parts.len(),
                header.sample_names.len()
            ),
        ));
    }

    let format_keys: Vec<&str> = parts[8].split(':').collect();
    let mut unique_keys = HashSet::new();
    for key in &format_keys {
        if !unique_keys.insert(*key) {
            return Err(TransformReject::new(
                RejectCode::InvalidGenotype,
                format!("duplicate FORMAT key {key}"),
            ));
        }
    }
    let gt_index = format_keys
        .iter()
        .position(|key| *key == "GT")
        .ok_or_else(|| {
            TransformReject::new(RejectCode::InvalidGenotype, "record FORMAT has no GT")
        })?;

    let mut carriers = Vec::new();
    let mut stats = RecordStats::default();
    let mut observed_ac = vec![0u32; context.alts.len()];
    let mut observed_an = 0u32;

    for (sample_id, sample_value) in header.sample_names.iter().zip(&parts[9..]) {
        stats.genotype_calls += 1;
        let values: Vec<&str> = sample_value.split(':').collect();
        if values.len() > format_keys.len() {
            return Err(TransformReject::new(
                RejectCode::InvalidGenotype,
                format!(
                    "sample {sample_id} has {} FORMAT values for {} keys",
                    values.len(),
                    format_keys.len()
                ),
            ));
        }
        let gt = values.get(gt_index).copied().unwrap_or(".");
        let Some((gt_alleles, gt_phased)) = parse_genotype(gt, sample_id)? else {
            stats.missing_genotypes += 1;
            continue;
        };
        let called_alleles = gt_alleles.iter().flatten().count();
        if called_alleles < gt_alleles.len() {
            stats.partially_called_genotypes += 1;
        }
        observed_an = observed_an
            .checked_add(u32::try_from(called_alleles).map_err(|_| {
                TransformReject::new(
                    RejectCode::InvalidGenotype,
                    "called allele count exceeds UInt32",
                )
            })?)
            .ok_or_else(|| TransformReject::new(RejectCode::InvalidGenotype, "AN overflow"))?;

        for allele in gt_alleles.iter().flatten() {
            if *allele as usize > context.alts.len() {
                return Err(TransformReject::new(
                    RejectCode::AltIndexOutOfRange,
                    format!(
                        "sample {sample_id} GT allele {allele} exceeds {} ALT alleles",
                        context.alts.len()
                    ),
                ));
            }
            if *allele > 0 {
                observed_ac[*allele as usize - 1] += 1;
            }
        }

        if gt_alleles.iter().all(|allele| matches!(allele, Some(0))) {
            stats.reference_genotypes += 1;
            continue;
        }

        let (genotype_fields, position_fields) =
            parse_format_fields(&format_keys, &values, gt_alleles.len(), sample_id)?;

        for (gt_position, alt_index) in gt_alleles.iter().copied().enumerate() {
            let Some(alt_index) = alt_index else {
                continue;
            };
            if alt_index == 0 {
                continue;
            }
            carriers.push(CarrierRecord {
                identity: context.identity.clone(),
                reference_genome: header.reference_genome,
                chrom: context.chrom.to_string(),
                position: context.position,
                alt_index,
                alt: context.alts[alt_index as usize - 1].clone(),
                sample_id: sample_id.clone(),
                genotype_position: u16::try_from(gt_position).map_err(|_| {
                    TransformReject::new(
                        RejectCode::InvalidGenotype,
                        "genotype position exceeds UInt16",
                    )
                })?,
                gt_alleles: gt_alleles.clone(),
                gt_phased,
                genotype_fields: genotype_fields.clone(),
                position_fields: position_fields[gt_position].clone(),
            });
        }
    }

    if observed_an != context.expected_an {
        return Err(TransformReject::new(
            RejectCode::AlleleCountMismatch,
            format!(
                "GT calls reconstruct AN={observed_an}, but INFO/AN={}",
                context.expected_an
            ),
        ));
    }
    if observed_ac != context.expected_ac {
        return Err(TransformReject::new(
            RejectCode::AlleleCountMismatch,
            format!(
                "GT calls reconstruct AC={observed_ac:?}, but INFO/AC={:?}",
                context.expected_ac
            ),
        ));
    }

    stats.carrier_rows = carriers.len();
    Ok((carriers, stats))
}

fn parse_format_fields(
    format_keys: &[&str],
    values: &[&str],
    ploidy: usize,
    sample_id: &str,
) -> Result<(FormatFields, PositionFormatFields), TransformReject> {
    let mut genotype_fields = BTreeMap::new();
    let mut position_fields = vec![BTreeMap::new(); ploidy];

    for (index, key) in format_keys.iter().enumerate() {
        if *key == "GT" {
            continue;
        }
        let raw = values.get(index).copied().unwrap_or(".");
        let value = optional_text(raw);
        if POSITION_FORMAT_FIELDS.contains(key) {
            if let Some(value) = value {
                let per_position: Vec<&str> = value.split(',').collect();
                if per_position.len() == ploidy {
                    for (position, position_value) in per_position.into_iter().enumerate() {
                        position_fields[position]
                            .insert((*key).to_string(), optional_text(position_value));
                    }
                } else if *key == "AL" && per_position.len() == 1 {
                    // Y1 SV records declare FORMAT/AL Number=1 and use it as a
                    // genotype-level value, while TR records carry one AL per GT position.
                    genotype_fields.insert((*key).to_string(), optional_text(per_position[0]));
                } else {
                    return Err(TransformReject::new(
                        RejectCode::CardinalityMismatch,
                        format!(
                            "sample {sample_id} FORMAT/{key} has {} values for ploidy {ploidy}",
                            per_position.len()
                        ),
                    ));
                }
            } else {
                for fields in &mut position_fields {
                    fields.insert((*key).to_string(), None);
                }
            }
        } else {
            genotype_fields.insert((*key).to_string(), value);
        }
    }

    Ok((genotype_fields, position_fields))
}

fn parse_genotype(
    raw: &str,
    sample_id: &str,
) -> Result<Option<(Vec<Option<u16>>, bool)>, TransformReject> {
    let has_phased = raw.contains('|');
    let has_unphased = raw.contains('/');
    if has_phased && has_unphased {
        return Err(TransformReject::new(
            RejectCode::InvalidGenotype,
            format!("sample {sample_id} GT {raw:?} mixes phased and unphased separators"),
        ));
    }

    let fields: Vec<&str> = if has_phased {
        raw.split('|').collect()
    } else if has_unphased {
        raw.split('/').collect()
    } else {
        vec![raw]
    };
    if fields.iter().all(|field| *field == "." || field.is_empty()) {
        return Ok(None);
    }
    let alleles = fields
        .iter()
        .map(|field| {
            if *field == "." || field.is_empty() {
                Ok(None)
            } else {
                field.parse::<u16>().map(Some).map_err(|error| {
                    TransformReject::new(
                        RejectCode::InvalidGenotype,
                        format!("sample {sample_id} has invalid GT allele {field:?}: {error}"),
                    )
                })
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some((alleles, has_phased)))
}

fn align_allele_lengths(
    ref_allele: &str,
    alts: &[String],
    source_allele_length: Option<i32>,
    source_svlen: Option<&[i32]>,
) -> Result<Vec<AlleleLength>, TransformReject> {
    if alts.len() == 1 {
        if let Some(value) = source_allele_length {
            return Ok(vec![AlleleLength {
                value,
                provenance: LengthProvenance::InfoAlleleLength,
            }]);
        }
        if let Some([value]) = source_svlen {
            return Ok(vec![AlleleLength {
                value: *value,
                provenance: LengthProvenance::InfoSvlen,
            }]);
        }
    }
    if let Some(values) = source_svlen {
        if values.len() == alts.len() {
            return Ok(values
                .iter()
                .map(|value| AlleleLength {
                    value: *value,
                    provenance: LengthProvenance::InfoSvlen,
                })
                .collect());
        }
    }

    let ref_len = i32::try_from(ref_allele.len())
        .map_err(|_| TransformReject::new(RejectCode::InvalidValue, "REF length exceeds Int32"))?;
    alts.iter()
        .map(|alt| {
            if alt.starts_with('<') || alt.contains('[') || alt.contains(']') {
                return Err(TransformReject::new(
                    RejectCode::MissingInfo,
                    format!("cannot derive a sequence length for symbolic ALT {alt:?}"),
                ));
            }
            let alt_len = i32::try_from(alt.len()).map_err(|_| {
                TransformReject::new(RejectCode::InvalidValue, "ALT length exceeds Int32")
            })?;
            Ok(AlleleLength {
                value: alt_len - ref_len,
                provenance: LengthProvenance::SequenceDerived,
            })
        })
        .collect()
}

fn parse_info(raw: &str) -> Result<BTreeMap<String, Option<String>>, TransformReject> {
    let mut info = BTreeMap::new();
    if raw == "." || raw.is_empty() {
        return Ok(info);
    }
    for entry in raw.split(';') {
        let (key, value) = match entry.split_once('=') {
            Some((key, value)) => (key, optional_text(value)),
            None => (entry, None),
        };
        if key.is_empty() {
            return Err(TransformReject::new(
                RejectCode::InvalidValue,
                "INFO contains an empty key",
            ));
        }
        if info.insert(key.to_string(), value).is_some() {
            return Err(TransformReject::new(
                RejectCode::InvalidValue,
                format!("INFO contains duplicate key {key}"),
            ));
        }
    }
    Ok(info)
}

fn parse_required_u32_list(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<Vec<u32>, TransformReject> {
    let raw = required_info(info, key)?;
    parse_number_list(raw, key, |value| value.parse::<u32>())
}

fn parse_required_float_list(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<Vec<f64>, TransformReject> {
    let raw = required_info(info, key)?;
    let values = parse_number_list(raw, key, |value| value.parse::<f64>())?;
    if values.iter().any(|value| !value.is_finite()) {
        return Err(TransformReject::new(
            RejectCode::InvalidValue,
            format!("INFO/{key} contains a non-finite value"),
        ));
    }
    Ok(values)
}

fn parse_required_u32(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<u32, TransformReject> {
    let raw = required_info(info, key)?;
    if raw.contains(',') {
        return Err(TransformReject::new(
            RejectCode::CardinalityMismatch,
            format!("INFO/{key} must be scalar"),
        ));
    }
    raw.parse::<u32>().map_err(|error| {
        TransformReject::new(
            RejectCode::InvalidValue,
            format!("invalid INFO/{key} value {raw:?}: {error}"),
        )
    })
}

fn parse_optional_u32_list(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<Option<Vec<u32>>, TransformReject> {
    info.get(key)
        .and_then(Option::as_deref)
        .map(|raw| parse_number_list(raw, key, |value| value.parse::<u32>()))
        .transpose()
}

fn parse_optional_float_list(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<Option<Vec<f64>>, TransformReject> {
    let values = info
        .get(key)
        .and_then(Option::as_deref)
        .map(|raw| parse_number_list(raw, key, |value| value.parse::<f64>()))
        .transpose()?;
    if values
        .as_ref()
        .is_some_and(|values| values.iter().any(|value| !value.is_finite()))
    {
        return Err(TransformReject::new(
            RejectCode::InvalidValue,
            format!("INFO/{key} contains a non-finite value"),
        ));
    }
    Ok(values)
}

fn parse_optional_u32_scalar(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<Option<u32>, TransformReject> {
    parse_optional_scalar(info, key, |value| value.parse::<u32>())
}

fn parse_optional_i32_scalar(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<Option<i32>, TransformReject> {
    parse_optional_scalar(info, key, |value| value.parse::<i32>())
}

fn parse_optional_i32_list(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<Option<Vec<i32>>, TransformReject> {
    info.get(key)
        .and_then(Option::as_deref)
        .map(|raw| parse_number_list(raw, key, |value| value.parse::<i32>()))
        .transpose()
}

fn parse_optional_scalar<T, E, F>(
    info: &BTreeMap<String, Option<String>>,
    key: &str,
    parse: F,
) -> Result<Option<T>, TransformReject>
where
    F: FnOnce(&str) -> Result<T, E>,
    E: std::fmt::Display,
{
    let Some(raw) = info.get(key).and_then(Option::as_deref) else {
        return Ok(None);
    };
    if raw.contains(',') {
        return Err(TransformReject::new(
            RejectCode::CardinalityMismatch,
            format!("INFO/{key} must be scalar"),
        ));
    }
    parse(raw).map(Some).map_err(|error| {
        TransformReject::new(
            RejectCode::InvalidValue,
            format!("invalid INFO/{key} value {raw:?}: {error}"),
        )
    })
}

fn parse_number_list<T, E, F>(raw: &str, key: &str, parse: F) -> Result<Vec<T>, TransformReject>
where
    F: Fn(&str) -> Result<T, E>,
    E: std::fmt::Display,
{
    raw.split(',')
        .map(|value| {
            if value.is_empty() || value == "." {
                return Err(TransformReject::new(
                    RejectCode::InvalidValue,
                    format!("INFO/{key} contains a missing array element"),
                ));
            }
            parse(value).map_err(|error| {
                TransformReject::new(
                    RejectCode::InvalidValue,
                    format!("invalid INFO/{key} value {value:?}: {error}"),
                )
            })
        })
        .collect()
}

fn validate_frequency(
    label: &str,
    ac: &[u32],
    an: u32,
    af: &[f64],
    alt_count: usize,
) -> Result<(), TransformReject> {
    validate_cardinality(&format!("AC_{label}"), ac.len(), alt_count)?;
    validate_cardinality(&format!("AF_{label}"), af.len(), alt_count)?;
    if an > 0 {
        for (index, (ac, af)) in ac.iter().zip(af).enumerate() {
            let expected = *ac as f64 / an as f64;
            if (expected - af).abs() > FREQUENCY_TOLERANCE {
                return Err(TransformReject::new(
                    RejectCode::FrequencyMismatch,
                    format!("{label} ALT {} has AC/AN={expected}, AF={af}", index + 1),
                ));
            }
        }
    }
    Ok(())
}

fn validate_cardinality(
    label: &str,
    actual: usize,
    expected: usize,
) -> Result<(), TransformReject> {
    if actual != expected {
        return Err(TransformReject::new(
            RejectCode::CardinalityMismatch,
            format!("{label} has {actual} values; expected {expected}"),
        ));
    }
    Ok(())
}

fn required_info<'a>(
    info: &'a BTreeMap<String, Option<String>>,
    key: &str,
) -> Result<&'a str, TransformReject> {
    info.get(key)
        .and_then(Option::as_deref)
        .ok_or_else(|| TransformReject::new(RejectCode::MissingInfo, format!("missing INFO/{key}")))
}

fn required_text<'a>(value: &'a str, field: &str) -> Result<&'a str, TransformReject> {
    if value.is_empty() || value == "." {
        Err(TransformReject::new(
            RejectCode::InvalidValue,
            format!("missing {field}"),
        ))
    } else {
        Ok(value)
    }
}

fn optional_text(value: &str) -> Option<String> {
    if value.is_empty() || value == "." {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_optional_float(raw: &str, field: &str) -> Result<Option<f64>, TransformReject> {
    if raw == "." || raw.is_empty() {
        return Ok(None);
    }
    let value = raw.parse::<f64>().map_err(|error| {
        TransformReject::new(
            RejectCode::InvalidValue,
            format!("invalid {field} value {raw:?}: {error}"),
        )
    })?;
    if !value.is_finite() {
        return Err(TransformReject::new(
            RejectCode::InvalidValue,
            format!("{field} must be finite"),
        ));
    }
    Ok(Some(value))
}

fn parse_field_definition(
    raw: &str,
    kind: &str,
) -> Result<(String, FieldDefinition), TransformReject> {
    let raw = raw
        .strip_suffix('>')
        .ok_or_else(|| header_error(format!("unterminated {kind} definition")))?;
    let attributes = parse_meta_attributes(raw)?;
    let id = attributes
        .get("ID")
        .cloned()
        .ok_or_else(|| header_error(format!("{kind} definition has no ID")))?;
    let number = attributes
        .get("Number")
        .cloned()
        .ok_or_else(|| header_error(format!("{kind}/{id} has no Number")))?;
    let value_type = attributes
        .get("Type")
        .cloned()
        .ok_or_else(|| header_error(format!("{kind}/{id} has no Type")))?;
    Ok((id, FieldDefinition { number, value_type }))
}

fn parse_meta_attributes(raw: &str) -> Result<BTreeMap<String, String>, TransformReject> {
    let mut fields = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in raw.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match byte {
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                fields.push(&raw[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    if quoted {
        return Err(header_error("unterminated quote in VCF header definition"));
    }
    fields.push(&raw[start..]);

    let mut attributes = BTreeMap::new();
    for field in fields {
        let Some((key, value)) = field.split_once('=') else {
            continue;
        };
        attributes.insert(key.to_string(), value.trim_matches('"').to_string());
    }
    Ok(attributes)
}

fn require_field(
    fields: &BTreeMap<String, FieldDefinition>,
    id: &str,
    number: &str,
    value_type: &str,
    kind: &str,
) -> Result<(), TransformReject> {
    let definition = fields
        .get(id)
        .ok_or_else(|| header_error(format!("missing {kind}/{id} definition")))?;
    if definition.number != number || definition.value_type != value_type {
        return Err(header_error(format!(
            "{kind}/{id} is Number={},Type={}; expected Number={number},Type={value_type}",
            definition.number, definition.value_type
        )));
    }
    Ok(())
}

fn discover_frequency_divisions(fields: &BTreeMap<String, FieldDefinition>) -> Vec<String> {
    let mut divisions = BTreeSet::new();
    for id in fields.keys() {
        let Some(division) = id.strip_prefix("AC_") else {
            continue;
        };
        if division == "grpmax" {
            continue;
        }
        if fields.contains_key(&format!("AN_{division}"))
            && fields.contains_key(&format!("AF_{division}"))
        {
            divisions.insert(division.to_string());
        }
    }
    divisions.into_iter().collect()
}

fn expected_frequency_divisions(cohort: Cohort) -> BTreeSet<String> {
    let populations = match cohort {
        Cohort::HgsvcHprc => HGSVC_POPULATIONS,
        Cohort::Aou => AOU_POPULATIONS,
    };
    let mut expected = BTreeSet::from(["XX".to_string(), "XY".to_string()]);
    for population in populations {
        expected.insert((*population).to_string());
        expected.insert(format!("{population}_XX"));
        expected.insert(format!("{population}_XY"));
    }
    expected
}

fn header_error(message: impl Into<String>) -> TransformReject {
    TransformReject::new(RejectCode::HeaderShape, message)
}
