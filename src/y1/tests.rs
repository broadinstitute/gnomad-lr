use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const HGSVC_FIXTURE: &str = include_str!("../../tests/fixtures/y1/hgsvc_hprc_trv_13_alt.vcf");
const HGSVC_COLLISION_FIXTURE: &str =
    include_str!("../../tests/fixtures/y1/hgsvc_hprc_collision_suffix.vcf");
const AOU_FIXTURE: &str = include_str!("../../tests/fixtures/y1/aou_summary_only_ins.vcf");
const SEX_CHROMOSOME_RECORDS: &str =
    include_str!("../../tests/fixtures/y1/hgsvc_hprc_sex_chromosome_bounded_records.vcf.records");

fn record_lines(fixture: &str) -> Vec<&str> {
    fixture
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

fn one_record(fixture: &str) -> &str {
    let records = record_lines(fixture);
    assert_eq!(records.len(), 1);
    records[0]
}

#[test]
fn validates_cohort_specific_header_shapes() {
    let hgsvc = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    assert_eq!(hgsvc.reference_genome, ReferenceGenome::Grch38);
    assert_eq!(hgsvc.sample_names.len(), 292);
    assert_eq!(hgsvc.frequency_divisions.len(), 20);
    assert!(hgsvc.frequency_divisions.contains(&"asj".to_string()));
    assert!(hgsvc.frequency_divisions.contains(&"sas_XY".to_string()));

    let aou = Y1Header::parse(AOU_FIXTURE, Cohort::Aou).unwrap();
    assert_eq!(aou.reference_genome, ReferenceGenome::Grch38);
    assert!(aou.sample_names.is_empty());
    assert_eq!(
        aou.frequency_divisions
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "XX".to_string(),
            "XY".to_string(),
            "afr".to_string(),
            "afr_XX".to_string(),
            "afr_XY".to_string(),
        ])
    );

    assert_eq!(
        Y1Header::parse(AOU_FIXTURE, Cohort::HgsvcHprc)
            .unwrap_err()
            .code,
        RejectCode::HeaderShape
    );
    assert_eq!(
        Y1Header::parse(HGSVC_FIXTURE, Cohort::Aou)
            .unwrap_err()
            .code,
        RejectCode::HeaderShape
    );
}

#[test]
fn transforms_source_derived_hgsvc_trv_without_losing_alt_or_phase_semantics() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let transformed = transform_record(&header, one_record(HGSVC_FIXTURE)).unwrap();
    let summary = &transformed.summary;

    assert_eq!(summary.identity.release, Release::Y1);
    assert_eq!(summary.identity.cohort, Cohort::HgsvcHprc);
    assert_eq!(summary.identity.source_variant_id, "chr22-20004491-TRV-21");
    assert_eq!(summary.reference_genome, ReferenceGenome::Grch38);
    assert_eq!(summary.alts.len(), 13);
    assert_eq!(summary.ac, vec![1, 5, 1, 1, 1, 12, 1, 2, 184, 2, 1, 2, 1]);
    assert_eq!(summary.af.len(), summary.alts.len());
    assert_eq!(summary.allele_lengths.len(), summary.alts.len());
    assert!(summary
        .allele_lengths
        .iter()
        .all(|length| length.provenance == LengthProvenance::SequenceDerived));
    for (alt, length) in summary.alts.iter().zip(&summary.allele_lengths) {
        assert_eq!(
            length.value,
            alt.len() as i32 - summary.ref_allele.len() as i32
        );
    }
    assert_eq!(summary.source_allele_length, None);
    assert_eq!(summary.source_svlen, None);
    assert_eq!(
        summary.source_info.get("TRID").and_then(Option::as_deref),
        Some("22-20004502-20004512-A")
    );

    assert_eq!(transformed.stats.genotype_calls, 292);
    assert_eq!(transformed.stats.missing_genotypes, 0);
    assert_eq!(transformed.stats.reference_genotypes, 125);
    assert_eq!(transformed.carriers.len(), 214);
    assert!(transformed
        .carriers
        .iter()
        .all(|carrier| !carrier.gt_phased));

    let mut reconstructed_ac = vec![0u32; summary.alts.len()];
    for carrier in &transformed.carriers {
        reconstructed_ac[carrier.alt_index as usize - 1] += 1;
        assert_eq!(carrier.alt, summary.alts[carrier.alt_index as usize - 1]);
    }
    assert_eq!(reconstructed_ac, summary.ac);

    let hg00097 = transformed
        .carriers
        .iter()
        .find(|carrier| carrier.sample_id == "HG00097")
        .unwrap();
    assert_eq!(hg00097.alt_index, 3);
    assert_eq!(hg00097.genotype_position, 1);
    assert_eq!(hg00097.gt_alleles, vec![Some(0), Some(3)]);
    assert_eq!(
        hg00097.position_fields.get("AL").and_then(Option::as_deref),
        Some("20")
    );
    assert_eq!(
        hg00097.position_fields.get("AP").and_then(Option::as_deref),
        Some("0.9")
    );
}

#[test]
fn transforms_aou_as_summary_only_without_fabricated_populations() {
    let header = Y1Header::parse(AOU_FIXTURE, Cohort::Aou).unwrap();
    let transformed = transform_record(&header, one_record(AOU_FIXTURE)).unwrap();
    let summary = transformed.summary;

    assert_eq!(summary.identity.source_variant_id, "chr22-20001505-INS-486");
    assert_eq!(summary.identity.cohort, Cohort::Aou);
    assert_eq!(summary.alts.len(), 1);
    assert_eq!(summary.ac, vec![1]);
    assert_eq!(summary.an, 2052);
    assert_eq!(summary.source_allele_length, Some(486));
    assert_eq!(summary.source_svlen, Some(vec![486]));
    assert_eq!(summary.allele_lengths[0].value, 486);
    assert_eq!(
        summary.allele_lengths[0].provenance,
        LengthProvenance::InfoAlleleLength
    );
    assert_eq!(summary.filters, vec!["SINGLE_READ_SUPPORT"]);
    assert!(transformed.carriers.is_empty());

    let divisions: BTreeSet<&str> = summary
        .frequencies
        .iter()
        .map(|frequency| frequency.division.as_str())
        .collect();
    assert_eq!(
        divisions,
        BTreeSet::from(["all", "XX", "XY", "afr", "afr_XX", "afr_XY"])
    );
    for unavailable in ["amr", "asj", "eas", "nfe", "sas"] {
        assert!(!divisions.contains(unavailable));
    }
}

#[test]
fn aggregate_only_replays_exact_sex_chromosome_failures_from_info_without_carriers() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let expected = [
        (
            "chrX-2781454-DEL-4",
            "6274c4b98d50b4c31af08eae219e119db4a8437fa24cff06c9742c9b4b205ed7",
            438,
            vec![1],
        ),
        (
            "chrX-2781514-C-A",
            "caffcfbf60dfbc79a16bdc0931b0cfdce6387cee2f1e2345d3774564ea6e56d3",
            365,
            vec![178],
        ),
        (
            "chrX-9999320-TRV-17",
            "2b419f8ccddb7378e0541993b85274efe815ec623096c37009e259f1f7cda9b7",
            438,
            vec![66, 9, 1, 9, 266, 9, 33, 1],
        ),
        (
            "chrY-25000057-A-G",
            "4e21084785d16e2fc1169a49a646b61cfa528cd8d99c82cc605f90d97b077d6b",
            73,
            vec![0],
        ),
        (
            "chrY-4999309-TRV-40",
            "d8a4d40a47996ce6879201ad1be60213c5acf7e9bafba3ec205bbe3bcc643d18",
            72,
            vec![0],
        ),
    ];
    let records = record_lines(SEX_CHROMOSOME_RECORDS);
    assert_eq!(records.len(), expected.len());
    for (record, (_, expected_sha256, _, _)) in records.iter().zip(&expected) {
        assert_eq!(
            format!("{:x}", Sha256::digest(record.as_bytes())),
            *expected_sha256
        );
    }

    let batch = transform_records_with_mode(
        &header,
        records.iter().copied(),
        Some(PrimaryLoadMode::AggregateOnlyNoCarriers),
    );
    assert_eq!(batch.report.source_records, 5);
    assert_eq!(batch.report.summary_rows, 5);
    assert_eq!(batch.report.rejected_records, 0);
    assert_eq!(batch.report.genotype_calls, 0);
    assert_eq!(batch.report.carrier_rows, 0);
    assert!(batch.carriers.is_empty());
    for (summary, (source_id, _, an, ac)) in batch.summaries.iter().zip(expected) {
        assert_eq!(summary.identity.source_variant_id, source_id);
        assert_eq!(summary.an, an);
        assert_eq!(summary.ac, ac);
        assert_eq!(
            summary
                .source_info
                .get("AN")
                .and_then(Option::as_deref)
                .unwrap()
                .parse::<u32>()
                .unwrap(),
            an
        );
    }
}

#[test]
fn aggregate_only_never_touches_format_or_genotype_columns() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let exact_failure = record_lines(SEX_CHROMOSOME_RECORDS)[1];
    let mut fixed_only: Vec<&str> = exact_failure.split('\t').take(8).collect();
    fixed_only.push("THIS_IS_NOT_FORMAT_OR_GT");
    let transformed = transform_record_with_mode(
        &header,
        &fixed_only.join("\t"),
        Some(PrimaryLoadMode::AggregateOnlyNoCarriers),
    )
    .unwrap();
    assert_eq!(transformed.summary.an, 365);
    assert_eq!(transformed.summary.ac, vec![178]);
    assert!(transformed.carriers.is_empty());
    assert_eq!(transformed.stats.genotype_calls, 0);
}

#[test]
fn aggregate_only_parser_mode_is_sex_chromosome_hgsvc_only_and_ordinary_remains_strict() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let exact_non_par_failure = record_lines(SEX_CHROMOSOME_RECORDS)[1];
    assert_eq!(
        transform_record(&header, exact_non_par_failure)
            .unwrap_err()
            .code,
        RejectCode::AlleleCountMismatch
    );
    assert_eq!(
        transform_record_with_mode(
            &header,
            one_record(HGSVC_FIXTURE),
            Some(PrimaryLoadMode::AggregateOnlyNoCarriers),
        )
        .unwrap_err()
        .code,
        RejectCode::InvalidValue
    );

    let aou = Y1Header::parse(AOU_FIXTURE, Cohort::Aou).unwrap();
    let aou_sex = one_record(AOU_FIXTURE).replacen("chr22", "chrX", 2);
    assert_eq!(
        transform_record_with_mode(
            &aou,
            &aou_sex,
            Some(PrimaryLoadMode::AggregateOnlyNoCarriers),
        )
        .unwrap_err()
        .code,
        RejectCode::InvalidValue
    );
}

#[test]
fn preserves_source_derived_collision_suffixes_byte_for_byte() {
    let header = Y1Header::parse(HGSVC_COLLISION_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let transformed = transform_record(&header, one_record(HGSVC_COLLISION_FIXTURE)).unwrap();

    assert_eq!(
        transformed.summary.identity.source_variant_id,
        "chr22-20147573-INS-2_2"
    );
    assert_eq!(transformed.summary.ref_allele, "T");
    assert_eq!(transformed.summary.alts, vec!["TTG"]);
}

#[test]
fn supports_alt_indices_above_u8_capacity() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let alts: Vec<String> = (0..256).map(unique_dna_alt).collect();
    let mut ac = vec!["0"; 256];
    ac[255] = "1";
    let mut af = vec!["0"; 256];
    af[255] = "0.0017123287671232876";
    let info = format!(
        "AC={};AN=584;AF={};allele_type=trv",
        ac.join(","),
        af.join(",")
    );
    let mut columns = vec![
        "chr22".to_string(),
        "20009999".to_string(),
        "chr22-20009999-TRV-u16".to_string(),
        "A".to_string(),
        alts.join(","),
        ".".to_string(),
        "PASS".to_string(),
        info,
        "GT".to_string(),
    ];
    columns.extend((0..292).map(|index| {
        if index == 0 {
            "0/256".to_string()
        } else {
            "0/0".to_string()
        }
    }));

    let transformed = transform_record(&header, &columns.join("\t")).unwrap();
    assert_eq!(transformed.summary.alts.len(), 256);
    assert_eq!(transformed.carriers.len(), 1);
    assert_eq!(transformed.carriers[0].alt_index, 256);
    assert_eq!(transformed.carriers[0].gt_alleles, vec![Some(0), Some(256)]);
}

#[test]
fn preserves_partial_genotype_positions_and_counts_only_called_alleles() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let mut columns = vec![
        "chr22".to_string(),
        "20005794".to_string(),
        "chr22-20005794-INS-1".to_string(),
        "T".to_string(),
        "TA".to_string(),
        "53".to_string(),
        "PASS".to_string(),
        "allele_length=1;allele_type=ins;AC=1;AN=1;AF=1".to_string(),
        "GT:DP:GQ".to_string(),
    ];
    columns.extend((0..292).map(|index| {
        if index == 0 {
            "./1:22:0".to_string()
        } else {
            "./.:.:.".to_string()
        }
    }));

    let transformed = transform_record(&header, &columns.join("\t")).unwrap();
    assert_eq!(transformed.stats.genotype_calls, 292);
    assert_eq!(transformed.stats.missing_genotypes, 291);
    assert_eq!(transformed.stats.partially_called_genotypes, 1);
    assert_eq!(transformed.stats.reference_genotypes, 0);
    assert_eq!(transformed.carriers.len(), 1);

    let carrier = &transformed.carriers[0];
    assert_eq!(carrier.sample_id, header.sample_names[0]);
    assert_eq!(carrier.alt_index, 1);
    assert_eq!(carrier.genotype_position, 1);
    assert_eq!(carrier.gt_alleles, vec![None, Some(1)]);
}

#[test]
fn counts_partial_reference_without_emitting_a_carrier() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let mut columns = vec![
        "chr22".to_string(),
        "20005794".to_string(),
        "chr22-20005794-T-A".to_string(),
        "T".to_string(),
        "A".to_string(),
        "53".to_string(),
        "PASS".to_string(),
        "allele_length=0;allele_type=snv;AC=0;AN=1;AF=0".to_string(),
        "GT".to_string(),
    ];
    columns.extend((0..292).map(|index| {
        if index == 0 {
            "./0".to_string()
        } else {
            "./.".to_string()
        }
    }));

    let transformed = transform_record(&header, &columns.join("\t")).unwrap();
    assert_eq!(transformed.stats.missing_genotypes, 291);
    assert_eq!(transformed.stats.partially_called_genotypes, 1);
    assert_eq!(transformed.stats.reference_genotypes, 0);
    assert!(transformed.carriers.is_empty());
}

#[test]
fn accepts_scalar_al_as_a_genotype_level_sv_field() {
    let header = Y1Header::parse(HGSVC_FIXTURE, Cohort::HgsvcHprc).unwrap();
    let mut columns = vec![
        "chr22".to_string(),
        "20006981".to_string(),
        "chr22-20006981-DEL-146".to_string(),
        "CA".to_string(),
        "C".to_string(),
        "1".to_string(),
        "PASS".to_string(),
        "allele_length=-1;allele_type=del;AC=1;AN=584;AF=0.0017123287671232876".to_string(),
        "GT:AL".to_string(),
    ];
    columns.extend((0..292).map(|index| {
        if index == 0 {
            "0|1:145".to_string()
        } else {
            "0/0:.".to_string()
        }
    }));

    let transformed = transform_record(&header, &columns.join("\t")).unwrap();
    assert_eq!(transformed.carriers.len(), 1);
    let carrier = &transformed.carriers[0];
    assert_eq!(carrier.gt_alleles, vec![Some(0), Some(1)]);
    assert_eq!(
        carrier.genotype_fields.get("AL").and_then(Option::as_deref),
        Some("145")
    );
    assert!(!carrier.position_fields.contains_key("AL"));
}

#[test]
fn rejects_array_mismatches_and_reports_every_source_record() {
    let header = Y1Header::parse(AOU_FIXTURE, Cohort::Aou).unwrap();
    let valid = one_record(AOU_FIXTURE);
    let invalid = valid.replacen("AC=1;", "AC=1,2;", 1);
    let batch = transform_records(&header, [valid, invalid.as_str()]);

    assert_eq!(batch.report.source_records, 2);
    assert_eq!(batch.report.summary_rows, 1);
    assert_eq!(batch.report.rejected_records, 1);
    assert_eq!(batch.report.rejects.len(), 1);
    assert_eq!(batch.report.rejects[0].record_number, Some(2));
    assert_eq!(
        batch.report.rejects[0].code,
        RejectCode::CardinalityMismatch
    );
}

#[test]
fn source_record_can_define_an_actual_task_boundary() {
    let header = Y1Header::parse(AOU_FIXTURE, Cohort::Aou).unwrap();
    let summary = transform_record(&header, one_record(AOU_FIXTURE))
        .unwrap()
        .summary;
    assert_eq!(summary.position, 20_001_505);

    let left_task = 20_000_000..=20_001_504;
    let right_task = 20_001_505..=20_010_000;
    assert!(!left_task.contains(&summary.position));
    assert!(right_task.contains(&summary.position));
}

fn unique_dna_alt(index: usize) -> String {
    let mut value = index;
    let mut suffix = ['A'; 8];
    for base in suffix.iter_mut().rev() {
        *base = match value & 0b11 {
            0 => 'A',
            1 => 'C',
            2 => 'G',
            _ => 'T',
        };
        value >>= 2;
    }
    format!("T{}", suffix.iter().collect::<String>())
}
