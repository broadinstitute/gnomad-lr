use super::*;
use serde_json::Value;

pub(super) fn fixtures() -> Vec<Value> {
    serde_json::from_str(include_str!("../fixtures/source-v1.json")).unwrap()
}

pub(super) fn task() -> ingest::SourceTask {
    ingest::SourceTask {
        contract: CONTRACT.into(),
        cohort: "hgsvc_hprc".into(),
        run_id: "test_1".into(),
        task_id: "histogram_0".into(),
        source_uri: "gs://fixture-bucket/fixture.tsv".into(),
        source_generation: "1".into(),
        source_size_bytes: 1,
        source_md5_base64: "1B2M2Y8AsgTpgAmY7PhCfg==".into(),
        clickhouse_endpoint: "http://127.0.0.1:8123".into(),
        database: "gnomad_lr_y1_scratch_histogram_hgsvc_hprc_test_1".into(),
        worker_principal: "histogram_test_writer".into(),
        allow_remote: false,
    }
}

pub(super) fn text_rows(rows: &[BTreeMap<String, String>]) -> String {
    let keys: Vec<_> = rows[0].keys().cloned().collect();
    let mut text = keys.join("\t") + "\n";
    for row in rows {
        text += &(keys
            .iter()
            .map(|k| row[k].as_str())
            .collect::<Vec<_>>()
            .join("\t")
            + "\n");
    }
    text
}

pub(super) fn fields(fixture: &Value) -> BTreeMap<String, String> {
    serde_json::from_value(fixture["fields"].clone()).unwrap()
}

fn parse(fields: &BTreeMap<String, String>) -> Result<SourceHistogramRow> {
    let names: Vec<_> = fields.keys().map(String::as_str).collect();
    let parts: Vec<_> = fields.values().map(String::as_str).collect();
    SourceHeader::parse(&names)?.row(&parts, &task(), 1)
}

#[test]
fn synthetic_source_fixtures_are_lossless() {
    let examples = fixtures();
    assert!(examples.iter().any(|f| f["category"] == "literal_N_motif"));
    assert!(examples.iter().any(|f| f["category"] == "wider_vc"));
    assert!(examples.iter().any(|f| f["category"] == "wider_blank_vc"));
    assert!(examples
        .iter()
        .any(|f| f["category"] == "chrY_single_or_double_diagonal"));
    for fixture in examples {
        assert_eq!(fixture["synthetic"], true);
        let fields = fields(&fixture);
        // Identities describe manufactured single-row TSVs, not real GCS objects.
        let template = if fixture["cohort"] == "aou" {
            include_str!("../fixtures/aou.tsv")
        } else {
            include_str!("../fixtures/hgsvc_hprc.tsv")
        };
        let header = template.lines().next().unwrap();
        let body = format!("{header}\n{}\n", header.split('\t')
            .map(|name| fields[name].as_str()).collect::<Vec<_>>().join("\t"));
        use base64::Engine;
        use md5::{Digest, Md5};
        assert_eq!(fixture["source"]["size_bytes"], body.len());
        assert_eq!(fixture["source"]["md5_base64"],
            base64::engine::general_purpose::STANDARD.encode(Md5::digest(body.as_bytes())));
        assert!(fixture["source"]["uri"].as_str().unwrap()
            .starts_with("gs://synthetic-histogram-fixtures/"));
        let mut source_task = task();
        source_task.cohort = fixture["cohort"].as_str().unwrap().into();
        source_task.source_uri = fixture["source"]["uri"].as_str().unwrap().into();
        source_task.source_generation = fixture["source"]["generation"].as_str().unwrap().into();
        source_task.source_size_bytes = fixture["source"]["size_bytes"].as_u64().unwrap();
        source_task.source_md5_base64 = fixture["source"]["md5_base64"].as_str().unwrap().into();
        // Reverse header order to exercise name-based, order-preserving capture.
        let names: Vec<_> = fields.keys().rev().map(String::as_str).collect();
        let parts: Vec<_> = names.iter().map(|n| fields[*n].as_str()).collect();
        let row = SourceHeader::parse(&names)
            .unwrap()
            .row(&parts, &source_task, 7)
            .unwrap_or_else(|e| panic!("{}: {e:#}", fields["LocusId"]));
        assert_eq!(row.source_header, names);
        assert_eq!(row.cohort, source_task.cohort);
        assert_eq!(row.source_generation, source_task.source_generation);
        assert_eq!(row.source_uri, source_task.source_uri);
        assert_eq!(row.source_size_bytes, source_task.source_size_bytes);
        assert_eq!(row.source_md5_base64, source_task.source_md5_base64);
        assert_eq!(row.row_ordinal, 7);
        assert_eq!(row.source_fields, fields);
        assert_eq!(row.locus_id, fields["LocusId"]);
        assert_eq!(row.source_interval, fields["Interval"]);
        assert_eq!(row.motif, fields["Motif"]);
        assert_eq!(
            row.source_vc.as_deref(),
            (!missing(&fields["VC"])).then_some(fields["VC"].as_str())
        );
        let roundtrip: SourceHistogramRow =
            serde_json::from_str(&serde_json::to_string(&row).unwrap()).unwrap();
        assert_eq!(row.source_fields, roundtrip.source_fields);
        assert_eq!(row.source_header, roundtrip.source_header);
    }
}

#[test]
fn legacy_contract_stays_distinct() {
    for fixture in fixtures().iter().filter(|f| {
        matches!(
            f["category"].as_str(),
            Some("wider_vc" | "wider_blank_vc" | "chrY_single_or_double_diagonal")
        )
    }) {
        let fields = fields(fixture);
        let names: Vec<_> = fields.keys().map(String::as_str).collect();
        let parts: Vec<_> = fields.values().map(String::as_str).collect();
        let header = Header::parse(&names).unwrap();
        assert!(super::super::parse_histogram_row(&parts, &header).is_err());
        assert!(parse(&fields).is_ok());
    }
}

#[test]
fn pair_lower_bound_does_not_infer_ploidy() {
    let alleles = parse_bins("3x:5,4x:2", 0, "test").unwrap();
    assert!(
        validate_source_pairs(&alleles, &parse_bins("3/3:3,3/4:2", 1, "test").unwrap()).is_ok()
    );
    assert!(
        validate_source_pairs(&alleles, &parse_bins("3/3:4,3/4:2", 1, "test").unwrap()).is_err()
    );
    assert!(validate_source_pairs(&alleles, &parse_bins("4/4:3", 1, "test").unwrap()).is_err());
    assert!(parse_bins("4/3:1", 1, "test").is_err());
}

#[test]
fn bad_grammar_arithmetic_and_nonfinite_summaries_fail() {
    let base = fields(&fixtures()[0]);
    for (key, value) in [
        ("VC", "cluster_unknown"),
        ("Interval", "1:1-2"),
        ("LocusId", "compound|id"),
        ("Motif", "*"),
        ("NumCalledAlleles", "0"),
        ("UniqueAlleleLengths", "0"),
        ("Mean", "NaN"),
        ("HemiAlleleMax", "-1"),
        ("ShortAlleleMax", "1e999"),
        ("Mean", "1e-999"),
    ] {
        let mut row = base.clone();
        row.insert(key.into(), value.into());
        assert!(parse(&row).is_err(), "accepted {key}={value}");
    }
    let mut row = base.clone();
    let marginal = row
        .keys()
        .find(|k| *k == "AlleleSizeHistogram__afr")
        .unwrap()
        .clone();
    row.insert(marginal, "0x:1".into());
    assert!(parse(&row).is_err());
}

#[test]
fn null_summaries_and_zero_call_rows_are_not_fabricated() {
    let mut row = fields(&fixtures()[0]);
    row.insert("HemiAllele99thPercentile".into(), ".".into());
    row.insert("HemiAlleleMax".into(), "".into());
    row.insert("ShortAlleleMax".into(), "".into());
    assert_eq!(
        parse(&row).unwrap().source_fields["HemiAllele99thPercentile"],
        "."
    );
    for (key, value) in row.iter_mut() {
        if key.starts_with("AlleleSizeHistogram")
            || key.starts_with("BiallelicHistogram")
            || SUMMARIES.contains(&key.as_str())
            || key.starts_with("ShortAllele")
            || key.starts_with("HemiAllele")
        {
            *value = "".into();
        }
    }
    row.insert("NumCalledAlleles".into(), "0".into());
    row.insert("UniqueAlleleLengths".into(), "0".into());
    let captured = parse(&row).unwrap();
    assert_eq!(captured.num_called_alleles, 0);
    assert_eq!(captured.source_fields["Mean"], "");
    row.insert("Mean".into(), "0".into());
    assert!(parse(&row).is_err());
}

#[test]
fn header_and_task_contracts_fail_closed() {
    let mut row = fields(&fixtures()[0]);
    row.insert("TRID".into(), "unknown-new-schema".into());
    assert!(parse(&row).is_err());
    let mut t = task();
    assert!(t.validate("histogram_0").is_ok());
    t.database = "gnomad_lr_y1_scratch_v5_current".into();
    assert!(t.validate("histogram_0").is_err());
    t.database = "gnomad_lr_y1_scratch_histogram_aou_test_1".into();
    assert!(t.validate("histogram_0").is_err());
    let mut t = task();
    t.source_generation = "01".into();
    assert!(t.validate("histogram_0").is_err());
    let mut t = task();
    t.clickhouse_endpoint += "/?database=live";
    assert!(t.validate("histogram_0").is_err());
    let mut json = serde_json::to_value(task()).unwrap();
    json["limit"] = 1.into();
    assert!(serde_json::from_value::<ingest::SourceTask>(json).is_err());
}
