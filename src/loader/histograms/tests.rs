use super::*;
use std::io::{Cursor, Read};

const HGSVC: &str = include_str!("fixtures/hgsvc_hprc.tsv");
const AOU: &str = include_str!("fixtures/aou.tsv");
const LEGACY: &str = "LocusId\tMotif\tAlleleSizeHistogram\tBiallelicHistogram\tMin\tMode\tMean\tStdev\tMedian\t99thPercentile\tMax\tShortAllele99thPercentile\tShortAlleleMax\tUniqueAlleleLengths\tNumCalledAlleles\tAlleleSizeHistogram:afr:female\tBiallelicHistogram:afr:female\n1-400000-400024-AT\tAT\t2x:1,3x:3\t2/3:1,3/3:1\t2\t3\t2.75\t0.43\t3\t3\t3\t3\t3\t2\t4\t2x:1,3x:3\t2/3:1,3/3:1\n";

fn first(source: &str) -> (Vec<String>, Vec<String>) {
    let mut lines = source.lines();
    (
        lines
            .next()
            .unwrap()
            .split('\t')
            .map(str::to_owned)
            .collect(),
        lines
            .next()
            .unwrap()
            .split('\t')
            .map(str::to_owned)
            .collect(),
    )
}

fn parse(names: &[String], values: &[String]) -> Result<Option<StrHistogramRow>> {
    let header = Header::parse(&names.iter().map(String::as_str).collect::<Vec<_>>())?;
    parse_histogram_row(
        &values.iter().map(String::as_str).collect::<Vec<_>>(),
        &header,
    )
}

fn set(names: &[String], values: &mut [String], name: &str, value: &str) {
    values[names.iter().position(|n| n == name).unwrap()] = value.to_owned();
}

fn assert_bad(source: &str, column: &str, value: &str, expected: &str) {
    let (names, mut values) = first(source);
    set(&names, &mut values, column, value);
    let err = parse(&names, &values).unwrap_err();
    assert!(
        format!("{err:#}").contains(expected),
        "{column}={value:?}: {err:#}"
    );
}

#[test]
fn synthetic_contract_rows_normalize_without_marginal_double_counting() {
    // Each joint i has u=i+2 and allele counts 4u,8u,4u at sizes 8,12,16.
    // Marginals overlap those joints; they must never increase the totals.
    for (source, width, population_count, called) in [(HGSVC, 57, 22, 1232), (AOU, 33, 6, 144)] {
        let mut stats = LoadStats::default();
        let mut rows = Vec::new();
        stream_histograms(Cursor::new(source), None, None, &mut stats, |r| {
            rows.push(serde_json::to_value(r)?);
            Ok(())
        })
        .unwrap();
        assert_eq!(source.lines().next().unwrap().split('\t').count(), width);
        assert_eq!(stats.total, 2);
        assert_eq!(stats.accepted, 2);
        assert_eq!(stats.submitted, 2);
        assert_eq!(stats.rejected + stats.empty, 0);
        let r = &rows[0];
        assert_eq!(r["chrom"], "chr1");
        assert_eq!(r["position"], 400000);
        assert_eq!(r["end_position"], 400024);
        assert_eq!(r["motif"], "AT");
        assert_eq!(r["min_repeats"], 8.0);
        assert_eq!(r["num_called_alleles"], called);
        assert_eq!(
            r["populations"].as_object().unwrap().len(),
            population_count
        );
        for key in r["populations"].as_object().unwrap().keys() {
            assert_eq!(key.split(':').count(), 3);
            assert!(!key.contains("__"));
        }
    }
    let (names, values) = first(AOU);
    let row = parse(&names, &values).unwrap().unwrap();
    assert_eq!(
        row.populations["AlleleSizeHistogram:afr:unknown"],
        "8x:16,12x:32,16x:16"
    );
    assert_eq!(
        row.populations["BiallelicHistogram:afr:unknown"],
        "8/12:16,12/16:16"
    );
    assert_eq!(row.allele_size_histogram, "8x:36,12x:72,16x:36");
}

#[test]
fn legacy_contract_and_reordered_headers() {
    for source in [LEGACY, HGSVC, AOU] {
        let (mut names, mut values) = first(source);
        let expected = serde_json::to_value(parse(&names, &values).unwrap().unwrap()).unwrap();
        names.reverse();
        values.reverse();
        assert_eq!(
            serde_json::to_value(parse(&names, &values).unwrap().unwrap()).unwrap(),
            expected
        );
    }
    let (names, values) = first(LEGACY);
    let row = parse(&names, &values).unwrap().unwrap();
    assert_eq!(row.mean_repeats, 2.75);
    assert_eq!(row.min_repeats, 2.0);
    assert_eq!(row.num_called_alleles, 4);
    assert_eq!(
        row.populations["AlleleSizeHistogram:afr:female"],
        "2x:1,3x:3"
    );
}

#[test]
fn legacy_unknown_sex_is_preserved() {
    let (mut names, values) = first(LEGACY);
    for name in &mut names {
        *name = name.replace(":female", ":unknown");
    }
    let row = parse(&names, &values).unwrap().unwrap();
    assert!(row
        .populations
        .contains_key("AlleleSizeHistogram:afr:unknown"));
}

#[test]
fn header_is_unique_complete_recognized_and_not_mixed() {
    let (names, values) = first(AOU);
    let mut bad = names.clone();
    bad[1] = bad[0].clone();
    assert!(parse(&bad, &values)
        .unwrap_err()
        .to_string()
        .contains("duplicate header"));
    let mut bad = names.clone();
    bad[1] = "".into();
    assert!(parse(&bad, &values)
        .unwrap_err()
        .to_string()
        .contains("empty header"));
    for required in CORE.iter().chain(NEW_CORE) {
        let index = names.iter().position(|n| n == required).unwrap();
        let mut bad = names.clone();
        bad.remove(index);
        assert!(parse(&bad, &values).is_err(), "missing {required}");
    }
    for name in [
        "Garbage",
        "AlleleSizeHistogram:afr:female",
        "AlleleSizeHistogram__afr_XX",
        "AlleleSizeHistogram__afr_female_extra",
        "AlleleSizeHistogram__unknown_female",
    ] {
        let mut bad = names.clone();
        bad[20] = name.into();
        assert!(parse(&bad, &values).is_err(), "bad header {name}");
    }
    let (mut names, values) = first(LEGACY);
    names[15] = "AlleleSizeHistogram__afr_female".into();
    assert!(parse(&names, &values).is_err());
}

#[test]
fn marginals_alone_do_not_establish_joint_strata() {
    let (names, values) = first(AOU);
    let selected: Vec<_> = names
        .iter()
        .zip(&values)
        .filter(|(name, _)| !name.contains("__afr_"))
        .collect();
    let (names, values): (Vec<_>, Vec<_>) = selected
        .into_iter()
        .map(|(n, v)| (n.clone(), v.clone()))
        .unzip();
    assert!(parse(&names, &values)
        .unwrap_err()
        .to_string()
        .contains("marginals alone"));
}

#[test]
fn every_supplied_marginal_and_joint_total_is_checked() {
    for name in [
        "AlleleSizeHistogram__afr",
        "AlleleSizeHistogram__female",
        "AlleleSizeHistogram__unknown",
    ] {
        assert_bad(AOU, name, "2x:1", "marginal");
        assert_bad(AOU, name, ".", "marginal");
    }
    assert_bad(AOU, "BiallelicHistogram__male", "2/3:1", "marginal");
    assert_bad(
        AOU,
        "AlleleSizeHistogram__afr_unknown",
        "2x:1",
        "do not sum",
    );
    assert_bad(AOU, "BiallelicHistogram__afr_unknown", "", "do not sum");
}

#[test]
fn malformed_bins_are_not_silently_accepted() {
    for value in [
        "8:36",
        "2x:-1",
        "2x:1.5",
        "2x:NaN",
        "2x:1,2x:1",
        "2x:1,02x:2",
        "2x:0",
        "2x:4294967296",
        "4294967296x:1",
        "2x:1,",
        "2x:1:2",
        "-1x:1",
        "2.5x:1",
        "null",
    ] {
        assert_bad(AOU, "AlleleSizeHistogram", value, "AlleleSizeHistogram");
    }
    for value in ["3/2:1", "2/3:1,2/3:2", "2:1", "2/3/4:1", "2/3:-1", "2/3:0"] {
        assert_bad(AOU, "BiallelicHistogram", value, "BiallelicHistogram");
    }
    assert_bad(
        AOU,
        "AlleleSizeHistogram__afr_female",
        "bad",
        "malformed bin",
    );
}

#[test]
fn required_values_identity_and_summary_consistency() {
    for name in ["NumCalledAlleles", "UniqueAlleleLengths"] {
        for value in ["", ".", "null", "-1", "1.0", "4294967296"] {
            assert_bad(AOU, name, value, name);
        }
    }
    for name in SUMMARIES {
        for value in [
            "", ".", "NaN", "inf", "null", "-1", "1e100", "1e-100", "1e-1000",
        ] {
            assert_bad(AOU, name, value, name);
        }
    }
    assert_bad(AOU, "NumCalledAlleles", "145", "bin count");
    assert_bad(AOU, "UniqueAlleleLengths", "2", "distinct allele bins");
    assert_bad(AOU, "Min", "1", "Min/Max");
    assert_bad(AOU, "Max", "5", "Min/Max");
    assert_bad(AOU, "Mode", "8", "Mode conflicts");
    assert_bad(AOU, "Mean", "17", "outside");
    assert_bad(AOU, "Motif", "CTT", "does not match");
    assert_bad(AOU, "Motif", "", "Motif");
    for locus in [
        "",
        "chr1-400000-400024-AT",
        "1-400024-400000-AT",
        "1-400000-400024-AT-1-500000-500024-GAC",
        "1-x-400024-AT",
    ] {
        assert_bad(AOU, "LocusId", locus, "LocusId");
    }
    assert_bad(AOU, "Interval", "1:400001-400024", "readiness blocker");
    assert_bad(AOU, "ShortAlleleMax", "not-a-number", "ShortAlleleMax");
}

#[test]
fn reviewed_impossible_statistics_are_rejected() {
    assert_bad(AOU, "Stdev", "1000", "Stdev exceeds range-based");
    assert_bad(AOU, "99thPercentile", "8", "Median exceeds 99thPercentile");
}

fn spread_fixture(bins: &str, called: &str, unique: &str) -> (Vec<String>, Vec<String>) {
    let (names, mut values) = first(LEGACY);
    for (name, value) in names.iter().zip(&mut values) {
        if name.starts_with("AlleleSizeHistogram") {
            *value = bins.into();
        } else if name.starts_with("BiallelicHistogram") || name.starts_with("ShortAllele") {
            *value = ".".into();
        }
    }
    set(&names, &mut values, "NumCalledAlleles", called);
    set(&names, &mut values, "UniqueAlleleLengths", unique);
    for (name, value) in SUMMARIES.iter().zip(["0", "0", "1", "1", "1", "2", "2"]) {
        set(&names, &mut values, name, value);
    }
    (names, values)
}

#[test]
fn stdev_bound_is_conservative_range_plus_float32_tolerance() {
    // Accept population/sample SD and values above the tight bound up to
    // the range: this guard deliberately does not recompute source summaries.
    for (bins, called, sample) in [
        ("0x:1,2x:1", "2", 2.0_f64.sqrt()),
        ("0x:2,2x:2", "4", (4.0_f64 / 3.0).sqrt()),
    ] {
        let (names, mut values) = spread_fixture(bins, called, "2");
        for stdev in [1.0, sample, 1.75, 2.0] {
            set(&names, &mut values, "Stdev", &stdev.to_string());
            let row = parse(&names, &values).unwrap().unwrap();
            assert_eq!(row.stdev_repeats, stdev as f32);
        }
        // Float32 storage slack applies at the conservative range bound.
        let rounded_up = f32::from_bits(2.0_f32.to_bits() + 1);
        set(&names, &mut values, "Stdev", &rounded_up.to_string());
        assert_eq!(
            parse(&names, &values).unwrap().unwrap().stdev_repeats,
            rounded_up
        );
        let too_large = f32::from_bits(2.0_f32.to_bits() + 4);
        set(&names, &mut values, "Stdev", &too_large.to_string());
        assert!(parse(&names, &values)
            .unwrap_err()
            .to_string()
            .contains("Stdev exceeds range-based"));
    }
}

#[test]
fn rounded_source_sample_stdev_is_preserved_but_impossible_spread_is_rejected() {
    let (names, mut values) = spread_fixture("0x:1,1x:1", "2", "2");
    for (name, value) in [
        ("Mean", "0.5"),
        ("Median", "0.5"),
        ("99thPercentile", "1"),
        ("Max", "1"),
        ("Stdev", "0.71"),
    ] {
        set(&names, &mut values, name, value);
    }
    // sqrt(0.5) serialized to two decimals exceeds the tight sample bound.
    assert_eq!(parse(&names, &values).unwrap().unwrap().stdev_repeats, 0.71);
    set(&names, &mut values, "Stdev", "1000");
    assert!(parse(&names, &values)
        .unwrap_err()
        .to_string()
        .contains("Stdev exceeds range-based"));
}

#[test]
fn singleton_and_constant_distributions_require_zero_spread() {
    for (bins, called) in [("3x:1", "1"), ("3x:4", "4")] {
        let (names, mut values) = spread_fixture(bins, called, "1");
        for name in SUMMARIES {
            set(
                &names,
                &mut values,
                name,
                if *name == "Stdev" { "0" } else { "3" },
            );
        }
        assert_eq!(parse(&names, &values).unwrap().unwrap().stdev_repeats, 0.0);
        set(&names, &mut values, "Stdev", "0.000001");
        assert!(parse(&names, &values)
            .unwrap_err()
            .to_string()
            .contains("Stdev exceeds range-based"));
    }
}

#[test]
fn percentile_order_allows_only_storage_scale_rounding_and_preserves_values() {
    let (names, mut values) = first(AOU);
    for p99 in [12.0_f32, 14.0, 16.0, f32::from_bits(12.0_f32.to_bits() - 1)] {
        set(&names, &mut values, "99thPercentile", &p99.to_string());
        let row = parse(&names, &values).unwrap().unwrap();
        assert_eq!(row.p99_repeats, p99);
        assert_eq!(row.median_repeats, 12.0);
    }
    for p99 in [f32::from_bits(12.0_f32.to_bits() - 4), 11.99, 8.0] {
        assert_bad(AOU, "99thPercentile", &p99.to_string(), "Median exceeds");
    }
    // The new tolerance must not relax the existing extrema/range gates.
    assert_bad(AOU, "99thPercentile", "16.00001", "outside");
    assert_bad(AOU, "Median", "7.999999", "outside");
}

#[test]
fn unsupported_scientific_fields_block_readiness_not_coerce_or_drop() {
    for name in ["VC", "HemiAllele99thPercentile", "HemiAlleleMax"] {
        for value in ["0", "3", "opaque"] {
            assert_bad(AOU, name, value, "readiness blocker");
        }
    }
}

#[test]
fn row_width_is_exact_including_trailing_empty_cells() {
    // The checked-in second row ends in a dot-null joint to avoid trailing
    // whitespace. Manufacture the equivalent empty terminal cell here.
    let (names, _) = first(AOU);
    let mut second: Vec<String> = AOU.lines().nth(2).unwrap()
        .split('\t').map(str::to_owned).collect();
    assert_eq!(second.last().unwrap(), ".");
    let expected = serde_json::to_value(parse(&names, &second).unwrap().unwrap()).unwrap();
    *second.last_mut().unwrap() = String::new();
    let input = format!("{}\n{}\n", names.join("\t"), second.join("\t"));
    let mut stats = LoadStats::default();
    stream_histograms(Cursor::new(input), None, None, &mut stats, |row| {
        assert_eq!(serde_json::to_value(row)?, expected);
        Ok(())
    }).unwrap();
    assert_eq!(stats.accepted, 1);
    let (names, values) = first(AOU);
    let mut bad = values.clone();
    bad.pop();
    assert!(parse(&names, &bad)
        .unwrap_err()
        .to_string()
        .contains("row width"));
    let mut bad = values;
    bad.push("".into());
    assert!(parse(&names, &bad)
        .unwrap_err()
        .to_string()
        .contains("row width"));
}

fn zero_call() -> (Vec<String>, Vec<String>) {
    let (names, mut values) = first(AOU);
    for (name, value) in names.iter().zip(&mut values) {
        if name.contains("Histogram")
            || SUMMARIES.contains(&name.as_str())
            || name.starts_with("ShortAllele")
        {
            *value = "".into();
        }
        if name == "NumCalledAlleles" || name == "UniqueAlleleLengths" {
            *value = "0".into();
        }
    }
    (names, values)
}

#[test]
fn zero_call_nullable_summaries_are_counted_empty_not_fabricated() {
    let (names, mut values) = zero_call();
    assert!(parse(&names, &values).unwrap().is_none());
    for value in &mut values {
        if value.is_empty() {
            *value = ".".into();
        }
    }
    assert!(parse(&names, &values).unwrap().is_none());
    let input = format!("\n{}\n\n{}\n", names.join("\t"), values.join("\t"));
    let mut stats = LoadStats::default();
    stream_histograms(Cursor::new(input), None, None, &mut stats, |_| {
        panic!("empty row must not be inserted")
    })
    .unwrap();
    assert_eq!(
        (
            stats.total,
            stats.accepted,
            stats.rejected,
            stats.empty,
            stats.blank_lines
        ),
        (1, 0, 0, 1, 2)
    );
    set(&names, &mut values, "Mean", "invalid");
    assert!(parse(&names, &values).is_err());
    set(&names, &mut values, "Mean", ".");
    set(&names, &mut values, "BiallelicHistogram", "2/3:1");
    assert!(parse(&names, &values).is_err());
}

#[test]
fn missing_pairs_are_not_invented_and_nullable_short_summaries_are_allowed() {
    let (names, mut values) = first(LEGACY);
    for (name, value) in names.iter().zip(&mut values) {
        if name.starts_with("BiallelicHistogram") || name.starts_with("ShortAllele") {
            *value = ".".into();
        }
    }
    let row = parse(&names, &values).unwrap().unwrap();
    assert_eq!(row.biallelic_histogram, "");
    assert_eq!(row.populations.len(), 1);
    // This is valid source representation, not a claim of diploid readiness:
    // the browser's separate autosomal diploid gate may reject it.
    assert_eq!(row.num_called_alleles, 4);
}

#[test]
fn observed_pairs_must_not_exceed_allele_bins() {
    assert_bad(
        LEGACY,
        "BiallelicHistogram",
        "2/2:2",
        "genotype bins exceed",
    );
}

#[test]
fn whole_stream_failure_reports_physical_row_and_partial_counts_without_retry() {
    let input = format!("\n{AOU}bad\n{AOU}");
    let mut stats = LoadStats::default();
    let mut inserted = 0;
    let err = stream_histograms(Cursor::new(input), None, None, &mut stats, |_| {
        inserted += 1;
        Ok(())
    })
    .unwrap_err();
    assert!(format!("{err:#}").contains("row 5: rejected histogram"));
    assert!(format!("{err:#}").contains("row width"));
    assert_eq!(inserted, 2);
    assert_eq!(
        (stats.total, stats.accepted, stats.rejected, stats.submitted),
        (3, 2, 1, 2)
    );
}

#[test]
fn insert_failure_is_propagated_once_with_row_number() {
    let mut stats = LoadStats::default();
    let mut attempts = 0;
    let err = stream_histograms(Cursor::new(AOU), None, None, &mut stats, |_| {
        attempts += 1;
        bail!("simulated insert failure")
    })
    .unwrap_err();
    assert!(format!("{err:#}").contains("row 2: histogram insert failed"));
    assert_eq!(attempts, 1);
    assert_eq!(stats.submitted, 0);
}

#[test]
fn region_filter_counts_valid_rows_but_does_not_hide_invalid_rows() {
    let filter = RegionFilter::new("chr22".into(), 0, u32::MAX);
    let mut stats = LoadStats::default();
    stream_histograms(Cursor::new(AOU), Some(&filter), None, &mut stats, |_| {
        panic!("out of region")
    })
    .unwrap();
    assert_eq!(
        (stats.total, stats.accepted, stats.filtered, stats.submitted),
        (2, 2, 2, 0)
    );
    let mut stats = LoadStats::default();
    let err = stream_histograms(
        Cursor::new(format!("{AOU}bad\n")),
        Some(&filter),
        None,
        &mut stats,
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("row 4"));
    assert_eq!(stats.rejected, 1);
}

#[test]
fn source_read_errors_propagate_with_physical_row() {
    struct FailedReader;
    impl std::io::Read for FailedReader {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("simulated read failure"))
        }
    }
    let reader = Cursor::new(AOU).chain(FailedReader);
    let mut stats = LoadStats::default();
    let err =
        stream_histograms(BufReader::new(reader), None, None, &mut stats, |_| Ok(())).unwrap_err();
    assert!(format!("{err:#}").contains("row 4: source read failed"));
    assert_eq!(stats.submitted, 2);
}

#[test]
fn failed_whole_file_load_after_a_flushed_batch_never_reports_success() {
    // Local HTTP fixture only: no real ClickHouse or cloud mutations. This
    // exercises the public loader across its actual 50,000-row flush boundary.
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(15)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        let mut length = None;
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = Some(value.trim().parse::<u64>().unwrap());
            }
        }
        let length = length.unwrap();
        assert_eq!(
            std::io::copy(&mut reader.by_ref().take(length), &mut std::io::sink()).unwrap(),
            length
        );
        reader
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        length
    });
    let path = std::env::temp_dir().join(format!(
        "gnomad-lr-histograms-{}-{}.tsv",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut source = std::io::BufWriter::new(std::fs::File::create(&path).unwrap());
    let (names, values) = first(LEGACY);
    writeln!(source, "{}", names.join("\t")).unwrap();
    let row = values.join("\t");
    for _ in 0..50_000 {
        writeln!(source, "{row}").unwrap();
    }
    writeln!(source, "malformed").unwrap();
    source.flush().unwrap();
    drop(source);
    let result = load_str_histograms(&url, path.to_str().unwrap(), None, None);
    std::fs::remove_file(path).unwrap();
    let err = result.unwrap_err().to_string();
    for expected in [
        "FAILED",
        "row 50002",
        "confirmed inserted=50000",
        "partial writes possible",
        "do not retry automatically",
        "rejected: 1",
    ] {
        assert!(err.contains(expected), "{err}");
    }
    assert!(server.join().unwrap() > 0);
}

#[test]
fn empty_input_header_only_and_limit_behavior_are_explicit() {
    for input in ["", "\n\n"] {
        let mut stats = LoadStats::default();
        assert!(
            stream_histograms(Cursor::new(input), None, None, &mut stats, |_| Ok(()))
                .unwrap_err()
                .to_string()
                .contains("missing histogram header")
        );
    }
    let header = AOU.lines().next().unwrap();
    let mut stats = LoadStats::default();
    stream_histograms(Cursor::new(header), None, None, &mut stats, |_| Ok(())).unwrap();
    assert_eq!(stats.total, 0);
    let mut stats = LoadStats::default();
    stream_histograms(Cursor::new(AOU), None, Some(1), &mut stats, |_| Ok(())).unwrap();
    assert_eq!(stats.total, 1);
    assert_eq!(stats.submitted, 1);
    let mut stats = LoadStats::default();
    stream_histograms(Cursor::new(AOU), None, Some(0), &mut stats, |_| Ok(())).unwrap();
    assert_eq!(stats.total, 0);
}
