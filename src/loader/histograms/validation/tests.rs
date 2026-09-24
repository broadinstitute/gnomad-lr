use super::*;
use crate::loader::immutable_gcs::{
    GcsObjectMetadata, GcsObjectRequest, GcsRangeResponse, ImmutableGcsBackend,
};
use std::io::Cursor;

const FIXTURE: &str = include_str!("../fixtures/aou.tsv");

fn args(body: &[u8]) -> ValidateHistogramsArgs {
    ValidateHistogramsArgs {
        source: "synthetic.tsv".into(),
        source_size_bytes: body.len() as u64,
        source_md5_base64: STANDARD.encode(Md5::digest(body)),
        source_generation: None,
        max_rows: None,
        max_bytes: None,
    }
}

fn run(body: &[u8], args: &ValidateHistogramsArgs) -> ValidationReport {
    let mut report = ValidationReport::new(args);
    validate_reader(Cursor::new(body), args, &mut report);
    assert_eq!(report.clickhouse_writes, 0);
    assert_eq!(report.filtered_rows, 0);
    report
}

#[test]
fn validation_reports_only_validated_canonical_gcs_identity_without_network() {
    let mut options = args(FIXTURE.as_bytes());
    options.source = "gs://synthetic/fixture.tsv".into();
    options.source_generation = Some("123".into());
    let mut report = ValidationReport::new(&options);
    assert!(report.source.is_none() && report.expected_generation.is_none());
    assert!(report.expected_md5_base64.is_none());
    let object = validated_gcs_object(&options, &mut report).unwrap();
    assert_eq!(report.source.as_deref(), Some("gs://synthetic/fixture.tsv"));
    assert_eq!(report.expected_generation.as_deref(), Some("123"));
    assert_eq!(report.expected_md5_base64, Some(options.source_md5_base64));
    assert_eq!(
        object.immutable_read_uri,
        "gs://synthetic/fixture.tsv?generation=123"
    );
    assert!(!report.gcs_metadata_verified && !report.complete_body_identity_verified);
}

#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    target_os = "macos"
))]
#[test]
fn validation_local_open_rechecks_swapped_paths() {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const CHILD_PATH: &str = "GNOMAD_LR_TEST_SWAPPED_LOCAL_PATH";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        // Reproduce the path-swap window deterministically, after the precheck.
        let path = path.to_str().unwrap();
        std::fs::write(path, b"regular before swap").unwrap();
        let before = std::fs::symlink_metadata(path).unwrap();
        // Keep the original inode allocated, so replacement cannot reuse it.
        std::fs::rename(path, format!("{path}.original")).unwrap();
        assert!(Command::new("mkfifo").arg(path).status().unwrap().success());
        assert!(open_regular_file_checked(path, &before).is_err());
        // The descriptor check also rejects devices (and NOFOLLOW rejects links).
        assert!(open_regular_file_checked("/dev/null", &before).is_err());
        std::fs::remove_file(path).unwrap();
        std::os::unix::fs::symlink("/dev/null", path).unwrap();
        assert!(open_regular_file_checked(path, &before).is_err());
        std::fs::remove_file(path).unwrap();
        std::fs::write(path, b"different regular file").unwrap();
        assert!(open_regular_file_checked(path, &before).is_err());
        return;
    }

    let directory = std::env::temp_dir().join(format!(
        "gnomad-lr-swap-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "loader::histograms::validation::tests::validation_local_open_rechecks_swapped_paths",
            "--nocapture",
        ])
        .env(CHILD_PATH, directory.join("source"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let timed_out = loop {
        if child.try_wait().unwrap().is_some() {
            break false;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            break true;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let output = child.wait_with_output().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    assert!(!timed_out, "path-swap open blocked on a FIFO");
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn validation_complete_body_identity_and_production_parser_agree() {
    for fixture in [FIXTURE, include_str!("../fixtures/hgsvc_hprc.tsv")] {
        let report = run(fixture.as_bytes(), &args(fixture.as_bytes()));
        assert_eq!(report.status, "complete_success", "{report:?}");
        assert!(report.eof_observed && report.complete_body_identity_verified);
        let mut stats = super::super::LoadStats::default();
        super::super::stream_histograms(Cursor::new(fixture), None, None, &mut stats, |_| Ok(()))
            .unwrap();
        assert_eq!(report.data_rows_examined, stats.total as u64);
        assert_eq!(report.accepted_rows, stats.accepted as u64);
        assert_eq!(report.empty_rows, stats.empty as u64);
    }
}

#[test]
fn validation_stops_on_first_invalid_row_without_private_diagnostics() {
    let body = format!(
        "{}\nprivate-sample-value\n{}\n",
        // Preserve trailing empty TSV cells; only remove the final line ending.
        FIXTURE.trim_end_matches('\n'),
        FIXTURE.lines().nth(1).unwrap()
    );
    let report = run(body.as_bytes(), &args(body.as_bytes()));
    assert!(report.failed());
    assert_eq!(report.accepted_rows, 2);
    assert_eq!(report.rejected_rows, 1);
    assert_eq!(report.data_rows_examined, 3);
    assert!(!report.complete_body_identity_verified);
    assert!(!serde_json::to_string(&report)
        .unwrap()
        .contains("private-sample-value"));
}

#[test]
fn validation_prefixes_never_claim_complete_identity_even_if_buffer_prefetched_eof() {
    let mut options = args(FIXTURE.as_bytes());
    options.max_rows = Some(1);
    let report = run(FIXTURE.as_bytes(), &options);
    assert_eq!(report.status, "bounded_validation");
    assert_eq!(report.accepted_rows, 1);
    assert!(!report.complete_body_identity_verified && !report.eof_observed);
    options.max_rows = None;
    for n in [
        0,
        1,
        FIXTURE.lines().next().unwrap().len() as u64 + 4,
        FIXTURE.len() as u64,
    ] {
        options.max_bytes = Some(n);
        let report = run(FIXTURE.as_bytes(), &options);
        assert_eq!(report.status, "bounded_validation", "{report:?}");
        assert!(!report.complete_body_identity_verified && !report.eof_observed);
        assert_eq!(report.bytes_read, n);
    }
}

#[test]
fn validation_complete_size_or_body_digest_mismatch_fails() {
    let mut options = args(FIXTURE.as_bytes());
    options.source_size_bytes += 1;
    let report = run(FIXTURE.as_bytes(), &options);
    assert_eq!(report.diagnostic, Some("complete_size_mismatch"));
    assert!(report.failed() && report.eof_observed && !report.complete_body_identity_verified);
    options.source_size_bytes -= 1;
    options.source_md5_base64 = STANDARD.encode([0; 16]);
    let report = run(FIXTURE.as_bytes(), &options);
    assert_eq!(report.diagnostic, Some("complete_checksum_mismatch"));
    assert!(report.failed() && !report.complete_body_identity_verified);
}

#[test]
fn validation_reuses_populated_vc_and_hemi_rejection() {
    for name in ["VC", "HemiAllele99thPercentile", "HemiAlleleMax"] {
        let mut lines = FIXTURE.lines();
        let header = lines.next().unwrap();
        let index = header.split('\t').position(|v| v == name).unwrap();
        let mut row: Vec<_> = lines.next().unwrap().split('\t').collect();
        row[index] = "123";
        let body = format!("{header}\n{}\n", row.join("\t"));
        let report = run(body.as_bytes(), &args(body.as_bytes()));
        assert_eq!(report.diagnostic, Some("invalid_histogram_row"));
        assert_eq!(report.rejected_rows, 1);
    }
}

#[test]
fn validation_handles_blank_crlf_missing_header_and_read_errors() {
    let body = format!("\n{}\n", FIXTURE.replace('\n', "\r\n"));
    let report = run(body.as_bytes(), &args(body.as_bytes()));
    assert_eq!(report.status, "complete_success");
    assert_eq!(report.blank_lines, 2);
    assert_eq!(run(b"", &args(b"")).diagnostic, Some("missing_header"));
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("private data"))
        }
    }
    let options = args(b"");
    let mut report = ValidationReport::new(&options);
    validate_reader(Broken, &options, &mut report);
    assert_eq!(report.diagnostic, Some("source_read_error"));
}

#[test]
fn validation_empty_rows_count_toward_limit_and_legacy_contract_is_shared() {
    let header = include_str!("../fixtures/invalid-row.tsv")
        .lines()
        .next()
        .unwrap();
    let body = format!("{header}\n1-10-20-A\tA\t.\t.\t.\t.\t.\t.\t.\t.\t.\t.\t.\t0\t0\t.\t.\n");
    let options = args(body.as_bytes());
    let report = run(body.as_bytes(), &options);
    assert_eq!(report.status, "complete_success");
    assert_eq!(report.header_contract, Some("legacy_15_core"));
    assert_eq!(report.empty_rows, 1);
    assert_eq!(report.accepted_rows, 0);
    let with_invalid_tail = format!("{body}bad\n");
    let mut options = args(with_invalid_tail.as_bytes());
    options.max_rows = Some(1);
    let report = run(with_invalid_tail.as_bytes(), &options);
    assert_eq!(report.status, "bounded_validation");
    assert_eq!(report.data_rows_examined, 1);
    assert_eq!(report.empty_rows, 1);
    assert_eq!(report.rejected_rows, 0);
    assert!(!report.complete_body_identity_verified);
}

#[test]
fn validation_resource_limit_fails_closed() {
    let body = vec![b'x'; MAX_LINE_BYTES as usize + 1];
    let report = run(&body, &args(&body));
    assert_eq!(report.diagnostic, Some("line_resource_limit_exceeded"));
    assert!(report.failed());
}

struct FakeGcs {
    body: Vec<u8>,
    substituted_generation: bool,
}
impl ImmutableGcsBackend for FakeGcs {
    fn metadata(&self, r: &GcsObjectRequest) -> anyhow::Result<GcsObjectMetadata> {
        Ok(GcsObjectMetadata {
            generation: r.generation.clone(),
            byte_size: r.byte_size,
            md5_base64: r.md5_base64.clone(),
        })
    }
    fn read_range(
        &self,
        r: &GcsObjectRequest,
        range: std::ops::Range<u64>,
    ) -> anyhow::Result<GcsRangeResponse> {
        Ok(GcsRangeResponse {
            generation: if self.substituted_generation {
                "999".into()
            } else {
                r.generation.clone()
            },
            total_size: r.byte_size,
            range_start: range.start,
            data: self.body[range.start as usize..range.end as usize].to_vec(),
        })
    }
}

#[test]
fn validation_gcs_rechecks_body_checksum_not_just_declared_metadata() {
    let options = args(FIXTURE.as_bytes());
    let object = ImmutableGcsObject {
        uri: "gs://synthetic/fixture.tsv".into(),
        generation: "123".into(),
        byte_size: options.source_size_bytes,
        checksum_algorithm: "md5_base64".into(),
        checksum: options.source_md5_base64.clone(),
        immutable_read_uri: "gs://synthetic/fixture.tsv?generation=123".into(),
    };
    for (bad_digest, bad_generation) in [(false, false), (true, false), (false, true)] {
        let mut body = FIXTURE.as_bytes().to_vec();
        // Valid different locus, same byte size. Metadata continues to claim original digest.
        if bad_digest {
            let offset = FIXTURE.find("1-400000").unwrap();
            body[offset] = b'2';
            let interval = FIXTURE.find("1:400000").unwrap();
            body[interval] = b'2';
        }
        let reader = ImmutableGcsReader::open(
            Arc::new(FakeGcs {
                body,
                substituted_generation: bad_generation,
            }),
            &object,
        )
        .unwrap();
        let mut report = ValidationReport::new(&options);
        validate_reader(reader, &options, &mut report);
        assert_eq!(
            report.complete_body_identity_verified,
            !bad_digest && !bad_generation
        );
        if bad_digest {
            assert_eq!(report.diagnostic, Some("complete_checksum_mismatch"));
        }
        if bad_generation {
            assert_eq!(report.diagnostic, Some("source_read_error"));
        }
    }
}
