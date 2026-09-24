use super::super::tests::{fields, fixtures, task, text_rows};
use super::*;

#[derive(Default)]
struct FakeSink {
    rows: Vec<SourceHistogramRow>,
    calls: usize,
    fail_call: Option<usize>,
}
impl SourceSink for FakeSink {
    fn insert(&mut self, rows: &[SourceHistogramRow]) -> Result<()> {
        self.calls += 1;
        if self.fail_call == Some(self.calls) {
            bail!("fake INSERT failure");
        }
        self.rows.extend_from_slice(rows);
        Ok(())
    }
}

fn identity(body: &[u8]) -> SourceTask {
    let mut t = task();
    // This is an offline fixture-body identity, NOT the complete source object.
    t.source_size_bytes = body.len() as u64;
    t.source_md5_base64 = STANDARD.encode(Md5::digest(body));
    t
}

fn run(body: &[u8], task: &SourceTask, sink: &mut FakeSink, batch: usize) -> CaptureReceipt {
    let mut report = CaptureReceipt::new(task);
    capture(body, task, sink, &mut report, batch);
    report
}

#[test]
fn complete_eof_identity_and_multiple_contexts_preserved_without_pooling() {
    let row = fields(&fixtures()[0]);
    let mut wider = row.clone();
    wider.insert("Interval".into(), "1:1-999999".into());
    wider.insert("VC".into(), "1:1-999999".into());
    let body = text_rows(&[row.clone(), wider.clone(), row.clone()]);
    let task = identity(body.as_bytes());
    let mut sink = FakeSink::default();
    let r = run(body.as_bytes(), &task, &mut sink, 2);
    assert!(r.complete() && r.eof_observed && r.complete_body_identity_verified);
    assert_eq!(r.completeness, "full");
    assert!(!r.gcs_metadata_verified); // Never claim remote provenance from local bytes.
    assert_eq!(r.bytes_read, body.len() as u64);
    assert_eq!(r.computed_md5_base64, task.source_md5_base64);
    assert_eq!(r.rows_insert_acknowledged, 3);
    assert_eq!(
        sink.rows.iter().map(|r| r.row_ordinal).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(sink.rows[0].source_fields, row);
    assert_eq!(sink.rows[1].source_fields, wider);
    assert_eq!(sink.rows[2].source_fields, sink.rows[0].source_fields);
}

#[test]
fn checksum_and_size_failures_after_partial_writes_never_flush_final_batch() {
    let row = fields(&fixtures()[0]);
    let body = text_rows(&[row.clone(), row.clone(), row]);
    for mismatch in ["size", "md5"] {
        let mut t = identity(body.as_bytes());
        if mismatch == "size" {
            t.source_size_bytes += 1;
        } else {
            t.source_md5_base64 = "1B2M2Y8AsgTpgAmY7PhCfg==".into();
        }
        let mut sink = FakeSink::default();
        let r = run(body.as_bytes(), &t, &mut sink, 2);
        assert!(
            !r.complete()
                && r.eof_observed
                && !r.complete_body_identity_verified
                && r.partial_writes_possible
        );
        assert_eq!(r.rows_insert_acknowledged, 2);
        assert_eq!(sink.calls, 1);
        assert_eq!(r.completeness, "partial");
    }
}

#[test]
fn malformed_row_after_partial_write_stops_immediately() {
    let row = fields(&fixtures()[0]);
    let mut bad = row.clone();
    bad.insert("NumCalledAlleles".into(), "bad".into());
    let body = text_rows(&[row.clone(), bad, row]);
    let t = identity(body.as_bytes());
    let mut sink = FakeSink::default();
    let r = run(body.as_bytes(), &t, &mut sink, 1);
    assert_eq!(r.status, "failed_partial");
    assert_eq!(r.diagnostic, "invalid_source_row");
    assert_eq!(r.data_rows_examined, 2);
    assert_eq!(r.validated_rows, 1);
    assert_eq!(r.rows_insert_acknowledged, 1);
    assert!(r.partial_writes_possible && !r.eof_observed);
    assert_eq!(sink.calls, 1);
}

#[test]
fn insert_failures_are_not_retried_or_counted_as_acknowledged() {
    let row = fields(&fixtures()[0]);
    let body = text_rows(&[row.clone(), row.clone(), row]);
    let t = identity(body.as_bytes());
    let mut sink = FakeSink {
        fail_call: Some(2),
        ..Default::default()
    };
    let r = run(body.as_bytes(), &t, &mut sink, 1);
    assert_eq!(r.rows_insert_attempted, 2);
    assert_eq!(r.rows_insert_acknowledged, 1);
    assert_eq!(r.diagnostic, "insert_error");
    assert_eq!(sink.calls, 2);
    assert!(!r.eof_observed && r.partial_writes_possible);
    let mut sink = FakeSink {
        fail_call: Some(1),
        ..Default::default()
    };
    let r = run(body.as_bytes(), &t, &mut sink, 10);
    assert!(r.eof_observed && r.complete_body_identity_verified);
    assert!(!r.complete());
    assert_eq!(r.rows_insert_acknowledged, 0);
    assert_eq!(sink.calls, 1);
}

struct BrokenReader {
    prefix: std::io::Cursor<Vec<u8>>,
}
impl Read for BrokenReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = self.prefix.read(out)?;
        if n == 0 {
            Err(std::io::Error::other("test source failure"))
        } else {
            Ok(n)
        }
    }
}

#[test]
fn read_error_after_partial_writes_is_not_eof() {
    let body = text_rows(&[fields(&fixtures()[0])]);
    let t = identity(body.as_bytes());
    let mut sink = FakeSink::default();
    let mut r = CaptureReceipt::new(&t);
    capture(
        BrokenReader {
            prefix: std::io::Cursor::new(body.into_bytes()),
        },
        &t,
        &mut sink,
        &mut r,
        1,
    );
    assert_eq!(r.diagnostic, "source_read_error");
    assert!(!r.eof_observed);
    assert!(r.partial_writes_possible);
    assert_eq!(r.rows_insert_acknowledged, 1);
}

#[test]
fn no_final_newline_crlf_repeated_header_and_truncation() {
    let row = fields(&fixtures()[0]);
    let body = text_rows(&[row]);
    for valid in [
        body.trim_end_matches('\n').to_string(),
        body.replace('\n', "\r\n"),
    ] {
        let mut sink = FakeSink::default();
        assert!(run(valid.as_bytes(), &identity(valid.as_bytes()), &mut sink, 10).complete());
    }
    let mut repeated = body.clone();
    repeated += body.lines().next().unwrap();
    let r = run(
        repeated.as_bytes(),
        &identity(repeated.as_bytes()),
        &mut FakeSink::default(),
        1,
    );
    assert_eq!(r.diagnostic, "invalid_source_row");
    let cut = &body.as_bytes()[..body.len() - 20];
    assert!(!run(
        cut,
        &identity(body.as_bytes()),
        &mut FakeSink::default(),
        10
    )
    .complete());
    assert!(!run(b"", &identity(b""), &mut FakeSink::default(), 10).complete());
    assert!(!run(b"\n", &identity(b"\n"), &mut FakeSink::default(), 10).complete());
}

#[derive(Default)]
struct FakeStore {
    sink: FakeSink,
    receipts: Vec<CaptureReceipt>,
    receipt_response_loss: Option<usize>,
    data_response_loss: bool,
}
impl SourceSink for FakeStore {
    fn insert(&mut self, rows: &[SourceHistogramRow]) -> Result<()> {
        self.sink.insert(rows)?;
        // Model a server commit followed by lost response, not only fail-before-write.
        if self.data_response_loss {
            bail!("lost INSERT response");
        }
        Ok(())
    }
}
impl CaptureStore for FakeStore {
    fn receipt(&mut self, r: &CaptureReceipt) -> Result<()> {
        self.receipts.push(r.clone());
        if self.receipt_response_loss == Some(self.receipts.len()) {
            bail!("lost receipt response");
        }
        Ok(())
    }
}

#[test]
fn receipt_lifecycle_quarantines_commit_then_error_without_retry() {
    let body = text_rows(&[fields(&fixtures()[0])]);
    let t = identity(body.as_bytes());
    let mut store = FakeStore {
        receipt_response_loss: Some(1),
        ..Default::default()
    };
    let r = capture_lifecycle(&t, &mut store, || -> Result<&[u8]> {
        panic!("opened after uncertain reservation")
    });
    assert_eq!(r.diagnostic, "reservation_insert_error");
    assert!(!r.complete());
    assert_eq!(store.receipts.len(), 1);
    assert_eq!(store.sink.calls, 0);
    assert_eq!(store.receipts[0].status, "started");

    let mut store = FakeStore::default();
    let r = capture_lifecycle(&t, &mut store, || -> Result<&[u8]> {
        bail!("fake source open failure")
    });
    assert_eq!(r.diagnostic, "source_open_or_identity_error");
    assert_eq!(store.receipts.len(), 2);
    assert_eq!(store.sink.calls, 0);
    assert!(!r.gcs_metadata_verified);

    let mut store = FakeStore {
        receipt_response_loss: Some(2),
        ..Default::default()
    };
    let r = capture_lifecycle(&t, &mut store, || Ok(body.as_bytes()));
    assert_eq!(r.diagnostic, "receipt_insert_error");
    assert!(!r.complete());
    assert!(r.complete_body_identity_verified && r.partial_writes_possible);
    assert_eq!(store.receipts.len(), 2);
    assert_eq!(store.sink.calls, 1);
    // A success-looking DB receipt cannot override the worker's transport failure.
    assert_eq!(store.receipts[1].status, "complete_success");

    let mut store = FakeStore {
        data_response_loss: true,
        ..Default::default()
    };
    let r = capture_lifecycle(&t, &mut store, || Ok(body.as_bytes()));
    assert_eq!(r.diagnostic, "insert_error");
    assert!(!r.complete());
    assert_eq!(r.rows_insert_acknowledged, 0);
    assert_eq!(r.rows_insert_attempted, 1);
    assert_eq!(store.sink.rows.len(), 1);
    assert_eq!(store.sink.calls, 1);
    assert_eq!(store.receipts[1].status, "failed_partial");
}

#[test]
fn byte_batch_budget_and_resource_limits() {
    let mut row = fields(&fixtures()[0]);
    // Source bin grammar tolerates outer spaces; retain those bytes faithfully.
    row.get_mut("AlleleSizeHistogram")
        .unwrap()
        .push_str(&" ".repeat(3 * 1024 * 1024));
    let body = text_rows(&[row.clone(), row.clone(), row]);
    let mut t = identity(body.as_bytes());
    t.source_md5_base64 = "1B2M2Y8AsgTpgAmY7PhCfg==".into();
    let mut sink = FakeSink::default();
    let r = run(body.as_bytes(), &t, &mut sink, BATCH_ROWS);
    // Byte budget flushed despite fewer than 5000 rows; checksum failure never
    // flushes a pending batch or retries one already sent.
    assert_eq!(sink.calls, 1);
    assert_eq!(r.rows_insert_acknowledged, 3);
    assert_eq!(r.diagnostic, "complete_checksum_mismatch");
    // Otherwise-valid unique names: removing the resource guard must make the
    // over-limit case pass, unlike a duplicate/unknown-header negative fixture.
    let mut names: Vec<String> = fields(&fixtures()[0]).into_keys().collect();
    for n in names.len()..512 {
        names.push(format!("AlleleSizeHistogram__zz{n}"));
    }
    let parse_names = |names: &[String]| {
        SourceHeader::parse(&names.iter().map(String::as_str).collect::<Vec<_>>())
    };
    assert!(parse_names(&names).is_ok());
    names.push("AlleleSizeHistogram__zz512".into());
    assert!(parse_names(&names).is_err());
    let mut names: Vec<String> = fields(&fixtures()[0]).into_keys().collect();
    let prefix = "AlleleSizeHistogram__";
    let remaining = 128 * 1024 - names.iter().map(String::len).sum::<usize>() - prefix.len();
    names.push(format!("{prefix}{}", "z".repeat(remaining)));
    assert!(parse_names(&names).is_ok());
    names.last_mut().unwrap().push('z');
    assert!(parse_names(&names).is_err());
    let oversized = vec![b'x'; MAX_LINE_BYTES as usize + 1];
    let r = run(
        &oversized,
        &identity(&oversized),
        &mut FakeSink::default(),
        1,
    );
    assert_eq!(r.diagnostic, "line_resource_limit_exceeded");
    assert_eq!(r.rows_insert_attempted, 0);
}

#[tokio::test]
async fn worker_rejects_retry_and_invalid_tasks_before_io() {
    use genohype_pool::distributed::message::TaskDescriptor;
    let make = |attempt| TaskDescriptor {
        id: "histogram_0".into(),
        task_type: "custom".into(),
        label: None,
        index: Some(0),
        total: Some(1),
        payload: serde_json::to_value(task()).unwrap(),
        assignment_attempt: Some(attempt),
        lease_token: Some("test-lease".into()),
    };
    let job = serde_json::json!({"action": "load_histogram_source_v1"});
    assert!(handle_tasks(&job, vec![]).await.is_err());
    assert!(handle_tasks(&job, vec![make(1), make(1)]).await.is_err());
    assert!(handle_tasks(&job, vec![make(2)]).await.is_err());
    let mut no_lease = make(1);
    no_lease.lease_token = None;
    assert!(handle_tasks(&job, vec![no_lease]).await.is_err());
    let mut unknown = make(1);
    unknown.payload["limit"] = 1.into();
    assert!(handle_tasks(&job, vec![unknown]).await.is_err());
    let mut bounded_job = job.clone();
    bounded_job["limit"] = 1.into();
    assert!(handle_tasks(&bounded_job, vec![make(1)]).await.is_err());
}
