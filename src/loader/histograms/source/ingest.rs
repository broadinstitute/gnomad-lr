//! One immutable whole-object task per fresh cohort candidate. No retries,
//! filtering, serving promotion, or implicit schema creation.
use super::{SourceHeader, SourceHistogramRow, CONTRACT, RECEIPTS_TABLE, TABLE};
use crate::loader::immutable_gcs::{HttpGcsBackend, ImmutableGcsObject, ImmutableGcsReader};
use crate::y1::{AuthSource, ClickHouseTarget, TargetKind};
use anyhow::{bail, ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

const MAX_LINE_BYTES: u64 = 16 * 1024 * 1024;
const BATCH_ROWS: usize = 5_000;
const BATCH_SOURCE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceTask {
    pub contract: String,
    pub cohort: String,
    pub run_id: String,
    pub task_id: String,
    pub source_uri: String,
    pub source_generation: String,
    pub source_size_bytes: u64,
    pub source_md5_base64: String,
    pub clickhouse_endpoint: String,
    pub database: String,
    pub worker_principal: String,
    pub allow_remote: bool,
}

impl SourceTask {
    pub fn object(&self) -> ImmutableGcsObject {
        ImmutableGcsObject {
            uri: self.source_uri.clone(),
            generation: self.source_generation.clone(),
            byte_size: self.source_size_bytes,
            checksum_algorithm: "md5_base64".into(),
            checksum: self.source_md5_base64.clone(),
            immutable_read_uri: format!(
                "{}?generation={}",
                self.source_uri, self.source_generation
            ),
        }
    }

    pub fn validate(&self, descriptor_id: &str) -> Result<ClickHouseTarget> {
        ensure!(
            self.contract == CONTRACT,
            "unsupported histogram source contract"
        );
        ensure!(
            matches!(self.cohort.as_str(), "hgsvc_hprc" | "aou"),
            "unsupported cohort"
        );
        ensure!(
            !self.run_id.is_empty()
                && self.run_id.len() <= 80
                && self
                    .run_id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
            "run_id must be a bounded lowercase identifier"
        );
        ensure!(
            !self.task_id.is_empty()
                && self.task_id.len() <= 200
                && self
                    .task_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
                && self.task_id == descriptor_id,
            "task ID mismatch/invalid ID"
        );
        let expected = format!(
            "gnomad_lr_y1_scratch_histogram_{}_{}",
            self.cohort, self.run_id
        );
        ensure!(self.database == expected, "histogram source capture requires its own exact cohort/run candidate database: {expected}");
        self.object().request()?;
        ClickHouseTarget::new(
            &self.clickhouse_endpoint,
            &self.database,
            TargetKind::Scratch,
            AuthSource::PasswordlessUser {
                username: self.worker_principal.clone(),
            },
            self.allow_remote,
            false,
        )
    }
}

/// Source bytes and capture success are separate: identity can be complete even
/// when the final INSERT failed. Counts are acknowledged rows, never a claim of
/// transactional durability. Failed requests may have committed additional rows.
#[derive(Debug, Clone, Serialize)]
pub struct CaptureReceipt {
    pub contract: String,
    pub cohort: String,
    pub run_id: String,
    pub task_id: String,
    pub source_uri: String,
    pub source_generation: String,
    pub source_size_bytes: u64,
    pub source_md5_base64: String,
    pub database: String,
    pub status: String,
    pub completeness: String,
    pub diagnostic: String,
    pub gcs_metadata_verified: bool,
    pub eof_observed: bool,
    pub complete_body_identity_verified: bool,
    pub bytes_read: u64,
    pub computed_md5_base64: String,
    pub data_rows_examined: u64,
    pub validated_rows: u64,
    pub zero_called_rows: u64,
    pub rows_insert_attempted: u64,
    pub rows_insert_acknowledged: u64,
    pub partial_writes_possible: bool,
}

impl CaptureReceipt {
    fn new(task: &SourceTask) -> Self {
        Self {
            contract: CONTRACT.into(),
            cohort: task.cohort.clone(),
            run_id: task.run_id.clone(),
            task_id: task.task_id.clone(),
            source_uri: task.source_uri.clone(),
            source_generation: task.source_generation.clone(),
            source_size_bytes: task.source_size_bytes,
            source_md5_base64: task.source_md5_base64.clone(),
            database: task.database.clone(),
            status: "started".into(),
            completeness: "partial".into(),
            diagnostic: String::new(),
            gcs_metadata_verified: false,
            eof_observed: false,
            complete_body_identity_verified: false,
            bytes_read: 0,
            computed_md5_base64: String::new(),
            data_rows_examined: 0,
            validated_rows: 0,
            zero_called_rows: 0,
            rows_insert_attempted: 0,
            rows_insert_acknowledged: 0,
            partial_writes_possible: false,
        }
    }
    pub fn complete(&self) -> bool {
        self.status == "complete_success"
    }
    fn fail(&mut self, diagnostic: &str) {
        self.status = "failed_partial".into();
        self.diagnostic = diagnostic.into();
        self.partial_writes_possible = self.rows_insert_attempted > 0;
    }
}

pub(super) trait SourceSink {
    fn insert(&mut self, rows: &[SourceHistogramRow]) -> Result<()>;
}
impl SourceSink for ClickHouseTarget {
    fn insert(&mut self, rows: &[SourceHistogramRow]) -> Result<()> {
        self.insert_json_each_row(TABLE, rows)
    }
}

trait CaptureStore: SourceSink {
    fn receipt(&mut self, receipt: &CaptureReceipt) -> Result<()>;
}
impl CaptureStore for ClickHouseTarget {
    fn receipt(&mut self, receipt: &CaptureReceipt) -> Result<()> {
        self.insert_json_each_row(RECEIPTS_TABLE, &[receipt])
    }
}

struct DigestReader<R> {
    inner: R,
    digest: Md5,
    bytes: u64,
}
impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(out)?;
        self.digest.update(&out[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

fn flush(
    sink: &mut impl SourceSink,
    batch: &mut Vec<SourceHistogramRow>,
    report: &mut CaptureReceipt,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    report.rows_insert_attempted += batch.len() as u64;
    sink.insert(batch)?;
    report.rows_insert_acknowledged += batch.len() as u64;
    batch.clear();
    Ok(())
}

/// Used with a fake sink/reader in tests; production only supplies the verified
/// generation-qualified reader. Bounded tests are not generation attestations.
pub(super) fn capture(
    reader: impl Read,
    task: &SourceTask,
    sink: &mut impl SourceSink,
    report: &mut CaptureReceipt,
    batch_rows: usize,
) {
    let tracked = DigestReader {
        inner: reader,
        digest: Md5::new(),
        bytes: 0,
    };
    let mut reader = BufReader::new(tracked);
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;
    let mut header_bytes = 0usize;
    let mut header = None;
    let mut line = Vec::new();
    let mut diagnostic = "source_read_error";
    let result = (|| -> Result<()> {
        ensure!(batch_rows > 0, "zero batch size");
        loop {
            line.clear();
            diagnostic = "source_read_error";
            (&mut reader)
                .take(MAX_LINE_BYTES + 1)
                .read_until(b'\n', &mut line)?;
            diagnostic = "line_resource_limit_exceeded";
            ensure!(line.len() as u64 <= MAX_LINE_BYTES, "source line too long");
            if line.is_empty() {
                report.eof_observed = true;
                diagnostic = "missing_header";
                ensure!(header.is_some(), "missing source header");
                diagnostic = "complete_size_mismatch";
                ensure!(
                    reader.get_ref().bytes == task.source_size_bytes,
                    "source size mismatch"
                );
                diagnostic = "complete_checksum_mismatch";
                ensure!(
                    STANDARD.encode(reader.get_ref().digest.clone().finalize())
                        == task.source_md5_base64,
                    "source checksum mismatch"
                );
                report.complete_body_identity_verified = true;
                report.completeness = "full".into();
                // Do not flush the last batch until full identity is proved.
                diagnostic = "insert_error";
                flush(sink, &mut batch, report)?;
                return Ok(());
            }
            // Hash includes exact line terminators; lexemes retain empty/dot
            // fields, while CRLF and LF have the same parsed row semantics.
            if line.ends_with(b"\n") {
                line.pop();
                if line.ends_with(b"\r") {
                    line.pop();
                }
            }
            diagnostic = "invalid_utf8";
            let text = std::str::from_utf8(&line)?;
            diagnostic = "blank_line";
            ensure!(!text.is_empty(), "unexpected blank line");
            let parts: Vec<_> = text.split('\t').collect();
            let Some(header) = &header else {
                diagnostic = "invalid_header";
                header = Some(SourceHeader::parse(&parts)?);
                header_bytes = line.len();
                continue;
            };
            report.data_rows_examined += 1;
            diagnostic = "invalid_source_row";
            let row = header.row(&parts, task, report.data_rows_examined)?;
            report.validated_rows += 1;
            report.zero_called_rows += u64::from(row.num_called_alleles == 0);
            batch.push(row);
            // Include both copies of header names, not just source values.
            // This bounds batching, not total allocator/serialization overhead.
            batch_bytes += line.len() + 2 * header_bytes;
            if batch.len() >= batch_rows || batch_bytes >= BATCH_SOURCE_BYTES {
                diagnostic = "insert_error";
                flush(sink, &mut batch, report)?;
                batch_bytes = 0;
            }
        }
    })();
    report.bytes_read = reader.get_ref().bytes;
    report.computed_md5_base64 = STANDARD.encode(reader.get_ref().digest.clone().finalize());
    if result.is_ok() {
        report.status = "complete_success".into();
    } else {
        report.fail(diagnostic);
    }
    // No drop flush and no retry. Raw cell/parser error strings are not logged.
}

/// Executes only an already-authorized task. Caller must reject reassignment
/// attempts and enforce exclusive one-shot ownership of this fresh candidate.
/// Both tables must be provisioned from the separate source DDL beforehand.
pub fn load(task: &SourceTask) -> Result<CaptureReceipt> {
    let mut target = task.validate(&task.task_id)?;
    target.attest_current_user(&task.worker_principal)?;
    target.attest_synchronous_inserts()?;
    let count = target.query_text(&format!("SELECT (SELECT count() FROM {TABLE}) + (SELECT count() FROM {RECEIPTS_TABLE}) FORMAT TabSeparated"), &[])?;
    ensure!(
        count.trim() == "0",
        "histogram candidate is not fresh; no retry/append permitted"
    );
    let report = capture_lifecycle(task, &mut target, || {
        ImmutableGcsReader::open(Arc::new(HttpGcsBackend::new()?), &task.object())
    });
    if !report.complete() {
        tracing::error!(
            "histogram_source_receipt={}",
            serde_json::to_string(&report)?
        );
        bail!("histogram source capture failed: {}; acknowledged rows={}; partial writes possible; abandon candidate, no automatic retry", report.diagnostic, report.rows_insert_acknowledged);
    }
    tracing::info!(
        "histogram_source_receipt={}",
        serde_json::to_string(&report)?
    );
    Ok(report)
}

/// Injectable receipt lifecycle. Production opener attests immutable GCS
/// metadata; tests use fake readers, never actual remote provenance.
fn capture_lifecycle<R: Read>(
    task: &SourceTask,
    store: &mut impl CaptureStore,
    open: impl FnOnce() -> Result<R>,
) -> CaptureReceipt {
    let mut report = CaptureReceipt::new(task);
    // Lost reservation response can leave a durable marker. Do not open the
    // source or make a second DB request after this uncertainty.
    if store.receipt(&report).is_err() {
        report.fail("reservation_insert_error");
        return report;
    }
    match open() {
        Ok(reader) => {
            report.gcs_metadata_verified = true;
            capture(reader, task, store, &mut report, BATCH_ROWS);
        }
        Err(_) => report.fail("source_open_or_identity_error"),
    }
    if store.receipt(&report).is_err() {
        report.fail("receipt_insert_error");
    }
    report
}

/// Genohype entry point: one full immutable source per assignment, no fallback
/// to job-level source/target/limits. Requeued assignments cannot write again.
pub async fn handle_tasks(
    payload: &serde_json::Value,
    tasks: Vec<genohype_pool::distributed::message::TaskDescriptor>,
) -> Result<genohype_pool::TaskResult> {
    // Reject accidental legacy job-level filters/targets rather than silently
    // ignoring them and doing a broader full-object load than requested.
    ensure!(
        payload.as_object().is_some_and(|p| p.len() == 1
            && p.get("action").and_then(serde_json::Value::as_str)
                == Some("load_histogram_source_v1")),
        "source histogram job payload must contain only its explicit action"
    );
    ensure!(
        tasks.len() == 1,
        "source histogram assignment must contain exactly one task"
    );
    let descriptor = &tasks[0];
    ensure!(
        descriptor.task_type == "custom",
        "source histogram requires custom task"
    );
    ensure!(
        descriptor.assignment_attempt == Some(1),
        "source histogram capture forbids retries/reassignment; use a fresh candidate"
    );
    ensure!(
        descriptor
            .lease_token
            .as_deref()
            .is_some_and(|t| !t.trim().is_empty() && t.len() <= 1024),
        "source histogram requires a valid assignment lease"
    );
    let task: SourceTask = serde_json::from_value(descriptor.payload.clone())?;
    task.validate(&descriptor.id)?;
    let report = tokio::task::spawn_blocking(move || load(&task)).await??;
    Ok(genohype_pool::TaskResult::success(
        usize::try_from(report.rows_insert_acknowledged)?,
        Some(serde_json::json!({
            "action": "load_histogram_source_v1", "source_capture_only": true,
            "published": false, "receipt": report
        })),
    ))
}

#[cfg(test)]
mod tests;
