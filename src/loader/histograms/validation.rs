//! Read-only adapter around the production Header/row parser. No insert callback,
//! database configuration, or database client is reachable from this module.
//! Reports deliberately omit parser error text: it can contain private cell data.
use super::{parse_histogram_row, Header};
use crate::cli::ValidateHistogramsArgs;
use crate::loader::immutable_gcs::{HttpGcsBackend, ImmutableGcsObject, ImmutableGcsReader};
use base64::{engine::general_purpose::STANDARD, Engine};
use md5::{Digest, Md5};
use serde::Serialize;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

// Resource guard, not an alternative interpretation of the source contract.
// Oversized lines fail closed. Parser memory is independent of file length;
// HTTP response buffering assumes conforming GCS responses (see the CLI docs).
const MAX_LINE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub struct ValidationReport {
    schema_version: u32,
    pub status: &'static str,
    pub diagnostic: Option<&'static str>,
    source: Option<String>,
    expected_generation: Option<String>,
    expected_size_bytes: u64,
    expected_md5_base64: Option<String>,
    max_rows: Option<u64>,
    max_bytes: Option<u64>,
    max_line_bytes: u64,
    /// GCS generation, size and metadata MD5 were checked, not body integrity.
    pub gcs_metadata_verified: bool,
    /// Local checksums never attest that a file came from a particular GCS generation.
    local_origin_attested: bool,
    pub eof_observed: bool,
    pub complete_body_identity_verified: bool,
    pub header_contract: Option<&'static str>,
    pub lines_examined: u64,
    pub data_rows_examined: u64,
    pub accepted_rows: u64,
    pub empty_rows: u64,
    pub rejected_rows: u64,
    pub blank_lines: u64,
    pub bytes_read: u64,
    pub bytes_in_examined_lines: u64,
    /// No region filtering is supported: every examined row uses the strict parser.
    filtered_rows: u64,
    clickhouse_writes: u64,
}

impl ValidationReport {
    fn new(args: &ValidateHistogramsArgs) -> Self {
        Self {
            schema_version: 1,
            status: "stopped_first_error",
            diagnostic: None,
            // Never serialize unvalidated identity arguments, even on early errors.
            source: None,
            expected_generation: None,
            expected_size_bytes: args.source_size_bytes,
            expected_md5_base64: None,
            max_rows: args.max_rows,
            max_bytes: args.max_bytes,
            max_line_bytes: MAX_LINE_BYTES,
            gcs_metadata_verified: false,
            local_origin_attested: false,
            eof_observed: false,
            complete_body_identity_verified: false,
            header_contract: None,
            lines_examined: 0,
            data_rows_examined: 0,
            accepted_rows: 0,
            empty_rows: 0,
            rejected_rows: 0,
            blank_lines: 0,
            bytes_read: 0,
            bytes_in_examined_lines: 0,
            filtered_rows: 0,
            clickhouse_writes: 0,
        }
    }

    pub fn failed(&self) -> bool {
        self.status == "stopped_first_error"
    }
}

/// Open read-only inputs only. A local file is verified against the supplied body
/// identity, not claimed to be a generation-attested copy of a remote object.
pub fn validate(args: &ValidateHistogramsArgs) -> ValidationReport {
    let mut report = ValidationReport::new(args);
    if !STANDARD
        .decode(&args.source_md5_base64)
        .is_ok_and(|d| d.len() == 16)
    {
        report.diagnostic = Some("invalid_md5_base64");
        return report;
    }
    report.expected_md5_base64 = Some(args.source_md5_base64.clone());
    let opened = (|| -> anyhow::Result<Box<dyn Read>> {
        if args.source.starts_with("gs://") {
            // Reject malformed identity before obtaining credentials or accessing GCS.
            let object = validated_gcs_object(args, &mut report)?;
            let reader = ImmutableGcsReader::open(Arc::new(HttpGcsBackend::new()?), &object)?;
            report.gcs_metadata_verified = true;
            Ok(Box::new(reader))
        } else {
            anyhow::ensure!(
                args.source_generation.is_none(),
                "local generation unsupported"
            );
            anyhow::ensure!(!args.source.contains("://"), "unsupported source scheme");
            let file = open_regular_file(&args.source)?;
            report.source = Some(args.source.clone());
            Ok(Box::new(file))
        }
    })();
    match opened {
        Ok(reader) => validate_reader(reader, args, &mut report),
        Err(_) => report.diagnostic = Some("source_open_or_identity_error"),
    }
    report
}

fn validated_gcs_object(
    args: &ValidateHistogramsArgs,
    report: &mut ValidationReport,
) -> anyhow::Result<ImmutableGcsObject> {
    let generation = args
        .source_generation
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("generation required"))?;
    let object = ImmutableGcsObject {
        uri: args.source.clone(),
        generation: generation.clone(),
        byte_size: args.source_size_bytes,
        checksum_algorithm: "md5_base64".into(),
        checksum: args.source_md5_base64.clone(),
        immutable_read_uri: format!("{}?generation={generation}", args.source),
    };
    let request = object.request()?;
    // Only the canonical identity accepted by the immutable reader is reportable.
    report.source = Some(format!("gs://{}/{}", request.bucket, request.object));
    report.expected_generation = Some(request.generation);
    report.expected_md5_base64 = Some(request.md5_base64);
    Ok(object)
}

/// Reject special files before opening, then verify the opened descriptor too.
/// Nonblocking open closes the FIFO path-swap hang between those two checks.
fn open_regular_file(path: &str) -> anyhow::Result<std::fs::File> {
    let before = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(before.is_file(), "regular local file required");
    open_regular_file_checked(path, &before)
}

#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    target_os = "macos"
))]
fn open_regular_file_checked(
    path: &str,
    before: &std::fs::Metadata,
) -> anyhow::Result<std::fs::File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    // Platform open(2) ABI flags: O_NONBLOCK | O_NOFOLLOW | O_NOCTTY.
    // Keep this scoped to the supported deployment/development targets rather
    // than assuming that Unix flag values are portable or adding a dependency.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    const FLAGS: i32 = 0x800 | 0x20000 | 0x100;
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    const FLAGS: i32 = 0x800 | 0x8000 | 0x100;
    #[cfg(target_os = "macos")]
    const FLAGS: i32 = 0x4 | 0x100 | 0x20000;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FLAGS)
        .open(path)?;
    let after = file.metadata()?;
    anyhow::ensure!(
        after.is_file() && before.dev() == after.dev() && before.ino() == after.ino(),
        "regular local file changed during open"
    );
    Ok(file)
}

#[cfg(not(any(
    all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    target_os = "macos"
)))]
fn open_regular_file_checked(
    _path: &str,
    _before: &std::fs::Metadata,
) -> anyhow::Result<std::fs::File> {
    // No fallback to a potentially blocking open on an unsupported platform.
    anyhow::bail!("safe local file opening unsupported on this platform")
}

struct DigestReader<R> {
    inner: R,
    digest: Md5,
    bytes: u64,
}

impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(output)?;
        self.digest.update(&output[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

fn validate_reader(
    reader: impl Read,
    args: &ValidateHistogramsArgs,
    report: &mut ValidationReport,
) {
    let tracked = DigestReader {
        inner: reader.take(args.max_bytes.unwrap_or(u64::MAX)),
        digest: Md5::new(),
        bytes: 0,
    };
    let mut reader = BufReader::new(tracked);
    let mut header = None;
    let mut line = Vec::new();
    loop {
        if header.is_some()
            && args
                .max_rows
                .is_some_and(|n| report.data_rows_examined >= n)
        {
            report.status = "bounded_validation";
            break;
        }
        line.clear();
        let read = (&mut reader)
            .take(MAX_LINE_BYTES + 1)
            .read_until(b'\n', &mut line);
        if read.is_err() {
            report.diagnostic = Some("source_read_error");
            break;
        }
        // A byte budget ending in the middle of a line is not a source EOF and
        // must not manufacture a row out of a truncated prefix.
        let at_byte_limit = args.max_bytes.is_some_and(|n| reader.get_ref().bytes >= n);
        if (line.is_empty() || !line.ends_with(b"\n")) && at_byte_limit {
            report.status = "bounded_validation";
            break;
        }
        if line.len() as u64 > MAX_LINE_BYTES {
            report.diagnostic = Some("line_resource_limit_exceeded");
            break;
        }
        if line.is_empty() {
            report.eof_observed = true;
            if header.is_none() {
                report.diagnostic = Some("missing_header");
            } else if reader.get_ref().bytes != args.source_size_bytes {
                report.diagnostic = Some("complete_size_mismatch");
            } else if STANDARD.encode(reader.get_ref().digest.clone().finalize())
                != args.source_md5_base64
            {
                report.diagnostic = Some("complete_checksum_mismatch");
            } else {
                report.complete_body_identity_verified = true;
                report.status = "complete_success";
            }
            break;
        }
        report.lines_examined += 1;
        report.bytes_in_examined_lines += line.len() as u64;
        // Match std::io::Lines, used by the production loader, exactly.
        if line.ends_with(b"\n") {
            line.pop();
            if line.ends_with(b"\r") {
                line.pop();
            }
        }
        let Ok(text) = std::str::from_utf8(&line) else {
            report.diagnostic = Some("invalid_utf8");
            break;
        };
        if text.is_empty() {
            report.blank_lines += 1;
            continue;
        }
        let parts: Vec<_> = text.split('\t').collect();
        let Some(parsed_header) = &header else {
            match Header::parse(&parts) {
                Ok(parsed) => {
                    report.header_contract = Some(if parsed.updated {
                        "updated_19_core"
                    } else {
                        "legacy_15_core"
                    });
                    header = Some(parsed);
                }
                Err(_) => {
                    report.diagnostic = Some("invalid_header");
                    break;
                }
            }
            continue;
        };
        report.data_rows_examined += 1;
        match parse_histogram_row(&parts, parsed_header) {
            Ok(Some(_)) => report.accepted_rows += 1,
            Ok(None) => report.empty_rows += 1,
            Err(_) => {
                report.rejected_rows += 1;
                report.diagnostic = Some("invalid_histogram_row");
                break;
            }
        }
    }
    report.bytes_read = reader.get_ref().bytes;
}

#[cfg(test)]
mod tests;
