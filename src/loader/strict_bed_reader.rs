//! Error-bearing tabix/BGZF BED reader for strict Y1 ancillary loads.
//!
//! This intentionally does not replace the legacy `BedStream`. Every open,
//! index, decode, worker, and line-shape failure is observable as an error item.

use anyhow::{bail, Context};
use genohype_core::io::get_reader;
use noodles::bgzf;
use noodles::csi::BinningIndex;
use noodles::tabix;
use std::io::{BufRead, BufReader, Read};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;

const ZERO_CHUNK_VALIDATION_MAX_DECOMPRESSED_BYTES: u64 = 1024 * 1024;

enum StreamMessage {
    Line(String),
    Error(anyhow::Error),
    Done,
}

/// Coordinates returned only after a source-specific validator has accepted
/// the complete row shape and all typed invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedBedRecord {
    pub chrom: String,
    pub start0: u32,
    pub end0: u32,
}

/// Source-specific contract applied before interval spill filtering.
///
/// This prevents the indexed reader from silently discarding a malformed row
/// merely because its coordinates lie outside the requested interval.
pub trait StrictBedRecordValidator: Send + 'static {
    fn validate(&self, line: &str) -> anyhow::Result<ValidatedBedRecord>;
}

impl<F> StrictBedRecordValidator for F
where
    F: Fn(&str) -> anyhow::Result<ValidatedBedRecord> + Send + 'static,
{
    fn validate(&self, line: &str) -> anyhow::Result<ValidatedBedRecord> {
        self(line)
    }
}

/// A strict, error-bearing stream of raw non-comment BED records.
pub struct StrictBedStream {
    lines: StrictBedLines,
}

impl StrictBedStream {
    /// Open an explicitly indexed BED source for a one-based inclusive browser
    /// interval. The equivalent BED interval is `[start - 1, stop)`.
    pub fn open_region<V>(
        bed_path: &str,
        index_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
        validator: V,
    ) -> anyhow::Result<Self>
    where
        V: StrictBedRecordValidator,
    {
        if start == 0 || start > stop {
            bail!("browser interval must be nonempty and one-based inclusive");
        }
        if bed_path.trim().is_empty() || index_path.trim().is_empty() {
            bail!("strict BED source and explicit index paths must be nonempty");
        }

        let index = load_tabix_index(index_path)
            .with_context(|| format!("failed to load required tabix index {index_path}"))?;
        let index_header = index
            .header()
            .ok_or_else(|| anyhow::anyhow!("tabix index {index_path} has no header"))?;
        let line_comment_prefix = index_header.line_comment_prefix();
        let ref_seq_id = index_header
            .reference_sequence_names()
            .iter()
            .position(|name| {
                let bytes: &[u8] = name.as_ref();
                bytes == chrom.as_bytes()
            })
            .ok_or_else(|| {
                anyhow::anyhow!("chrom {chrom} not found in tabix index {index_path}")
            })?;

        // noodles intervals are one-based. This is the same genomic interval as
        // BED/tabix [start - 1, stop); post-filtering below uses BED start0.
        let interval_start = noodles::core::Position::try_from(start as usize)?;
        let interval_end = noodles::core::Position::try_from(stop as usize)?;
        let interval = noodles::core::region::Interval::from(interval_start..=interval_end);
        let chunks = index
            .query(ref_seq_id, interval)
            .with_context(|| format!("failed to query tabix chunks for {chrom}:{start}-{stop}"))?;

        // Open synchronously even for a zero-chunk query. A successful zero-row
        // receipt still attests that the declared source object was openable;
        // missing/revoked sources must never be converted to clean EOF.
        let reader = open_bed_source(bed_path)?;
        if chunks.is_empty() {
            validate_zero_chunk_source(reader, &index, &validator).with_context(|| {
                format!("zero-chunk source/index validation failed for {chrom}:{start}-{stop}")
            })?;
            unreachable!("zero-chunk completion is disabled until an exact binding exists");
        }
        let chrom = chrom.to_string();
        let source_start0 = start - 1;
        let source_end0 = stop;
        let lines = StrictBedLines::spawn(move |sender| {
            let mut bgzf_data = bgzf::Reader::new(reader);
            let query = noodles::csi::io::Query::new(&mut bgzf_data, chunks);
            let mut buffered = BufReader::new(query);
            stream_records(
                &mut buffered,
                sender,
                &chrom,
                source_start0,
                source_end0,
                line_comment_prefix,
                &validator,
            )
        });
        Ok(Self { lines })
    }

    pub fn records(self) -> StrictBedLines {
        self.lines
    }
}

/// Iterator returned by [`StrictBedStream::records`]. It terminates after the
/// first failure and never converts a worker/channel failure into clean EOF.
pub struct StrictBedLines {
    receiver: Option<Receiver<StreamMessage>>,
    worker: Option<JoinHandle<()>>,
    terminal: bool,
}

impl StrictBedLines {
    fn spawn<F>(worker: F) -> Self
    where
        F: FnOnce(&SyncSender<StreamMessage>) -> anyhow::Result<()> + Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(1024);
        let handle = std::thread::spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| worker(&sender)));
            let message = match result {
                Ok(Ok(())) => StreamMessage::Done,
                Ok(Err(error)) => StreamMessage::Error(error),
                Err(payload) => {
                    let detail = payload
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("unknown panic payload");
                    StreamMessage::Error(anyhow::anyhow!(
                        "strict BED worker panicked before completion: {detail}"
                    ))
                }
            };
            let _ = sender.send(message);
        });
        Self {
            receiver: Some(receiver),
            worker: Some(handle),
            terminal: false,
        }
    }
}

impl Iterator for StrictBedLines {
    type Item = anyhow::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }
        let message = self.receiver.as_ref()?.recv();
        match message {
            Ok(StreamMessage::Line(line)) => Some(Ok(line)),
            Ok(StreamMessage::Error(error)) => {
                self.terminal = true;
                Some(Err(error))
            }
            Ok(StreamMessage::Done) => {
                self.terminal = true;
                None
            }
            Err(_) => {
                self.terminal = true;
                Some(Err(anyhow::anyhow!(
                    "strict BED worker terminated without a completion receipt"
                )))
            }
        }
    }
}

impl Drop for StrictBedLines {
    fn drop(&mut self) {
        // Dropping the receiver first releases a producer blocked on backpressure.
        self.receiver.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn validate_zero_chunk_source<R, V>(
    reader: R,
    index: &tabix::Index,
    validator: &V,
) -> anyhow::Result<()>
where
    R: Read,
    V: StrictBedRecordValidator,
{
    let header = index.header().expect("open_region requires a tabix header");
    let comment_prefix = header.line_comment_prefix();
    let line_skip_count = u64::from(header.line_skip_count());
    let limited = bgzf::Reader::new(reader).take(ZERO_CHUNK_VALIDATION_MAX_DECOMPRESSED_BYTES + 1);
    let mut reader = BufReader::new(limited);
    let mut bytes = Vec::new();
    let mut decoded_bytes = 0u64;
    let mut line_number = 0u64;
    let mut source_records = 0u64;

    // A zero-chunk result cannot use the ordinary indexed query to establish
    // compatibility. Within a strict diagnostic budget, decode through BGZF
    // EOF so a valid first record cannot hide later corruption, and interpret
    // headers exactly as declared by the TBI metadata. Larger objects fail at
    // the budget rather than turning an already doomed zero-chunk request into
    // an unbounded scan. None of this proves that the index belongs to the
    // source, so D0 always refuses completion below until an external immutable
    // exact source+index binding token is implemented.
    loop {
        bytes.clear();
        let count = reader
            .read_until(b'\n', &mut bytes)
            .context("zero-chunk BGZF validation read failed")?;
        if count == 0 {
            break;
        }
        decoded_bytes += count as u64;
        if decoded_bytes > ZERO_CHUNK_VALIDATION_MAX_DECOMPRESSED_BYTES {
            bail!(
                "zero-chunk validation exceeded its {}-byte decompressed diagnostic budget",
                ZERO_CHUNK_VALIDATION_MAX_DECOMPRESSED_BYTES
            );
        }
        line_number += 1;
        if !bytes.ends_with(b"\n") {
            bail!("truncated nonempty BED line in zero-chunk BGZF source");
        }
        bytes.pop();
        if bytes.ends_with(b"\r") {
            bytes.pop();
        }
        if line_number <= line_skip_count
            || bytes.is_empty()
            || bytes.first().copied() == Some(comment_prefix)
        {
            continue;
        }

        let line = std::str::from_utf8(&bytes).context("BED line is not valid UTF-8")?;
        let record = validator.validate(line)?;
        source_records += 1;
        let indexed_chrom = header.reference_sequence_names().iter().any(|name| {
            let bytes: &[u8] = name.as_ref();
            bytes == record.chrom.as_bytes()
        });
        if !indexed_chrom {
            bail!(
                "BGZF source record chromosome {} is absent from the tabix index",
                record.chrom
            );
        }
    }

    if reader.get_ref().get_ref().virtual_position().compressed() == 0 {
        bail!("zero-chunk source is empty or lacks a decodable BGZF container");
    }
    if line_number < line_skip_count {
        bail!("tabix line-skip metadata exceeds the BGZF source line count");
    }
    match (
        source_records,
        index.last_first_record_start_position().is_some(),
    ) {
        (0, true) => bail!("tabix index advertises records but the BGZF source has none"),
        (1.., false) => bail!("BGZF source has records but the tabix index advertises none"),
        _ => {}
    }

    bail!("zero-chunk completion requires an immutable exact source+index binding token; no such runtime binding is implemented")
}

fn stream_records<R, V>(
    reader: &mut R,
    sender: &SyncSender<StreamMessage>,
    expected_chrom: &str,
    source_start0: u32,
    source_end0: u32,
    line_comment_prefix: u8,
    validator: &V,
) -> anyhow::Result<()>
where
    R: BufRead,
    V: StrictBedRecordValidator,
{
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        let count = reader
            .read_until(b'\n', &mut bytes)
            .context("BGZF/tabix line read failed")?;
        if count == 0 {
            return Ok(());
        }
        if !bytes.ends_with(b"\n") {
            bail!("truncated nonempty BED line at end of indexed stream");
        }
        bytes.pop();
        if bytes.ends_with(b"\r") {
            bytes.pop();
        }
        if bytes.is_empty() {
            // noodles can expose one exact empty separator where BGZF chunks meet.
            continue;
        }
        let line = std::str::from_utf8(&bytes).context("BED line is not valid UTF-8")?;
        if bytes.first().copied() == Some(line_comment_prefix) {
            continue;
        }

        // Validate the entire source-specific record before considering whether
        // it is an off-interval chunk spill row.
        let record = validator.validate(line)?;
        if record.chrom != expected_chrom
            || record.start0 < source_start0
            || record.start0 >= source_end0
        {
            continue;
        }
        sender
            .send(StreamMessage::Line(line.to_string()))
            .map_err(|_| anyhow::anyhow!("strict BED receiver dropped before completion"))?;
    }
}

fn open_bed_source(bed_path: &str) -> anyhow::Result<genohype_core::io::BoxedReader> {
    get_reader(bed_path).with_context(|| format!("failed to open strict BED source {bed_path}"))
}

fn load_tabix_index(index_path: &str) -> anyhow::Result<tabix::Index> {
    let reader = get_reader(index_path)
        .with_context(|| format!("failed to open tabix index {index_path}"))?;
    let mut tbi_reader = tabix::io::Reader::new(reader);
    tbi_reader
        .read_index()
        .with_context(|| format!("failed to decode tabix index {index_path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::y1::methylation::{parse_methylation_source_record, MethylationSourceType};
    use noodles::core::Position;
    use noodles::csi::binning_index::index::reference_sequence::bin::Chunk;
    use std::io::{Cursor, Write};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "gnomad-lr-strict-bed-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn total_validator() -> impl StrictBedRecordValidator {
        |line: &str| {
            let record = parse_methylation_source_record(line)?;
            if record.source_type != MethylationSourceType::Total {
                bail!("fixture source type is not Total");
            }
            Ok(ValidatedBedRecord {
                chrom: record.chrom,
                start0: record.source_start0,
                end0: record.source_end0,
            })
        }
    }

    fn collect_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<String>> {
        let owned = bytes.to_vec();
        StrictBedLines::spawn(move |sender| {
            stream_records(
                &mut Cursor::new(owned),
                sender,
                "chr22",
                99,
                200,
                b'#',
                &total_validator(),
            )
        })
        .collect()
    }

    fn indexed_fixture_with_header(
        label: &str,
        index_chrom: &str,
        header: noodles::csi::binning_index::index::Header,
        records: &[(u32, u32, &str)],
    ) -> (PathBuf, PathBuf) {
        let bed_path = temp_path(label).with_extension("bed.gz");
        let index_path = PathBuf::from(format!("{}.tbi", bed_path.display()));
        let file = std::fs::File::create(&bed_path).unwrap();
        let mut writer = bgzf::Writer::new(file);
        let mut indexer = tabix::index::Indexer::default();
        indexer.set_header(header);
        let mut chunk_start = writer.virtual_position();
        for (start0, end0, line) in records {
            writeln!(writer, "{line}").unwrap();
            // Force each indexed record across a real BGZF block boundary.
            writer.flush().unwrap();
            let chunk_end = writer.virtual_position();
            indexer
                .add_record(
                    index_chrom,
                    Position::try_from((*start0 + 1) as usize).unwrap(),
                    Position::try_from(*end0 as usize).unwrap(),
                    Chunk::new(chunk_start, chunk_end),
                )
                .unwrap();
            chunk_start = chunk_end;
        }
        writer.finish().unwrap();
        let index = indexer.build();
        let file = std::fs::File::create(&index_path).unwrap();
        let mut index_writer = tabix::io::Writer::new(file);
        index_writer.write_index(&index).unwrap();
        (bed_path, index_path)
    }

    fn indexed_fixture_with_records(
        label: &str,
        index_chrom: &str,
        records: &[(u32, u32, &str)],
    ) -> (PathBuf, PathBuf) {
        indexed_fixture_with_header(
            label,
            index_chrom,
            noodles::csi::binning_index::index::header::Builder::bed().build(),
            records,
        )
    }

    fn indexed_fixture(label: &str) -> (PathBuf, PathBuf) {
        indexed_fixture_with_records(
            label,
            "chr22",
            &[
                (99, 100, "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50"),
                (149, 150, "chr22\t149\t150\t25\tTotal\t4\t1\t3\t25"),
            ],
        )
    }

    fn remove_fixture(bed_path: &PathBuf, index_path: &PathBuf) {
        let _ = std::fs::remove_file(bed_path);
        let _ = std::fs::remove_file(index_path);
    }

    #[test]
    fn exact_empty_chunk_separator_is_ignored_without_losing_or_duplicating_keys() {
        let input = b"#header\nchr22\t99\t100\t80\tTotal\t2\t1\t1\t50\n\nchr22\t149\t150\t25\tTotal\t4\t1\t3\t25\nchr22\t200\t201\t50\tTotal\t2\t1\t1\t50\n";
        let rows = collect_from_bytes(input).unwrap();
        assert_eq!(
            rows,
            vec![
                "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50".to_string(),
                "chr22\t149\t150\t25\tTotal\t4\t1\t3\t25".to_string(),
            ]
        );
    }

    #[test]
    fn truncated_and_malformed_nonempty_lines_propagate() {
        let truncated = collect_from_bytes(b"chr22\t99\t100\t80\tTotal\t2\t1\t1\t50").unwrap_err();
        assert!(truncated
            .to_string()
            .contains("truncated nonempty BED line"));

        let malformed = collect_from_bytes(b"chr22\tnot-a-position\t100\t80\tTotal\t2\t1\t1\t50\n")
            .unwrap_err();
        assert!(malformed
            .to_string()
            .contains("methylation start0 is not a UInt32"));
    }

    #[test]
    fn malformed_four_eight_and_ten_field_spill_rows_fail_before_filtering() {
        let malformed = [
            // Before, inside, and after the requested [99, 200) start range.
            "chr22\t98\t99\t80",
            "chr22\t149\t150\t80\tTotal\t2\t1\t1",
            "chr22\t200\t201\t80\tTotal\t2\t1\t1\t50\textra",
        ];
        for line in malformed {
            let input = format!("{line}\n");
            let error = collect_from_bytes(input.as_bytes()).unwrap_err();
            assert!(
                error.to_string().contains("exactly nine"),
                "unexpected error for {line}: {error:#}"
            );
        }
    }

    #[test]
    fn malformed_rows_at_real_bgzf_chunk_boundaries_are_not_discarded() {
        for (label, malformed) in [
            ("four", "chr22\t149\t150\t80"),
            ("eight", "chr22\t149\t150\t80\tTotal\t2\t1\t1"),
            ("ten", "chr22\t149\t150\t80\tTotal\t2\t1\t1\t50\textra"),
        ] {
            let fixture_label = format!("malformed-boundary-{label}");
            let (bed_path, index_path) = indexed_fixture_with_records(
                &fixture_label,
                "chr22",
                &[
                    (99, 100, "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50"),
                    (149, 150, malformed),
                ],
            );
            let result: anyhow::Result<Vec<_>> = StrictBedStream::open_region(
                bed_path.to_str().unwrap(),
                index_path.to_str().unwrap(),
                "chr22",
                100,
                150,
                total_validator(),
            )
            .unwrap()
            .records()
            .collect();
            remove_fixture(&bed_path, &index_path);
            assert!(
                result.unwrap_err().to_string().contains("exactly nine"),
                "malformed {label}-field boundary row was not rejected"
            );
        }
    }

    #[test]
    fn background_bgzf_error_and_worker_panic_are_error_items_not_eof() {
        let mut bgzf_failure =
            StrictBedLines::spawn(|_| Err(anyhow::anyhow!("injected BGZF CRC failure")));
        assert!(bgzf_failure
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("BGZF CRC"));
        assert!(bgzf_failure.next().is_none());

        let mut panic_failure = StrictBedLines::spawn(|_| -> anyhow::Result<()> {
            panic!("injected worker termination")
        });
        assert!(panic_failure
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("worker panicked"));
        assert!(panic_failure.next().is_none());
    }

    #[test]
    fn real_bgzf_tabix_blocks_return_each_boundary_key_exactly_once() {
        let (bed_path, index_path) = indexed_fixture("boundary");
        let rows: anyhow::Result<Vec<_>> = StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            100,
            150,
            total_validator(),
        )
        .unwrap()
        .records()
        .collect();
        remove_fixture(&bed_path, &index_path);
        assert_eq!(
            rows.unwrap(),
            [
                "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50".to_string(),
                "chr22\t149\t150\t25\tTotal\t4\t1\t3\t25".to_string(),
            ]
        );
    }

    #[test]
    fn real_corrupt_bgzf_error_propagates_through_the_indexed_query() {
        let (bed_path, index_path) = indexed_fixture("corrupt-bgzf");
        let source_len = std::fs::metadata(&bed_path).unwrap().len() as usize;
        std::fs::write(&bed_path, vec![0u8; source_len]).unwrap();
        let result: anyhow::Result<Vec<_>> = StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            100,
            150,
            total_validator(),
        )
        .unwrap()
        .records()
        .collect();
        remove_fixture(&bed_path, &index_path);
        assert!(result.is_err());
    }

    #[test]
    fn zero_chunk_query_is_not_completion_without_an_exact_binding() {
        let (bed_path, index_path) = indexed_fixture("valid-empty-region");
        let error = StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        )
        .err()
        .expect("unbound zero-chunk query unexpectedly completed");
        remove_fixture(&bed_path, &index_path);
        assert!(format!("{error:#}").contains("immutable exact source+index binding token"));
    }

    #[test]
    fn stale_same_chromosome_index_cannot_complete_a_zero_chunk_query() {
        let (source_path, source_index) = indexed_fixture_with_records(
            "stale-same-chrom-source",
            "chr22",
            &[(99, 100, "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50")],
        );
        let (stale_source, stale_index) = indexed_fixture_with_records(
            "stale-same-chrom-index",
            "chr22",
            &[(999, 1000, "chr22\t999\t1000\t80\tTotal\t2\t1\t1\t50")],
        );
        let error = StrictBedStream::open_region(
            source_path.to_str().unwrap(),
            stale_index.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        )
        .err()
        .expect("stale same-chromosome index unexpectedly completed");
        remove_fixture(&source_path, &source_index);
        remove_fixture(&stale_source, &stale_index);
        assert!(format!("{error:#}").contains("immutable exact source+index binding token"));
    }

    #[test]
    fn in_range_source_with_a_zero_chunk_index_is_rejected() {
        let (source_path, source_index) = indexed_fixture_with_records(
            "in-range-zero-index-source",
            "chr22",
            &[(99, 100, "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50")],
        );
        let (stale_source, stale_index) = indexed_fixture_with_records(
            "in-range-zero-index-index",
            "chr22",
            &[(
                10_000_000,
                10_000_001,
                "chr22\t10000000\t10000001\t80\tTotal\t2\t1\t1\t50",
            )],
        );
        let error = StrictBedStream::open_region(
            source_path.to_str().unwrap(),
            stale_index.to_str().unwrap(),
            "chr22",
            100,
            100,
            total_validator(),
        )
        .err()
        .expect("in-range source with zero index chunks unexpectedly completed");
        remove_fixture(&source_path, &source_index);
        remove_fixture(&stale_source, &stale_index);
        assert!(format!("{error:#}").contains("immutable exact source+index binding token"));
    }

    #[test]
    fn zero_chunk_scan_honors_tbi_line_skip_and_comment_metadata() {
        let header = noodles::csi::binning_index::index::header::Builder::bed()
            .set_line_comment_prefix(b'@')
            .set_line_skip_count(1)
            .build();
        let (bed_path, index_path) = indexed_fixture_with_header(
            "declared-header",
            "chr22",
            header,
            &[(99, 100, "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50")],
        );
        let file = std::fs::File::create(&bed_path).unwrap();
        let mut writer = bgzf::Writer::new(file);
        writeln!(writer, "track name=declared-by-line-skip").unwrap();
        writeln!(writer, "@declared comment").unwrap();
        writeln!(writer, "chr22\t99\t100\t80\tTotal\t2\t1\t1\t50").unwrap();
        writer.finish().unwrap();

        let error = StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        )
        .err()
        .expect("unbound declared-header source unexpectedly completed");
        remove_fixture(&bed_path, &index_path);
        assert!(format!("{error:#}").contains("immutable exact source+index binding token"));
    }

    #[test]
    fn late_bgzf_corruption_is_detected_before_zero_chunk_refusal() {
        let (bed_path, index_path) = indexed_fixture("late-corruption");
        let mut bytes = std::fs::read(&bed_path).unwrap();
        let first_block_size = usize::from(u16::from_le_bytes([bytes[16], bytes[17]])) + 1;
        let second_block_size = usize::from(u16::from_le_bytes([
            bytes[first_block_size + 16],
            bytes[first_block_size + 17],
        ])) + 1;
        let second_crc = first_block_size + second_block_size - 8;
        bytes[second_crc] ^= 0x01;
        std::fs::write(&bed_path, bytes).unwrap();

        let error = StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        )
        .err()
        .expect("late-corrupt zero-chunk source unexpectedly completed");
        remove_fixture(&bed_path, &index_path);
        let detail = format!("{error:#}");
        assert!(detail.contains("zero-chunk BGZF validation read failed"));
        assert!(!detail.contains("immutable exact source+index binding token"));
    }

    #[test]
    fn zero_chunk_validation_has_a_bounded_decompressed_scan_budget() {
        let (bed_path, index_path) = indexed_fixture("bounded-zero-chunk-scan");
        let file = std::fs::File::create(&bed_path).unwrap();
        let mut writer = bgzf::Writer::new(file);
        let comment = format!("#{}", "x".repeat(1023));
        for _ in 0..=ZERO_CHUNK_VALIDATION_MAX_DECOMPRESSED_BYTES / 1024 {
            writeln!(writer, "{comment}").unwrap();
        }
        writer.finish().unwrap();

        let error = StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        )
        .err()
        .expect("oversized zero-chunk diagnostic unexpectedly completed");
        remove_fixture(&bed_path, &index_path);
        assert!(format!("{error:#}").contains("decompressed diagnostic budget"));
    }

    #[test]
    fn corrupt_bgzf_is_rejected_even_when_the_valid_index_returns_zero_chunks() {
        let (bed_path, index_path) = indexed_fixture("corrupt-empty-region");
        let source_len = std::fs::metadata(&bed_path).unwrap().len() as usize;
        std::fs::write(&bed_path, vec![0u8; source_len]).unwrap();
        let error = match StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        ) {
            Ok(_) => panic!("corrupt zero-chunk source unexpectedly succeeded"),
            Err(error) => error,
        };
        remove_fixture(&bed_path, &index_path);
        assert!(error.to_string().contains("zero-chunk source/index"));
    }

    #[test]
    fn zero_chunk_query_rejects_a_source_index_chromosome_mismatch() {
        let (source_path, source_index) = indexed_fixture_with_records(
            "mismatch-source",
            "chr21",
            &[(99, 100, "chr21\t99\t100\t80\tTotal\t2\t1\t1\t50")],
        );
        let (indexed_source, index_path) = indexed_fixture("mismatch-index");
        let error = match StrictBedStream::open_region(
            source_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        ) {
            Ok(_) => panic!("mismatched zero-chunk source/index unexpectedly succeeded"),
            Err(error) => error,
        };
        remove_fixture(&source_path, &source_index);
        remove_fixture(&indexed_source, &index_path);
        assert!(format!("{error:#}").contains("absent from the tabix index"));
    }

    #[test]
    fn zero_chunk_query_still_opens_the_declared_source() {
        let (bed_path, index_path) = indexed_fixture("empty-region");
        std::fs::remove_file(&bed_path).unwrap();
        let error = match StrictBedStream::open_region(
            bed_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            "chr22",
            10_000_000,
            10_010_000,
            total_validator(),
        ) {
            Ok(_) => panic!("missing zero-chunk source unexpectedly returned clean EOF"),
            Err(error) => error,
        };
        let _ = std::fs::remove_file(index_path);
        assert!(error
            .to_string()
            .contains("failed to open strict BED source"));
    }

    #[test]
    fn source_open_and_missing_or_invalid_tabix_failures_are_synchronous() {
        let missing_source = temp_path("missing-source");
        let source_error = match open_bed_source(missing_source.to_str().unwrap()) {
            Ok(_) => panic!("missing source unexpectedly opened"),
            Err(error) => error,
        };
        assert!(source_error
            .to_string()
            .contains("failed to open strict BED source"));

        let missing = temp_path("missing");
        let error = match StrictBedStream::open_region(
            missing.to_str().unwrap(),
            missing.with_extension("tbi").to_str().unwrap(),
            "chr22",
            100,
            200,
            total_validator(),
        ) {
            Ok(_) => panic!("missing index unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("failed to load required tabix index"));

        let invalid = temp_path("invalid");
        std::fs::write(&invalid, b"not a tabix index").unwrap();
        let error = load_tabix_index(invalid.to_str().unwrap()).unwrap_err();
        assert!(error.to_string().contains("failed to decode tabix index"));
        std::fs::remove_file(invalid).unwrap();
    }
}
