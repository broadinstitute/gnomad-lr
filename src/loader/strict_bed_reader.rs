//! Error-bearing tabix/BGZF BED reader for strict Y1 ancillary loads.
//!
//! This intentionally does not replace the legacy `BedStream`. Every open,
//! index, decode, worker, and line-shape failure is observable as an error item.

use anyhow::{bail, Context};
use genohype_core::io::get_reader;
use noodles::bgzf;
use noodles::csi::BinningIndex;
use noodles::tabix;
use std::io::{BufRead, BufReader};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::JoinHandle;
use tracing::info;

enum StreamMessage {
    Line(String),
    Error(anyhow::Error),
    Done,
}

/// A strict, error-bearing stream of raw non-comment BED records.
pub struct StrictBedStream {
    lines: StrictBedLines,
}

impl StrictBedStream {
    /// Open an explicitly indexed BED source for a one-based inclusive browser
    /// interval. The equivalent BED interval is `[start - 1, stop)`.
    pub fn open_region(
        bed_path: &str,
        index_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
    ) -> anyhow::Result<Self> {
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

        if chunks.is_empty() {
            info!("strict BED: no chunks for {chrom}:{start}-{stop}");
            return Ok(Self {
                lines: StrictBedLines::completed(),
            });
        }

        // Open synchronously: a missing/unreadable source cannot disappear in a
        // detached worker before the caller receives an attempt failure.
        let reader = open_bed_source(bed_path)?;
        let chrom = chrom.to_string();
        let source_start0 = start - 1;
        let source_end0 = stop;
        let lines = StrictBedLines::spawn(move |sender| {
            let mut bgzf_data = bgzf::Reader::new(reader);
            let query = noodles::csi::io::Query::new(&mut bgzf_data, chunks);
            let mut buffered = BufReader::new(query);
            stream_records(&mut buffered, sender, &chrom, source_start0, source_end0)
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
    fn completed() -> Self {
        let (sender, receiver) = mpsc::sync_channel(1);
        let _ = sender.send(StreamMessage::Done);
        Self {
            receiver: Some(receiver),
            worker: None,
            terminal: false,
        }
    }

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

fn stream_records<R: BufRead>(
    reader: &mut R,
    sender: &SyncSender<StreamMessage>,
    expected_chrom: &str,
    source_start0: u32,
    source_end0: u32,
) -> anyhow::Result<()> {
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
        if line.starts_with('#') {
            continue;
        }

        let mut fields = line.splitn(4, '\t');
        let chrom = fields.next().unwrap_or_default();
        let start0 = fields
            .next()
            .ok_or_else(|| anyhow::anyhow!("nonempty BED line has no start0 column"))?
            .parse::<u32>()
            .context("BED start0 is not a UInt32")?;
        let end0 = fields
            .next()
            .ok_or_else(|| anyhow::anyhow!("nonempty BED line has no end0 column"))?
            .parse::<u32>()
            .context("BED end0 is not a UInt32")?;
        if end0 <= start0 {
            bail!("BED interval must have end0 greater than start0");
        }
        if chrom != expected_chrom || start0 < source_start0 || start0 >= source_end0 {
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
    use std::io::Cursor;
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

    fn collect_from_bytes(bytes: &[u8]) -> anyhow::Result<Vec<String>> {
        let owned = bytes.to_vec();
        StrictBedLines::spawn(move |sender| {
            stream_records(&mut Cursor::new(owned), sender, "chr22", 99, 200)
        })
        .collect()
    }

    #[test]
    fn exact_empty_chunk_separator_is_ignored_without_losing_or_duplicating_keys() {
        let input = b"#header\nchr22\t99\t100\t1\n\nchr22\t149\t150\t2\nchr22\t200\t201\toutside\n";
        let rows = collect_from_bytes(input).unwrap();
        assert_eq!(
            rows,
            vec![
                "chr22\t99\t100\t1".to_string(),
                "chr22\t149\t150\t2".to_string(),
            ]
        );
    }

    #[test]
    fn truncated_and_malformed_nonempty_lines_propagate() {
        let truncated = collect_from_bytes(b"chr22\t99\t100\t1").unwrap_err();
        assert!(truncated
            .to_string()
            .contains("truncated nonempty BED line"));

        let malformed = collect_from_bytes(b"chr22\tnot-a-position\t100\t1\n").unwrap_err();
        assert!(malformed.to_string().contains("BED start0 is not a UInt32"));
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
