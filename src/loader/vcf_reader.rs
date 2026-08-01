//! VCF streaming reader using genohype-core's IO layer + noodles BGZF.
//!
//! Supports two modes:
//! - `open()`: streams all records from byte 0 (header + data)
//! - `open_region()`: uses a tabix index to seek to a specific genomic region,
//!   reading only the relevant BGZF blocks via HTTP range requests on GCS.
//!
//! Both modes yield raw tab-delimited lines one at a time without buffering.

use anyhow::Context;
use genohype_core::io::get_reader;
use noodles::bgzf;
use noodles::csi::BinningIndex;
use noodles::tabix;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use tracing::info;

/// A lightweight VCF line reader that streams from GCS via genohype-core.
/// Records are yielded one at a time without buffering the entire file.
pub struct VcfStream {
    pub sample_names: Vec<String>,
    lines: Box<dyn Iterator<Item = anyhow::Result<String>> + Send>,
}

impl VcfStream {
    /// Open a bgzipped VCF and stream all records.
    pub fn open(vcf_path: &str) -> anyhow::Result<Self> {
        let reader = get_reader(vcf_path)?;
        let bgzf_reader = bgzf::Reader::new(reader);
        let buf_reader = BufReader::new(bgzf_reader);

        let mut lines_iter = buf_reader.lines();

        // Read header
        let sample_names = loop {
            match lines_iter.next() {
                Some(Ok(line)) => {
                    if line.starts_with("##") {
                        continue;
                    }
                    if line.starts_with("#CHROM") {
                        let parts: Vec<&str> = line.split('\t').collect();
                        if parts.len() < 8 {
                            anyhow::bail!("invalid #CHROM header: expected at least 8 columns");
                        }
                        let names = if parts.len() > 9 {
                            parts[9..].iter().map(|s| s.to_string()).collect()
                        } else {
                            Vec::new()
                        };
                        info!("Found {} samples in VCF header", names.len());
                        break names;
                    }
                }
                Some(Err(e)) => return Err(e.into()),
                None => anyhow::bail!("VCF header not found"),
            }
        };

        let streaming_iter = lines_iter.map(|result| result.map_err(anyhow::Error::from));

        Ok(VcfStream {
            sample_names,
            lines: Box::new(streaming_iter),
        })
    }

    /// Open a bgzipped VCF and stream only records in the given region.
    /// Legacy callers may fall back to a full scan when the adjacent TBI is absent.
    pub fn open_region(vcf_path: &str, chrom: &str, start: u32, stop: u32) -> anyhow::Result<Self> {
        Self::open_region_with_index_policy(vcf_path, chrom, start, stop, false)
    }

    /// Open a bounded region and require the adjacent TBI.
    /// Y1 paths use this contract so a missing/corrupt index cannot become a full scan.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open_region_required_index(
        vcf_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
    ) -> anyhow::Result<Self> {
        Self::open_region_with_index_policy(vcf_path, chrom, start, stop, true)
    }

    fn open_region_with_index_policy(
        vcf_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
        require_index: bool,
    ) -> anyhow::Result<Self> {
        let tbi_path = format!("{vcf_path}.tbi");
        let index = match load_tabix_index(&tbi_path) {
            Ok(index) => index,
            Err(error) if !require_index => {
                info!(
                    "No usable tabix index at {}, falling back to full scan: {}",
                    tbi_path, error
                );
                return Self::open(vcf_path);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("required tabix index is unavailable at {tbi_path}"));
            }
        };

        info!("Using tabix index for {}:{}-{}", chrom, start, stop);

        let header_reader = get_reader(vcf_path)?;
        let bgzf_header = bgzf::Reader::new(header_reader);
        let mut vcf_header_reader = noodles::vcf::io::Reader::new(bgzf_header);
        let header = vcf_header_reader.read_header()?;
        let sample_names: Vec<String> = header
            .sample_names()
            .iter()
            .map(|sample| sample.to_string())
            .collect();
        info!("Found {} samples in VCF header", sample_names.len());

        let index_header = index
            .header()
            .ok_or_else(|| anyhow::anyhow!("tabix index has no header"))?;
        let ref_seq_id = index_header
            .reference_sequence_names()
            .iter()
            .position(|name| {
                let bytes: &[u8] = name.as_ref();
                bytes == chrom.as_bytes()
            })
            .ok_or_else(|| anyhow::anyhow!("chrom {chrom} not found in tabix index"))?;

        let interval_start = noodles::core::Position::try_from(start.max(1) as usize)?;
        let interval_end = noodles::core::Position::try_from(stop as usize)?;
        let interval = noodles::core::region::Interval::from(interval_start..=interval_end);
        let chunks = index.query(ref_seq_id, interval)?;

        if chunks.is_empty() {
            info!("No indexed chunks for region {}:{}-{}", chrom, start, stop);
            return Ok(VcfStream {
                sample_names,
                lines: Box::new(std::iter::empty()),
            });
        }

        info!("Found {} chunks for region", chunks.len());
        let vcf_path_owned = vcf_path.to_string();
        let chrom_owned = chrom.to_string();
        let (tx, rx) = mpsc::sync_channel::<anyhow::Result<String>>(1024);

        std::thread::spawn(move || {
            let reader = match get_reader(&vcf_path_owned) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ =
                        tx.send(Err(error).with_context(|| {
                            format!("failed to open indexed VCF {vcf_path_owned}")
                        }));
                    return;
                }
            };
            let mut bgzf_data = bgzf::Reader::new(reader);
            let query = noodles::csi::io::Query::new(&mut bgzf_data, chunks);
            let mut buf_query = BufReader::new(query);
            let mut bytes = Vec::new();
            let tail = loop {
                bytes.clear();
                let count = match buf_query.read_until(b'\n', &mut bytes) {
                    Ok(count) => count,
                    Err(error) => {
                        let _ = tx.send(Err(error.into()));
                        return;
                    }
                };
                if count == 0 {
                    break None;
                }
                if !bytes.ends_with(b"\n") {
                    // A CSI query stops at its virtual chunk end even when its
                    // final buffered read has spilled into the next VCF row.
                    // Complete that row from the underlying BGZF stream before
                    // deciding whether it is an off-interval spill record.
                    break Some(std::mem::take(&mut bytes));
                }
                bytes.pop();
                if let Err(error) = send_indexed_record(&mut bytes, &tx, &chrom_owned, start, stop)
                {
                    let _ = tx.send(Err(error));
                    return;
                }
            };
            drop(buf_query);

            if let Some(mut tail) = tail {
                match bgzf_data.read_until(b'\n', &mut tail) {
                    Ok(_) => {}
                    Err(error) => {
                        let _ = tx.send(Err(error.into()));
                        return;
                    }
                }
                if tail.ends_with(b"\n") {
                    tail.pop();
                }
                if let Err(error) = send_indexed_record(&mut tail, &tx, &chrom_owned, start, stop) {
                    let _ = tx.send(Err(error));
                }
            }
        });

        Ok(VcfStream {
            sample_names,
            lines: Box::new(rx.into_iter()),
        })
    }

    /// Iterate over data lines. I/O and BGZF decoding errors are never discarded.
    pub fn records(self) -> impl Iterator<Item = anyhow::Result<String>> + Send {
        self.lines
    }
}

fn send_indexed_record(
    bytes: &mut Vec<u8>,
    tx: &mpsc::SyncSender<anyhow::Result<String>>,
    expected_chrom: &str,
    start: u32,
    stop: u32,
) -> anyhow::Result<()> {
    if bytes.ends_with(b"\r") {
        bytes.pop();
    }
    let line = std::str::from_utf8(bytes).context("indexed VCF record is not valid UTF-8")?;
    let pos_end = line
        .find('\t')
        .ok_or_else(|| anyhow::anyhow!("indexed VCF record has no CHROM/POS separator"))?;
    let line_chrom = &line[..pos_end];
    if line_chrom != expected_chrom {
        return Ok(());
    }
    let next_tab = line[pos_end + 1..]
        .find('\t')
        .ok_or_else(|| anyhow::anyhow!("indexed VCF record has no POS/ID separator"))?;
    let pos_str = &line[pos_end + 1..pos_end + 1 + next_tab];
    let pos = pos_str
        .parse::<u32>()
        .map_err(|error| anyhow::anyhow!("invalid indexed VCF position {pos_str:?}: {error}"))?;
    if pos < start || pos > stop {
        return Ok(());
    }

    tx.send(Ok(line.to_string()))
        .map_err(|_| anyhow::anyhow!("indexed VCF receiver dropped before completion"))
}

/// Read and preserve the raw VCF header required by the strict Y1 contract.
pub fn read_header_text(vcf_path: &str) -> anyhow::Result<String> {
    let reader = get_reader(vcf_path)?;
    let bgzf_reader = bgzf::Reader::new(reader);
    let buf_reader = BufReader::new(bgzf_reader);
    let mut lines = Vec::new();

    for line in buf_reader.lines() {
        let line = line?;
        if !line.starts_with('#') {
            break;
        }
        let is_columns = line.starts_with("#CHROM");
        lines.push(line);
        if is_columns {
            return Ok(lines.join("\n"));
        }
    }

    anyhow::bail!("VCF header not found in {vcf_path}")
}

fn load_tabix_index(tbi_path: &str) -> anyhow::Result<tabix::Index> {
    let reader = get_reader(tbi_path)?;
    let mut tbi_reader = tabix::io::Reader::new(reader);
    Ok(tbi_reader.read_index()?)
}

/// Parse a VCF INFO field string into key-value pairs (zero-allocation, borrows input).
/// Flag fields (no `=`) get value `None`.
pub fn parse_info_field_shallow(info_str: &str) -> HashMap<&str, Option<&str>> {
    let mut info = HashMap::new();
    for entry in info_str.split(';') {
        if let Some(eq_pos) = entry.find('=') {
            let key = &entry[..eq_pos];
            let val = &entry[eq_pos + 1..];
            info.insert(key, Some(val));
        } else {
            info.insert(entry, None);
        }
    }
    info
}

/// Extract a float from the info map.
pub fn info_float(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<f32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| if *v == "." { None } else { v.parse().ok() })
}

/// Extract an int from the info map.
pub fn info_int(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<i32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| if *v == "." { None } else { v.parse().ok() })
}

/// Extract a u32 from the info map.
pub fn info_u32(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<u32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| if *v == "." { None } else { v.parse().ok() })
}

/// Extract the first comma-separated value as a string.
pub fn info_first(info: &HashMap<&str, Option<&str>>, key: &str) -> String {
    info.get(key)
        .and_then(|v| v.as_ref())
        .map(|v| {
            if *v == "." {
                return String::new();
            }
            v.split(',').next().unwrap_or("").to_string()
        })
        .unwrap_or_default()
}

/// Check if an INFO flag is present.
pub fn info_flag(info: &HashMap<&str, Option<&str>>, key: &str) -> bool {
    info.contains_key(key)
}

/// Extract the first element of a Number=A field as f32.
pub fn info_first_float(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<f32> {
    info.get(key).and_then(|v| v.as_ref()).and_then(|v| {
        if *v == "." {
            return None;
        }
        v.split(',').next().and_then(|s| s.parse().ok())
    })
}

/// Extract the first element of a Number=A field as u32.
pub fn info_first_u32(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<u32> {
    info.get(key).and_then(|v| v.as_ref()).and_then(|v| {
        if *v == "." {
            return None;
        }
        v.split(',').next().and_then(|s| s.parse().ok())
    })
}

#[cfg(test)]
mod tests {
    use super::VcfStream;
    use noodles::bgzf;
    use noodles::core::Position;
    use noodles::csi::binning_index::index::reference_sequence::bin::Chunk;
    use noodles::tabix;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn indexed_fixture(label: &str, records: &[(u32, &str)]) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "gnomad-lr-vcf-{label}-{}-{nonce}.vcf.gz",
            std::process::id()
        ));
        let index_path = PathBuf::from(format!("{}.tbi", path.display()));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = bgzf::Writer::new(file);
        writeln!(writer, "##fileformat=VCFv4.3").unwrap();
        writeln!(writer, "##contig=<ID=chr1,length=100000>").unwrap();
        writeln!(writer, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO").unwrap();
        writer.flush().unwrap();

        let mut indexer = tabix::index::Indexer::default();
        indexer.set_header(noodles::csi::binning_index::index::header::Builder::vcf().build());
        for (position, line) in records {
            let chunk_start = writer.virtual_position();
            writeln!(writer, "{line}").unwrap();
            let chunk_end = writer.virtual_position();
            let position = Position::try_from(*position as usize).unwrap();
            indexer
                .add_record(
                    "chr1",
                    position,
                    position,
                    Chunk::new(chunk_start, chunk_end),
                )
                .unwrap();
        }
        writer.finish().unwrap();

        let file = std::fs::File::create(&index_path).unwrap();
        let mut index_writer = tabix::io::Writer::new(file);
        index_writer.write_index(&indexer.build()).unwrap();
        (path, index_path)
    }

    fn remove_fixture(path: &PathBuf, index_path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(index_path);
    }

    #[test]
    fn record_iteration_exposes_background_errors() {
        let stream = VcfStream {
            sample_names: Vec::new(),
            lines: Box::new(vec![Err(anyhow::anyhow!("fixture I/O failure"))].into_iter()),
        };
        let error = stream.records().next().unwrap().unwrap_err();
        assert_eq!(error.to_string(), "fixture I/O failure");
    }

    #[test]
    fn indexed_chunk_tail_is_completed_without_losing_or_duplicating_records() {
        let prefix = "chr1\t10000\t.\tA\tC\t.\tPASS\tPAD=";
        let first = format!("{prefix}{}", "x".repeat(8187 - prefix.len()));
        assert_eq!(first.len(), 8187);
        let second = "chr1\t20000\t.\tG\tT\t.\tPASS\t.";
        let third = "chr1\t20001\t.\tC\tA\t.\tPASS\t.";
        let (path, index_path) = indexed_fixture(
            "chunk-tail",
            &[(10_000, &first), (20_000, second), (20_001, third)],
        );

        let first_rows: anyhow::Result<Vec<_>> =
            VcfStream::open_region_required_index(path.to_str().unwrap(), "chr1", 10_000, 10_000)
                .unwrap()
                .records()
                .collect();
        let adjacent_rows: anyhow::Result<Vec<_>> =
            VcfStream::open_region_required_index(path.to_str().unwrap(), "chr1", 20_000, 20_001)
                .unwrap()
                .records()
                .collect();
        remove_fixture(&path, &index_path);

        assert_eq!(first_rows.unwrap(), [first]);
        assert_eq!(
            adjacent_rows.unwrap(),
            [second.to_string(), third.to_string()]
        );
    }

    #[test]
    fn malformed_complete_indexed_record_is_not_suppressed() {
        let (path, index_path) = indexed_fixture("malformed", &[(10_000, "chr1")]);
        let result: anyhow::Result<Vec<_>> =
            VcfStream::open_region_required_index(path.to_str().unwrap(), "chr1", 10_000, 10_000)
                .unwrap()
                .records()
                .collect();
        remove_fixture(&path, &index_path);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no CHROM/POS separator"));
    }

    #[test]
    fn bounded_strict_reads_require_a_tabix_index() {
        let path = std::env::temp_dir().join(format!(
            "gnomad-lr-missing-index-{}-{}.vcf.gz",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let error =
            match VcfStream::open_region_required_index(path.to_str().unwrap(), "chr22", 1, 10) {
                Ok(_) => panic!("strict indexed read unexpectedly succeeded"),
                Err(error) => error,
            };
        assert!(error.to_string().contains("required tabix index"));
    }
}
