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
            let buf_query = BufReader::new(query);

            for line_result in buf_query.lines() {
                let line = match line_result {
                    Ok(line) => line,
                    Err(error) => {
                        let _ = tx.send(Err(error.into()));
                        return;
                    }
                };

                let Some(pos_end) = line.find('\t') else {
                    let _ = tx.send(Err(anyhow::anyhow!(
                        "indexed VCF record has no CHROM/POS separator"
                    )));
                    return;
                };
                let line_chrom = &line[..pos_end];
                if line_chrom != chrom_owned {
                    continue;
                }
                let Some(next_tab) = line[pos_end + 1..].find('\t') else {
                    let _ = tx.send(Err(anyhow::anyhow!(
                        "indexed VCF record has no POS/ID separator"
                    )));
                    return;
                };
                let pos_str = &line[pos_end + 1..pos_end + 1 + next_tab];
                let pos = match pos_str.parse::<u32>() {
                    Ok(pos) => pos,
                    Err(error) => {
                        let _ = tx.send(Err(anyhow::anyhow!(
                            "invalid indexed VCF position {pos_str:?}: {error}"
                        )));
                        return;
                    }
                };
                if pos < start || pos > stop {
                    continue;
                }

                if tx.send(Ok(line)).is_err() {
                    break;
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
