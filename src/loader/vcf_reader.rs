//! VCF streaming reader using genohype-core's IO layer + noodles BGZF.
//!
//! Supports two modes:
//! - `open()`: streams all records from byte 0 (header + data)
//! - `open_region()`: uses a tabix index to seek to a specific genomic region,
//!   reading only the relevant BGZF blocks via HTTP range requests on GCS.
//!
//! Both modes yield raw tab-delimited lines one at a time without buffering.

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
    lines: Box<dyn Iterator<Item = String> + Send>,
}

impl VcfStream {
    /// Open a bgzipped VCF and stream all records.
    pub fn open(vcf_path: &str) -> anyhow::Result<Self> {
        let reader = get_reader(vcf_path)?;
        let bgzf_reader = bgzf::Reader::new(reader);
        let buf_reader = BufReader::new(bgzf_reader);

        let mut sample_names = Vec::new();
        let mut lines_iter = buf_reader.lines();

        // Read header
        loop {
            match lines_iter.next() {
                Some(Ok(line)) => {
                    if line.starts_with("##") {
                        continue;
                    }
                    if line.starts_with("#CHROM") {
                        let parts: Vec<&str> = line.split('\t').collect();
                        sample_names = parts[9..].iter().map(|s| s.to_string()).collect();
                        info!("Found {} samples in VCF header", sample_names.len());
                        break;
                    }
                }
                Some(Err(e)) => return Err(e.into()),
                None => anyhow::bail!("VCF header not found"),
            }
        }

        let streaming_iter = lines_iter.filter_map(|r| r.ok());

        Ok(VcfStream {
            sample_names,
            lines: Box::new(streaming_iter),
        })
    }

    /// Open a bgzipped VCF and stream only records in the given region,
    /// using a tabix index for efficient seeking.
    ///
    /// Falls back to streaming `open()` if no .tbi index is found.
    pub fn open_region(
        vcf_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
    ) -> anyhow::Result<Self> {
        // Try to load the tabix index
        let tbi_path = format!("{}.tbi", vcf_path);
        let index = match load_tabix_index(&tbi_path) {
            Some(idx) => idx,
            None => {
                info!("No tabix index found at {}, falling back to full scan", tbi_path);
                return Self::open(vcf_path);
            }
        };

        info!("Using tabix index for {}:{}-{}", chrom, start, stop);

        // Read the header (need sample names)
        let header_reader = get_reader(vcf_path)?;
        let bgzf_header = bgzf::Reader::new(header_reader);
        let mut vcf_header_reader = noodles::vcf::io::Reader::new(bgzf_header);
        let header = vcf_header_reader.read_header()?;

        let sample_names: Vec<String> = header
            .sample_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        info!("Found {} samples in VCF header", sample_names.len());

        // Resolve chrom to reference sequence ID in the index
        let index_header = index.header().ok_or_else(|| {
            anyhow::anyhow!("tabix index has no header")
        })?;
        let ref_seq_id = index_header
            .reference_sequence_names()
            .iter()
            .position(|name| {
                let bytes: &[u8] = name.as_ref();
                bytes == chrom.as_bytes()
            })
            .ok_or_else(|| anyhow::anyhow!("chrom {} not found in tabix index", chrom))?;

        // Build the interval (noodles positions are 1-based)
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

        // Stream records via a background thread to avoid the borrow issue.
        // The thread owns the BGZF reader + Query and sends lines through a channel.
        let vcf_path_owned = vcf_path.to_string();
        let chrom_owned = chrom.to_string();
        let (tx, rx) = mpsc::sync_channel::<String>(1024);

        std::thread::spawn(move || {
            let reader = match get_reader(&vcf_path_owned) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut bgzf_data = bgzf::Reader::new(reader);
            let query = noodles::csi::io::Query::new(&mut bgzf_data, chunks);
            let buf_query = BufReader::new(query);

            for line_result in buf_query.lines() {
                let line = match line_result {
                    Ok(l) => l,
                    Err(_) => continue,
                };

                // Post-filter by chrom and position (tabix is block-level)
                if let Some(pos_end) = line.find('\t') {
                    let line_chrom = &line[..pos_end];
                    if line_chrom != chrom_owned {
                        continue;
                    }
                    if let Some(next_tab) = line[pos_end + 1..].find('\t') {
                        let pos_str = &line[pos_end + 1..pos_end + 1 + next_tab];
                        if let Ok(pos) = pos_str.parse::<u32>() {
                            if pos < start || pos > stop {
                                continue;
                            }
                        }
                    }
                }

                if tx.send(line).is_err() {
                    break; // receiver dropped
                }
            }
        });

        Ok(VcfStream {
            sample_names,
            lines: Box::new(rx.into_iter()),
        })
    }

    /// Iterate over data lines.
    pub fn records(self) -> impl Iterator<Item = String> + Send {
        self.lines
    }
}

/// Try to load a tabix index from a path (local or GCS).
fn load_tabix_index(tbi_path: &str) -> Option<tabix::Index> {
    let reader = get_reader(tbi_path).ok()?;
    let mut tbi_reader = tabix::io::Reader::new(reader);
    tbi_reader.read_index().ok()
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
            if *v == "." { return String::new(); }
            v.split(',').next().unwrap_or("").to_string()
        })
        .unwrap_or_default()
}

/// Get an info string value, returning empty string for flags or missing.
pub fn info_str(info: &HashMap<&str, Option<&str>>, key: &str) -> String {
    match info.get(key) {
        Some(Some(v)) if *v != "." => v.to_string(),
        _ => String::new(),
    }
}

/// Check if an INFO flag is present.
pub fn info_flag(info: &HashMap<&str, Option<&str>>, key: &str) -> bool {
    info.contains_key(key)
}

/// Extract the first element of a Number=A field as f32.
pub fn info_first_float(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<f32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| {
            if *v == "." { return None; }
            v.split(',').next().and_then(|s| s.parse().ok())
        })
}

/// Extract the first element of a Number=A field as u32.
pub fn info_first_u32(info: &HashMap<&str, Option<&str>>, key: &str) -> Option<u32> {
    info.get(key)
        .and_then(|v| v.as_ref())
        .and_then(|v| {
            if *v == "." { return None; }
            v.split(',').next().and_then(|s| s.parse().ok())
        })
}
