//! Simplified BED stream reader using tabix for region seeking.
//!
//! Unlike VcfStream, BED files have no column header — just optional `##` comment
//! lines followed by tab-delimited data. This reader skips comments and yields
//! raw lines for the requested region.

use genohype_core::io::get_reader;
use noodles::bgzf;
use noodles::csi::BinningIndex;
use noodles::tabix;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use tracing::info;

/// A lightweight BED line reader that streams from GCS via tabix index.
pub struct BedStream {
    lines: Box<dyn Iterator<Item = String> + Send>,
}

impl BedStream {
    /// Open a tabix-indexed BED file and stream only records in the given region.
    ///
    /// The `.tbi` index is expected at `{bed_path}.tbi`.
    /// Lines starting with `#` are skipped (metadata comments).
    pub fn open_region(
        bed_path: &str,
        chrom: &str,
        start: u32,
        stop: u32,
    ) -> anyhow::Result<Self> {
        // Load the tabix index
        let tbi_path = format!("{}.tbi", bed_path);
        let index = load_tabix_index(&tbi_path)
            .ok_or_else(|| anyhow::anyhow!("No tabix index found at {}", tbi_path))?;

        info!("BED: using tabix index for {}:{}-{}", chrom, start, stop);

        // Resolve chrom to reference sequence ID in the index
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
            .ok_or_else(|| anyhow::anyhow!("chrom {} not found in tabix index", chrom))?;

        // Build the interval (noodles positions are 1-based)
        let interval_start = noodles::core::Position::try_from(start.max(1) as usize)?;
        let interval_end = noodles::core::Position::try_from(stop as usize)?;
        let interval = noodles::core::region::Interval::from(interval_start..=interval_end);

        let chunks = index.query(ref_seq_id, interval)?;

        if chunks.is_empty() {
            info!("BED: no indexed chunks for region {}:{}-{}", chrom, start, stop);
            return Ok(BedStream {
                lines: Box::new(std::iter::empty()),
            });
        }

        info!("BED: found {} chunks for region", chunks.len());

        // Stream records via a background thread (same pattern as VcfStream)
        let bed_path_owned = bed_path.to_string();
        let chrom_owned = chrom.to_string();
        let (tx, rx) = mpsc::sync_channel::<String>(1024);

        std::thread::spawn(move || {
            let reader = match get_reader(&bed_path_owned) {
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

                // Skip comment/metadata lines
                if line.starts_with('#') {
                    continue;
                }

                // Post-filter by chrom and position (tabix is block-level)
                if let Some(first_tab) = line.find('\t') {
                    let line_chrom = &line[..first_tab];
                    if line_chrom != chrom_owned {
                        continue;
                    }
                    // BED pos1 is column 1 (0-based coordinate)
                    if let Some(second_tab) = line[first_tab + 1..].find('\t') {
                        let pos_str = &line[first_tab + 1..first_tab + 1 + second_tab];
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

        Ok(BedStream {
            lines: Box::new(rx.into_iter()),
        })
    }

    /// Iterate over data lines.
    pub fn lines(self) -> impl Iterator<Item = String> + Send {
        self.lines
    }
}

/// Try to load a tabix index from a path (local or GCS).
fn load_tabix_index(tbi_path: &str) -> Option<tabix::Index> {
    let reader = get_reader(tbi_path).ok()?;
    let mut tbi_reader = tabix::io::Reader::new(reader);
    tbi_reader.read_index().ok()
}
