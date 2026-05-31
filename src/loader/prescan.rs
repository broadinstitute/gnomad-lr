//! Pre-scan pass to build the enveloped variant map.
//!
//! Ports `build_enveloped_map` from the Python loader (lines 263-310).
//! For each VCF line with `TR_ENVELOPED` flag, records which TR envelops it.
//! Returns: HashMap<trid, Vec<variant_id>> (enveloping TR → enveloped variants).

use std::collections::HashMap;
use super::vcf_reader::{VcfStream, parse_info_field};
use tracing::info;

/// Build the enveloped map: TRID → list of enveloped variant IDs.
pub fn build_enveloped_map(
    vcf_path: &str,
    region_chrom: &str,
    region_start: u32,
    region_stop: u32,
) -> anyhow::Result<HashMap<String, Vec<String>>> {
    info!("Pre-pass: building enveloped_ids map...");

    let mut enveloped_map: HashMap<String, Vec<String>> = HashMap::new();

    let stream = VcfStream::open_region(vcf_path, region_chrom, region_start, region_stop)?;
    for line in stream.records() {
        // Quick filter before parsing
        if !line.contains("TR_ENVELOPED") {
            continue;
        }

        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 8 {
            continue;
        }

        let chrom_field = parts[0];
        if chrom_field != region_chrom {
            continue;
        }

        let pos: u32 = match parts[1].parse() {
            Ok(p) => p,
            Err(_) => continue,
        };
        if pos < region_start || pos > region_stop {
            continue;
        }

        let info = parse_info_field(parts[7]);
        if !info.contains_key("TR_ENVELOPED") {
            continue;
        }

        // variant_id from VCF ID column, strip "chr"
        let variant_id = parts[2].replacen("chr", "", 1);

        // TRID is the ID of the enveloping TR
        let trid = match info.get("TRID") {
            Some(Some(v)) if !v.is_empty() => v.replacen("chr", "", 1),
            _ => continue,
        };

        enveloped_map.entry(trid).or_default().push(variant_id);
    }

    let total_enveloped: usize = enveloped_map.values().map(|v| v.len()).sum();
    info!(
        "Pre-pass complete: {} enveloped variants across {} TRs",
        total_enveloped,
        enveloped_map.len()
    );

    Ok(enveloped_map)
}
