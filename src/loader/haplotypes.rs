//! Haplotype loader: ports the core haplotype unpacking from load_haplotypes_vcf
//! (Python lines 692-970).
//!
//! For each VCF variant record in the region:
//! - Parse INFO fields (AF, AC, AN, population AFs, cadd, phylop, etc.)
//! - For each of ~292 samples, parse FORMAT (GT, GQ, DP, RNC, AM, AP, MC)
//! - Split diploid GT into 2 haplotype rows (strand 1 and 2)
//! - Yield HaplotypeRow structs → batch insert via ClickHouseInserter

use crate::clickhouse::ClickHouseInserter;
use crate::loader::vcf_reader::*;
use crate::models::HaplotypeRow;
use std::collections::HashMap;
use tracing::info;

const BATCH_SIZE: usize = 50000;

pub fn load_haplotypes(
    ch_url: &str,
    vcf_path: &str,
    region_chrom: &str,
    region_start: u32,
    region_stop: u32,
) -> anyhow::Result<()> {
    info!("Loading haplotypes from VCF...");
    info!("VCF: {}", vcf_path);
    info!("Region: {}:{}-{}", region_chrom, region_start, region_stop);

    let stream = VcfStream::open_region(vcf_path, region_chrom, region_start, region_stop)?;
    let sample_names = stream.sample_names.clone();
    let mut inserter = ClickHouseInserter::new(ch_url, "lr_haplotypes", BATCH_SIZE);
    let mut variants_seen: u64 = 0;

    for line in stream.records() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 10 {
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

        variants_seen += 1;
        let ref_allele = parts[3];
        let alt_field = parts[4];
        let alt_list: Vec<&str> = alt_field.split(',').collect();
        let qual_str = parts[5];
        let filter_field = parts[6];
        let info_str = parts[7];
        let format_field = parts[8];

        let qual: f32 = if qual_str == "." { 0.0 } else { qual_str.parse().unwrap_or(0.0) };
        let filters: Vec<String> = if filter_field == "." || filter_field == "PASS" {
            vec![]
        } else {
            filter_field.split(';').map(|s| s.to_string()).collect()
        };

        let info = parse_info_field(info_str);

        let af = info_first_float(&info, "AF").unwrap_or(0.0);
        let ac = info_first_u32(&info, "AC").unwrap_or(0);
        let an = info_u32(&info, "AN").unwrap_or(0);
        let allele_type = info_str_val(&info, "allele_type");
        let gnomad_v4_match_type = info_str_val(&info, "gnomAD_V4_match_type");

        let af_afr = info_first_float(&info, "AF_afr");
        let af_amr = info_first_float(&info, "AF_amr");
        let af_eas = info_first_float(&info, "AF_eas");
        let af_nfe = info_first_float(&info, "AF_nfe");
        let af_sas = info_first_float(&info, "AF_sas");

        let cadd_phred = info_float(&info, "cadd_phred")
            .or_else(|| info_float(&info, "CADD_PHRED_score"));
        let phylop = info_float(&info, "phylop");

        let dbsnp_id = info_str_val(&info, "dbSNP_ID");
        let tr_id = info_str_val(&info, "TRID");
        let tr_motifs = info_str_val(&info, "MOTIFS");
        let tr_struc = info_str_val(&info, "STRUC");

        // SV consequence predictions
        let mut sv_consequences: Vec<String> = Vec::new();
        for (key, val) in &info {
            if key.starts_with("PREDICTED_") {
                let consequence_type = &key["PREDICTED_".len()..];
                let gene_name = val.as_deref().unwrap_or("");
                if gene_name.is_empty() {
                    sv_consequences.push(consequence_type.to_string());
                } else {
                    sv_consequences.push(format!("{}:{}", consequence_type, gene_name));
                }
            }
        }

        let rsid = if parts[2] != "." { parts[2].to_string() } else { String::new() };

        // Parse FORMAT fields
        let format_keys: Vec<&str> = format_field.split(':').collect();

        for (i, sample_data) in parts[9..].iter().enumerate() {
            if *sample_data == "." || sample_data.starts_with("./.") {
                continue;
            }

            let fmt_values: Vec<&str> = sample_data.split(':').collect();
            let fmt: HashMap<&str, &str> = format_keys
                .iter()
                .zip(fmt_values.iter())
                .map(|(&k, &v)| (k, v))
                .collect();

            let gt = *fmt.get("GT").unwrap_or(&"./.");
            if gt == "./." || gt == "0|0" || gt == "0/0" {
                continue;
            }

            // Check RNC
            let rnc = *fmt.get("RNC").unwrap_or(&"..");
            if rnc != ".." {
                let rnc_bytes = rnc.as_bytes();
                if rnc_bytes.len() >= 2 && rnc_bytes[0] != b'.' && rnc_bytes[1] != b'.' {
                    continue;
                }
            }

            let (gt_parts_str, gt_phased) = if gt.contains('|') {
                (gt.split('|').collect::<Vec<&str>>(), true)
            } else {
                (gt.split('/').collect::<Vec<&str>>(), false)
            };

            let gt_alleles: Vec<u8> = gt_parts_str
                .iter()
                .filter(|a| **a != ".")
                .filter_map(|a| a.parse().ok())
                .collect();

            // Determine which strands carry an alt allele
            let mut strands_with_alleles: Vec<(u8, usize, usize)> = Vec::new(); // (strand, gt_idx, hap_idx)
            if gt_parts_str.len() >= 1 && gt_parts_str[0] != "." && gt_parts_str[0] != "0" {
                if let Ok(gt_idx) = gt_parts_str[0].parse::<usize>() {
                    strands_with_alleles.push((1, gt_idx, 0));
                }
            }
            if gt_parts_str.len() >= 2 && gt_parts_str[1] != "." && gt_parts_str[1] != "0" {
                if let Ok(gt_idx) = gt_parts_str[1].parse::<usize>() {
                    strands_with_alleles.push((2, gt_idx, 1));
                }
            }
            if strands_with_alleles.is_empty() {
                continue;
            }

            let sample_id = &sample_names[i];
            let dp: Option<u16> = fmt.get("DP").and_then(|v| v.parse().ok());
            let gq: Option<u16> = fmt.get("GQ").and_then(|v| v.parse().ok());

            for (strand, gt_idx, hap_idx) in &strands_with_alleles {
                if *gt_idx < 1 || *gt_idx - 1 >= alt_list.len() {
                    continue;
                }
                let specific_alt = alt_list[gt_idx - 1];
                let computed_length = specific_alt.len() as i32 - ref_allele.len() as i32;

                // Per-haplotype FORMAT values
                let allele_methylation = get_hap_float(&fmt, "AM", *hap_idx);
                let allele_purity = get_hap_float(&fmt, "AP", *hap_idx);
                let motif_counts = get_motif_counts(&fmt, *hap_idx);

                let row = HaplotypeRow {
                    chrom: chrom_field.to_string(),
                    position: pos,
                    sample_id: sample_id.clone(),
                    strand: *strand,
                    ref_allele: ref_allele.to_string(),
                    alt: specific_alt.to_string(),
                    rsid: rsid.clone(),
                    qual,
                    filters: filters.clone(),
                    info_af: af,
                    info_ac: ac,
                    info_an: an,
                    allele_type: allele_type.clone(),
                    allele_length: computed_length,
                    gnomad_v4_match_type: gnomad_v4_match_type.clone(),
                    info_af_afr: af_afr,
                    info_af_amr: af_amr,
                    info_af_eas: af_eas,
                    info_af_nfe: af_nfe,
                    info_af_sas: af_sas,
                    gt_alleles: gt_alleles.clone(),
                    gt_phased: if gt_phased { 1 } else { 0 },
                    depth: dp,
                    genotype_quality: gq,
                    cadd_phred,
                    phylop,
                    sv_consequences: sv_consequences.clone(),
                    dbsnp_id: dbsnp_id.clone(),
                    tr_id: tr_id.clone(),
                    tr_motifs: tr_motifs.clone(),
                    tr_struc: tr_struc.clone(),
                    allele_methylation,
                    motif_counts,
                    allele_purity,
                };

                // Use to_json() to rename ref_allele → ref
                inserter.insert_raw(row.to_json().to_string())?;
            }
        }
    }

    inserter.finish()?;
    info!(
        "Haplotype loading complete: {} rows from {} variant sites",
        inserter.total_rows(),
        variants_seen
    );
    Ok(())
}

/// Get a string value from the info map, returning empty string for flags/missing.
fn info_str_val(info: &HashMap<String, Option<String>>, key: &str) -> String {
    match info.get(key) {
        Some(Some(v)) if v != "." => v.clone(),
        _ => String::new(),
    }
}

/// Extract a per-haplotype float from a comma-separated FORMAT field.
fn get_hap_float(fmt: &HashMap<&str, &str>, key: &str, hap_idx: usize) -> Option<f32> {
    let val = fmt.get(key)?;
    if *val == "." {
        return None;
    }
    let parts: Vec<&str> = val.split(',').collect();
    parts.get(hap_idx).and_then(|v| {
        if *v == "." || v.is_empty() {
            None
        } else {
            v.parse().ok()
        }
    })
}

/// Extract motif counts from MC FORMAT field for a specific haplotype.
fn get_motif_counts(fmt: &HashMap<&str, &str>, hap_idx: usize) -> Vec<u16> {
    let mc_val = match fmt.get("MC") {
        Some(v) if *v != "." => *v,
        _ => return vec![],
    };
    let parts: Vec<&str> = mc_val.split(',').collect();
    let mc_str = match parts.get(hap_idx) {
        Some(v) if *v != "." && !v.is_empty() => *v,
        _ => return vec![],
    };
    mc_str
        .split('_')
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect()
}
