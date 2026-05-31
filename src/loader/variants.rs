//! Variant loader: ports load_variants_vcf from the Python loader (lines 490-631).
//!
//! Extracts site-level variant information from VCF records and loads into
//! lr_variants ClickHouse table.

use crate::clickhouse::ClickHouseInserter;
use crate::domain::{self, consequence_rank, OMIT_CONSEQUENCE_TERMS};
use crate::loader::prescan::build_enveloped_map;
use crate::loader::vcf_reader::*;
use crate::models::VariantRow;
use std::collections::HashMap;
use tracing::info;

const BATCH_SIZE: usize = 50000;

pub fn load_variants(
    ch_url: &str,
    vcf_path: &str,
    region_chrom: &str,
    region_start: u32,
    region_stop: u32,
) -> anyhow::Result<()> {
    info!("Loading variants from VCF (site-level)...");
    info!("VCF: {}", vcf_path);
    info!("Region: {}:{}-{}", region_chrom, region_start, region_stop);

    // Pre-pass to build enveloped map
    let enveloped_map = build_enveloped_map(vcf_path, region_chrom, region_start, region_stop)?;

    // Main pass
    let stream = VcfStream::open_region(vcf_path, region_chrom, region_start, region_stop)?;
    let mut inserter = ClickHouseInserter::new(ch_url, "lr_variants", BATCH_SIZE);
    let chrom_num = domain::compute_chrom_number(region_chrom);
    let mut count: u64 = 0;

    for line in stream.records() {
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

        let ref_allele = parts[3];
        let alt = parts[4].split(',').next().unwrap_or("");
        let filter_field = parts[6];
        let info_str_raw = parts[7];

        // variant_id from VCF ID column, strip "chr"
        let variant_id = parts[2].replacen("chr", "", 1);

        let info = parse_info_field(info_str_raw);

        let allele_type = info_str_val(&info, "allele_type");
        let filters: Vec<String> = if filter_field == "." || filter_field == "PASS" {
            vec![]
        } else {
            filter_field.split(';').map(|s| s.to_string()).collect()
        };

        let xpos = chrom_num as f64 * 1_000_000_000.0 + pos as f64;

        // VEP parsing
        let (transcript_consequences, genes, intergenic, major_consequence) =
            parse_vep_entries(&info);

        // Frequencies
        let freq_json = build_freq_json(&info);

        // Short read matches (Number=. type, take first)
        let short_read_match_id = info_first(&info, "gnomAD_V4_match_ID");
        let short_read_match_type = info_first(&info, "gnomAD_V4_match_type");
        let short_read_match_source = info_first(&info, "gnomAD_V4_match_source");

        // TR fields
        let is_likely_tr: u8 = if info_flag(&info, "TR_PARSED") { 1 } else { 0 };

        let enveloping_tr_id = if info_flag(&info, "TR_ENVELOPED") {
            info_str_val(&info, "TRID").replacen("chr", "", 1)
        } else {
            String::new()
        };

        let enveloped_ids = if allele_type == "trv" {
            enveloped_map
                .get(&variant_id)
                .cloned()
                .unwrap_or_default()
        } else {
            vec![]
        };

        let motifs_raw = info_str_val(&info, "MOTIFS");
        let motifs: Vec<String> = if motifs_raw.is_empty() {
            vec![]
        } else {
            motifs_raw.split(',').map(|s| s.to_string()).collect()
        };

        let gene_region = info_str_val(&info, "REGION");
        let gnomad_str = info_str_val(&info, "gnomAD_STR");

        // main_reference_region: only for trv
        let main_reference_region_json = if allele_type == "trv" {
            let chrom_stripped = chrom_field.strip_prefix("chr").unwrap_or(chrom_field);
            serde_json::json!({
                "reference_genome": "GRCh38",
                "chrom": chrom_stripped,
                "start": pos,
                "stop": pos as u64 + ref_allele.len() as u64,
            })
            .to_string()
        } else {
            String::new()
        };

        let rsids: Vec<String> = if parts[2] != "." {
            vec![parts[2].to_string()]
        } else {
            vec![]
        };

        let cadd_phred = info_float(&info, "cadd_phred")
            .or_else(|| info_float(&info, "CADD_PHRED_score"));
        let phylop = info_float(&info, "phylop");

        // Extract info_AF for the denormalized column
        let info_af = info_first_float(&info, "AF").unwrap_or(0.0);

        let row = VariantRow {
            chrom: chrom_field.to_string(),
            position: pos,
            ref_allele: ref_allele.to_string(),
            alt: alt.to_string(),
            variant_id,
            xpos,
            rsids,
            allele_type,
            filters,
            intergenic,
            gene_region,
            major_consequence,
            end: info_int(&info, "END").map(|v| v as u32),
            length: info_int(&info, "SVLEN"),
            cadd_phred,
            phylop,
            short_read_match_id,
            short_read_match_type,
            short_read_match_source,
            enveloping_tr_id,
            enveloped_ids,
            motifs,
            is_likely_tr,
            gnomad_str,
            info_af,
            freq_json,
            transcript_consequences_json: serde_json::to_string(&transcript_consequences)?,
            genes_json: serde_json::to_string(&genes)?,
            main_reference_region_json,
        };

        inserter.insert(&row)?;
        count += 1;
    }

    inserter.finish()?;
    info!("Variant loading complete: {} variant docs inserted", count);
    Ok(())
}

/// Get a string value from the info map.
fn info_str_val(info: &HashMap<String, Option<String>>, key: &str) -> String {
    match info.get(key) {
        Some(Some(v)) if v != "." => v.clone(),
        _ => String::new(),
    }
}

/// Parse VEP from INFO field.
/// Returns (transcript_consequences, genes, intergenic, major_consequence).
fn parse_vep_entries(
    info: &HashMap<String, Option<String>>,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>, u8, String) {
    let vep_str = match info.get("vep") {
        Some(Some(v)) if !v.is_empty() => v.clone(),
        _ => return (vec![], vec![], 0, String::new()),
    };

    let entries: Vec<&str> = vep_str.split(',').collect();
    let mut genes_set: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    let mut intergenic: u8 = 0;
    let mut transcript_consequences: Vec<serde_json::Value> = Vec::new();

    for entry_str in &entries {
        let fields: Vec<&str> = entry_str.split('|').collect();

        // Genes: collect from all entries with symbol and ensembl_id
        if fields.len() > 4 {
            let symbol = fields[3];
            let ensembl_id = fields[4];
            if !symbol.is_empty() && ensembl_id.starts_with("ENSG") {
                genes_set.insert((symbol.to_string(), ensembl_id.to_string()));
            }
        }

        // Intergenic: feature_type is empty
        if fields.len() > 5 && fields[5].is_empty() {
            intergenic = 1;
        }

        // Only PICK=1 entries (index 22) for transcript consequences
        if fields.len() <= 22 || fields[22] != "1" {
            continue;
        }
        // Only Transcript feature_type (index 5)
        if fields.len() <= 5 || fields[5] != "Transcript" {
            continue;
        }

        let consequence_terms: Vec<String> = fields[1]
            .split('&')
            .filter(|t| !t.is_empty() && !OMIT_CONSEQUENCE_TERMS.contains(t))
            .map(|t| t.to_string())
            .collect();
        if consequence_terms.is_empty() {
            continue;
        }

        let hgvsc = if fields.len() > 10 && !fields[10].is_empty() {
            fields[10].split(':').last().unwrap_or("").to_string()
        } else {
            String::new()
        };
        let hgvsp = if fields.len() > 11 && !fields[11].is_empty() {
            fields[11].split(':').last().unwrap_or("").to_string()
        } else {
            String::new()
        };

        let mc = consequence_terms
            .iter()
            .min_by_key(|t| consequence_rank(t))
            .cloned()
            .unwrap_or_default();

        let mut tc = serde_json::json!({
            "consequence_terms": consequence_terms,
            "gene_symbol": if fields.len() > 3 { fields[3] } else { "" },
            "gene_id": if fields.len() > 4 { fields[4] } else { "" },
            "transcript_id": if fields.len() > 6 { fields[6] } else { "" },
            "is_canonical": fields.len() > 27 && fields[27] == "YES",
            "major_consequence": mc,
        });

        let obj = tc.as_object_mut().unwrap();
        if !hgvsc.is_empty() {
            obj.insert("hgvsc".to_string(), serde_json::Value::String(hgvsc));
        }
        if !hgvsp.is_empty() {
            obj.insert("hgvsp".to_string(), serde_json::Value::String(hgvsp));
        }
        if fields.len() > 45 && !fields[45].is_empty() {
            obj.insert(
                "sift_prediction".to_string(),
                serde_json::Value::String(fields[45].to_string()),
            );
        }
        if fields.len() > 46 && !fields[46].is_empty() {
            obj.insert(
                "polyphen_prediction".to_string(),
                serde_json::Value::String(fields[46].to_string()),
            );
        }
        if fields.len() > 47 && !fields[47].is_empty() {
            obj.insert(
                "domains".to_string(),
                serde_json::json!([fields[47]]),
            );
        }

        transcript_consequences.push(tc);
    }

    // Compute top-level major_consequence
    let major_consequence = if !transcript_consequences.is_empty() {
        let empty_vec = vec![];
        let all_terms: Vec<&str> = transcript_consequences
            .iter()
            .flat_map(|tc| {
                tc["consequence_terms"]
                    .as_array()
                    .unwrap_or(&empty_vec)
                    .iter()
                    .filter_map(|v| v.as_str())
            })
            .collect();
        all_terms
            .iter()
            .min_by_key(|t| consequence_rank(t))
            .map(|s| s.to_string())
            .unwrap_or_default()
    } else {
        String::new()
    };

    let mut genes_sorted: Vec<(String, String)> = genes_set.into_iter().collect();
    genes_sorted.sort();
    let genes: Vec<serde_json::Value> = genes_sorted
        .into_iter()
        .map(|(symbol, ensembl_id)| {
            serde_json::json!({
                "symbol": symbol,
                "ensembl_id": ensembl_id,
            })
        })
        .collect();

    (transcript_consequences, genes, intergenic, major_consequence)
}

/// Build the nested frequency JSON matching LongReadVariantFrequencies.
fn build_freq_json(info: &HashMap<String, Option<String>>) -> String {
    let populations_list = ["afr", "amr", "asj", "eas", "nfe", "sas"];
    let sexes = ["XX", "XY"];

    // Build divisions list
    let mut divisions: Vec<String> = Vec::new();
    for pop in &populations_list {
        divisions.push(pop.to_string());
        for sex in &sexes {
            divisions.push(format!("{}_{}", pop, sex));
        }
    }
    divisions.push("XX".to_string());
    divisions.push("XY".to_string());

    let safe_float = |key: &str| -> f64 {
        info.get(key)
            .and_then(|v| v.as_ref())
            .and_then(|v| {
                if v == "." { return None; }
                v.split(',').next().and_then(|s| s.parse().ok())
            })
            .unwrap_or(0.0)
    };

    let safe_int = |key: &str| -> i64 {
        info.get(key)
            .and_then(|v| v.as_ref())
            .and_then(|v| {
                if v == "." { return None; }
                v.split(',').next().and_then(|s| s.parse().ok())
            })
            .unwrap_or(0)
    };

    let all_freq = serde_json::json!({
        "ac": safe_int("AC"),
        "an": safe_int("AN"),
        "af": safe_float("AF"),
        "homozygote_ref_count": safe_int("nhomref"),
        "homozygote_alt_count": safe_int("nhomalt"),
        "heterozygote_count": safe_int("nhet"),
        "homozygote_ref_freq": safe_float("freq_homref"),
        "homozygote_alt_freq": safe_float("freq_homalt"),
        "heterozygote_freq": safe_float("freq_het"),
    });

    let mut populations: Vec<serde_json::Value> = Vec::new();
    for division in &divisions {
        let suffix = format!("_{}", division);
        let pop = serde_json::json!({
            "id": division,
            "ac": safe_int(&format!("AC{}", suffix)),
            "an": safe_int(&format!("AN{}", suffix)),
            "af": safe_float(&format!("AF{}", suffix)),
            "homozygote_ref_count": safe_int(&format!("nhomref{}", suffix)),
            "homozygote_alt_count": safe_int(&format!("nhomalt{}", suffix)),
            "heterozygote_count": safe_int(&format!("nhet{}", suffix)),
            "homozygote_ref_freq": safe_float(&format!("freq_homref{}", suffix)),
            "homozygote_alt_freq": safe_float(&format!("freq_homalt{}", suffix)),
            "heterozygote_freq": safe_float(&format!("freq_het{}", suffix)),
        });
        populations.push(pop);
    }

    serde_json::json!({
        "all": all_freq,
        "populations": populations,
    })
    .to_string()
}
