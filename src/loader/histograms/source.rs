//! Updated 19-core source capture, deliberately NOT the legacy serving model.
//! Lexical/arithmetic validation is not producer attestation or biological ploidy.
use super::{add_bins, integer, missing, parse_bins, Bins, Header, KINDS, SUMMARIES};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod ingest;

pub const CONTRACT: &str = "updated_histogram_source_v1";
pub const TABLE: &str = "lr_str_source_histograms_v1";
pub const RECEIPTS_TABLE: &str = "lr_str_source_histogram_receipts_v1";

/// Every source string, including empty and dot null spellings, survives in
/// source_fields. source_header retains order. Typed fields are query helpers,
/// not normalized/replaced source values. Coordinates are source-native.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceHistogramRow {
    pub contract: String,
    pub cohort: String,
    pub run_id: String,
    pub task_id: String,
    pub source_uri: String,
    pub source_generation: String,
    pub source_size_bytes: u64,
    pub source_md5_base64: String,
    pub row_ordinal: u64,
    pub locus_id: String,
    pub motif: String,
    pub chrom: String,
    pub locus_start: u32,
    pub locus_end: u32,
    pub source_interval: String,
    pub context_chrom: String,
    pub context_start: u32,
    pub context_end: u32,
    pub source_vc: Option<String>,
    pub num_called_alleles: u32,
    pub unique_allele_lengths: u32,
    pub source_header: Vec<String>,
    pub source_fields: BTreeMap<String, String>,
}

fn chromosome(chrom: &str) -> Result<()> {
    ensure!(
        matches!(chrom, "X" | "Y" | "M" | "MT")
            || chrom
                .parse::<u8>()
                .is_ok_and(|n| (1..=22).contains(&n) && n.to_string() == chrom),
        "unsupported chromosome"
    );
    Ok(())
}

fn interval(text: &str) -> Result<(&str, u32, u32)> {
    let (chrom, span) = text
        .split_once(':')
        .context("Interval: expected chrom:start-end")?;
    chromosome(chrom)?;
    let (start, end) = span
        .split_once('-')
        .context("Interval: expected start-end")?;
    let (start, end) = (
        integer(start, "interval start")?,
        integer(end, "interval end")?,
    );
    ensure!(start < end, "interval start must precede end");
    Ok((chrom, start, end))
}

/// Necessary lower bound only: each diagonal may represent one OR two copies.
/// Off-diagonals consume one observation at each endpoint. Remaining alleles
/// may be unpaired; this does not infer ploidy, phase, or biological genotypes.
fn validate_source_pairs(alleles: &Bins, pairs: &Bins) -> Result<()> {
    let mut lower = Bins::new();
    for ((a, b), n) in pairs {
        for value in std::iter::once(a).chain((a != b).then_some(b)) {
            let total = lower.entry((*value, 0)).or_default();
            *total = total
                .checked_add(*n)
                .context("source pair count overflow")?;
        }
    }
    for (key, n) in lower {
        ensure!(
            n <= *alleles.get(&key).unwrap_or(&0),
            "source pair lower bound exceeds allele observations"
        );
    }
    Ok(())
}

/// Preserve summary strings rather than casting into legacy Float32 storage.
/// Only syntax/finite nonnegative representability is asserted, not a particular
/// quantile/Hemi/Short definition or recomputation from an unbound producer.
fn source_summary(text: &str) -> Result<Option<f64>> {
    if missing(text) {
        return Ok(None);
    }
    ensure!(
        text.bytes()
            .all(|b| b.is_ascii_digit() || b".+-eE".contains(&b)),
        "invalid summary syntax"
    );
    let n: f64 = text.parse().context("invalid summary number")?;
    ensure!(
        n.is_finite() && n >= 0.0,
        "summary must be finite and nonnegative"
    );
    let nonzero = text
        .split(['e', 'E'])
        .next()
        .unwrap_or("")
        .bytes()
        .any(|b| matches!(b, b'1'..=b'9'));
    ensure!(n != 0.0 || !nonzero, "summary underflow");
    Ok(Some(n))
}

pub(super) struct SourceHeader {
    parsed: Header,
    names: Vec<String>,
}

impl SourceHeader {
    pub(super) fn parse(parts: &[&str]) -> Result<Self> {
        ensure!(
            parts.len() <= 512 && parts.iter().map(|s| s.len()).sum::<usize>() <= 128 * 1024,
            "source header resource limit exceeded"
        );
        let parsed = Header::parse(parts)?;
        ensure!(
            parsed.updated,
            "source capture requires updated 19-core/__ contract"
        );
        Ok(Self {
            parsed,
            names: parts.iter().map(|s| s.to_string()).collect(),
        })
    }

    pub(super) fn row(
        &self,
        parts: &[&str],
        task: &ingest::SourceTask,
        ordinal: u64,
    ) -> Result<SourceHistogramRow> {
        let header = &self.parsed;
        ensure!(parts.len() == self.names.len(), "source row width mismatch");
        let get = |name| header.get(parts, name);
        let locus: Vec<_> = get("LocusId").split('-').collect();
        ensure!(locus.len() == 4, "expected one resolved named LocusId");
        chromosome(locus[0])?;
        let (start, end) = (
            integer(locus[1], "locus start")?,
            integer(locus[2], "locus end")?,
        );
        ensure!(start < end, "locus start must precede end");
        let motif = get("Motif");
        ensure!(
            !motif.is_empty() && motif.bytes().all(|b| b"ACGTN".contains(&b)) && motif == locus[3],
            "invalid or mismatched literal motif"
        );
        let (context_chrom, context_start, context_end) = interval(get("Interval"))?;
        ensure!(
            context_chrom == locus[0] && context_start <= start && end <= context_end,
            "source interval must contain the named locus (no coordinate conversion)"
        );
        let vc = get("VC");
        if !missing(vc) {
            interval(vc)?;
        }

        let aggregate = [
            parse_bins(get(KINDS[0]), 0, KINDS[0])?,
            parse_bins(get(KINDS[1]), 1, KINDS[1])?,
        ];
        let called = integer(get("NumCalledAlleles"), "NumCalledAlleles")?;
        let unique = integer(get("UniqueAlleleLengths"), "UniqueAlleleLengths")?;
        let total = aggregate[0].values().try_fold(0u64, |a, b| {
            a.checked_add(*b).context("allele count overflow")
        })?;
        ensure!(
            u64::from(called) == total,
            "called count differs from allele sum"
        );
        ensure!(
            unique as usize == aggregate[0].len(),
            "unique count differs from allele bins"
        );
        validate_source_pairs(&aggregate[0], &aggregate[1])?;
        let parsed = header
            .distributions
            .iter()
            .map(|c| Ok((c, parse_bins(parts[c.index], c.kind, "source stratum")?)))
            .collect::<Result<Vec<_>>>()?;
        for (kind, expected) in aggregate.iter().enumerate() {
            let mut combined = Bins::new();
            for (c, bins) in &parsed {
                if c.kind == kind && c.ancestry.is_some() && c.sex.is_some() {
                    add_bins(&mut combined, bins)?;
                }
            }
            ensure!(
                &combined == expected,
                "joint strata do not sum to aggregate"
            );
        }
        for (c, bins) in &parsed {
            if c.kind == 1 {
                let alleles = parsed
                    .iter()
                    .find(|(a, _)| a.kind == 0 && a.ancestry == c.ancestry && a.sex == c.sex);
                ensure!(
                    bins.is_empty() || alleles.is_some(),
                    "source pair stratum lacks allele counterpart"
                );
                if let Some((_, alleles)) = alleles {
                    validate_source_pairs(alleles, bins)?;
                }
            }
            if c.ancestry.is_some() && c.sex.is_some() {
                continue;
            }
            let mut combined = Bins::new();
            for (child, bins) in &parsed {
                if child.kind == c.kind
                    && child.ancestry.is_some()
                    && child.sex.is_some()
                    && (c.ancestry.is_none() || c.ancestry == child.ancestry)
                    && (c.sex.is_none() || c.sex == child.sex)
                {
                    add_bins(&mut combined, bins)?;
                }
            }
            ensure!(
                &combined == bins,
                "source marginal differs from joint strata"
            );
        }
        for name in SUMMARIES.iter().copied().chain([
            "ShortAllele99thPercentile",
            "ShortAlleleMax",
            "HemiAllele99thPercentile",
            "HemiAlleleMax",
        ]) {
            let value = source_summary(get(name)).with_context(|| format!("invalid {name}"))?;
            if called == 0 {
                ensure!(value.is_none(), "zero-call source row has defined summary");
            }
        }
        Ok(SourceHistogramRow {
            contract: CONTRACT.into(),
            cohort: task.cohort.clone(),
            run_id: task.run_id.clone(),
            task_id: task.task_id.clone(),
            source_uri: task.source_uri.clone(),
            source_generation: task.source_generation.clone(),
            source_size_bytes: task.source_size_bytes,
            source_md5_base64: task.source_md5_base64.clone(),
            row_ordinal: ordinal,
            locus_id: get("LocusId").into(),
            motif: motif.into(),
            chrom: locus[0].into(),
            locus_start: start,
            locus_end: end,
            source_interval: get("Interval").into(),
            context_chrom: context_chrom.into(),
            context_start,
            context_end,
            source_vc: (!missing(vc)).then(|| vc.into()),
            num_called_alleles: called,
            unique_allele_lengths: unique,
            source_header: self.names.clone(),
            source_fields: self
                .names
                .iter()
                .cloned()
                .zip(parts.iter().map(|s| s.to_string()))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests;
