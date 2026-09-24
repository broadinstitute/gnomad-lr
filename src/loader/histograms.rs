//! Fail-closed STR histogram reader. Only disjoint ancestry x sex bins enter
//! `populations`; ancestry-only and sex-only columns are checked, never added.

use crate::clickhouse::ClickHouseInserter;
use crate::loader::RegionFilter;
use crate::models::StrHistogramRow;
use anyhow::{bail, ensure, Context, Result};
use genohype_core::io::get_reader;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};
use tracing::{error, info};

const CORE: &[&str] = &[
    "LocusId",
    "Motif",
    "AlleleSizeHistogram",
    "BiallelicHistogram",
    "Min",
    "Mode",
    "Mean",
    "Stdev",
    "Median",
    "99thPercentile",
    "Max",
    "ShortAllele99thPercentile",
    "ShortAlleleMax",
    "UniqueAlleleLengths",
    "NumCalledAlleles",
];
const NEW_CORE: &[&str] = &[
    "Interval",
    "VC",
    "HemiAllele99thPercentile",
    "HemiAlleleMax",
];
const SUMMARIES: &[&str] = &[
    "Min",
    "Mode",
    "Mean",
    "Stdev",
    "Median",
    "99thPercentile",
    "Max",
];
const KINDS: &[&str] = &["AlleleSizeHistogram", "BiallelicHistogram"];
type Bins = BTreeMap<(u32, u32), u64>;

#[derive(Debug)]
struct DistributionColumn {
    index: usize,
    kind: usize,
    ancestry: Option<String>,
    sex: Option<String>,
}

#[derive(Debug)]
struct Header {
    columns: HashMap<String, usize>,
    distributions: Vec<DistributionColumn>,
    updated: bool,
}

fn is_sex(s: &str) -> bool {
    matches!(s, "female" | "male" | "unknown")
}

fn is_ancestry(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

impl Header {
    fn parse(parts: &[&str]) -> Result<Self> {
        let mut columns = HashMap::new();
        for (i, name) in parts.iter().enumerate() {
            ensure!(!name.is_empty(), "empty header name at column {}", i + 1);
            ensure!(
                columns.insert((*name).to_owned(), i).is_none(),
                "duplicate header {name}"
            );
        }
        for name in CORE {
            ensure!(
                columns.contains_key(*name),
                "missing required header {name}"
            );
        }
        let updated = NEW_CORE.iter().any(|name| columns.contains_key(*name));
        if updated {
            for name in NEW_CORE {
                ensure!(
                    columns.contains_key(*name),
                    "incomplete updated 19-core contract: missing {name}"
                );
            }
        }
        let mut distributions = Vec::new();
        for (index, name) in parts.iter().enumerate() {
            if CORE.contains(name) || (updated && NEW_CORE.contains(name)) {
                continue;
            }
            let mut recognized = None;
            for (kind, prefix) in KINDS.iter().enumerate() {
                if updated {
                    if let Some(suffix) = name.strip_prefix(&format!("{prefix}__")) {
                        let (ancestry, sex) = if let Some((ancestry, sex)) = suffix.rsplit_once('_')
                        {
                            ensure!(
                                is_ancestry(ancestry) && !is_sex(ancestry) && is_sex(sex),
                                "unsupported/ambiguous stratum header {name}"
                            );
                            (Some(ancestry.to_owned()), Some(sex.to_owned()))
                        } else if is_sex(suffix) {
                            (None, Some(suffix.to_owned()))
                        } else {
                            ensure!(is_ancestry(suffix), "unsupported ancestry header {name}");
                            (Some(suffix.to_owned()), None)
                        };
                        recognized = Some(DistributionColumn {
                            index,
                            kind,
                            ancestry,
                            sex,
                        });
                    }
                } else if let Some(suffix) = name.strip_prefix(&format!("{prefix}:")) {
                    let fields: Vec<_> = suffix.split(':').collect();
                    ensure!(
                        fields.len() == 2 && is_ancestry(fields[0]) && is_sex(fields[1]),
                        "unsupported legacy stratum header {name}"
                    );
                    recognized = Some(DistributionColumn {
                        index,
                        kind,
                        ancestry: Some(fields[0].to_owned()),
                        sex: Some(fields[1].to_owned()),
                    });
                }
            }
            distributions.push(recognized.with_context(|| format!("unsupported header {name}; expected legacy 15-core/: strata or updated 19-core/__ strata, not a mixed contract"))?);
        }
        for kind in 0..KINDS.len() {
            ensure!(
                distributions
                    .iter()
                    .any(|d| d.kind == kind && d.ancestry.is_some() && d.sex.is_some()),
                "{} requires disjoint ancestry x sex headers; marginals alone cannot be normalized",
                KINDS[kind]
            );
        }
        Ok(Self {
            columns,
            distributions,
            updated,
        })
    }

    fn get<'a>(&self, parts: &'a [&str], name: &str) -> &'a str {
        parts[self.columns[name]]
    }
}

fn missing(s: &str) -> bool {
    s.is_empty() || s == "."
}

fn integer(s: &str, label: &str) -> Result<u32> {
    ensure!(
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
        "{label}: expected nonnegative integer, got {s:?}"
    );
    s.parse()
        .with_context(|| format!("{label}: integer exceeds UInt32: {s:?}"))
}

fn summary(s: &str, label: &str) -> Result<Option<f32>> {
    if missing(s) {
        return Ok(None);
    }
    let value: f64 = s
        .parse()
        .with_context(|| format!("{label}: invalid number {s:?}"))?;
    ensure!(
        value.is_finite() && value >= 0.0,
        "{label}: expected finite nonnegative number, got {s:?}"
    );
    let stored = value as f32;
    let nonzero_mantissa = s
        .split(['e', 'E'])
        .next()
        .unwrap_or("")
        .bytes()
        .any(|b| matches!(b, b'1'..=b'9'));
    ensure!(
        stored.is_finite() && (stored != 0.0 || !nonzero_mantissa),
        "{label}: value {s:?} overflows/underflows Float32 storage"
    );
    Ok(Some(stored))
}

/// Allow one Float32 relative epsilon for comparisons of stored summaries.
/// This bounds numerical slack at the storage scale, not an assumed decimal
/// precision or rounding convention of the source. Compare in Float64 so the
/// tolerance arithmetic itself does not round back to Float32.
fn summary_at_most(value: f32, upper: f64) -> bool {
    let value = f64::from(value);
    let tolerance = f64::from(f32::EPSILON) * value.abs().max(upper.abs());
    value <= upper + tolerance
}

fn parse_bins(s: &str, kind: usize, label: &str) -> Result<Bins> {
    let mut bins = Bins::new();
    if missing(s) {
        return Ok(bins);
    }
    for raw in s.split(',') {
        let (bin, count) = raw
            .trim()
            .split_once(':')
            .with_context(|| format!("{label}: malformed bin {raw:?}"))?;
        let count = integer(count, label)?;
        ensure!(
            count > 0,
            "{label}: zero-frequency bin {raw:?} is not an observed allele/genotype"
        );
        let key = if kind == 0 {
            let repeats = bin
                .strip_suffix('x')
                .with_context(|| format!("{label}: malformed allele bin {raw:?}"))?;
            (integer(repeats, label)?, 0)
        } else {
            let (short, long) = bin
                .split_once('/')
                .with_context(|| format!("{label}: malformed genotype bin {raw:?}"))?;
            let (short, long) = (integer(short, label)?, integer(long, label)?);
            ensure!(short <= long, "{label}: unordered genotype {raw:?}");
            (short, long)
        };
        ensure!(
            bins.insert(key, u64::from(count)).is_none(),
            "{label}: duplicate bin {bin}"
        );
    }
    Ok(bins)
}

fn add_bins(target: &mut Bins, bins: &Bins) -> Result<()> {
    for (key, count) in bins {
        let total = target.entry(*key).or_default();
        *total = total
            .checked_add(*count)
            .context("histogram count overflow")?;
    }
    Ok(())
}

fn encode_bins(bins: &Bins, kind: usize) -> String {
    bins.iter()
        .map(|((a, b), count)| {
            if kind == 0 {
                format!("{a}x:{count}")
            } else {
                format!("{a}/{b}:{count}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Validate observed pairs as a subset of called alleles, without assuming that
/// all calls are diploid or inventing pairs for remaining alleles.
fn validate_pairs(alleles: &Bins, pairs: &Bins, label: &str) -> Result<()> {
    let mut paired = Bins::new();
    for ((a, b), count) in pairs {
        for value in [a, b] {
            let total = paired.entry((*value, 0)).or_default();
            *total = total.checked_add(*count).context("paired count overflow")?;
        }
    }
    for (key, count) in paired {
        ensure!(
            count <= *alleles.get(&key).unwrap_or(&0),
            "{label}: genotype bins exceed called allele bin {}",
            key.0
        );
    }
    Ok(())
}

/// None means a *validated* zero-call row. Non-nullable legacy storage cannot
/// represent its undefined summaries; it is counted as empty, never coerced to 0.
fn parse_histogram_row(parts: &[&str], header: &Header) -> Result<Option<StrHistogramRow>> {
    ensure!(
        parts.len() == header.columns.len(),
        "row width {} differs from header width {}",
        parts.len(),
        header.columns.len()
    );
    let get = |name| header.get(parts, name);
    let locus: Vec<_> = get("LocusId").split('-').collect();
    ensure!(
        locus.len() == 4,
        "LocusId: expected a single chrom-start-end-motif locus; compound loci are unsupported"
    );
    let chrom = locus[0];
    ensure!(
        matches!(chrom, "X" | "Y" | "M" | "MT")
            || chrom
                .parse::<u8>()
                .is_ok_and(|n| (1..=22).contains(&n) && n.to_string() == chrom),
        "LocusId: unsupported chromosome {chrom:?}"
    );
    let position = integer(locus[1], "LocusId start")?;
    let end_position = integer(locus[2], "LocusId end")?;
    ensure!(position < end_position, "LocusId: start must precede end");
    let motif = get("Motif");
    ensure!(
        !motif.is_empty() && motif.bytes().all(|b| b"ACGTN".contains(&b)),
        "Motif: unsupported/compound repeat unit {motif:?}"
    );
    ensure!(motif == locus[3], "Motif does not match LocusId motif");
    if header.updated {
        // This is a literal identity guard on the observed source representation,
        // not an interpretation or coordinate conversion of Interval.
        ensure!(get("Interval") == format!("{}:{}-{}", locus[0], locus[1], locus[2]),
            "Interval: unsupported representation or LocusId disagreement; readiness blocker (no coordinate semantics inferred)");
        for name in ["VC", "HemiAllele99thPercentile", "HemiAlleleMax"] {
            ensure!(missing(get(name)), "{name}: populated value {:?} cannot be represented by the admitted model; readiness blocker: source semantics/schema approval required", get(name));
        }
    }
    let aggregate = [
        parse_bins(get(KINDS[0]), 0, KINDS[0])?,
        parse_bins(get(KINDS[1]), 1, KINDS[1])?,
    ];
    let called = integer(get("NumCalledAlleles"), "NumCalledAlleles")?;
    let unique = integer(get("UniqueAlleleLengths"), "UniqueAlleleLengths")?;
    let total = aggregate[0].values().try_fold(0u64, |sum, n| {
        sum.checked_add(*n).context("allele count overflow")
    })?;
    ensure!(
        u64::from(called) == total,
        "NumCalledAlleles does not equal aggregate allele bin count ({called} != {total})"
    );
    ensure!(
        unique as usize == aggregate[0].len(),
        "UniqueAlleleLengths does not equal distinct allele bins"
    );
    validate_pairs(&aggregate[0], &aggregate[1], "aggregate")?;

    let mut populations = HashMap::new();
    let mut parsed = Vec::new();
    for col in &header.distributions {
        let label = format!(
            "{} ancestry={:?} sex={:?}",
            KINDS[col.kind], col.ancestry, col.sex
        );
        let bins = parse_bins(parts[col.index], col.kind, &label)?;
        if let (Some(ancestry), Some(sex)) = (&col.ancestry, &col.sex) {
            if !bins.is_empty() {
                populations.insert(
                    format!("{}:{ancestry}:{sex}", KINDS[col.kind]),
                    encode_bins(&bins, col.kind),
                );
            }
        }
        parsed.push((col, bins));
    }
    for (kind, expected) in aggregate.iter().enumerate() {
        let mut combined = Bins::new();
        for (col, bins) in &parsed {
            if col.kind == kind && col.ancestry.is_some() && col.sex.is_some() {
                add_bins(&mut combined, bins)?;
            }
        }
        ensure!(
            &combined == expected,
            "{}: disjoint ancestry x sex strata do not sum to aggregate",
            KINDS[kind]
        );
    }
    for (col, expected) in &parsed {
        if col.ancestry.is_some() && col.sex.is_some() {
            if col.kind == 1 {
                let alleles = parsed
                    .iter()
                    .find(|(a, _)| a.kind == 0 && a.ancestry == col.ancestry && a.sex == col.sex);
                ensure!(
                    expected.is_empty() || alleles.is_some(),
                    "genotype stratum has no corresponding allele stratum"
                );
                if let Some((_, alleles)) = alleles {
                    validate_pairs(alleles, expected, "population")?;
                }
            }
            continue;
        }
        let mut combined = Bins::new();
        for (child, bins) in &parsed {
            if child.kind == col.kind
                && child.ancestry.is_some()
                && child.sex.is_some()
                && (col.ancestry.is_none() || col.ancestry == child.ancestry)
                && (col.sex.is_none() || col.sex == child.sex)
            {
                add_bins(&mut combined, bins)?;
            }
        }
        ensure!(
            &combined == expected,
            "{}: marginal ancestry={:?} sex={:?} conflicts with disjoint strata",
            KINDS[col.kind],
            col.ancestry,
            col.sex
        );
    }

    let stats: Vec<_> = SUMMARIES
        .iter()
        .map(|name| summary(get(name), name))
        .collect::<Result<_>>()?;
    // Short-allele summaries were never stored. Validate syntax, but do not
    // invent a definition of their quantile/maximum or replace nulls with zero.
    for name in ["ShortAllele99thPercentile", "ShortAlleleMax"] {
        summary(get(name), name)?;
    }
    if called == 0 {
        return Ok(None);
    }
    let stats: Vec<f32> = stats
        .into_iter()
        .zip(SUMMARIES)
        .map(|(v, name)| v.with_context(|| format!("{name}: missing summary for nonzero calls")))
        .collect::<Result<_>>()?;
    let min = aggregate[0].keys().next().unwrap().0;
    let max = aggregate[0].keys().next_back().unwrap().0;
    // Float32 is the existing storage contract. Reject unrepresentable bin
    // extrema rather than silently losing integer precision during comparison.
    ensure!(
        (min as f32) as f64 == min as f64 && (max as f32) as f64 == max as f64,
        "repeat bin extrema cannot be represented exactly by Float32 summaries"
    );
    ensure!(
        stats[0] == min as f32 && stats[6] == max as f32,
        "Min/Max conflict with aggregate allele bins"
    );
    for i in [1, 2, 4, 5] {
        ensure!(
            (min as f32..=max as f32).contains(&stats[i]),
            "{} falls outside aggregate allele bins",
            SUMMARIES[i]
        );
    }
    // Conservative range bound, not a recomputation of the source Stdev.
    // This covers population/sample denominators and normal source rounding
    // for integer-valued extrema without assuming a quantization algorithm.
    // Singleton and constant distributions still require zero spread.
    let stdev_upper = f64::from(max) - f64::from(min);
    ensure!(
        summary_at_most(stats[3], stdev_upper),
        "Stdev exceeds range-based population/sample upper bound ({stdev_upper})"
    );
    // Quantiles are ordered regardless of interpolation convention. Do not
    // recompute them or replace the source values; only allow storage-scale
    // numerical slack. The existing exact Min/Max range checks still apply.
    ensure!(
        summary_at_most(stats[4], f64::from(stats[5])),
        "Median exceeds 99thPercentile beyond Float32 rounding tolerance"
    );
    let highest = aggregate[0].values().max().unwrap();
    ensure!(
        aggregate[0]
            .iter()
            .any(|((n, _), count)| *n as f64 == stats[1] as f64 && count == highest),
        "Mode conflicts with aggregate allele bins"
    );
    Ok(Some(StrHistogramRow {
        chrom: format!("chr{chrom}"),
        position,
        end_position,
        motif: motif.to_owned(),
        allele_size_histogram: encode_bins(&aggregate[0], 0),
        biallelic_histogram: encode_bins(&aggregate[1], 1),
        min_repeats: stats[0],
        mode_repeats: stats[1],
        mean_repeats: stats[2],
        stdev_repeats: stats[3],
        median_repeats: stats[4],
        p99_repeats: stats[5],
        max_repeats: stats[6],
        unique_allele_lengths: unique,
        num_called_alleles: called,
        populations,
    }))
}

/// total = accepted + rejected + empty (nonblank data rows examined).
/// accepted includes region-filtered rows; submitted is the subset sent to the
/// inserter, NOT a durable commit count. A limit bounds submitted rows only.
#[derive(Debug, Default)]
struct LoadStats {
    total: usize,
    accepted: usize,
    rejected: usize,
    empty: usize,
    blank_lines: usize,
    filtered: usize,
    submitted: usize,
}

fn stream_histograms(
    reader: impl BufRead,
    region: Option<&RegionFilter>,
    limit: Option<usize>,
    stats: &mut LoadStats,
    mut insert: impl FnMut(&StrHistogramRow) -> Result<()>,
) -> Result<()> {
    let mut header = None;
    let mut lines = reader.lines().enumerate();
    loop {
        if header.is_some() && limit.is_some_and(|max| stats.submitted >= max) {
            break;
        }
        let Some((index, line)) = lines.next() else {
            break;
        };
        let line_number = index + 1;
        let line = line.with_context(|| format!("row {line_number}: source read failed"))?;
        if line.is_empty() {
            stats.blank_lines += 1;
            continue;
        }
        let parts: Vec<_> = line.split('\t').collect();
        let Some(header) = &header else {
            header = Some(
                Header::parse(&parts)
                    .with_context(|| format!("row {line_number}: invalid histogram header"))?,
            );
            continue;
        };
        stats.total += 1;
        let row = match parse_histogram_row(&parts, header) {
            Ok(Some(row)) => row,
            Ok(None) => {
                stats.empty += 1;
                continue;
            }
            Err(e) => {
                stats.rejected += 1;
                return Err(e).with_context(|| format!("row {line_number}: rejected histogram"));
            }
        };
        stats.accepted += 1;
        if region.is_some_and(|filter| !filter.contains(&row.chrom, row.position)) {
            stats.filtered += 1;
            continue;
        }
        insert(&row).with_context(|| format!("row {line_number}: histogram insert failed"))?;
        stats.submitted += 1;
    }
    ensure!(header.is_some(), "missing histogram header (empty input)");
    Ok(())
}

/// A parse/read/insert error aborts immediately. Earlier batches may already be
/// durable; this legacy whole-file loader is not transactional or retry-safe.
/// No automatic retry, finish/flush, or success receipt is issued on failure.
pub fn load_str_histograms(
    ch_url: &str,
    gcs_path: &str,
    region: Option<&RegionFilter>,
    limit: Option<usize>,
) -> Result<usize> {
    info!("Loading STR histograms from {gcs_path} (region={region:?}, limit={limit:?})");
    let mut stats = LoadStats::default();
    let mut inserter = ClickHouseInserter::new(ch_url, "lr_str_histograms", 50_000);
    let result = (|| {
        let reader = BufReader::new(get_reader(gcs_path)?);
        stream_histograms(reader, region, limit, &mut stats, |row| {
            inserter.insert(row)
        })?;
        inserter
            .finish()
            .context("final histogram batch flush failed")
    })();
    if let Err(err) = result {
        error!("STR histogram load FAILED: {stats:?}; confirmed inserted={}; partial writes possible; no retry", inserter.total_rows());
        bail!("STR histogram load FAILED for {gcs_path}: {stats:?}; confirmed inserted={}; partial writes possible, do not retry automatically: {err:#}", inserter.total_rows());
    }
    info!(
        "STR histogram load complete (bounded={}): {stats:?}; confirmed inserted={}",
        region.is_some() || limit.is_some(),
        inserter.total_rows()
    );
    Ok(stats.submitted)
}

pub mod validation;
/// Separate, lossless updated-source capture; never a legacy canonical row.
pub mod source;

#[cfg(test)]
mod tests;
