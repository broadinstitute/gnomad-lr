use anyhow::bail;

/// Return the legacy mirror identity for a supported Y1 cohort and contig.
pub(crate) fn canonical_y1_mirror_uri(cohort: &str, chrom: &str) -> anyhow::Result<String> {
    if !matches!(cohort, "hgsvc_hprc" | "aou") {
        bail!("unsupported Y1 cohort {cohort:?}");
    }
    grch38_contig_length(chrom)?;
    Ok(format!(
        "gs://gnomad-lr-data/y1/sources/{cohort}/vcfs/gnomAD_LR_Y1.{cohort}.{chrom}.vcf.gz"
    ))
}

/// Accept only the legacy mirror or one release-scoped refresh namespace.
/// The exact cohort/contig filename is unchanged; URI queries, fragments,
/// traversal, extra path segments, and alternate buckets cannot match.
/// Object generations are validated separately by the immutable reader.
pub(crate) fn validate_y1_mirror_uri(uri: &str, cohort: &str, chrom: &str) -> anyhow::Result<()> {
    let canonical = canonical_y1_mirror_uri(cohort, chrom)?;
    if uri == canonical {
        return Ok(());
    }
    if let Some(rest) = uri.strip_prefix("gs://gnomad-lr-data/y1/refreshes/") {
        if let Some((release_id, suffix)) = rest.split_once("/sources/") {
            let bytes = release_id.as_bytes();
            let valid_id = bytes.len() == 21
                && bytes[..8].iter().all(u8::is_ascii_digit)
                && bytes[8] == b'-'
                && bytes[9..]
                    .iter()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b));
            let expected_suffix = canonical
                .strip_prefix("gs://gnomad-lr-data/y1/sources/")
                .unwrap();
            if valid_id && suffix == expected_suffix {
                return Ok(());
            }
        }
    }
    bail!("source must exactly match a legacy or release-scoped immutable Y1 cohort/contig mirror identity")
}

/// Return the canonical GRCh38 primary-assembly length; MT remains unavailable.
pub(crate) fn grch38_contig_length(chrom: &str) -> anyhow::Result<u32> {
    let length = match chrom {
        "chr1" => 248_956_422,
        "chr2" => 242_193_529,
        "chr3" => 198_295_559,
        "chr4" => 190_214_555,
        "chr5" => 181_538_259,
        "chr6" => 170_805_979,
        "chr7" => 159_345_973,
        "chr8" => 145_138_636,
        "chr9" => 138_394_717,
        "chr10" => 133_797_422,
        "chr11" => 135_086_622,
        "chr12" => 133_275_309,
        "chr13" => 114_364_328,
        "chr14" => 107_043_718,
        "chr15" => 101_991_189,
        "chr16" => 90_338_345,
        "chr17" => 83_257_441,
        "chr18" => 80_373_285,
        "chr19" => 58_617_616,
        "chr20" => 64_444_167,
        "chr21" => 46_709_983,
        "chr22" => 50_818_468,
        "chrX" => 156_040_895,
        "chrY" => 57_227_415,
        "chrM" | "chrMT" | "MT" => {
            bail!("GRCh38 mitochondrial contig is unavailable for Y1 primary loading")
        }
        _ => bail!("unsupported or non-canonical GRCh38 contig {chrom:?}"),
    };
    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_each_available_canonical_contig() {
        let expected = [
            ("chr1", 248_956_422),
            ("chr2", 242_193_529),
            ("chr3", 198_295_559),
            ("chr4", 190_214_555),
            ("chr5", 181_538_259),
            ("chr6", 170_805_979),
            ("chr7", 159_345_973),
            ("chr8", 145_138_636),
            ("chr9", 138_394_717),
            ("chr10", 133_797_422),
            ("chr11", 135_086_622),
            ("chr12", 133_275_309),
            ("chr13", 114_364_328),
            ("chr14", 107_043_718),
            ("chr15", 101_991_189),
            ("chr16", 90_338_345),
            ("chr17", 83_257_441),
            ("chr18", 80_373_285),
            ("chr19", 58_617_616),
            ("chr20", 64_444_167),
            ("chr21", 46_709_983),
            ("chr22", 50_818_468),
            ("chrX", 156_040_895),
            ("chrY", 57_227_415),
        ];
        for (chrom, length) in expected {
            assert_eq!(grch38_contig_length(chrom).unwrap(), length, "{chrom}");
        }
    }

    #[test]
    fn rejects_mt_aliases_and_noncanonical_names() {
        for chrom in ["chrM", "chrMT", "MT", "22", "X", "chr23", ""] {
            assert!(grch38_contig_length(chrom).is_err(), "{chrom}");
        }
    }

    #[test]
    fn refresh_mirror_has_a_closed_namespace_and_exact_identity() {
        for cohort in ["aou", "hgsvc_hprc"] {
            for chrom in (1..=22)
                .map(|n| format!("chr{n}"))
                .chain(["chrX".into(), "chrY".into()])
            {
                let canonical = canonical_y1_mirror_uri(cohort, &chrom).unwrap();
                let refresh = canonical.replace(
                    "/y1/sources/",
                    "/y1/refreshes/20260923-012345abcdef/sources/",
                );
                validate_y1_mirror_uri(&canonical, cohort, &chrom).unwrap();
                validate_y1_mirror_uri(&refresh, cohort, &chrom).unwrap();
                assert!(validate_y1_mirror_uri(&refresh, cohort, "chrM").is_err());
                assert!(validate_y1_mirror_uri(&refresh, "aou-shadow", &chrom).is_err());
                for bad in [
                    refresh.replace("012345abcdef", "../sources"),
                    refresh.replace("012345abcdef", "012345ABCDEf"),
                    refresh.replace("012345abcdef", "012345abcde"),
                    refresh.replace("20260923-", "2026092-"),
                    refresh.replace("20260923-", "２０２６０９２３-"),
                    refresh.replace("/sources/", "/sources/../sources/"),
                    refresh.replace("/vcfs/", "/vcfs/%2e%2e/"),
                    refresh.replace("gnomad-lr-data/", "other-bucket/"),
                    format!("{refresh}?generation=1"),
                    format!("{refresh}#1"),
                    format!("{refresh}\n"),
                    format!("{refresh}/"),
                    refresh.replace(&format!(".{chrom}."), ".chrM."),
                ] {
                    assert!(
                        validate_y1_mirror_uri(&bad, cohort, &chrom).is_err(),
                        "{bad:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn mirror_uri_is_an_exact_cohort_and_contig_identity() {
        assert_eq!(
            canonical_y1_mirror_uri("aou", "chr1").unwrap(),
            "gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.chr1.vcf.gz"
        );
        assert!(canonical_y1_mirror_uri("aou-shadow", "chr1").is_err());
        assert!(canonical_y1_mirror_uri("aou", "chrM").is_err());
    }
}
