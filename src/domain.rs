use std::collections::HashMap;
use once_cell::sync::Lazy;

/// 1000 Genomes subpopulation → superpopulation mapping
pub static SUBPOP_TO_SUPERPOP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // AFR
    for s in ["ACB", "ASW", "ESN", "GWD", "LWK", "MSL", "YRI", "MKK", "ASL"] {
        m.insert(s, "AFR");
    }
    // AMR
    for s in ["CLM", "MXL", "PEL", "PUR"] {
        m.insert(s, "AMR");
    }
    // EAS
    for s in ["CDX", "CHB", "CHS", "JPT", "KHV"] {
        m.insert(s, "EAS");
    }
    // EUR
    for s in ["GBR", "FIN", "IBS", "TSI"] {
        m.insert(s, "EUR");
    }
    // SAS
    for s in ["BEB", "GIH", "ITU", "PJL", "STU"] {
        m.insert(s, "SAS");
    }
    m
});

/// V3 VCF GCS paths per chromosome
static V3_BASE: &str = "gs://fc-fd42e80c-b41e-4e60-a9cf-b7c0ade168c4/submissions/0d296ecd-ab99-4c53-83c4-dfde50cd4081/RenameInfoFields";

pub static GCS_VCF_V3_PATHS: Lazy<HashMap<&'static str, String>> = Lazy::new(|| {
    let entries: Vec<(&str, &str)> = vec![
        ("chr1",  "b66e7ba2-d740-4c78-84b4-ea345ff89f44/call-ConcatVcfs/chr1.renamed.vcf.gz"),
        ("chr2",  "ce2ecb33-eb67-4bae-b325-ed42280b9a46/call-ConcatVcfs/chr2.renamed.vcf.gz"),
        ("chr3",  "56dea9d0-edb4-4f25-b1cb-172ba6c73d12/call-ConcatVcfs/chr3.renamed.vcf.gz"),
        ("chr4",  "f5fc8cb9-2d16-40e8-92b1-0a0a399114b4/call-ConcatVcfs/chr4.renamed.vcf.gz"),
        ("chr5",  "4b5ffb30-bf76-4aa5-b98d-0aa102312481/call-ConcatVcfs/chr5.renamed.vcf.gz"),
        ("chr6",  "1132d9ba-f502-443d-b751-2599af7f553f/call-ConcatVcfs/chr6.renamed.vcf.gz"),
        ("chr7",  "80c73ad5-5200-4b76-9199-d8da534db189/call-ConcatVcfs/chr7.renamed.vcf.gz"),
        ("chr8",  "990d2cd2-e7cd-42c1-92cd-06b12711a75c/call-ConcatVcfs/chr8.renamed.vcf.gz"),
        ("chr9",  "f0604a1e-ba8a-4748-995d-bb1973ef4981/call-ConcatVcfs/chr9.renamed.vcf.gz"),
        ("chr10", "2b753806-fa6b-40fa-be20-96887e73114d/call-ConcatVcfs/chr10.renamed.vcf.gz"),
        ("chr11", "35613034-ce04-4f03-80bb-e15af0e42b89/call-ConcatVcfs/chr11.renamed.vcf.gz"),
        ("chr12", "d650620a-d2a1-4986-b663-b7ee38faa50d/call-ConcatVcfs/chr12.renamed.vcf.gz"),
        ("chr13", "8851c55d-6154-46de-9243-1b9bcad36eb1/call-ConcatVcfs/chr13.renamed.vcf.gz"),
        ("chr14", "bf585eb5-0fcb-42db-8efe-665ab1709512/call-ConcatVcfs/chr14.renamed.vcf.gz"),
        ("chr15", "2b24f653-84d1-4a40-a9fa-c3e002bbff4b/call-ConcatVcfs/chr15.renamed.vcf.gz"),
        ("chr16", "e8c5f7d3-7253-4624-892a-f2a00be6ca8d/call-ConcatVcfs/chr16.renamed.vcf.gz"),
        ("chr17", "80636f0a-6251-49eb-9401-7e89e0b0a7b4/call-ConcatVcfs/chr17.renamed.vcf.gz"),
        ("chr18", "a6aac8a6-ca9b-4da6-9198-a5a5281307cb/call-ConcatVcfs/chr18.renamed.vcf.gz"),
        ("chr19", "83032557-01f0-4f58-b318-c0be2b4165c6/call-ConcatVcfs/chr19.renamed.vcf.gz"),
        ("chr20", "4e9d6bf4-51e1-4e48-83a5-d0ddebbc4715/call-ConcatVcfs/chr20.renamed.vcf.gz"),
        ("chr21", "735254cf-4444-4e63-9054-c51ecaa0c9c0/call-ConcatVcfs/chr21.renamed.vcf.gz"),
        ("chr22", "6afe82e1-1300-4312-98bf-11b02b50477b/call-ConcatVcfs/chr22.renamed.vcf.gz"),
        ("chrX",  "276dc205-af0b-4ecf-b899-a70779c79028/call-ConcatVcfs/chrX.renamed.vcf.gz"),
        ("chrY",  "49134603-969c-4746-aadb-7ab40a129feb/call-ConcatVcfs/chrY.renamed.vcf.gz"),
    ];
    entries
        .into_iter()
        .map(|(chr, suffix)| (chr, format!("{}/{}", V3_BASE, suffix)))
        .collect()
});

/// Resolve the VCF path for a given chromosome. Uses the V3 GCS paths.
pub fn resolve_vcf_path(chrom: &str) -> Option<String> {
    GCS_VCF_V3_PATHS.get(chrom).cloned()
}

/// Consequence terms ranked by severity (lower index = more severe)
pub static CONSEQUENCE_TERMS: &[&str] = &[
    "transcript_ablation", "splice_acceptor_variant", "splice_donor_variant",
    "stop_gained", "frameshift_variant", "stop_lost", "start_lost",
    "initiator_codon_variant", "transcript_amplification", "inframe_insertion",
    "inframe_deletion", "missense_variant", "protein_altering_variant",
    "splice_region_variant", "incomplete_terminal_codon_variant",
    "start_retained_variant", "stop_retained_variant", "synonymous_variant",
    "coding_sequence_variant", "mature_miRNA_variant", "5_prime_UTR_variant",
    "3_prime_UTR_variant", "non_coding_transcript_exon_variant",
    "non_coding_exon_variant", "intron_variant", "NMD_transcript_variant",
    "non_coding_transcript_variant", "nc_transcript_variant",
    "upstream_gene_variant", "downstream_gene_variant", "TFBS_ablation",
    "TFBS_amplification", "TF_binding_site_variant",
    "regulatory_region_ablation", "regulatory_region_amplification",
    "feature_elongation", "regulatory_region_variant", "feature_truncation",
    "intergenic_variant",
];

/// Get the rank of a consequence term (lower = more severe). Returns 999 for unknown.
pub fn consequence_rank(term: &str) -> usize {
    CONSEQUENCE_TERMS.iter().position(|&t| t == term).unwrap_or(999)
}

/// Consequence terms to omit from transcript consequences
pub static OMIT_CONSEQUENCE_TERMS: &[&str] = &[
    "upstream_gene_variant",
    "downstream_gene_variant",
];

/// Compute chromosome number for xpos calculation
pub fn compute_chrom_number(chrom: &str) -> u64 {
    let c = chrom.strip_prefix("chr").unwrap_or(chrom);
    match c {
        "X" => 23,
        "Y" => 24,
        "M" | "MT" => 25,
        _ => c.parse::<u64>().unwrap_or(0),
    }
}
