# Y1 ancillary inventory and availability contract

Inventory date: 2026-07-27. This inventory covers the Rust loader repository and
`gnomad-browser` commit `13691578a2423048bfa39efcc8ff2f456ed6c408`.
The machine-readable source of truth is
[`sources/y1/ancillary-source-manifest.json`](../sources/y1/ancillary-source-manifest.json).
No entry is currently authorized for Y1 serving. Exact evidence requests are
tracked in [`y1-ancillary-blockers.md`](y1-ancillary-blockers.md).

## Serving-path matrix

| Modality | Classification / owner | Pinned source and contract | Loader / storage | API / browser / export | Cache and validation status |
|---|---|---|---|---|---|
| Aggregate sequencing coverage | **Unresolved, blocked**; HGSVC/HPRC candidate only. AoU unavailable. | gnomAD LR v2 GCS generation `1774409101272208`, MD5 `39vOhBWrDjYSk269CMwPhA==`, GRCh38, 1-based per-base rows, aggregate expected from 292 samples. Exact Y1 run linkage is missing. | Legacy `src/loader/coverage.rs` → unscoped `lr_coverage`; silently skips/zero-fills malformed values. Isolated DDL: `lr_y1_coverage{_staging,}`. | `fetchLRCoverageForRegion` / `Query.lr_coverage`; `HaplotypeRegionPage/LRCoverageTrack.tsx`; TSV export filename is region-only. | No release/cohort/run arguments or ancillary cache identity. No Y1 source/count/repeat acceptance. Must return unavailable for both cohorts until accepted; never use for AoU. |
| Per-sample CpG methylation | **Unresolved 232-sample assay subset, blocked**; HGSVC/HPRC candidate only. AoU unavailable. | 232 immutable BED/TBI pairs in `methylation-sample-manifest.json`; GRCh38 BED 0-based half-open; 60 of 292 roster samples explicitly absent, zero unexpected. Scientific sample-selection provenance is missing. | Legacy `src/loader/methylation.rs` trusts free-form sample ID, skips malformed rows, writes unscoped `lr_methylation`. Isolated DDL preserves `source_start0`, `source_end0`, and 1-based `position`. | `fetchMethylationForRegion` / `Query.methylation`; consumed by `LongReadUnifiedView`, `LongReadHaplotypeView`, `HaplotypeRegionPage`, and `HaplotypeTrack`. | Query and local React state are unscoped; cohort switching can retain stale state. No complete readability/schema/count acceptance. Missing sample/site means unavailable, not zero. |
| Methylation summary | **Derived, blocked with detail run**; HGSVC/HPRC only. | Must derive from exactly one accepted active methylation run; not an independent source. | Legacy incremental `lr_methylation_summary_mv` is unscoped and cannot isolate retries. Isolated `lr_y1_methylation_summary` is materialized only after accepted detail reconciliation. | `fetchMethylationSummaryForRegion` / `Query.methylation_summary`; same three browser views and methylation summary track. | No identity or cache key. Summary/detail reconciliation unrun. |
| Methylation outliers | **Derived query, blocked with detail run**; HGSVC/HPRC only. | Query-time statistic over accepted detail and summary rows; no independent source. | Legacy self-join in `haplotype-queries.ts`; no Y1 table required. | `fetchMethylationOutliersForRegion` / `Query.methylation_outliers`; same three views auto-fetch top samples. | Current query joins only on `pos1` and is unscoped; Y1 query must join full release/cohort/reference/run/chrom/position identity. No acceptance. |
| STR allele-frequency histograms | **Unresolved, blocked**; HGSVC/HPRC candidate only. AoU unavailable. | gnomAD LR v2 GCS generation `1774408600324182`, MD5 `q/JrPpRfAfmVvDfpo5BGbQ==`; 37-column aggregate/population TSV. Exact coordinate convention, Y1 run linkage, and count reconciliation are unresolved. | Legacy `src/loader/histograms.rs` silently skips/zero-fills and writes unscoped `lr_str_histograms`. Isolated DDL: `lr_y1_str_histograms{_staging,}`. | `fetchSTRHistogram` / `Query.lr_str_histogram`; rendered in LR STR detail consumers through the returned variant histogram. | Unscoped query and no run-aware cache identity. No Y1 acceptance; never use for AoU. |
| Variant density / SNV density | **Derived from primary active Y1 summaries**, not an independent ancillary source. | No source object. Browser bins the already fetched active variant response. | No loader/table. | `LongReadVariantPage/VariantDensityTrack.tsx`, `LRUniqueDensityTrack`, and `LongReadVariantTrack.tsx`. | Identity inherits the primary variant query only. Bin boundaries/totals still require deterministic tests; no separate ancillary pointer. |
| Genes / transcripts | **Shared reference candidate**, cohort-independent only when reference genome and annotation release are fixed. | Explicit blocked manifest entry `shared-grch38-gene-annotations-unresolved`; current browser gene index/static-data provenance is outside this repository and is not pinned to an immutable artifact. | Legacy Python `load_real_haplotype_data.py genes`; no Rust ancillary loader/table. | Gene/region resolution and LR views consume browser gene queries. | Existing Y1 primary query carries cohort, but annotation release/source commit is not proven here. Block ancillary publication claims; do not duplicate genes per cohort. |
| Variant annotations / in-silico predictors | **Part of primary Y1 allele contract**, not ancillary publication. | Pinned chr22 predictor sidecar is handled by the primary loader and `lr_y1_alleles`; see summary/carrier acceptance artifacts. | `src/y1/storage.rs`, `lr_y1_alleles{_staging,}`. | Visible variant annotations and exports use primary Y1 identity. | Governed by primary run pointer, not ancillary pointer. |
| Recombination rate | **Shared reference candidate**, cohort-independent if GRCh38 map/version is pinned. | Explicit blocked manifest entry `shared-grch38-recombination-map-unresolved`; current `fetchRecombinationRate` calls a mutable live UCSC `hg38/recomb1000GAvg` API without a pinned map artifact. | No Rust loader or ClickHouse table. | `Query.recombination_rate`, haplotype views. | Unscoped by reference-map version and not cached with ancillary identity. Blocked from a provenance-complete Y1 bundle until pinned. |

No separate REST ancillary endpoint or server-side precomputed density table was found.
The relevant GraphQL implementation is
`graphql-api/src/{queries/haplotype-queries.ts,graphql/resolvers/haplotypes.ts,graphql/types/haplotype.graphql}`.
The current ancillary GraphQL fields accept genomic arguments only; they do not
accept release, cohort, reference genome, or active ancillary run.

## Availability contract

1. `LR_Y1_ENABLED=false` keeps all existing legacy tables, GraphQL behavior, and
   browser behavior unchanged.
2. A Y1 query resolves the latest `lr_y1_active_ancillary` row by
   `(release, cohort, reference_genome, modality)` and then queries only its
   `ancillary_run_id`. There is no fallback to a legacy table.
3. HGSVC/HPRC returns a modality only after its candidate run is accepted and
   active. Absence of a pointer is an explicit `unavailable` result.
4. AoU has no coverage, methylation, STR histogram, or sample-track source.
   These fields must resolve to unavailable without querying HGSVC/HPRC tables.
5. Cache/export identity is
   `(release, cohort, reference_genome, modality, ancillary_run_id, query shape)`.
   Browser cohort or pointer changes clear in-memory ancillary state.
6. BED methylation `[start0,end0)` is retained verbatim. Browser/VCF position is
   `start0 + 1`; browser closed intervals are converted to BED/tabix as
   `[browser_start - 1, browser_stop)`. Coverage positions are already 1-based.
   STR conversion remains blocked until its source convention is proven.

## Intentional gaps

- The observed methylation inventory is exactly 232/292 roster samples. The 60
  absent IDs are listed in the sample manifest. This is an assay subset, not
  complete cohort coverage, and its scientific selection criterion is unknown.
- No AoU ancillary source was found; no empty AoU manifest entries were created.
- Coverage and STR objects are immutable but only labeled `v2`; their exact Y1
  pipeline/run provenance is unresolved.
- Genes and recombination maps are real LR-view dependencies but are not pinned
  to immutable annotation/map releases in the current Y1 repository.
- No 10 kb, 1 Mb, Linux RSS, repeated-run, or full-chr22 ancillary acceptance
  has been performed. Consequently all publication modes fail closed.
