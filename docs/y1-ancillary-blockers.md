# Y1 ancillary publication blockers

Status date: 2026-07-27. No ancillary modality is authorized for Y1 loading or
publication. The checked manifest therefore contains no `accepted_y1` source,
and operators must not create placeholder runs or use legacy objects as
substitutes.

## Evidence requested from the data-production team

### Aggregate sequencing coverage

Provide the immutable workflow/run record that produced GCS generation
`1774409101272208` of `hgsvc_hprc.coverage.tsv.gz`, including pipeline commit,
input callset/release, the exact 292-sample roster (and exclusions), GRCh38 and
coordinate declaration, column-generation/downsampling algorithm, whole-object
row/contig counts, and an independently reproducible count for
`chr22:20,000,000-21,000,000`. Confirm that `pos` and `pos2` are intentionally
identical one-based positions. Existing generation, size, and MD5 evidence is
necessary but does not establish Y1 cohort ownership.

### STR allele-frequency histograms

Provide the workflow/run record and STR catalog release that produced GCS
generation `1774408600324182`, plus the exact checked 37-column schema,
`LocusId` start/end coordinate convention and boundary examples, population and
sex-division definitions, missing-value rules, sample exclusions, and
reconciliation of `NumCalledAlleles` to the 292-sample Y1 roster. Provide
whole-object and chr22 locus/count totals. Do not infer coordinates from the
legacy loader's offsets.

### Per-sample methylation

Provide an authoritative assay inventory or production record explaining why
232 of the 292 HGSVC/HPRC Y1 roster samples were processed, naming the 60
intentional omissions and any assay/QC eligibility rule. It must identify the
pb-cpg-tools and aligner workflow commits/models, input alignment releases,
GRCh38 reference artifact, sample-ID mapping, BED schema/coordinate convention,
and QC thresholds. The repository already pins all 232 BED/TBI generations,
sizes, and MD5 checksums; object existence alone does not establish that the
subset is scientifically intended.

### Genes and transcripts

Provide the annotation release (for example, a precise GENCODE release),
immutable source artifact and checksum, transcript-selection rules, GRCh38
reference identity, browser index build recipe/commit, coordinate convention,
license, and production owner approval. The current browser index/static-data
provenance is not an acceptable Y1 publication identity.

### Recombination map

Provide the exact underlying GRCh38 map artifact and release for the current
UCSC `recomb1000GAvg` track, immutable checksum/size, source population and map
construction citation, UCSC database snapshot or import version, coordinate and
interpolation conventions, expected contigs, license, and production owner
approval. A live UCSC API response is mutable and cannot be pinned as a Y1
source.

## Consequences

- HGSVC/HPRC coverage, methylation detail/summary/outliers, STR histograms,
  genes-as-a-Y1-bundle claim, and recombination-map publication remain blocked.
- AoU coverage, methylation, STR histograms, and sample tracks are explicitly
  unavailable; HGSVC/HPRC data must never be used as a fallback.
- Variant density remains a deterministic presentation derived from the active
  primary Y1 run, not an ancillary source. Variant annotations remain governed
  by that primary run.
- Because there is no provenance-approved modality, the 10 kb and 1 Mb load,
  publish, activate, rollback, RSS, storage, API, and browser acceptance ladder
  cannot truthfully run. No ancillary run IDs or acceptance reports should be
  fabricated.
