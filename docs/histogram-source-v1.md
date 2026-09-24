# Updated histogram source capture v1 (not serving admission)

`load_histogram_source_v1` is a separate Genohype custom action. It never calls
`load_histograms`, never constructs `StrHistogramRow`, and never writes/changes
`lr_str_histograms` or an existing Y1 canonical table. Legacy validation guards
remain unchanged. `updated_histogram_source_v1` is a **capture** discriminator,
not the browser's proposed `str_context_completion_v2` serving contract.

## Destination and provisioning

Each cohort/run gets a **new, empty** database named exactly:

```
gnomad_lr_y1_scratch_histogram_<cohort>_<run_id>
```

Cohort is `hgsvc_hprc` or `aou`; run ID is 1–80 lowercase ASCII letters, digits,
or underscores. The worker requires the exact cohort/run-derived database, a
separate HTTP(S) endpoint, explicit named passwordless principal, and explicit
`allow_remote` acknowledgement for non-loopback. Passwordless access is confined
to loopback/literal private IPs by the existing Y1 target. No job-level target or
source fallback exists. In particular, `gnomad_lr_y1_scratch_v5_current`, default,
legacy and serving databases cannot be selected.

Provision these **new** tables explicitly with the two standalone DDL files:

- `sql/lr_str_source_histograms_v1.sql`
- `sql/lr_str_source_histogram_receipts_v1.sql`

No DDL is executed by the worker or added to `init-y1`/legacy initialization.
No ALTER, DROP, copy from old canonical tables, or publication is provided.
Use a dedicated principal with SELECT/INSERT on only this candidate; require
synchronous INSERTs. The principal must be exclusively assigned to this one-shot
campaign. Coordinator retries must be disabled; one whole-source task per
assignment. Worker rejects `assignment_attempt != 1`, missing lease, and reused
candidates. A freshness SELECT plus append-only start receipt is **not an atomic
lock**: the operator/coordinator must not launch two first assignments against
the same candidate. A failed/uncertain INSERT is never retried: abandon the
candidate and provision a distinct run/database for any explicitly authorized
rerun. There is no transaction or serving acceptance claim.

## Exact job/task payload

Job payload must be exactly `{"action":"load_histogram_source_v1"}`; extra
job-level fields are rejected, not silently ignored. Each custom task
payload contains **only** the following fields (unknown fields, including region,
limit and legacy gcs_path/clickhouse_url, are errors):

```json
{
  "contract": "updated_histogram_source_v1",
  "cohort": "hgsvc_hprc",
  "run_id": "refresh_20260923_r1",
  "task_id": "custom_0",
  "source_uri": "gs://fc-a22a385b-ed3b-45ba-9d6c-87ca01c1e6b8/gnomAD_LR_vcfs/hgsvc_hprc/data/hgsvc_hprc.af_histograms.tsv",
  "source_generation": "1789761285157516",
  "source_size_bytes": 3010278628,
  "source_md5_base64": "Djg0d9C9w4cEByhnAeH44w==",
  "clickhouse_endpoint": "http://192.0.2.1:8123",
  "database": "gnomad_lr_y1_scratch_histogram_hgsvc_hprc_refresh_20260923_r1",
  "worker_principal": "histogram_refresh_hgsvc_writer",
  "allow_remote": true
}
```

Endpoint above is an intentionally invalid-for-passwordless documentation IP;
replace it only in an authorized campaign. `task_id` must equal the Genohype
TaskDescriptor ID. AoU uses its own database/task/principal and frozen identity:
URI suffix `aou/data/aou_phase1.af_histograms.tsv`, generation `1790116043533826`,
size `4248882818`, MD5 `rvQJhVZP3eLWzsvTOGp3nw==`. No cohort source default.
The capture contract permits other explicitly frozen inputs of the same schema;
it does not attest that cohort metadata in an operator-supplied task is true.

## Physical row schema / losslessness

Table: **`lr_str_source_histograms_v1`**. Full column types are in its DDL.

- Identity: `contract`, `cohort`, `run_id`, `task_id`, `source_uri`,
  `source_generation`, `source_size_bytes`, `source_md5_base64`, `row_ordinal`.
- Original named component: `locus_id`, `motif`.
- Parsed named query keys: `chrom`, `locus_start`, `locus_end`.
- Independent original context: `source_interval`; parsed `context_chrom`,
  `context_start`, `context_end`; `source_vc Nullable(String)`.
- Parsed counts: `num_called_alleles`, `unique_allele_lengths`.
- **`source_header Array(String)`** retains all column names in source order;
  **`source_fields Map(String,String)`** retains every original cell, including
  all histograms, overlapping marginals, joint strata, summaries, empty strings
  and dot null spellings. Example: `source_fields['HemiAlleleMax']`. Always check
  `mapContains` when consuming fields; ClickHouse's absent-map-key default must
  not manufacture a missing numeric zero. Source schema requires all 19 cores.

Physical identity is `(cohort,run_id,source_uri,source_generation,row_ordinal)`.
Ordinal is **1-based data row after the one header**, not a physical line index;
blank rows and repeated headers fail, rather than changing the convention.
The table uses ordinary MergeTree: no replacing, deduplication, first-row-wins,
or aggregation of repeated named loci/contexts. All original records survive,
even exact repeated tuples. Uniqueness is reconciled before projection, not
silently assumed by capture. The ordinal also distinguishes exact duplicates.

`source_fields` is the authoritative summary storage: null is represented by its
original empty/dot text, not coerced to 0 or a Float32. Source VC's nullable helper
does not erase its raw spelling. Line endings are not stored per row, but the
checksum includes their exact original bytes. This is lossless field capture,
not a promise to reconstruct the input byte-for-byte from table rows alone.

Coordinates retain the source numbers with **no POS±1 conversion**. Named locus
and context are separately parsed, and context must contain the named locus on
the same chromosome. A wider interval with either populated or blank VC is valid.
VC, when populated, is independently interval-grammar checked, not a numeric
variant count; it is preserved even if different from Interval. Motifs are exact
literal uppercase A/C/G/T/N strings. NGC and GCN are valid, and N is **not** a
wildcard. No motif rotation/reverse-complement or compound-ID guessing occurs.

## Validation and limits

This source contract shares strict 19-core header/stratum recognition and lexical
bin parsing with legacy code, but not its Interval equality, empty Hemi/VC guards,
Float32 projection, or two-copy pair interpretation. Unknown headers fail closed
until a distinct reviewed contract supports them (e.g. TRID is not guessed).

- Exact width; single resolved LocusId with matching Motif; UInt32 nonnegative
  coordinates/bin sizes/counts; positive frequencies and no duplicate bins.
- Allele sums = NumCalledAlleles; unique allele bins = UniqueAlleleLengths.
- For **both** distributions, disjoint ancestry × source-sex joints sum to the
  aggregate. Every supplied ancestry-only/sex-only marginal equals its joints.
  Marginals are preserved, never counted as additional observations.
- Pair grammar requires ordered `a/b:n`. For each size, off-diagonal occurrences
  plus diagonals counted **once** cannot exceed allele observations. Check
  aggregate and every supplied joint/marginal. This safe necessary lower bound
  neither proves two copies nor reconstructs ploidy. Extra unpaired allele
  observations are allowed. No partners, biological pairs, or counts fabricated.
- All summaries accept missing or finite nonnegative numeric strings within
  Float64 validation range; raw text is stored without rounding. They are not
  recomputed or assigned producer-version-dependent meanings. Zero-call rows
  require all histograms empty, zero unique count, and undefined summaries;
  they are retained/countable, not dropped or plotted as size-zero bins.
- Resource guards: 16 MiB maximum line, 512 columns / 128 KiB header-name bytes.
  Batches flush at 5,000 rows **or** 8 MiB estimated source/header bytes (including
  both per-row copies of header names); one row can exceed that batch threshold.
  This is not a bound on total allocator/serialization overhead. No row/byte/region
  truncation options. Existing immutable reader
  validates generation, size and metadata MD5 and every range's generation/size.
  MD5 is independently recomputed over the complete byte stream at EOF.

A complete body identity is **capture integrity**, not signed producer semantics,
biological validation, primary-binding proof, or a deployment receipt.

## Capture receipts and partial writes

`lr_str_source_histogram_receipts_v1` receives one `started` and one terminal row.
Rows carry the same source/cohort/run/task/database identity. Terminal fields:

- `status`: `complete_success` or `failed_partial`;
- `completeness`: `partial` until EOF+size+MD5 all verify, otherwise `full`;
- `diagnostic`: stable redacted reason, no raw cell/credential output;
- `gcs_metadata_verified`, `eof_observed`, `complete_body_identity_verified`;
- `bytes_read`, `computed_md5_base64` (a **prefix** digest unless identity is full);
- `data_rows_examined`, `validated_rows`, `zero_called_rows`;
- `rows_insert_attempted`, `rows_insert_acknowledged`, `partial_writes_possible`.

A full-identity receipt can still have failed status (final INSERT/receipt failure).
Acknowledged rows are successful HTTP INSERT acknowledgements; a failed request
may have written more. The reader may prefetch beyond the last validated row, so
bytes read are not a line-position proof. The final short batch is held until full
identity verification; earlier full batches can already be written. First bad
row/read/INSERT aborts, does not flush remaining data, and warns of partial writes.
A failed/uncertain reservation logs `reservation_insert_error` and never opens
the source or issues a second database request. No success until identity complete
**and** all inserts and terminal receipt have been acknowledged. Receipt persistence failure logs a failed receipt and returns
error; the DB may contain a previously accepted terminal row if the response was
lost, so transport failure must quarantine the candidate despite that row.

The same terminal receipt is logged as `histogram_source_receipt=<JSON>`.
Successful Genohype result metadata contains `action`, `source_capture_only:true`,
`published:false`, and `receipt`; item count is acknowledged source rows. A missing
terminal receipt, outstanding started marker, failed worker result or uncertain
transport is not success. Independently reconcile physical rows and source-key
uniqueness under a write fence before any adapter consumes a candidate.

## SOURCE-TO-SERVING adapter contract (browser sibling)

Source capture is independent of refreshed-primary readiness. A later adapter
must produce an **explicitly new** `str_context_completion_v2` serving product and
receipt; it must never route these rows as legacy `available_exact` / `str_completion`.
Keep legacy exact-source/primary-AN invariants intact. No such adapter is run here.

1. Select a fully reconciled capture run with exact source identity, reference
   genome/coordinate interpretation evidence, exclusive writer fence, zero missing
   physical ordinals, expected row count, and exact physical-key uniqueness.
   Capture's source-native bounds must not be labelled start0 until the separate
   coordinate convention is validated. Source measurement kind/unit/version stays
   unknown unless separately evidenced; an upstream code match alone is not proof.
2. Define logical context by exact `(source identity,cohort,locus_id,source_interval,
   raw VC)` tuple. Check duplicate context payloads explicitly; never `ANY JOIN`,
   sum contexts, select first, or hide multiplicity with argMax. One retained source
   ordinal/context per projection row; duplicate tuples require reconciliation.
3. Match the **named component** to the primary page's canonical parser output by
   exact cohort/reference/chrom/named-start/end/literal motif. Preserve ordered
   component index and the complete source key/context. No overlap-only match,
   wildcard N, arbitrary motif normalization, TRID split shortcut, or wider-context
   coordinates substituted for named coordinates. A one-component page is the
   conservative initial display scope. Compound associations can remain unavailable.
4. Count distinct eligible contexts before selection: zero => unavailable; one =>
   named-component association; multiple => ambiguous/unavailable pending explicit
   reconciliation. This association is not proof of independently localized
   measurement or identity with the primary genotyped record. Broader context must
   be visible in plot labels/details, even when VC is blank.
5. **Primary-record binding is optional and separate.** Only explicit evidence may
   bind primary database/run/task/accepted attempt/source variant identity. Matching
   motif, Interval, or AN alone does not prove it; these TSVs omit TRID. Primary AN
   remains primary AN. Histogram `num_called_alleles` is the source distribution's
   AN only; mismatch does not forbid source capture or a context-labelled plot.
6. Allele distributions may use `source_fields['AlleleSizeHistogram']` and validated
   disjoint `__ancestry_sex` joints. Do not count overlapping marginals twice. Label
   conservatively **source repeat-size distribution (record context …)**; no total
   allele length, constituent total repeat count or independently localized LPS
   claim based only on matching public producer code. Unit assertions need evidence.
7. Source BiallelicHistogram is preserved **pair encoding**, not attested diploid
   genotypes. Diagonal can represent one or two alleles; Hemi/Short do not supply
   missing ploidy or hemizygote counts. The safe lower bound is never a diploid plot
   gate. A reviewed neutral source-size-pair plot may display raw encodings with an
   explicit single-or-double diagonal explanation. Biological genotype plots stay
   unavailable unless a separate contract proves their meaning; even per-size
   two-copy equality is arithmetic evidence, not producer/ploidy attestation.
8. Missing row/dataset or ambiguous mapping => unavailable, not zero. Present empty
   stratum/zero-call row => no observed distribution, not a size-zero observation.
   Size bin `0x:n` is real when present. Empty/dot optional summaries remain null;
   nullable unavailable pair plot must not become an empty purported genotype set.

A serving-v2 receipt additionally binds projection rules/digest, semantics evidence,
selected primary snapshot/accepted attempts, mapping counts/statuses, per-contig
conservation, primary-binding/AN-concordance status, and plot-specific availability.
Source capture's receipt deliberately makes none of those serving assertions.

## Fixtures and validation evidence

`src/loader/histograms/fixtures/source-v1.json` contains **fully synthetic** normal
rows, wider VC context, wider blank-VC context, chrY diagonal-pair edge cases, and
literal NAT/TCN motifs. Loci, distributions, strata, summaries and contexts were
manufactured independently; only contract header vocabulary is retained. The TSV
parser fixtures are synthetic too. See `src/loader/histograms/fixtures/README.md`
for the arithmetic recipe and deliberate missingness/pair cases.

JSON metadata uses explicitly fake GCS identities, artificial generations, and
size/MD5 computed from each synthetic single-row TSV. Tests rederive these hashes;
stream tests additionally compute their own synthetic stream-body identities.
No fixture contains captured rows, real range receipts, or real-source provenance.
These are offline regression examples, not evidence of production source readiness,
producer semantics, or data-publication approval.

Tests cover unchanged legacy rejection, all-field JSON preservation, summary nulls,
malformed data/marginals, safe pair bounds, duplicate/context retention, complete
EOF identity, checksum/size/read/parse/insert failure after partial writes, no retry,
no final flush on failure, resource bounds, original header order, worker/target
guards, and commit-then-response-loss for data/reservation/terminal receipts. Production-worker build/load,
ClickHouse DDL execution and serving reconciliation are separate authorized steps.
