# Histogram contract fixtures

All fixture payloads are **synthetic**, independently manufactured arithmetic
examples, not captured, renamed, perturbed, or sampled cohort rows. Only the
contract header vocabulary/order is retained. Cohort names select schemas, not
real participant populations. No real-source publication approval is claimed.

`hgsvc_hprc.tsv` (57 columns) and `aou.tsv` (33 columns) each contain two invented
loci: `1-400000-400024-AT` and `1-500000-500024-GAC`. Number joint columns in header
order from i=0 and set u=i+2. Row 1 uses sizes 8,12,16 with allele counts 4u,8u,4u
and pairs 8/12:4u,12/16:4u. Row 2 uses sizes 0,6,18 with the same count formula,
pairs 0/6:4u,6/18:4u, and an empty final joint. Aggregate and marginal columns
are sums of the appropriate joints, never independent data. Summaries are
computed from these invented distributions. Empty/dot optional summaries and
the empty joint exercise missingness without implying size-zero observations.
Terminal missing cells use dot spelling to avoid trailing whitespace; tests also
manufacture the equivalent empty terminal cell to verify exact TSV width.

`source-v1.json` contains eight explicitly synthetic field maps: normal rows,
wider context with/without VC, chrY diagonal examples, and literal NAT/TCN motifs.
Named starts are 400000 then 600000 through 1200000 in steps of 100000; end=start+24.
Wider context is start-80 through start+120, and populated VC is start-40 through
start+60. Normal distributions follow row 1 above; records 4 and 7 follow row 2.
The chrY examples instead use allele counts 3u,2u at sizes 8,12 and pairs
8/8:3u,12/12:u, deliberately violating a two-copy diagonal interpretation while
satisfying the source lower bound. Hemi summaries are populated only for these
invented chrY cases; they do not attest ploidy or producer semantics.

JSON identities use fake `gs://synthetic-histogram-fixtures/` URIs and artificial
generations 1–8. Each size/MD5 describes just its synthetic single-row TSV,
serialized in its cohort's header order with LF endings (including a final LF).
Tests independently recompute these identities. There are no real range receipts,
source-body hashes, or captured provenance records in these fixtures.

The legacy regression is a small internally consistent 15-core/colon-key fixture
in `../tests.rs`. Header permutations and malformed cases are generated in tests.

## Deliberate readiness limits

- Only joint ancestry x source-sex distributions become canonical population
  keys. Marginals must agree with their joint children. `unknown` sex remains
  `unknown`; no sex or ploidy is inferred.
- Nonempty VC or hemi summary values are a hard readiness blocker, including
  numeric zero. Their scientific meaning and storage are not guessed or dropped.
- Interval must literally match the observed `chrom:start-end` projection of
  LocusId. No independent coordinate semantics or interval conversion is assumed.
- Compound loci and unsupported motif/chromosome representations are rejected.
- Validated zero-call rows with nullable summaries are counted as `empty`, not
  inserted into the legacy non-nullable summary schema. This is explicit absence,
  not a synthetic all-zero row. No model/DDL changes or database migration needed.
- A missing genotype histogram is not manufactured. The parser checks supplied
  pairs as a subset of allele bins, without asserting diploidy; the browser has
  additional autosomal diploid/AN/identity gates. Parser success alone is not
  browser admission, and synthetic loci do not prove real-source readiness.
- Existing short-allele summaries have numeric/null syntax checked but are not
  stored (unchanged legacy model); their scientific quantile semantics are not
  inferred. Aggregate summaries are checked against established bin bounds and
  mode, not recomputed using an assumed quantile/rounding algorithm.
- Loads fail on the first malformed/unsupported row. Previous flushed batches
  may remain. Errors include physical row number, examined-row counters and a
  partial-write/no-auto-retry warning. No rollback or automatic retry is attempted.
- `total = accepted + rejected + empty` counts nonblank data rows examined;
  `blank_lines` is separate. `accepted` includes valid region-filtered rows;
  `submitted` counts rows handed to the inserter, not durable writes. Completion
  logs separately report confirmed inserts and whether region/limit bounded the
  read. Header-only input succeeds with zero rows; missing header fails.
