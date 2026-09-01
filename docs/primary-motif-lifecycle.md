# Y1 primary-motif product lifecycle

The optional product remains outside the frozen Y1 v5 initializer and browser/API.
The checked registry is still `CANDIDATE_PENDING_SCIENCE`; production can build a
candidate, but `verify-y1-primary-motif` and `finalize-y1-primary-motif` reject it
until a separately reviewed registry is supplied.

## Supported commands

- `init-y1-primary-motif` creates or exactly attests the five optional tables.
- `produce-y1-primary-motif` resolves one accepted-frozen primary run and its exact
  task/attempt rows, binds the checked per-contig VCF/TBI generations, creates the
  `planned` revision, reads every registered locus, writes all allele frequency
  strata and (for HGSVC/HPRC) all accepted-metadata genotype strata, and appends
  `produced` with physical RowBinary counts/hashes. AoU writes typed genotype
  unavailability and no pair/margin rows.
- `reconcile-y1-primary-motif` rereads the immutable VCF/TBI and accepted metadata,
  recomputes every expected aggregate row, compares full semantic rows (not only
  counts), and emits `Y1_PRIMARY_MOTIF_INDEPENDENT_RECONCILIATION_V1` for the
  existing verify/finalize gates.
- Manual `transition-y1-primary-motif` is restricted to `--to failed`; callers
  cannot forge `producing` or `produced`.

Audit report paths are create-new. Product table insertion is restart-aware: an
empty table is inserted once; an already populated table must exactly equal a
fresh generation-qualified recomputation, which covers an acknowledged insert or
an ambiguous client timeout without blind duplicate insertion.

## Deliberately unclosed

1. The reconciler is a separate read-only command and independently rereads source
   and metadata, but currently shares the Rust pure aggregation and row-builder
   implementation with the producer. A truly implementation-independent Python
   parser/reconciler remains needed to eliminate common-mode algorithm defects.
2. The disposable ClickHouse lifecycle test uses synthetic reviewed aggregate rows
   to exercise init, physical corruption failure, writer fencing, independent
   receipt verification, and finalization. It does not emulate the GCS JSON API;
   generation-substitution and range corruption remain covered by the immutable
   reader's Rust fake-backend tests rather than this ClickHouse test.
3. Concurrent producer processes for the same `product_run_id` are not a
   supported recovery mode. One dedicated writer owner must run or resume a run;
   ClickHouse `MergeTree` tables do not provide a cross-table transaction or
   uniqueness constraint. Exact readback handles sequential retries and ambiguous
   HTTP completion, but not two simultaneous empty-table inserts.
4. No full-genome campaign wrapper, pool task, serving activation, API, or browser
   work is included. No real candidate can pass finalization while the checked
   registry remains pending science review.
