# Y1 HGSVC/HPRC metadata publication runbook

This workflow is isolated from the legacy `load metadata` command and
`lr_sample_metadata`. It publishes immutable run-scoped rows and changes serving
behavior only through `lr_y1_active_metadata`.

## Inputs and invariants

The checked source manifest is
`sources/y1/metadata-source-manifest.json`. Reconciliation verifies the byte size
and SHA-256 of each repository-owned input before opening ClickHouse. The roster
is the ordered 292-sample `#CHROM` header projection from GCS generation
`1785158335564090`; carriers are never used to discover samples.

Do not update an expected count to accommodate drift. Pin and review a new
manifest and source set instead. Never import supplemental `Population` or
`color`. Scientific conflict adjudication requires a separate reviewed override
source; there is no hard-coded override path.

## Initialize a target

```sh
cargo run -- init-y1 \
  --endpoint http://127.0.0.1:8123 \
  --database gnomad_lr_y1_scratch_metadata \
  --target-kind scratch --auth-source none
```

Serving targets additionally require `--allow-serving`; remote targets require
`--allow-remote` and `--auth-source environment`.

## Reconcile twice in scratch

```sh
for run in metadata-rehearsal-a metadata-rehearsal-b; do
  cargo run -- reconcile-y1-metadata \
    --endpoint http://127.0.0.1:8123 \
    --database gnomad_lr_y1_scratch_metadata \
    --target-kind scratch --auth-source none \
    --metadata-run-id "$run" \
    --source-manifest sources/y1/metadata-source-manifest.json \
    --report "artifacts/$run.json" \
    --publisher-identity "$USER"
done
```

Compare `rows_sha256` and `audit_sha256` in both reports. They must be identical.
Each run emits:

- `<run>.json`: full rows, audit, counts, hashes, and carrier joins;
- `<run>.compact.json`: the 62 fallback decisions and two retained conflicts;
- `<run>.audit.jsonl`: deterministic row-level audit plus four duplicate events.

Expected distribution is AFR 97, EAS 61, AMR 52, SAS 44, EUR 37, ASJ 1.

## Carrier gates and serving publication

Repeat reconciliation against the target containing each accepted carrier run.
Pass every run under test explicitly:

```sh
cargo run -- reconcile-y1-metadata ... \
  --metadata-run-id metadata-serving-001 \
  --source-manifest sources/y1/metadata-source-manifest.json \
  --report artifacts/metadata-serving-001.json \
  --publisher-identity release-bot \
  --carrier-run-id HGSVC_10KB_RUN \
  --carrier-run-id HGSVC_1MB_V3_RUN \
  --carrier-run-id HGSVC_CHR22_RUN
```

Each report records carrier-row and distinct-carrier counts and requires zero
unmatched and zero one-to-many sample joins. Publication does not activate the
run.

## Activate and roll back

Activation is serving-only and validates accepted ledger state and 292 unique
canonical rows.

```sh
cargo run -- activate-y1-metadata ... \
  --metadata-run-id metadata-serving-001 \
  --activated-by release-manager
```

The command reports the previous run ID. Rollback appends a new pointer revision;
it never rewrites metadata:

```sh
cargo run -- rollback-y1-metadata ... \
  --metadata-run-id PREVIOUS_RUN_ID \
  --activated-by release-manager
```

A rejected candidate cannot call activation because activation resolves the
latest run-ledger state and requires `accepted`.

## Serving query contract

HGSVC/HPRC Y1 queries must resolve:

```sql
SELECT argMax(metadata_run_id, revision)
FROM lr_y1_active_metadata
WHERE release = 'y1'
  AND cohort = 'hgsvc_hprc'
  AND reference_genome = 'GRCh38'
```

and include that run ID in both the metadata query and cache key. Do not create an
AoU pointer. When `LR_Y1_ENABLED` is false, retain the legacy metadata query.

## Cutover evidence

Archive both repeatability reports, all three carrier-join reports, active-pointer
queries before/after activation/rollback, and browser smoke-test results. The
browser gate covers diploid labels, ancestry bars/legend, cluster stacks,
genealogy leaves, and a <=100 kb Haplotype View; it requires no metadata GraphQL
errors, no grey samples caused by missing metadata, and AoU summary-only behavior.
