# chr22 phased-methylation mirror canary

This path loads **raw source hap1/hap2 labels only** from the accepted mirror into `lr_y1_methylation_phased_staging` on a fresh candidate. It has no original Terra fallback, finalizer, serving activation, phase join, or orientation mapping. `joinable_to_vcf=false`; orientation is `UNCONFIRMED`.

## Checked inputs

- Genohype core, pool, lockfile, and install target: `39b2f2a99f191acb49cf18d9a96a26fb62cd29d4`.
- Ledger: `sources/y1/methylation-phased-mirror-ledger.json`, declared content SHA-256 `97355c54...3241`, raw SHA-256 `7f4e15a9...b78b`, 924 objects / 231 samples / 127,463,220,748 bytes.
- Tasks: `manifests/y1/phased-methylation-mirror-chr22-canary.json`, exactly 462 in sample then hap1/hap2 order. Regenerate/check with:

```bash
python3 scripts/generate-y1-phased-mirror-canary.py --check
python3 scripts/test-generate-y1-phased-mirror-canary.py
```

## Required preflight (not performed by this repository change)

1. Provision a fresh isolated private candidate and exact scratch database. Do not modify either existing LR ClickHouse VM.
2. Create passwordless user `gnomad_lr_y1_phased_worker`; restrict broad `default`; grant this user only required `SELECT` plus `INSERT` on candidate `lr_y1_methylation_phased_staging`.
3. Build the exact clean release worker and verify its embedded identity/SHA. The worker rejects dirty, unversioned, test, or mismatched identities and rejects an actual `currentUser()` other than the named principal.
4. Verify candidate schema v5, empty staging, zero final/active rows, coordinator receipts at exact Genohype revision, pool cost/cleanup identity, and the exact runtime payload below.

## Zero-worker submit, then one-worker canary

These commands are a run recipe, **not authorization to execute**. Fill only the fresh candidate values and exact committed build values; do not add password fields.

```bash
# Create the fresh-named pool from genohype.phased-canary.toml. Its profile has
# starting_workers=0; confirm status shows coordinator + zero workers.
genohype --config genohype.phased-canary.toml pool create lr-phased-chr22-canary --wait
genohype --config genohype.phased-canary.toml pool status lr-phased-chr22-canary

BACKEND_REVISION='<exact-clean-committed-40-hex>'
BUILD_IDENTITY="gnomad-lr/${BACKEND_REVISION}/x86_64-linux-release/features-clickhouse"
CANDIDATE_ENDPOINT='http://<private-rfc1918-address>:8123'
CANDIDATE_DATABASE='gnomad_lr_y1_scratch_phased_canary_v5_<fresh_candidate_name>'
PAYLOAD="$(jq -cn \
  --arg endpoint "$CANDIDATE_ENDPOINT" \
  --arg database "$CANDIDATE_DATABASE" \
  --arg revision "$BACKEND_REVISION" \
  --arg build "$BUILD_IDENTITY" \
  '{action:"load_y1_phased_mirror_chr22",schema_version:1,
    contract_id:"mirror-only-chr22-source-phased-canary-v1",
    run_id:"y1-phased-mirror-chr22-canary-v1",
    ledger_content_sha256:"97355c54eef458b56f31a318c740dddaff7261a0d76b1d83be5078b4efb13241",
    ledger_raw_sha256:"7f4e15a93920c842b11fc24ed3ee96aebefcc42549e001431164c2631e54b78b",
    expected_backend_revision:$revision,expected_worker_build_identity:$build,batch_records:250,
    target:{endpoint:$endpoint,database:$database,
      authentication:"named_passwordless_private_user",
      worker_principal:"gnomad_lr_y1_phased_worker"}}')"

# Submit while there are still zero workers, record the exact job ID, then scale
# only to one. Do not enable autoscaling.
genohype --config genohype.phased-canary.toml pool submit lr-phased-chr22-canary \
  --batch-size 1 -- \
  custom --payload "$PAYLOAD" \
  --manifest manifests/y1/phased-methylation-mirror-chr22-canary.json
genohype --config genohype.phased-canary.toml pool scale lr-phased-chr22-canary --workers 1
```

Stop after the first accepted task pair is inspected unless the separately approved canary contract explicitly authorizes continued processing. Acceptance must reconcile Genohype's exact-job terminal receipts to the per-task sample/haplotype, source/index generations/sizes/MD5s, assignment attempt, build, principal, schema/table, rows/rejects, and content hash. Physical rows from failed/requeued attempt IDs are not accepted. No final or serving table is touched by this worker.

## Local reusable integration

```bash
make clickhouse-up
GNOMAD_LR_LOCAL_CLICKHOUSE_MIRROR_URL=http://127.0.0.1:8123 \
  cargo test --locked --features clickhouse \
  y1::phased_pool::tests::local_clickhouse_two_haplotype_tasks_touch_only_staging \
  -- --ignored --exact
make clickhouse-down
```

The harness creates a uniquely named disposable database and named no-password user, runs one fixture hap1 and one fixture hap2 task, proves two staging rows are readable and final/active/summary/availability tables remain empty, then drops only those disposable resources.
