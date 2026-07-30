# gnomad-lr

Rust loaders that transform gnomAD long-read VCF, coverage, methylation, STR histogram, and sample metadata sources into the ClickHouse tables consumed by the gnomAD browser.

## Y1 status

The existing `load`/`run` paths and `sql/lr_{variants,haplotypes}.sql` still implement the legacy browser contract. They are **not compatible with the cohort-aware Y1 HGSVC/HPRC and AoU sources**: the legacy shape loses multiallelic arrays, lacks release/cohort identity, and cannot represent AoU's summary-only records safely.

Y1 development now has a separate pure transformation layer plus repository-owned `sql/y1/` canonical summary, ALT-expanded, frequency, and carrier tables with attempt identity, together with metadata, ancillary-ledger, and active-pointer tables. Primary Y1 rows have no second `_staging` copy; ancillary and phased-methylation staging remains separate. Ancillary source candidates and the deterministic 232-sample methylation assay-subset manifest are fail-closed; none is yet authorized for Y1 serving. See [`docs/y1-ancillary-inventory.md`](docs/y1-ancillary-inventory.md) and [`docs/y1-ancillary-runbook.md`](docs/y1-ancillary-runbook.md). Do not point the legacy commands at Y1 objects, initialize `gnomad_lr_y1_pilot` with legacy DDL, or write surveyed Y1 data to remote ClickHouse before acceptance.

## Reproducible build

`genohype-core` and `genohype-pool` are fetched from the public Genohype repository at one immutable revision (recorded in both `Cargo.toml` and `Cargo.lock`). A sibling `../genohype` checkout is **not** required.

ClickHouse support is a default, required feature and is passed explicitly by all supported build targets:

```bash
make test                  # host tests, locked dependencies
make release               # host release binary
make worker                # Linux x86_64 worker for GCP (requires zig/cargo-zigbuild)
make verify                # clean-checkout checks used by CI
```

The distributed orchestrator is the separate `genohype` CLI. Install the compatible pinned revision, including its ClickHouse feature, with:

```bash
make install-genohype
```

## ClickHouse environments

The repository-owned DDL for the current legacy browser contract lives at the top of `sql/` and is embedded in the binary's `init` command. Y1 v5 DDL lives separately in `sql/y1/` and is initialized only by `init-y1`. Every major/full Y1 load targets a fresh Terraform-provisioned private ClickHouse instance; the whole instance is the candidate and each primary logical table has exactly one physical table. A database remains useful for local smoke isolation, but there is no in-instance primary active/candidate copy or staging-to-final publication phase. Legacy HTTP requests preserve a URL's `database` query parameter:

```text
http://127.0.0.1:8123/?database=gnomad_lr_smoke
```

The Y1 target contract is stricter: endpoint, database, target kind, and auth source are separate values. It rejects `default`, credentials or `database=...` embedded in the endpoint, unsafe database names, unauthenticated remote endpoints, and serving targets without an additional acknowledgement. Initialization performs no in-place migration or `ALTER`: it accepts only an empty isolated database with a `_v5_` name token, or an already exact live schema carrying the full checked-Y1 attestation. That attestation proves schema shape only and is never load authorization. For example, after an administrator creates a fresh database:

```bash
target/debug/gnomad-lr init-y1 \
  --endpoint http://127.0.0.1:8123 \
  --database gnomad_lr_y1_scratch_v5_local \
  --target-kind scratch \
  --auth-source none
```

Remote targets require `--allow-remote --auth-source environment`; administrator/finalizer credentials are read from `Y1_CLICKHOUSE_USER` and `Y1_CLICKHOUSE_PASSWORD` by default. Serving-class schema operations additionally require `--allow-serving`. These acknowledgements do not make surveyed Y1 writes acceptable before the remaining acceptance gates pass.

Every v5 primary writer must use one dedicated ClickHouse principal. Its exact username is supplied as `--worker-principal` (and as `target.worker_principal` in a pool payload), while its credentials are read from `Y1_CLICKHOUSE_WORKER_USER` and `Y1_CLICKHOUSE_WORKER_PASSWORD`. The authenticated username must match exactly or loading/finalization fails closed. Provisioning must grant that user only the `SELECT, INSERT` access needed on the fresh candidate database; the separate finalizer administrator must be able to `ALTER USER` and read `system.users`/`system.processes`. For example (password and administrator grants remain secret/infrastructure-managed):

```sql
CREATE USER gnomad_lr_y1_worker IDENTIFIED WITH sha256_password BY 'REPLACE_SECRET' SETTINGS async_insert = 0;
GRANT SELECT, INSERT ON gnomad_lr_y1_scratch_v5_fresh.* TO gnomad_lr_y1_worker;
```

Finalization appends `freezing`, attests that task leases are terminal, executes `ALTER USER <worker> SETTINGS readonly = 1, async_insert = 0`, verifies those settings through the worker credentials, drains that principal's active `INSERT` queries from `system.processes`, and reattests terminal leases before any snapshot. The worker fence is never lifted by this binary. A missing principal, credential, privilege, or fence attestation stops finalization without a snapshot.

The bounded source path is scratch-only, requires an adjacent TBI plus immutable source metadata, writes a machine-readable report, and publishes only when every source record produces one summary with zero structured rejects:

```bash
target/debug/gnomad-lr load-y1-interval \
  --endpoint http://127.0.0.1:8126 \
  --database gnomad_lr_y1_scratch_v5_demo \
  --target-kind scratch --auth-source environment \
  --worker-principal gnomad_lr_y1_worker \
  --cohort aou \
  --vcf gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.chr22.vcf.gz \
  --source-generation GENERATION --source-checksum MD5_BASE64 \
  --source-size-bytes SOURCE_SIZE_BYTES \
  --index-generation TBI_GENERATION --index-checksum TBI_MD5_BASE64 \
  --region chr22:20000000-20010000 \
  --batch-records 250 \
  --report-path /tmp/aou-10kb.json
```

The only general-purpose phased-methylation write remains a narrower single-owner smoke. `smoke-y1-phased-methylation` has no manifest, URI, sample, haplotype, layer, interval, serving-target, authentication-mode, credential-variable, expected-principal, retry, or concurrency options: the binary pins the repository v2 manifest, environment credentials `Y1_CLICKHOUSE_WORKER_USER`/`Y1_CLICKHOUSE_WORKER_PASSWORD`, principal `gnomad_lr_y1_worker`, and only HG00097/hap1/chr22:20000000-20010000. It rejects unversioned, dirty, test, and other non-release build identities before any ClickHouse request. It requires a unique database beginning `gnomad_lr_y1_scratch_phased_methylation_smoke_v5_`, exact `currentUser()`, synchronous inserts, the exact schema-v5 attestation, and a database with no rows except its schema receipt. It buffers and validates the bounded generation-qualified BED/TBI read, inserts once into `lr_y1_methylation_phased_staging`, then compares an exact row count and ordered RowBinary key/content SHA-256 hashes before creating a new JSON receipt. It never writes final phased rows, summaries, availability, joins, pointers, or accepted serving state. The exact proposed remote command and cleanup boundary are checked in at `manifests/y1/phased-methylation-smoke-hg00097-hap1-chr22-755b45d3.json`.

`evaluate-y1-phased-methylation` is a second, fixed product-evaluation exception rather than a general loader. Its database is code-pinned to `gnomad_lr_y1_scratch_phased_methylation_evaluation_v5_hg00097_chr22_47040000_47050000_v1`; callers can supply only the endpoint, remote acknowledgement, and new receipt path. It resolves the exact frozen HG00097 hap1 and hap2 BED/TBI identities for chr22:47,040,000-47,050,000, completely parses both sources before one synchronous insert, and rereads exact row counts and ordered key/content hashes for hap1, hap2, and the combined table. A successful receipt says `joinable_to_vcf=false` and `orientation_status=UNCONFIRMED`. The coordinator wrapper must drop the database on any nonzero exit or rejected receipt, and retain it only after full verification.

Records are transformed and inserted directly into the canonical attempt-scoped tables in bounded batches (250 records by default). Full-run finalization database-fences the dedicated writer, verifies every terminal attempt and physical count, synchronously removes all strictly attributable nonaccepted-attempt rows (including partial table writes), computes ordered RowBinary SHA-256 digests, durably freezes the run, rereads the same rows, and only then records `accepted_frozen`. A retry from `frozen` or `accepted_frozen` revalidates the exact manifest, independent counts, counts, receipt, and digests; accepted retries return the persisted machine report. It never copies rows into a second primary table set. This prevents a 1 Mb genotype-rich interval from being materialized as one multi-gigabyte client-side insert. Complete source INFO JSON is stored once on the source-record summary; ALT-expanded rows reference the same run/cohort/source identity instead of duplicating multi-megabyte record payloads for every ALT.

A rejected attempt remains visible in the immutable attempt ledger; its primary rows are deleted before a full run can be accepted. The schema-v5 binary does not compile primary materialize/activate/rollback commands or export their legacy `published` acceptance route. Environment-level endpoint cutover is a later, separately authorized external operation that must consume exact `accepted_frozen` evidence; finalization neither activates serving nor applies Terraform. Existing instances remain the rollback environment.

Recommended promotion path:

1. **Local Docker/Colima** for schema, parser, synthetic retry/publication, and bounded source smoke tests.
2. **Dedicated GCP dev ClickHouse** only when testing distributed pool networking or realistic write concurrency.
3. **Production ClickHouse** only for an explicit final check, using the smoke runner's `gnomad_lr_smoke_*` database guard. Direct remote endpoints additionally require `--allow-remote`. Never smoke-test in `default`.

`localhost:8125` is conventionally an SSH tunnel to the production VM; it is not a separate ClickHouse instance. A pool worker cannot use that laptop-local URL—use the VM's internal URL plus an isolated database parameter instead.

For a distributed smoke test that needs its own VM, Genohype can manage the lifecycle explicitly:

```bash
genohype clickhouse create gnomad-lr-smoke-ch \
  --machine-type e2-standard-4 --disk-size-gb 100 --zone us-east1-c
# run the pool smoke against the URL reported by `genohype clickhouse show`
genohype clickhouse destroy gnomad-lr-smoke-ch
```

### Local ClickHouse

The project uses a pinned ClickHouse LTS image, a project-scoped Compose volume, and `127.0.0.1` to avoid macOS IPv6 surprises. Its passwordless development user is published on loopback only. The smoke tooling requires Python 3.11+ and Docker Compose v2.

```bash
make clickhouse-up
make init                   # initialize all seven legacy-contract objects in CLICKHOUSE_URL
make clickhouse-down
make clickhouse-reset       # destructive: removes only the local Compose volume
```

Override ports when another stack owns `8123`/`9000`:

```bash
CLICKHOUSE_HTTP_PORT=8124 CLICKHOUSE_NATIVE_PORT=9001 \
  CLICKHOUSE_URL=http://127.0.0.1:8124 make smoke
```

## Legacy source-backed plumbing smoke

`development/smoke.toml` is the checked-in `legacy_v1_plumbing` manifest. It validates bounded source access, parser execution, and isolated writes; passing it is not Y1 acceptance. It pins:

- one indexed chr22 VCF interval, capped by VCF record count;
- one indexed methylation BED/sample over the same interval, capped by row count;
- bounded prefixes of the sequential coverage and histogram files;
- the HPRC metadata source.

Run the complete smoke workflow:

```bash
gcloud auth application-default login   # once, if ADC is not already configured
make smoke
```

The runner:

1. accepts only the explicit `legacy_v1_plumbing` profile and labels its output as non-Y1;
2. refuses a non-loopback host unless `--allow-remote` is present;
3. permits only databases named `gnomad_lr_smoke` or `gnomad_lr_smoke_*`;
4. recreates that isolated database by default;
5. initializes the legacy-contract schema;
6. invokes every loader with `--region` and/or `--limit` bounds;
7. fails if any expected table (including the methylation materialized view) is empty.

Useful variants:

```bash
# Inspect the exact commands without building or connecting
python3 scripts/smoke.py --no-build --dry-run

# Exercise selected loaders
python3 scripts/smoke.py --only vcf,methylation

# Final check through the loopback production tunnel; the isolated-DB guard still applies
python3 scripts/smoke.py \
  --clickhouse-url http://127.0.0.1:8125 \
  --database gnomad_lr_smoke_my_branch

# A genuinely remote/internal endpoint requires an extra acknowledgement
python3 scripts/smoke.py \
  --clickhouse-url 'http://192.168.0.6:8123' \
  --database gnomad_lr_smoke_my_branch \
  --allow-remote
```

Each direct legacy loader also exposes bounded controls. These examples use the legacy mirrored source and must not be adapted to Y1 until the v2 contract is implemented:

```bash
target/debug/gnomad-lr load all \
  --region chr22:20000000-21000000 --limit 100 \
  --vcf-path gs://gnomad-lr-data/vcf/v3/chr22.renamed.vcf.gz \
  --clickhouse-url 'http://127.0.0.1:8123/?database=gnomad_lr_smoke'

target/debug/gnomad-lr load coverage --limit 1000 ...
target/debug/gnomad-lr load histograms --limit 1000 ...
target/debug/gnomad-lr load methylation --chrom chr22 --start 20000000 \
  --stop 21000000 --limit 1000 ...
```

## Distributed pool workflow

Build the custom worker before creating/updating a Genohype pool:

```bash
make worker
make install-genohype
genohype pool create lr --wait
```

`genohype.toml` points the pool at `target/release/gnomad-lr-worker`. Submit smoke manifests to an isolated dev database before a full load. For pool jobs, the ClickHouse URL must be reachable from GCP workers, for example:

```text
http://192.168.0.6:8123/?database=gnomad_lr_smoke_pool
```

The `run` command now emits 1-based, inclusive, non-overlapping VCF regions, avoiding duplicate rows at task boundaries:

```bash
gnomad-lr run --chroms chr22 --skip-index \
  --clickhouse-url 'http://192.168.0.6:8123/?database=gnomad_lr_smoke_pool'
```

Do not promote Y1 data to the production `default` database through this legacy path. The Y1 tables preserve canonical arrays and derive ALT-expanded/frequency/carrier query shapes. Major loads use a fresh isolated instance and freeze one canonical attempt-scoped table set in place; they do not materialize an inactive copy. Serving cutover remains separately authorized and all dual-cohort full-load, activation, metadata/ancillary, phased-methylation, and browser/API gates remain in force. Production acceptance also remains blocked until VCF and index reads are generation-qualified and their observed object identities are persisted end to end; the current pinned Genohype I/O API reads bare mutable URIs.
