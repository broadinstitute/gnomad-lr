# gnomad-lr

Rust loaders that transform gnomAD long-read VCF, coverage, methylation, STR histogram, and sample metadata sources into the ClickHouse tables consumed by the gnomAD browser.

## Y1 status

The existing `load`/`run` paths and `sql/lr_{variants,haplotypes}.sql` still implement the legacy browser contract. They are **not compatible with the cohort-aware Y1 HGSVC/HPRC and AoU sources**: the legacy shape loses multiallelic arrays, lacks release/cohort identity, and cannot represent AoU's summary-only records safely.

Y1 development now has a separate pure transformation layer plus repository-owned `sql/y1/` staging, canonical-summary, ALT-expanded, frequency, carrier, metadata, ancillary-ledger, and active-pointer tables. Ancillary source candidates and the deterministic 232-sample methylation assay-subset manifest are fail-closed; none is yet authorized for Y1 serving. See [`docs/y1-ancillary-inventory.md`](docs/y1-ancillary-inventory.md) and [`docs/y1-ancillary-runbook.md`](docs/y1-ancillary-runbook.md). Do not point the legacy commands at Y1 objects, initialize `gnomad_lr_y1_pilot` with legacy DDL, or write surveyed Y1 data to remote ClickHouse before acceptance.

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

The repository-owned DDL for the current legacy browser contract lives at the top of `sql/` and is embedded in the binary's `init` command. Y1 v3 DDL lives separately in `sql/y1/` and is initialized only by `init-y1`. Use a ClickHouse **database** (the ClickHouse equivalent of an isolated index) to separate smoke data from serving data. Legacy HTTP requests preserve a URL's `database` query parameter:

```text
http://127.0.0.1:8123/?database=gnomad_lr_smoke
```

The Y1 target contract is stricter: endpoint, database, target kind, and auth source are separate values. It rejects `default`, credentials or `database=...` embedded in the endpoint, unsafe database names, unauthenticated remote endpoints, and serving targets without an additional acknowledgement. For example, after an administrator creates the database:

```bash
target/debug/gnomad-lr init-y1 \
  --endpoint http://127.0.0.1:8123 \
  --database gnomad_lr_y1_scratch_local \
  --target-kind scratch \
  --auth-source none
```

Remote targets require `--allow-remote --auth-source environment`; credentials are read from `Y1_CLICKHOUSE_USER` and `Y1_CLICKHOUSE_PASSWORD` by default. Serving-class schema operations additionally require `--allow-serving`. These acknowledgements do not make surveyed Y1 writes acceptable before the remaining acceptance gates pass.

The bounded source path is scratch-only, requires an adjacent TBI plus immutable source metadata, writes a machine-readable report, and publishes only when every source record produces one summary with zero structured rejects:

```bash
target/debug/gnomad-lr load-y1-interval \
  --endpoint http://127.0.0.1:8126 \
  --database gnomad_lr_y1_scratch_demo \
  --target-kind scratch --auth-source none \
  --cohort aou \
  --vcf gs://gnomad-lr-data/y1/sources/aou/vcfs/gnomAD_LR_Y1.aou.chr22.vcf.gz \
  --source-generation GENERATION --source-checksum MD5_BASE64 \
  --index-generation TBI_GENERATION --index-checksum TBI_MD5_BASE64 \
  --region chr22:20000000-20010000 \
  --batch-records 250 \
  --report-path /tmp/aou-10kb.json
```

Records are transformed and staged in bounded batches (250 records by default), while validation and fail-closed publication still apply to the complete requested interval. This prevents a 1 Mb genotype-rich interval from being materialized as one multi-gigabyte client-side insert. Complete source INFO JSON is stored once on the source-record summary; ALT-expanded rows reference the same run/cohort/source identity instead of duplicating multi-megabyte record payloads for every ALT.

A rejected attempt remains visible in the ledgers and staging tables but produces no canonical rows. Interval runs cannot activate a serving partition.

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

Do not promote Y1 data to the production `default` database through this legacy path. The v2 development tables preserve canonical arrays and derive ALT-expanded/frequency/carrier query shapes. Task attempts are immutable; only one accepted attempt per task is materialized, repeat materialization replaces the inactive run partition, and an active pointer is separate. Activation is restricted to immutable, independently counted full-chromosome serving runs. Y1 promotion still requires dual-cohort 10 kb acceptance, metadata/ancillary gates, and cohort-aware browser/API changes.
