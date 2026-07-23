# gnomad-lr

Rust loaders that transform gnomAD long-read VCF, coverage, methylation, STR histogram, and sample metadata sources into the ClickHouse tables consumed by the gnomAD browser.

## Y1 status

The current primary VCF loader and `sql/lr_{variants,haplotypes}.sql` implement the legacy browser contract. They are **not compatible with the cohort-aware Y1 HGSVC/HPRC and AoU sources**: the legacy shape loses multiallelic arrays, lacks release/cohort identity, and cannot represent AoU's summary-only records safely.

Do not point the current VCF load commands at Y1 objects or initialize `gnomad_lr_y1_pilot` with the legacy primary DDL. The source survey and detailed v2 reconciliation are planning artifacts, not runtime configuration in this repository.

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

The repository-owned DDL for the current legacy browser contract lives in `sql/` and is embedded in the binary's `init` command. The two primary definitions require a v2 replacement before Y1. Use a ClickHouse **database** (the ClickHouse equivalent of an isolated index) to separate smoke data from serving data. All HTTP requests preserve a URL's `database` query parameter:

```text
http://127.0.0.1:8123/?database=gnomad_lr_smoke
```

Recommended promotion path:

1. **Local Docker/Colima** for schema, parser, and bounded source smoke tests.
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

Do not promote Y1 data to the production `default` database through this legacy path. Y1 promotion requires a reviewed v2 schema, dual-cohort 10 kb acceptance, retry-safe staging/publication, metadata/ancillary gates, and browser contract changes.
