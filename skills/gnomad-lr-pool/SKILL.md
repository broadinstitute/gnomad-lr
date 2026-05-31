---
name: gnomad-lr-pool
description: Manage the gnomad-lr distributed pool for loading long-read VCF data into ClickHouse. Use for creating pools, deploying binaries, submitting jobs, diagnosing failures, and tuning performance.
---

You are an expert in managing the gnomad-lr distributed VCF loading pipeline. This pipeline uses genohype's pool system to distribute VCF→ClickHouse loading across GCP worker VMs.

## Architecture Overview

```
Laptop (orchestrator)          GCP (gnomadev project)
─────────────────────          ────────────────────────
gnomad-lr run                  lr-coordinator (genohype binary)
  ├─ check .tbi indexes          ├─ /work, /complete, /status
  ├─ submit index job             ├─ AIMD batch sizing
  └─ submit load job              └─ dashboard at :3000

                               lr-worker-0..N (gnomad-lr binary)
                                 ├─ poll coordinator for tasks
                                 ├─ stream VCF from GCS via tabix
                                 └─ insert into ClickHouse

                               gnomad-lr-data-vm (us-east1-c)
                                 └─ ClickHouse at 192.168.0.6:8123
                                    └─ data on /data (200GB SSD)
```

## Key Repositories and Files

| Location | Purpose |
|----------|---------|
| `/Users/msolomon/code/genohype-eco/gnomad-lr/` | Custom worker binary + orchestrator |
| `/Users/msolomon/code/genohype-eco/genohype/` | Pool infra, VCF indexing, cloud management |
| `gnomad-lr/src/pool.rs` | TaskHandler — dispatches "index" and "load" actions |
| `gnomad-lr/src/orchestrate.rs` | `gnomad-lr run` — check indexes → index → load |
| `gnomad-lr/src/loader/vcf_reader.rs` | VcfStream with tabix indexed seeking |
| `gnomad-lr/src/loader/variants.rs` | Variant parsing + CH insertion |
| `gnomad-lr/src/loader/haplotypes.rs` | Haplotype expansion + CH insertion |
| `gnomad-lr/src/clickhouse.rs` | ClickHouseInserter (HTTP batch inserts) |
| `gnomad-lr/genohype.toml` | Pool config (machine type, workers, network) |
| `gnomad-lr/Makefile` | `make worker` builds Linux cross-compiled binary |
| `genohype/core/src/vcf/index.rs` | `build_tabix_index()` / `write_tabix_index()` |
| `genohype/core/src/vcf/reader.rs` | VcfDataSource with indexed query support |
| `genohype/pool/src/traits.rs` | TaskHandler, TaskResult traits |
| `genohype/cli/src/cloud/pool/lifecycle.rs` | Pool VM creation |
| `genohype/cli/src/cloud/pool/binary.rs` | Binary staging and deployment |

## GCP Configuration

| Resource | Value |
|----------|-------|
| Project | `gnomadev` |
| Zone | `us-east1-b` (workers), `us-east1-c` (ClickHouse VM) |
| Network | `gnomad-v4-dev` |
| Subnet | `gnomad-v4-dev-main` |
| Service Account | `gnomad-lr-sa@gnomadev.iam.gserviceaccount.com` |
| VCF Bucket | `gs://gnomad-lr-data/vcf/v3/<chrom>.renamed.vcf.gz` |
| Pool Config | `gs://gnomad-lr-data/pool-ops/` |
| ClickHouse | `192.168.0.6:8123` on `gnomad-lr-data-vm` |

**IMPORTANT:** Default gcloud project must be `gnomadev` for pool commands to work:
```bash
gcloud config set project gnomadev
```

## Commands

### Build
```bash
cd /Users/msolomon/code/genohype-eco/gnomad-lr

# Build macOS binary
cargo build --release

# Build Linux worker binary (cross-compile)
make worker
# Output: target/release/gnomad-lr-worker

# Build both
make
```

### Create Pool
```bash
# Create pool (reads genohype.toml for config)
genohype pool create lr --workers 32

# Check status
genohype pool status lr

# Destroy pool
genohype pool destroy lr
```

### Deploy Custom Binary to Workers

The startup script deploys the standard genohype binary. Workers need our gnomad-lr binary instead. After pool creation:

```bash
# 1. Stage binary to GCS
gcloud storage cp target/release/gnomad-lr-worker \
  gs://gnomad-lr-data/pool-ops/bin/gnomad-lr-worker-latest

# 2. Deploy to all workers (batches of 8 to avoid IAP rate limiting)
for i in $(seq 0 31); do
  gcloud compute ssh lr-worker-$i --project=gnomadev --zone=us-east1-b \
    --tunnel-through-iap --command="
    sudo systemctl stop genohype-worker
    sudo gsutil cp gs://gnomad-lr-data/pool-ops/bin/gnomad-lr-worker-latest /usr/local/bin/genohype
    sudo chmod +x /usr/local/bin/genohype
    sudo systemctl start genohype-worker
  " 2>/dev/null &
  if [ $(( (i+1) % 8 )) -eq 0 ]; then wait; fi
done
wait

# 3. Verify (should say "gnomAD Long Read VCF loader")
gcloud compute ssh lr-worker-0 --project=gnomadev --zone=us-east1-b \
  --tunnel-through-iap --command="/usr/local/bin/genohype --help | head -1"
```

### Service Account Setup

Workers need the `gnomad-lr-sa` service account for GCS access. Currently requires stop/start:

```bash
# Stop all workers
gcloud compute instances list --project=gnomadev --filter="name~^lr-worker" \
  --format="value(name)" | xargs \
  gcloud compute instances stop --project=gnomadev --zone=us-east1-b --quiet

# Set SA on all workers
for i in $(seq 0 31); do
  gcloud compute instances set-service-account lr-worker-$i \
    --project=gnomadev --zone=us-east1-b \
    --service-account=gnomad-lr-sa@gnomadev.iam.gserviceaccount.com \
    --scopes=cloud-platform
done

# Start all workers (WARNING: this re-downloads standard binary!)
gcloud compute instances list --project=gnomadev --filter="name~^lr-worker" \
  --format="value(name)" | xargs \
  gcloud compute instances start --project=gnomadev --zone=us-east1-b

# Then redeploy our binary (see above)
```

### Run the Full Pipeline
```bash
# Full 24-chromosome run (index missing → load all)
gnomad-lr run

# Test with specific chromosomes
gnomad-lr run --chroms chr22

# Skip indexing (if .tbi files exist)
gnomad-lr run --chroms chr21,chr22 --skip-index

# Only index (no loading)
gnomad-lr run --index-only

# Custom region size (default 2MB)
gnomad-lr run --region-size 5000000
```

### Dashboard Access
```bash
# SSH tunnel to coordinator
gcloud compute ssh lr-coordinator --project=gnomadev --zone=us-east1-b \
  --tunnel-through-iap -- -L 3000:localhost:3000 -N -f

# Open http://localhost:3000/dashboard
```

### ClickHouse Monitoring
```bash
# SSH to CH VM
gcloud compute ssh gnomad-lr-data-vm --project=gnomadev --zone=us-east1-c \
  --tunnel-through-iap

# Check table sizes
clickhouse-client --query "
  SELECT table, formatReadableSize(sum(bytes_on_disk)) as size, sum(rows) as rows
  FROM system.parts WHERE active AND database='default'
  GROUP BY table ORDER BY sum(bytes_on_disk) DESC
"

# Check disk usage
df -h /data

# Check recent insert performance
clickhouse-client --query "
  SELECT query_duration_ms, written_rows, formatReadableSize(written_bytes)
  FROM system.query_log
  WHERE type='QueryFinish' AND query_kind='Insert'
  ORDER BY event_time DESC LIMIT 10
"
```

## Troubleshooting

### Workers have wrong binary
**Symptom:** `Failed to deserialize job spec from payload: missing field 'type'`
**Cause:** Workers are running standard genohype instead of gnomad-lr
**Fix:** Redeploy (see Deploy section above). Verify with:
```bash
gcloud compute ssh lr-worker-0 ... --command="/usr/local/bin/genohype --help | head -1"
# Should say: "gnomAD Long Read VCF loader for ClickHouse"
# NOT: "Hail Table Decoder and Converter"
```

### GCS 403 Forbidden
**Symptom:** `The operation lacked the necessary privileges to complete`
**Cause:** Worker SA doesn't have access to VCF bucket
**Fix:** Check SA is set correctly:
```bash
gcloud compute ssh lr-worker-0 ... --command="
  curl -s http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email \
    -H 'Metadata-Flavor: Google'
"
# Should return: gnomad-lr-sa@gnomadev.iam.gserviceaccount.com
```

### Tasks timeout (600s)
**Symptom:** `Partition N timed out (worker: lr-worker-X), rescheduling`
**Cause:** Task takes longer than coordinator timeout, or worker crashed
**Check worker logs:**
```bash
gcloud compute ssh lr-worker-X --project=gnomadev --zone=us-east1-b \
  --tunnel-through-iap --command="journalctl -u genohype-worker --no-pager -n 30"
```

### OOM on workers
**Symptom:** `genohype-worker.service: A process of this unit has been killed by the OOM killer`
**Cause:** VCF region too large or VcfStream buffering
**Fix:** Reduce `--region-size` or use larger VMs. With indexed streaming reader, memory should be ~2GB per task.

### Coordinator not found
**Symptom:** `No coordinator found for pool 'lr'`
**Cause:** Default gcloud project is wrong
**Fix:** `gcloud config set project gnomadev`

### ClickHouse data disk not mounted
**Symptom:** Disk full on `/` but `/data` is empty
**Fix:**
```bash
sudo mount /dev/sda /data
# Verify fstab has: /dev/sda /data ext4 defaults,nofail 0 2
```

## Performance Tuning

| Parameter | Location | Default | Notes |
|-----------|----------|---------|-------|
| Region size | `gnomad-lr run --region-size` | 2MB | Smaller = more tasks, more parallelism |
| CH batch size | `loader/variants.rs`, `loader/haplotypes.rs` | 50,000 | Larger = fewer HTTP round-trips |
| Worker count | `genohype.toml` `workers` | 32 | More workers = more GCS/CH parallelism |
| Machine type | `genohype.toml` `machine_type` | n1-standard-2 | 2 vCPU, 7.5GB RAM is sufficient |
| Task parallelism | `pool.rs` `handle_load_tasks` | All tasks in batch | Tasks within a batch run in parallel |

## Data Inventory

| Resource | Size | Location |
|----------|------|----------|
| 24 VCFs | ~40GB compressed | `gs://gnomad-lr-data/vcf/v3/` |
| Tabix indexes | ~36KB each | `gs://gnomad-lr-data/vcf/v3/*.tbi` (chr21, chr22 only so far) |
| CH lr_haplotypes | ~1.6GB (chr21+22) | `gnomad-lr-data-vm:/data/clickhouse/` |
| CH lr_variants | ~1.5GB (chr21+22) | `gnomad-lr-data-vm:/data/clickhouse/` |
| CH disk total | 200GB SSD | `/dev/sda` mounted at `/data` |
