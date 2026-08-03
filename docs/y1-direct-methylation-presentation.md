# One-shot source-haplotype methylation presentation load

This is an isolated presentation campaign, not Y1 acceptance/finalization. It writes
only `lr_y1_methylation_source_haplotype_presentation`; it creates no accepted run,
primary ledger, serving activation, browser state, or VCF-orientation join. Run only
against a newly created disposable database, with coordinator `max-retries=0`. Any
failed task invalidates the whole database; drop it and restart from zero.

## DDL

Execute [`sql/y1/lr_y1_methylation_source_haplotype_presentation.sql`](../sql/y1/lr_y1_methylation_source_haplotype_presentation.sql)
in the fresh database before submission. The stable key is the worker's versioned
SHA-256 of `(chrom,pos1,pos2,sample_id,source_haplotype)`. `pos1,pos2` retain source
BED 0-based half-open coordinates. `source_haplotype` is only the source BED label
1 or 2; the table deliberately has no orientation field.

## Manifest and request

The pinned v2 source has 231 source-present samples, two haplotype objects, and 24
canonical GRCh38 contigs: an upper-bound shape of **231 × 2 × 24 = 11,088 tasks**.
The generator reads each generation-pinned TBI header and omits contigs not actually
present, so its printed `expected_task_count_from_tbi_inventory` is the campaign's
exact expected count (11,088 only when all 462 indices name all 24 contigs).

```bash
python3 scripts/generate-y1-direct-methylation-presentation-manifest.py \
  --clickhouse-url 'http://192.168.0.15:8123/?database=<fresh_database>' \
  --output manifests/y1/direct-methylation-presentation.json
```

The read-only inventory uses `gcloud storage cat --range=0-65535` against the exact
GCS generation. For an independently archived inventory, pass
`--contig-inventory inventory.json`, whose keys are `HG00097:hap1` etc. and values
are TBI contig-name arrays.

Submit the custom manifest with this inline request payload and coordinator retries
disabled:

```json
{"action":"load_methylation_source_haplotype","batch_records":50000}
```

Every task contains its own ClickHouse URL and complete BED/TBI generation, size,
and MD5 identity. The worker validates every task and URL in an assignment before
opening a source or writing. Source open/index/decode/parse/read failures propagate;
a failed receipt reports zero accepted `items_processed`. Successful task receipts
report exact inserted rows and `vcf_orientation_joined:false`.

## Post-load checks

Require all counts below to be zero, then compare `count()` to the sum of successful
exact-job receipt `items_processed` and require the exact expected task receipt count.

```sql
SELECT count() - uniqExact(stable_key) AS duplicate_keys
FROM lr_y1_methylation_source_haplotype_presentation;

SELECT
  countIf(source_haplotype NOT IN (1, 2)) AS bad_haplotype,
  countIf(pos2 != pos1 + 1) AS bad_interval,
  countIf(NOT isFinite(methylation) OR methylation < 0 OR methylation > 100) AS bad_score,
  countIf(chrom NOT IN ('chr1','chr2','chr3','chr4','chr5','chr6','chr7','chr8',
    'chr9','chr10','chr11','chr12','chr13','chr14','chr15','chr16','chr17','chr18',
    'chr19','chr20','chr21','chr22','chrX','chrY')) AS bad_contig
FROM lr_y1_methylation_source_haplotype_presentation;

SELECT chrom, count(), min(pos1), max(pos2), uniqExact(sample_id),
       uniqExact(source_haplotype)
FROM lr_y1_methylation_source_haplotype_presentation
GROUP BY chrom ORDER BY chrom;
```

Also range-check each chromosome against its GRCh38 length (the task validator requires
`start=1, stop=contig_length`) and retain the generated manifest, request payload,
worker attestation, exact job receipts, and these aggregates as presentation artifacts.
