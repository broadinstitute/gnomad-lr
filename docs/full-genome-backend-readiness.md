# 128-worker full-genome backend readiness

This is a design/preflight record only. No pool lifecycle command, job submission,
ClickHouse request, or GCS operation is part of this preparation.

## Safe prerequisites added

- `scripts/convert-y1-full-genome-source-inventory.py` validates a checked
  post-mirror inventory and converts one contig only when both cohorts' VCF/TBI
  pairs have exact destination generations, sizes, MD5s, and canonical mirror
  URIs matching the immutable sources. A proposed URI is never existence
  evidence. The raw approved inventory and mirror evidence remain Flow artifacts,
  not repository fixtures. Converter tests build a compact synthetic 48-pair
  inventory in memory, including negative existence, identity, URI, digest, and
  MT cases.
- `scripts/generate-y1-grch38-contig-manifest.py` generates or checks an exact,
  gap-free, non-overlapping manifest for one available GRCh38 contig. Its chr22
  output is byte-for-structure compatible with the existing generator when the
  controlled-failure extension is unused. Per-contig manifests intentionally keep
  source identity, retries, and eventual finalization independently bounded.
  chrM is not in the default contig set: it is accepted only from a schema-v2
  per-contig immutable source contract with `mt_enabled: true`. MT is explicitly
  unavailable because neither cohort has an immutable chrM/chrMT VCF/TBI pair.
  The checked inventory therefore materializes exactly 24 source manifests
  (`sources/y1/primary-source-chr{1..22,X,Y}.json`), containing 96 immutable
  object identities for 48 cohort/contig pairs. Their deterministic file hashes
  are pinned in `sources/y1/primary-source-manifests.sha256`.
- `scripts/verify-worker-artifact.py` performs an offline ELF64/x86_64 check,
  requires a build identity bound to the expected full Git revision, rejects
  development/dirty provenance, and emits the binary SHA-256 for deployment
  attestation. It does not compare a deployed fleet; that check must remain a
  separate read-only preflight after an operator explicitly identifies a pool.
- `genohype.full-genome-128.toml` defines a dormant profile selected only through
  `genohype --config genohype.full-genome-128.toml`. It has 128 desired Spot
  workers, `starting_workers = 0`, private-only instances, externally managed
  firewall rules, separate coordinator/worker identities, and the distinct ops
  path `gs://gnomad-lr-data/pool-ops/full-genome-128/ops.db`.

### Offline source-manifest conversion

After an authorized mirror operation, reconcile the approved source inventory
with the completed mirror evidence outside the repository. The checked inventory
must record each destination URI, generation, size, MD5, and a true source
size/MD5 match. Convert and then re-check one contig as follows:

```bash
python3 scripts/convert-y1-full-genome-source-inventory.py \
  --inventory /path/to/refreshed-inventory.json --contig chr21 \
  --output sources/y1/primary-source-chr21.json
python3 scripts/convert-y1-full-genome-source-inventory.py \
  --inventory /path/to/refreshed-inventory.json --contig chr21 \
  --output sources/y1/primary-source-chr21.json --check
```

Conversion is offline and performs no GCS operation. It fails if either cohort
has only a proposed URI, if a destination generation is absent, if destination
size/MD5 differs from source, or if the URI differs from the Rust canonical
contract:
`gs://gnomad-lr-data/y1/sources/{cohort}/vcfs/gnomAD_LR_Y1.{cohort}.{chrom}.vcf.gz[.tbi]`.
The checked post-mirror evidence reconciles 96/96 objects and 48/48 pairs; only
the compact per-contig contracts and their hash ledger belong in the repository.

## Blocking issue: retry identity and lease fencing in pinned Genohype

Pinned revision `15ea8c387d53b150449cf109ab0005a7d8d655ca` regenerates a custom
`TaskDescriptor` from the unchanged manifest whenever a partition is assigned
(`cli/src/distributed/message.rs`, `JobSpec::generate_tasks`). Worker-death and
failure paths increment `retry_counts` and put the same partition index back on
`pending_partitions` (`cli/src/distributed/coordinator/state.rs` and
`coordinator/mod.rs`). Neither `TaskDescriptor` nor `CompleteRequest` contains an
assignment attempt/lease token. Completion checks only coordinator `session_id`,
not the current assignment. Therefore a retry receives the original
`attempt_id`, and a late worker from the same coordinator session can still
complete a reassigned task.

The backend's current uncommitted claim fencing correctly fails closed on reuse
of an immutable `attempt_id`; it cannot make the coordinator issue a new ID or
reject a stale completion. A 128-worker Spot run must remain blocked until the
pinned Genohype revision is updated.

### Bounded upstream implementation

1. Add `assignment_attempt: u32` and a random `lease_token: String` to assigned
   task descriptors (or an assignment envelope), and echo both in
   `CompleteRequest` and heartbeat/current-task state.
2. Persist current assignment `(partition, worker, attempt, lease_token)` in
   coordinator state. Increment attempt atomically on every assignment,
   including worker-death, timeout, and explicit failure requeues.
3. For manifest-backed custom jobs, pass coordinator assignment metadata outside
   the deny-unknown-fields domain payload, then have gnomad-lr derive the durable
   attempt ID from `(manifest attempt prefix, assignment_attempt, lease_token)`.
4. Accept completion and heartbeat ownership only when worker, attempt, token,
   partition, and session all match the current assignment. Stale reports are
   acknowledged as stale but must not change completed/pending state.
5. Add coordinator tests for failure requeue, worker-death requeue, late
   completion after reassignment, coordinator restart/session change, and unique
   attempt IDs across all assignments. Then pin both `genohype-core` and
   `genohype-pool` plus the installed CLI to that same reviewed revision.

## Ownership boundary

At preflight the worktree already had modifications to `Makefile`,
`genohype.toml`, `src/{cli,main,pool}.rs`, and the Y1
finalizer/interval/storage modules, plus unrelated methylation and publication
work and a deleted skill. Those files were preserved and not edited here.
Consequently generic Rust finalization remains a prerequisite: parameterize the
current chr22 length/chromosome checks and publication request by the canonical
GRCh38 contig while retaining the existing chr22 command behavior and tests.
This must be integrated by the owner of the active finalizer changes.
