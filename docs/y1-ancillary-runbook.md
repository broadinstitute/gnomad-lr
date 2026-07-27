# Y1 ancillary candidate runbook

This runbook documents the isolated contract and its safety gates. The current
source inventory authorizes **no serving input**, so activation steps are design
only until provenance and acceptance reports change an entry to
`allowed_serving_mode: accepted_y1`. Legacy commands and tables are unchanged.

## 1. Verify inventory and initialize an isolated target

```bash
python3 scripts/verify-y1-ancillary-manifests.py

gnomad-lr init-y1 \
  --endpoint http://127.0.0.1:8123 \
  --database gnomad_lr_y1_scratch_ancillary \
  --target-kind scratch --auth-source none
```

`init-y1` creates the ancillary ledgers, per-modality staging/canonical tables,
and pointer table in addition to the primary and metadata Y1 tables. It does not
alter or initialize the unscoped legacy ancillary tables.

## 2. Candidate identity

Choose a new immutable `ancillary_run_id` for each modality/source version.
Record in `lr_y1_ancillary_runs`:

- release, cohort, reference genome, modality, source version, manifest hash;
- scope (`interval`, `full_chromosome`, or `full_release`);
- source/canonical/reject counts, content hash, timing, and Linux peak RSS.

Each task retry gets a new `attempt_id` in
`lr_y1_ancillary_task_attempts`. Never delete or overwrite an attempt. Exactly
one accepted attempt must exist per task before canonical materialization.
Candidate rows include all identity columns and remain inactive.

## 3. Required loader behavior (not yet implemented)

The next loader increment must add explicit Y1 commands; do not adapt the
legacy `load coverage|methylation|histograms` commands operationally.

- Resolve the source by manifest ID, verify generation, byte size, checksum,
  cohort, schema, coordinate convention, and sidecar before reading.
- Require the existing Y1 target/auth safety contract and a scratch target for
  bounded acceptance.
- Reject malformed rows with structured codes; do not continue with zero values.
- For methylation, derive `sample_id` only from the manifest entry and require
  the adjacent generation-pinned TBI.
- Emit stable row/key/content hashes, rejects, elapsed time, and Linux peak RSS.

## 4. Acceptance ladder

For `chr22:20,000,000-20,010,000`, independently derive source rows and samples,
load every available candidate, and require exact reconciliation and zero
unexplained rejects. Repeat under a distinct run ID and compare keys, counts,
and hashes. Run the representative 1 Mb interval where feasible. For
methylation, reconcile detail and summary sample counts per position and prove
all 60 absent roster samples remain unavailable.

Before full-chr22 publication, repeat the full chromosome, inject a failed task
and accepted retry, verify accepted-attempt selection, and record actual Linux
RSS. This gate does not authorize other contigs.

## 5. Materialize, activate, and roll back

A single publisher copies only accepted-attempt rows into the inactive canonical
run partition, builds methylation summary rows from that same detail run, and
reconciles exact counts and content hashes. Repeat materialization must be
idempotent.

Activation is one append-only pointer revision in `lr_y1_active_ancillary` per
modality. A guarded publisher must reject activation unless the run is accepted,
full scope is appropriate, source identity is immutable and authorized, counts
and hashes match, rejects are zero or explicitly adjudicated, and the target is
a serving-class Y1 database. The current repository deliberately exposes no
ancillary activation CLI yet.

Rollback appends a later pointer revision naming a previously accepted run; it
does not mutate either run. Query, export, density, and cache paths must all
resolve the same latest pointer. Rehearse pointer A → B → A and compare every
path before production enablement.

## 6. AoU and browser smoke

AoU must resolve coverage, methylation, outliers, summaries, histograms, and
sample tracks as unavailable without touching an HGSVC/HPRC canonical table.
Exercise HGSVC/HPRC → AoU → HGSVC/HPRC in region, gene, and variant views and
assert that request/cache identities differ. Missing tracks show explicit
unavailable guidance; methylation also retains the existing large-region zoom
guidance. When `LR_Y1_ENABLED=false`, run the legacy smoke suite unchanged.
