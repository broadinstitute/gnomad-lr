#!/usr/bin/env bash
# Reproduce the clean-checkout build checks used by CI.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/verify-pins.py
python3 scripts/verify-manifests.py
python3 scripts/verify-y1-primary-motif-foundation.py
python3 scripts/test-verify-y1-primary-motif-foundation.py
python3 scripts/test-generate-y1-chr22-manifest.py
python3 scripts/test-convert-y1-full-genome-source-inventory.py
python3 scripts/test-generate-y1-grch38-contig-manifest.py
python3 scripts/test-generate-y1-phased-mirror-canary.py
python3 scripts/test-generate-y1-direct-methylation-presentation-manifest.py
python3 scripts/test-reconcile-y1-chr22-source.py
python3 scripts/test-reconcile-y1-contig-source.py
python3 scripts/test-verify-y1-chr22-signatures.py
python3 scripts/test-verify-worker-artifact.py
python3 scripts/test-y1-fixed-commands.py
python3 scripts/generate-y1-chr22-manifest.py --source-manifest sources/y1/primary-source-manifest.json --cohort hgsvc_hprc --run-id ignored-when-checking --attempt ignored-when-checking --output manifests/y1/hgsvc-hprc-chr22-1mb.json --check
python3 scripts/generate-y1-chr22-manifest.py --source-manifest sources/y1/primary-source-manifest.json --cohort aou --run-id ignored-when-checking --attempt ignored-when-checking --output manifests/y1/aou-chr22-1mb.json --check
python3 scripts/generate-y1-chr22-manifest.py --source-manifest sources/y1/primary-source-manifest.json --cohort hgsvc_hprc --run-id ignored-when-checking --attempt ignored-when-checking --output manifests/y1/hgsvc-hprc-chr22-1mb-r3.json --check
python3 scripts/generate-y1-chr22-manifest.py --source-manifest sources/y1/primary-source-manifest.json --cohort aou --run-id ignored-when-checking --attempt ignored-when-checking --output manifests/y1/aou-chr22-1mb-r3.json --check
python3 scripts/generate-y1-phased-mirror-canary.py --check
cargo metadata --locked --format-version 1 --no-deps >/dev/null
cargo test --locked --features clickhouse
cargo build --locked --features clickhouse

target/debug/gnomad-lr --version
target/debug/gnomad-lr --help >/dev/null
target/debug/gnomad-lr init --help >/dev/null
target/debug/gnomad-lr init-y1 --help >/dev/null
if target/debug/gnomad-lr init-y1 \
    --endpoint http://127.0.0.1:8123 \
    --database default \
    --target-kind scratch \
    --auth-source none >/dev/null 2>&1; then
  echo "Y1 target safety check accepted the default database" >&2
  exit 1
fi
if target/debug/gnomad-lr init-y1 \
    --endpoint 'http://127.0.0.1:8123/?database=gnomad_lr_y1_scratch_bad' \
    --database gnomad_lr_y1_scratch_bad \
    --target-kind scratch \
    --auth-source none >/dev/null 2>&1; then
  echo "Y1 target safety check accepted a database embedded in the endpoint" >&2
  exit 1
fi
if target/debug/gnomad-lr init-y1 \
    --endpoint http://192.0.2.1:8123 \
    --database gnomad_lr_y1_scratch_bad \
    --target-kind scratch \
    --auth-source none >/dev/null 2>&1; then
  echo "Y1 target safety check accepted an unauthenticated remote endpoint" >&2
  exit 1
fi
python3 scripts/smoke.py --no-build --dry-run >/dev/null
if python3 scripts/smoke.py --no-build --dry-run --database default >/dev/null 2>&1; then
  echo "smoke safety check accepted the default database" >&2
  exit 1
fi
if python3 scripts/smoke.py --no-build --dry-run \
    --clickhouse-url http://192.0.2.1:8123 >/dev/null 2>&1; then
  echo "smoke safety check accepted a remote host without --allow-remote" >&2
  exit 1
fi

echo "All gnomad-lr verification checks passed."
