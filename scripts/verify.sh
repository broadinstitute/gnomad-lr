#!/usr/bin/env bash
# Reproduce the clean-checkout build checks used by CI.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 scripts/verify-pins.py
cargo metadata --locked --format-version 1 --no-deps >/dev/null
cargo test --locked --features clickhouse
cargo build --locked --features clickhouse

target/debug/gnomad-lr --version
target/debug/gnomad-lr --help >/dev/null
target/debug/gnomad-lr init --help >/dev/null
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
