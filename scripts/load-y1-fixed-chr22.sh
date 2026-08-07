#!/usr/bin/env bash
# Load the fixed disposable Y1 chr22 database through the proven Genohype pool path.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB="gnomad_lr_y1_scratch_v5_current"
PRINCIPAL="gnomad_lr_y1_pool_writer"
POOL="lr"
PROJECT="gnomadev"
POOL_ZONE="us-east1-b"
CH_VM="gnomad-lr-y1-clickhouse"
CH_ZONE="us-east1-c"
CH_LOCAL="http://127.0.0.1:8126"
CH_PRIVATE="http://192.168.0.15:8123"
GENOHYPE_BIN="${GENOHYPE_BIN:-}"
GENOHYPE_WORKER_BIN="${GENOHYPE_WORKER_BIN:-}"
MAX_WORKERS=8
MAX_SCALE_ATTEMPTS=5
TIMEOUT_SECONDS=7200
EXECUTE=false
CONFIRM=""
ARTIFACTS=""
OPERATOR="${USER:-operator}@$(hostname -s 2>/dev/null || printf unknown)"

usage() {
  cat <<EOF
Usage: scripts/load-y1-fixed-chr22.sh [OPTIONS]

Dry-run is the default. The execution path creates and fills only $DB.

Options:
  --execute                         perform cloud mutations
  --confirm-empty-fixed-database $DB
                                    required with --execute
  --max-workers N                  post-gate pool size (default: 8; maximum: 8)
  --timeout-seconds N               receipt polling timeout per phase (default: 7200)
  --artifacts DIR                   receipt directory (default: a new directory in /tmp)
  --operator ID                     identity persisted by finalization
  -h, --help                        show this help

Environment:
  GENOHYPE_BIN                       genohype executable override
  GENOHYPE_WORKER_BIN                genohype-worker executable override

Executable overrides must resolve to regular executable files. When unset, the
commands are discovered on PATH. The command refuses an existing database,
principal, pool, pool firewall, or ops
prefix. It scales one worker first for each cohort, validates an accepted exact-job
receipt, then scales to N. On success it preserves manifests, job receipts,
finalization reports, metadata report, checkpoints, and a summary in DIR.
EOF
}

while (($#)); do
  case "$1" in
    --execute) EXECUTE=true; shift ;;
    --confirm-empty-fixed-database) CONFIRM="${2:-}"; shift 2 ;;
    --max-workers) MAX_WORKERS="${2:-}"; shift 2 ;;
    --timeout-seconds) TIMEOUT_SECONDS="${2:-}"; shift 2 ;;
    --artifacts) ARTIFACTS="${2:-}"; shift 2 ;;
    --operator) OPERATOR="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$MAX_WORKERS" =~ ^[1-8]$ ]] || { echo "--max-workers must be an integer from 1 through 8" >&2; exit 2; }
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || { echo "--timeout-seconds must be a positive integer" >&2; exit 2; }
[[ -n "$OPERATOR" ]] || { echo "--operator must not be empty" >&2; exit 2; }

cat <<EOF
Y1 fixed chr22 load plan
  mode:            $($EXECUTE && printf execute || printf dry-run)
  database:        $DB (must not exist)
  principal:       $PRINCIPAL (must not exist)
  cohorts:         HGSVC/HPRC chr22 + AoU chr22
  metadata:        exactly 292 HGSVC/HPRC samples
  pool:            $POOL, 0 -> 1 gate -> $MAX_WORKERS -> 0, once per cohort
  receipts:        exact job ID, 51/51 accepted, 0 failed attempts, 0 rejects
  cleanup:         destroy pool/firewall/ops prefix; drop disposable principal
  retained:        accepted fixed database and run/finalization/metadata receipts
EOF

if ! $EXECUTE; then
  echo "DRY-RUN: no build, network request, cloud command, or local write was performed."
  echo "Execute only after dropping any prior fixed database:"
  echo "  $0 --execute --confirm-empty-fixed-database $DB --artifacts /path/to/new/receipts"
  exit 0
fi

[[ "$CONFIRM" == "$DB" ]] || {
  echo "--execute requires --confirm-empty-fixed-database $DB" >&2
  exit 2
}

resolve_executable() {
  local env_name="$1" command_name="$2" configured="$3" candidate
  if [[ -n "$configured" ]]; then
    candidate="$configured"
  else
    candidate="$(command -v "$command_name" 2>/dev/null || true)"
    [[ -n "$candidate" ]] || {
      echo "missing executable: set $env_name or put $command_name on PATH" >&2
      return 1
    }
  fi
  if [[ "$candidate" != */* ]]; then
    candidate="$(command -v "$candidate" 2>/dev/null || true)"
  fi
  [[ -n "$candidate" && -f "$candidate" && -x "$candidate" ]] || {
    echo "$env_name does not resolve to a regular executable file" >&2
    return 1
  }
  if [[ "$candidate" != /* ]]; then
    candidate="$(cd -P "$(dirname "$candidate")" && pwd)/$(basename "$candidate")"
  fi
  printf '%s\n' "$candidate"
}

for command in curl gcloud git make python3 shasum; do
  command -v "$command" >/dev/null || { echo "missing command: $command" >&2; exit 1; }
done
GH="$(resolve_executable GENOHYPE_BIN genohype "$GENOHYPE_BIN")"
COORD_BIN="$(resolve_executable GENOHYPE_WORKER_BIN genohype-worker "$GENOHYPE_WORKER_BIN")"

cd "$ROOT"
[[ -z "$(git status --porcelain)" ]] || { echo "repository must be clean" >&2; exit 1; }
REV="$(git rev-parse HEAD)"
BUILD="gnomad-lr/$REV/x86_64-linux-release/features-clickhouse"
TOKEN="$(date -u +%Y%m%dT%H%M%SZ)-$(python3 - <<'PY'
import secrets
print(secrets.token_hex(4))
PY
)"
H_RUN="y1-hgsvc-hprc-chr22-$TOKEN"
A_RUN="y1-aou-chr22-$TOKEN"
M_RUN="y1-hgsvc-hprc-metadata-$TOKEN"
OPS_PREFIX="gs://gnomad-lr-data/pool-ops/fixed-y1-$TOKEN"
ARTIFACTS="${ARTIFACTS:-${TMPDIR:-/tmp}/gnomad-lr-fixed-y1-$TOKEN}"
[[ ! -e "$ARTIFACTS" ]] || { echo "artifact path already exists: $ARTIFACTS" >&2; exit 1; }
mkdir -p "$ARTIFACTS"
chmod 700 "$ARTIFACTS"
CHECKPOINTS="$ARTIFACTS/checkpoints.log"
CFG="$ARTIFACTS/genohype.toml"
PAYLOAD="$ARTIFACTS/payload.json"
H_MANIFEST="$ARTIFACTS/hgsvc-manifest.json"
A_MANIFEST="$ARTIFACTS/aou-manifest.json"
H_COUNTS="$ARTIFACTS/hgsvc-independent-counts.json"
A_COUNTS="$ARTIFACTS/aou-independent-counts.json"
CH_TUNNEL_PID=""
COORD_TUNNEL_PID=""
POOL_CREATED=false

checkpoint() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$CHECKPOINTS"; }

stop_tunnels() {
  for pid in "$COORD_TUNNEL_PID" "$CH_TUNNEL_PID"; do
    [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true
  done
}

on_exit() {
  status=$?
  if ((status != 0)); then
    checkpoint "FAILED status=$status; artifacts=$ARTIFACTS"
    if $POOL_CREATED; then
      "$GH" --config "$CFG" pool scale "$POOL" --workers 0 >>"$ARTIFACTS/failure-scale-zero.log" 2>&1 || true
    fi
  fi
  stop_tunnels
  exit "$status"
}
trap on_exit EXIT INT TERM

wait_http() {
  local url="$1" tries=60
  while ((tries--)); do
    if curl --silent --fail --max-time 2 "$url" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "endpoint did not become ready: $url" >&2
  return 1
}

ch_query() {
  curl --silent --show-error --fail-with-body --data-binary "$1" "$CH_LOCAL/"
}

# New GCE workers can report ready before their SSH endpoint accepts the binary
# upload. A failed scale may also leave some workers running, so always return to
# zero before retrying; this is the same fail-safe transition used by on_exit.
scale_workers_with_retry() {
  local workers="$1" label="$2" log="$3" attempt=1
  : >"$log"
  while ((attempt <= MAX_SCALE_ATTEMPTS)); do
    if "$GH" --config "$CFG" pool scale "$POOL" --workers "$workers" >>"$log" 2>&1; then
      return 0
    fi
    checkpoint "LOAD $label scale workers=$workers failed attempt=$attempt/$MAX_SCALE_ATTEMPTS"
    if ((attempt == MAX_SCALE_ATTEMPTS)); then
      return 1
    fi
    if ! "$GH" --config "$CFG" pool scale "$POOL" --workers 0 >>"$log" 2>&1; then
      echo "could not reset pool to zero after failed scale" >&2
      return 1
    fi
    checkpoint "LOAD $label scale retry reset workers=0"
    sleep $((attempt * 15))
    ((attempt += 1))
  done
}

write_pool_config() {
  cat >"$CFG" <<EOF
[defaults]
project = "$PROJECT"
zone = "$POOL_ZONE"
network = "gnomad-v4-dev"

[pools.$POOL]
machine_type = "n1-standard-2"
spot = true
starting_workers = 0
workers = $MAX_WORKERS
with_coordinator = true
worker_binary = "target/release/gnomad-lr-worker"
subnet = "gnomad-v4-dev-main"
pool_db_path = "$OPS_PREFIX/ops.db"
service_account = "gnomad-lr-sa@gnomadev.iam.gserviceaccount.com"
EOF
}

validate_receipts() {
  local file="$1" mode="$2" job="$3" run="$4" cohort="$5" manifest="$6"
  python3 - "$file" "$mode" "$job" "$run" "$cohort" "$BUILD" "$PRINCIPAL" "$manifest" <<'PY'
import json, sys
path, mode, job, run, cohort, build, principal, manifest_path = sys.argv[1:]
try:
    d = json.load(open(path))
except Exception:
    raise SystemExit(2)
manifest = {task["task_id"]: task for task in json.load(open(manifest_path))}
if d.get("job_id") != job or not d.get("job_found"):
    raise SystemExit("receipt response is not bound to the requested exact job")
if d.get("failed_attempt_count", 0) != 0:
    raise SystemExit("job has failed attempts")
receipts = d.get("receipts", [])
for receipt in receipts:
    if receipt.get("terminal_status") != "accepted" or receipt.get("worker_build_version") != build:
        raise SystemExit("terminal receipt has wrong status or worker build")
    attempts = receipt.get("report", {}).get("result_json", {}).get("attempts", [])
    if not attempts:
        raise SystemExit("terminal receipt lacks a Y1 attempt report")
    for attempt in attempts:
        if (attempt.get("run_id"), attempt.get("cohort"), attempt.get("worker_build_version"), attempt.get("worker_principal")) != (run, cohort, build, principal):
            raise SystemExit("terminal attempt identity mismatch")
        if attempt.get("state") != "accepted" or attempt.get("failure") is not None or attempt.get("counts", {}).get("rejects") != 0:
            raise SystemExit("terminal attempt was not cleanly accepted")
        task = manifest.get(attempt.get("task_id"))
        identity_fields = ("run_id", "cohort", "chrom", "start", "stop", "source_uri", "source_generation", "source_size_bytes", "source_checksum", "source_checksum_algorithm", "source_index_uri", "source_index_generation", "source_index_size_bytes", "source_index_checksum", "source_index_checksum_algorithm")
        if task is None or any(attempt.get(field) != task.get(field) for field in identity_fields):
            raise SystemExit("terminal attempt source/interval identity differs from the checked manifest")
if mode == "gate":
    if d.get("accepted_count", 0) < 1:
        raise SystemExit(2)
else:
    expected = (True, "completed", 51, 51, 51, 0)
    actual = (d.get("complete"), d.get("job_status"), d.get("expected_task_count"), d.get("accepted_count"), d.get("terminal_receipt_count"), d.get("failed_attempt_count"))
    if actual != expected:
        raise SystemExit(2)
PY
}

poll_receipts() {
  local mode="$1" job="$2" run="$3" cohort="$4" manifest="$5" output="$6"
  local deadline=$((SECONDS + TIMEOUT_SECONDS)) tmp="$output.tmp"
  while ((SECONDS < deadline)); do
    rm -f "$tmp"
    "$GH" --config "$CFG" pool receipts "$POOL" --job-id "$job" >"$tmp" 2>"$output.err" || true
    if validate_receipts "$tmp" "$mode" "$job" "$run" "$cohort" "$manifest"; then
      mv "$tmp" "$output"
      return 0
    else
      rc=$?
      if ((rc != 2)); then return "$rc"; fi
    fi
    sleep 10
  done
  echo "timed out waiting for $mode receipts for job $job" >&2
  return 1
}

new_job_id() {
  local before="$1" after="$2"
  python3 - "$before" "$after" <<'PY'
import json, sys
before = {x["job_id"] for x in json.load(open(sys.argv[1]))}
after = [x for x in json.load(open(sys.argv[2])) if x.get("job_id") not in before and x.get("input_path") == "custom" and x.get("total_tasks") == 51]
if len(after) != 1:
    raise SystemExit(f"expected exactly one new 51-task custom job, found {len(after)}")
print(after[0]["job_id"])
PY
}

submit_and_run() {
  local label="$1" run="$2" cohort="$3" manifest="$4"
  local before="$ARTIFACTS/$label-jobs-before.json" after="$ARTIFACTS/$label-jobs-after.json"
  curl --silent --show-error --fail http://127.0.0.1:3000/api/history/jobs >"$before"
  local payload_json
  payload_json="$(tr -d '\n' <"$PAYLOAD")"
  "$GH" --config "$CFG" pool submit "$POOL" --batch-size 1 -- \
    custom --payload "$payload_json" --manifest "$manifest" >"$ARTIFACTS/$label-submit.log" 2>&1
  local job="" history_tries=30
  while ((history_tries--)); do
    curl --silent --show-error --fail http://127.0.0.1:3000/api/history/jobs >"$after"
    if job="$(new_job_id "$before" "$after" 2>/dev/null)"; then break; fi
    job=""
    sleep 2
  done
  [[ -n "$job" ]] || { echo "could not identify the sole new exact job after submission" >&2; return 1; }
  printf '%s\n' "$job" >"$ARTIFACTS/$label-job-id.txt"
  checkpoint "LOAD $label submitted job=$job workers=0 tasks=51"

  scale_workers_with_retry 1 "$label" "$ARTIFACTS/$label-scale-1.log"
  poll_receipts gate "$job" "$run" "$cohort" "$manifest" "$ARTIFACTS/$label-one-worker-gate.json"
  checkpoint "LOAD $label one-worker gate accepted job=$job"

  scale_workers_with_retry "$MAX_WORKERS" "$label" "$ARTIFACTS/$label-scale-$MAX_WORKERS.log"
  poll_receipts complete "$job" "$run" "$cohort" "$manifest" "$ARTIFACTS/$label-durable-receipts.json"
  checkpoint "LOAD $label complete job=$job accepted=51/51 failed_attempts=0 rejects=0"
  "$GH" --config "$CFG" pool scale "$POOL" --workers 0 >"$ARTIFACTS/$label-scale-0.log" 2>&1
}

checkpoint "PREPARE start token=$TOKEN revision=$REV artifacts=$ARTIFACTS"
make release worker >"$ARTIFACTS/build.log" 2>&1
WORKER_SHA="$(shasum -a 256 target/release/gnomad-lr-worker | awk '{print $1}')"
python3 scripts/verify-worker-artifact.py \
  --binary target/release/gnomad-lr-worker \
  --expected-revision "$REV" --expected-build-identity "$BUILD" \
  --expected-sha256 "$WORKER_SHA" --report "$ARTIFACTS/worker-attestation.json" >/dev/null

python3 scripts/generate-y1-chr22-manifest.py --source-manifest sources/y1/primary-source-manifest.json \
  --cohort hgsvc_hprc --run-id "$H_RUN" --attempt "$TOKEN-hgsvc-a1" --output "$H_MANIFEST"
python3 scripts/generate-y1-chr22-manifest.py --source-manifest sources/y1/primary-source-manifest.json \
  --cohort aou --run-id "$A_RUN" --attempt "$TOKEN-aou-a1" --output "$A_MANIFEST"
python3 scripts/reconcile-y1-chr22-source.py --source-manifest sources/y1/primary-source-manifest.json \
  --cohort hgsvc_hprc --run-id "$H_RUN" --evidence-uri "artifact://$TOKEN/hgsvc-independent-counts" \
  --producer "scripts/reconcile-y1-chr22-source.py@$REV" --output "$H_COUNTS" >"$ARTIFACTS/hgsvc-reconcile.log"
python3 scripts/reconcile-y1-chr22-source.py --source-manifest sources/y1/primary-source-manifest.json \
  --cohort aou --run-id "$A_RUN" --evidence-uri "artifact://$TOKEN/aou-independent-counts" \
  --producer "scripts/reconcile-y1-chr22-source.py@$REV" --output "$A_COUNTS" >"$ARTIFACTS/aou-reconcile.log"
write_pool_config
cat >"$PAYLOAD" <<EOF
{"action":"load_y1_interval","target":{"endpoint":"$CH_PRIVATE","database":"$DB","authentication":"named_passwordless_private_user","worker_principal":"$PRINCIPAL"},"batch_records":250}
EOF
checkpoint "PREPARE build and independent counts complete"

# Refuse to collide with any prior fixed run before creating anything.
gcloud compute ssh "$CH_VM" --project "$PROJECT" --zone "$CH_ZONE" --tunnel-through-iap -- \
  -N -L 8126:localhost:8123 >"$ARTIFACTS/clickhouse-tunnel.log" 2>&1 &
CH_TUNNEL_PID=$!
wait_http "$CH_LOCAL/?query=SELECT%201"
[[ "$(ch_query "SELECT count() FROM system.databases WHERE name = '$DB' FORMAT TabSeparated")" == "0" ]] || { echo "$DB already exists; drop it explicitly or keep the accepted live database" >&2; exit 1; }
[[ "$(ch_query "SELECT count() FROM system.users WHERE name = '$PRINCIPAL' FORMAT TabSeparated")" == "0" ]] || { echo "$PRINCIPAL already exists" >&2; exit 1; }
[[ -z "$(gcloud compute instances list --project "$PROJECT" --filter="name~'^${POOL}-'" --format='value(name)')" ]] || { echo "pool instances already exist" >&2; exit 1; }
[[ -z "$(gcloud compute disks list --project "$PROJECT" --filter="name~'^${POOL}-'" --format='value(name)')" ]] || { echo "pool disks already exist" >&2; exit 1; }
[[ -z "$(gcloud compute firewall-rules list --project "$PROJECT" --filter="name='allow-hail-coord-int-${POOL}'" --format='value(name)')" ]] || { echo "pool firewall already exists" >&2; exit 1; }
[[ -z "$(gcloud storage ls "$OPS_PREFIX/**" 2>/dev/null || true)" ]] || { echo "ops prefix already exists: $OPS_PREFIX" >&2; exit 1; }

ch_query "CREATE DATABASE $DB" >/dev/null
target/release/gnomad-lr init-y1 --endpoint "$CH_LOCAL" --database "$DB" --target-kind scratch --auth-source none >"$ARTIFACTS/init-y1.log" 2>&1
ch_query "CREATE USER $PRINCIPAL IDENTIFIED WITH no_password SETTINGS async_insert = 0" >/dev/null
ch_query "GRANT SELECT, INSERT ON $DB.* TO $PRINCIPAL" >/dev/null
[[ "$(curl --silent --show-error --fail -H "X-ClickHouse-User: $PRINCIPAL" --data-binary "SELECT currentUser(), getSetting('readonly'), getSetting('async_insert') FORMAT TabSeparated" "$CH_LOCAL/")" == "$PRINCIPAL"$'\t'"0"$'\t'"false" ]] || { echo "writer attestation failed" >&2; exit 1; }
checkpoint "LOAD database initialized and writer attested"

"$GH" --config "$CFG" pool create "$POOL" --workers 0 --wait >"$ARTIFACTS/pool-create.log" 2>&1
POOL_CREATED=true
"$GH" --config "$CFG" pool update-binary "$POOL" --binary "$COORD_BIN" \
  --worker-binary target/release/gnomad-lr-worker --skip-build >"$ARTIFACTS/pool-update-binary.log" 2>&1
checkpoint "LOAD pool created and binaries updated workers=0 worker_sha=$WORKER_SHA"

gcloud compute ssh "${POOL}-coordinator" --project "$PROJECT" --zone "$POOL_ZONE" --tunnel-through-iap -- \
  -N -L 3000:localhost:3000 >"$ARTIFACTS/coordinator-tunnel.log" 2>&1 &
COORD_TUNNEL_PID=$!
wait_http http://127.0.0.1:3000/api/history/jobs

submit_and_run hgsvc "$H_RUN" hgsvc_hprc "$H_MANIFEST"
submit_and_run aou "$A_RUN" aou "$A_MANIFEST"
checkpoint "FINALIZE start workers=0"

target/release/gnomad-lr finalize-y1-chr22 --endpoint "$CH_LOCAL" --database "$DB" \
  --target-kind scratch --auth-source none --worker-principal "$PRINCIPAL" \
  --worker-auth-source passwordless-user --manifest "$H_MANIFEST" --independent-counts "$H_COUNTS" \
  --operator-identity "$OPERATOR" --report "$ARTIFACTS/hgsvc-finalization.json" \
  >"$ARTIFACTS/hgsvc-finalization.log" 2>&1
checkpoint "FINALIZE hgsvc accepted_frozen run=$H_RUN"
target/release/gnomad-lr finalize-y1-chr22 --endpoint "$CH_LOCAL" --database "$DB" \
  --target-kind scratch --auth-source none --worker-principal "$PRINCIPAL" \
  --worker-auth-source passwordless-user --manifest "$A_MANIFEST" --independent-counts "$A_COUNTS" \
  --operator-identity "$OPERATOR" --report "$ARTIFACTS/aou-finalization.json" \
  >"$ARTIFACTS/aou-finalization.log" 2>&1
checkpoint "FINALIZE aou accepted_frozen run=$A_RUN"

target/release/gnomad-lr reconcile-y1-metadata --endpoint "$CH_LOCAL" --database "$DB" \
  --target-kind scratch --auth-source none --metadata-run-id "$M_RUN" \
  --source-manifest sources/y1/metadata-source-manifest.json --report "$ARTIFACTS/metadata.json" \
  --publisher-identity "$OPERATOR" --carrier-run-id "$H_RUN" >"$ARTIFACTS/metadata.log" 2>&1

python3 - "$ARTIFACTS/hgsvc-finalization.json" "$ARTIFACTS/aou-finalization.json" "$ARTIFACTS/metadata.json" <<'PY'
import json, sys
expected = {
    "hgsvc_hprc": {"source_records":808853,"summaries":808853,"alleles":1046072,"frequencies":21967512,"carriers":38285467,"rejects":0},
    "aou": {"source_records":1166762,"summaries":1166762,"alleles":3152223,"frequencies":18913338,"carriers":0,"rejects":0},
}
for path in sys.argv[1:3]:
    d=json.load(open(path)); cohort=d["cohort"]
    if not d.get("accepted") or not d.get("frozen") or d.get("published") or d.get("manifest_tasks") != 51 or d.get("failed_attempts"):
        raise SystemExit(f"invalid finalization receipt: {path}")
    if d.get("expected_counts") != expected[cohort]:
        raise SystemExit(f"unexpected exact counts: {path}")
m=json.load(open(sys.argv[3]))
if m.get("counts",{}).get("roster_rows") != 292 or len(m.get("rows",[])) != 292:
    raise SystemExit("metadata receipt does not contain exactly 292 roster rows")
joins=m.get("carrier_joins",[])
if len(joins)!=1 or joins[0].get("distinct_carrier_samples")!=292 or joins[0].get("unmatched_samples")!=0 or joins[0].get("one_to_many_samples")!=0:
    raise SystemExit("metadata carrier join receipt failed")
PY
checkpoint "FINALIZE metadata accepted rows=292 run=$M_RUN"

# Successful disposable infrastructure cleanup. The fixed accepted database remains.
"$GH" --config "$CFG" pool destroy "$POOL" >"$ARTIFACTS/pool-destroy.log" 2>&1
POOL_CREATED=false
if gcloud compute firewall-rules describe "allow-hail-coord-int-${POOL}" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules delete "allow-hail-coord-int-${POOL}" --project "$PROJECT" --quiet >"$ARTIFACTS/firewall-delete.log" 2>&1
fi
if [[ -n "$(gcloud storage ls "$OPS_PREFIX/**" 2>/dev/null || true)" ]]; then
  gcloud storage rm --recursive "$OPS_PREFIX" >"$ARTIFACTS/ops-prefix-delete.log" 2>&1
fi
[[ -z "$(gcloud compute instances list --project "$PROJECT" --filter="name~'^${POOL}-'" --format='value(name)')" ]] || { echo "pool instances remain after destroy" >&2; exit 1; }
[[ -z "$(gcloud compute disks list --project "$PROJECT" --filter="name~'^${POOL}-'" --format='value(name)')" ]] || { echo "pool disks remain after destroy" >&2; exit 1; }
[[ -z "$(gcloud compute firewall-rules list --project "$PROJECT" --filter="name='allow-hail-coord-int-${POOL}'" --format='value(name)')" ]] || { echo "pool firewall remains after cleanup" >&2; exit 1; }
[[ -z "$(gcloud storage ls "$OPS_PREFIX/**" 2>/dev/null || true)" ]] || { echo "ops prefix remains after cleanup" >&2; exit 1; }
ch_query "DROP USER IF EXISTS $PRINCIPAL" >/dev/null
checkpoint "CLEANUP pool/firewall/ops prefix/writer removed; database retained"

python3 - "$ARTIFACTS/summary.json" <<PY
import json, pathlib, sys
summary={
 "schema_version":1,"database":"$DB","token":"$TOKEN","backend_revision":"$REV",
 "worker_build":"$BUILD","worker_sha256":"$WORKER_SHA","max_workers":$MAX_WORKERS,
 "runs":{"hgsvc_hprc":"$H_RUN","aou":"$A_RUN","metadata":"$M_RUN"},
 "jobs":{"hgsvc_hprc":pathlib.Path("$ARTIFACTS/hgsvc-job-id.txt").read_text().strip(),"aou":pathlib.Path("$ARTIFACTS/aou-job-id.txt").read_text().strip()},
 "counts":{"hgsvc_hprc":{"source_records":808853,"summaries":808853,"alleles":1046072,"frequencies":21967512,"carriers":38285467,"rejects":0},"aou":{"source_records":1166762,"summaries":1166762,"alleles":3152223,"frequencies":18913338,"carriers":0,"rejects":0},"metadata_rows":292},
 "cleanup":{"pool_destroyed":True,"ops_prefix":"deleted","principal":"dropped","database":"retained"}
}
pathlib.Path(sys.argv[1]).write_text(json.dumps(summary,indent=2)+"\n")
PY
checkpoint "COMPLETE database=$DB artifacts=$ARTIFACTS"
trap - EXIT INT TERM
stop_tunnels
echo "Accepted fixed Y1 database loaded. Receipts: $ARTIFACTS"
