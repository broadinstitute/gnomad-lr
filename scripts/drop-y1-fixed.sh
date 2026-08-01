#!/usr/bin/env bash
# Safely drop only the fixed disposable Y1 database and its database-scoped writer.
set -Eeuo pipefail

DB="gnomad_lr_y1_scratch_v5_current"
PRINCIPAL="gnomad_lr_y1_pool_writer"
POOL="lr"
PROJECT="gnomadev"
CH_VM="gnomad-lr-y1-clickhouse"
CH_ZONE="us-east1-c"
CH_LOCAL="http://127.0.0.1:8126"
EXECUTE=false
CONFIRM=""
ARTIFACTS=""

usage() {
  cat <<EOF
Usage: scripts/drop-y1-fixed.sh [OPTIONS]

Dry-run is the default. This command can target only $DB and $PRINCIPAL.

Options:
  --execute               perform the drop
  --confirm-drop $DB      exact confirmation required with --execute
  --artifacts DIR         new receipt directory (default: a new directory in /tmp)
  -h, --help              show this help

Execution refuses active $POOL pool instances/disks, active queries by the writer,
a non-v5 fixed database, or writer grants outside the fixed database. It records
pre-drop run/table counts and a post-drop receipt.
EOF
}

while (($#)); do
  case "$1" in
    --execute) EXECUTE=true; shift ;;
    --confirm-drop) CONFIRM="${2:-}"; shift 2 ;;
    --artifacts) ARTIFACTS="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

cat <<EOF
Y1 fixed drop plan
  mode:       $($EXECUTE && printf execute || printf dry-run)
  database:   $DB
  principal:  $PRINCIPAL (only if absent or granted solely on $DB)
  preflight:  exact v5 receipt, no pool resources, no active writer query
  receipt:    pre-drop accepted runs/table counts and post-drop absence
EOF

if ! $EXECUTE; then
  echo "DRY-RUN: no network request, cloud command, or local write was performed."
  echo "  $0 --execute --confirm-drop $DB --artifacts /path/to/new/receipts"
  exit 0
fi

[[ "$CONFIRM" == "$DB" ]] || { echo "--execute requires --confirm-drop $DB" >&2; exit 2; }
for command in curl gcloud python3; do
  command -v "$command" >/dev/null || { echo "missing command: $command" >&2; exit 1; }
done

TOKEN="$(date -u +%Y%m%dT%H%M%SZ)-$(python3 - <<'PY'
import secrets
print(secrets.token_hex(4))
PY
)"
ARTIFACTS="${ARTIFACTS:-${TMPDIR:-/tmp}/gnomad-lr-fixed-y1-drop-$TOKEN}"
[[ ! -e "$ARTIFACTS" ]] || { echo "artifact path already exists: $ARTIFACTS" >&2; exit 1; }
mkdir -p "$ARTIFACTS"
chmod 700 "$ARTIFACTS"
TUNNEL_PID=""

stop_tunnel() { [[ -z "$TUNNEL_PID" ]] || kill "$TUNNEL_PID" 2>/dev/null || true; }
on_exit() {
  local exit_status=$?
  stop_tunnel
  exit "$exit_status"
}
trap on_exit EXIT INT TERM

wait_http() {
  local tries=60
  while ((tries--)); do
    if curl --silent --fail --max-time 2 "$CH_LOCAL/?query=SELECT%201" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "ClickHouse tunnel did not become ready" >&2
  return 1
}
ch_query() { curl --silent --show-error --fail-with-body --data-binary "$1" "$CH_LOCAL/"; }

# A running worker can race a destructive command even if the writer is currently idle.
[[ -z "$(gcloud compute instances list --project "$PROJECT" --filter="name~'^${POOL}-'" --format='value(name)')" ]] || { echo "refusing drop while $POOL pool instances exist" >&2; exit 1; }
[[ -z "$(gcloud compute disks list --project "$PROJECT" --filter="name~'^${POOL}-'" --format='value(name)')" ]] || { echo "refusing drop while $POOL pool disks exist" >&2; exit 1; }

gcloud compute ssh "$CH_VM" --project "$PROJECT" --zone "$CH_ZONE" --tunnel-through-iap -- \
  -N -L 8126:localhost:8123 >"$ARTIFACTS/clickhouse-tunnel.log" 2>&1 &
TUNNEL_PID=$!
wait_http

[[ "$(ch_query "SELECT count() FROM system.databases WHERE name = '$DB' FORMAT TabSeparated")" == "1" ]] || { echo "fixed database does not exist: $DB" >&2; exit 1; }
ATTESTATION="$(ch_query "SELECT state, contract FROM $DB.lr_y1_schema_versions FINAL WHERE schema_scope = 'y1_full' AND schema_version = 5 FORMAT TabSeparated")"
[[ "$ATTESTATION" == $'applied\ty1_full_v5_single_primary_copy_schema_attestation_not_load_authorization' ]] || { echo "fixed database lacks the exact Y1 v5 schema receipt" >&2; exit 1; }
[[ "$(ch_query "SELECT count() FROM system.processes WHERE user = '$PRINCIPAL' FORMAT TabSeparated")" == "0" ]] || { echo "refusing drop while the disposable writer has active queries" >&2; exit 1; }

USER_EXISTS="$(ch_query "SELECT count() FROM system.users WHERE name = '$PRINCIPAL' FORMAT TabSeparated")"
if [[ "$USER_EXISTS" == "1" ]]; then
  ch_query "SHOW GRANTS FOR $PRINCIPAL FORMAT TabSeparated" >"$ARTIFACTS/writer-grants.txt"
  python3 - "$ARTIFACTS/writer-grants.txt" "$DB" "$PRINCIPAL" <<'PY'
import re, sys
lines=[x.strip() for x in open(sys.argv[1]) if x.strip()]
db=re.escape(sys.argv[2]); user=re.escape(sys.argv[3])
allowed=re.compile(rf"^GRANT (?:SELECT, INSERT|INSERT, SELECT|SELECT|INSERT) ON {db}\.\* TO {user}$")
if not lines or any(not allowed.fullmatch(x) for x in lines):
    raise SystemExit("refusing to drop writer with missing or non-fixed-database grants")
PY
elif [[ "$USER_EXISTS" != "0" ]]; then
  echo "unexpected writer existence result: $USER_EXISTS" >&2
  exit 1
else
  : >"$ARTIFACTS/writer-grants.txt"
fi

ch_query "SELECT run_id, cohort, chrom, argMax(state, revision) AS state FROM $DB.lr_y1_load_runs GROUP BY run_id, cohort, chrom ORDER BY cohort, run_id FORMAT JSONEachRow" >"$ARTIFACTS/pre-drop-load-runs.jsonl"
ch_query "SELECT table, sum(rows) AS rows FROM system.parts WHERE active AND database = '$DB' GROUP BY table ORDER BY table FORMAT JSONEachRow" >"$ARTIFACTS/pre-drop-table-counts.jsonl"
python3 - "$ARTIFACTS/pre-drop.json" "$USER_EXISTS" "$ATTESTATION" <<PY
import json, pathlib, sys
p=pathlib.Path(sys.argv[1])
p.write_text(json.dumps({"schema_version":1,"database":"$DB","principal":"$PRINCIPAL","principal_existed":sys.argv[2]=="1","schema_attestation":sys.argv[3],"checked_at":"$TOKEN"},indent=2)+"\n")
PY

# Remove credentials first so no new write can start between the last check and DROP DATABASE.
ch_query "DROP USER IF EXISTS $PRINCIPAL" >/dev/null
ch_query "DROP DATABASE $DB SYNC" >/dev/null
DB_AFTER="$(ch_query "SELECT count() FROM system.databases WHERE name = '$DB' FORMAT TabSeparated")"
USER_AFTER="$(ch_query "SELECT count() FROM system.users WHERE name = '$PRINCIPAL' FORMAT TabSeparated")"
[[ "$DB_AFTER" == "0" && "$USER_AFTER" == "0" ]] || { echo "post-drop absence check failed" >&2; exit 1; }

python3 - "$ARTIFACTS/drop-receipt.json" <<PY
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({"schema_version":1,"database":"$DB","database_dropped":True,"principal":"$PRINCIPAL","principal_dropped_or_absent":True,"completed_at":"$TOKEN","pre_drop_runs":"pre-drop-load-runs.jsonl","pre_drop_table_counts":"pre-drop-table-counts.jsonl"},indent=2)+"\n")
PY

trap - EXIT INT TERM
stop_tunnel
echo "Dropped $DB and its disposable principal. Receipt: $ARTIFACTS/drop-receipt.json"
