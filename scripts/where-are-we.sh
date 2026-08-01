#!/usr/bin/env bash
# Ask the systems where we are, instead of trusting a file someone updated by hand.
#
# Read-only. Prints repository, GCP, and ClickHouse state. Any probe that cannot
# reach its system says so rather than staying silent, so a partial answer is never
# mistaken for a complete one.
#
#   ./scripts/where-are-we.sh              # everything
#   ./scripts/where-are-we.sh --no-remote  # git only, no network

set -uo pipefail

PROJECT="${GNOMAD_LR_PROJECT:-gnomadev}"
CH_VM="${GNOMAD_LR_CH_VM:-gnomad-lr-y1-clickhouse}"
CH_ZONE="${GNOMAD_LR_CH_ZONE:-us-east1-c}"
BACKEND="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROWSER="${GNOMAD_LR_BROWSER:-$HOME/.local/share/grove/worktrees/gnomad-browser-eea698e2/gnomad-lr-xdg/gnomad-browser}"

REMOTE=true
[[ "${1:-}" == "--no-remote" ]] && REMOTE=false

hdr() { printf '\n\033[1m%s\033[0m\n' "$1"; }
gone() { printf '  unavailable — %s\n' "$1"; }

hdr "checked at"
date -u '+  %Y-%m-%dT%H:%M:%SZ'

# ---------------------------------------------------------------- repositories
hdr "repositories"
for repo in "$BACKEND" "$BROWSER"; do
  # .git is a file, not a directory, inside a git worktree
  if [[ ! -e "$repo/.git" ]]; then
    printf '  %-10s ' "$(basename "$repo")"; gone "not a git checkout at $repo"
    continue
  fi
  name="$(basename "$repo")"
  branch="$(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null)"
  head="$(git -C "$repo" rev-parse --short HEAD 2>/dev/null)"
  dirty="$(git -C "$repo" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"
  ahead="$(git -C "$repo" rev-list --count "@{upstream}..HEAD" 2>/dev/null || echo '?')"
  printf '  %-16s %-14s %s  %s ahead of upstream, %s uncommitted file(s)\n' \
    "$name" "$branch" "$head" "$ahead" "$dirty"
done

if ! $REMOTE; then
  printf '\n(skipped GCP and ClickHouse: --no-remote)\n'
  exit 0
fi

# ------------------------------------------------------------------------- GCP
hdr "GCP instances (project $PROJECT)"
if ! command -v gcloud >/dev/null 2>&1; then
  gone "gcloud not on PATH"
else
  out="$(gcloud compute instances list --project "$PROJECT" \
      --filter="name~'^gnomad-lr' OR name~'^lr-'" \
      --format='table[no-heading](name,status,machineType.basename(),zone.basename())' 2>&1)"
  if [[ $? -ne 0 || -z "$out" ]]; then
    printf '%s\n' "$out" | sed 's/^/  /' | head -5
    [[ -z "$out" ]] && printf '  no gnomad-lr or pool instances exist\n'
  else
    printf '%s\n' "$out" | sed 's/^/  /'
  fi
  pools="$(printf '%s\n' "$out" | grep -c '^lr-' || true)"
  printf '  pool VMs: %s\n' "${pools:-0}"
fi

# ------------------------------------------------------------------ ClickHouse
hdr "ClickHouse on $CH_VM"
CH_SQL=$(cat <<'SQL'
echo "## databases"
curl -s --max-time 30 --data-binary "SELECT name FROM system.databases WHERE name NOT IN ('system','default','information_schema','INFORMATION_SCHEMA') ORDER BY name FORMAT TabSeparated" http://127.0.0.1:8123/
echo "## accepted Y1 runs"
for db in $(curl -s --max-time 30 --data-binary "SELECT name FROM system.databases WHERE name LIKE 'gnomad_lr_y1%' FORMAT TabSeparated" http://127.0.0.1:8123/); do
  curl -s --max-time 30 --data-binary "SELECT '$db', cohort, run_id, chrom, state FROM $db.lr_y1_load_runs FINAL WHERE state = 'accepted_frozen' ORDER BY cohort, run_id FORMAT TabSeparated" http://127.0.0.1:8123/ 2>/dev/null
done
echo "## rows in LR tables"
curl -s --max-time 45 --data-binary "SELECT database, table, sum(rows) FROM system.parts WHERE active AND database LIKE 'gnomad_lr%' AND table IN ('lr_y1_summaries','lr_y1_alleles','lr_y1_frequencies','lr_y1_carriers','lr_y1_metadata','lr_y1_coverage','lr_y1_str_histograms','lr_y1_methylation','lr_y1_methylation_summary','lr_y1_methylation_availability') GROUP BY database, table ORDER BY database, table FORMAT TabSeparated" http://127.0.0.1:8123/
echo "## disk"
df -h /data | tail -1 | awk '{print "  /data "$2" total, "$4" free ("$5" used)"}'
SQL
)

if ch_out="$(gcloud compute ssh "$CH_VM" --project "$PROJECT" --zone "$CH_ZONE" \
      --tunnel-through-iap --command "$CH_SQL" 2>/dev/null)"; then
  printf '%s\n' "$ch_out" | awk '
    /^## / { printf "\n  %s\n", substr($0,4); next }
    NF     { printf "    %s\n", $0 }'
else
  gone "could not reach it. Needs gcloud auth and IAP access; IAP is blocked from a sandboxed shell."
fi

hdr "docs"
cat <<'EOF'
  ~/notebooks/genohype-eco/workspaces/gnomad-lr/concepts/y1-chr22-loading/
    overview.md               architecture and current behavior
    01-clickhouse-access.md   credentials and tunnels
    02-pool-operations.md     pool commands

  checks
    ./scripts/verify.sh
    python3 scripts/verify-y1-ancillary-manifests.py
EOF
