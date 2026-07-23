#!/usr/bin/env bash
# Manage the project-scoped local ClickHouse used by loader smoke tests.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/development/clickhouse.compose.yml"
PROJECT_NAME="gnomad-lr"
HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
CH_URL="http://127.0.0.1:${HTTP_PORT}"

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "Docker Compose is required (docker compose or docker-compose)." >&2
  exit 1
fi

compose() {
  "${COMPOSE[@]}" --project-name "$PROJECT_NAME" --file "$COMPOSE_FILE" "$@"
}

ensure_docker() {
  if docker info >/dev/null 2>&1; then
    return
  fi

  if [[ "$(uname -s)" == "Darwin" ]] && command -v colima >/dev/null 2>&1; then
    echo "[clickhouse] Docker is unavailable; starting Colima..."
    colima start
    for _ in $(seq 1 30); do
      docker info >/dev/null 2>&1 && return
      sleep 1
    done
  fi

  echo "Docker is not available. Start Docker/Colima and retry." >&2
  exit 1
}

wait_for_clickhouse() {
  for _ in $(seq 1 60); do
    if [[ "$(curl --fail --silent --max-time 2 --data-binary 'SELECT 1' "$CH_URL/" 2>/dev/null || true)" == "1" ]]; then
      echo "[clickhouse] ready at $CH_URL"
      return
    fi
    sleep 1
  done
  echo "ClickHouse did not become ready at $CH_URL" >&2
  compose logs clickhouse >&2 || true
  exit 1
}

case "${1:-up}" in
  up)
    ensure_docker
    compose up --detach --wait
    wait_for_clickhouse
    ;;
  down)
    ensure_docker
    compose down
    ;;
  reset)
    ensure_docker
    compose down --volumes --remove-orphans
    compose up --detach --wait
    wait_for_clickhouse
    ;;
  status)
    ensure_docker
    compose ps
    ;;
  logs)
    ensure_docker
    compose logs --follow clickhouse
    ;;
  *)
    echo "Usage: $0 {up|down|reset|status|logs}" >&2
    exit 2
    ;;
esac
