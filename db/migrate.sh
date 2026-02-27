#!/usr/bin/env bash
set -euo pipefail

# Apply SQL migrations into the running Postgres container and record what ran.
#
# Usage:
#   ./db/migrate.sh            # apply pending migrations
#   ./db/migrate.sh --status   # show applied/pending
#
# Ordering:
#   1) initdb/00_*.sql, initdb/10_*.sql (if present)
#   2) db/[0-9][0-9]_*.sql (numeric prefix)
#   3) db/migrations/[0-9][0-9]_*.sql (numeric prefix)
#   4) db/migrations/[0-9]{8}_*.sql (date prefix)
#   5) anything else (lexicographic)
#
# Tracks applied scripts in dashcam.schema_migrations using the script's *relative path*.

POSTGIS_CONTAINER="${POSTGIS_CONTAINER:-dashcam-postgis}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MODE="apply"
if [[ "${1:-}" == "--status" ]]; then
  MODE="status"
elif [[ -n "${1:-}" ]]; then
  echo "Unknown arg: $1"
  echo "Usage: $0 [--status]"
  exit 2
fi

require() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1"; exit 1; }; }
require docker
require sort
require sed
require awk
require find

if ! docker ps --format '{{.Names}}' | grep -qx "$POSTGIS_CONTAINER"; then
  echo "Container not running: $POSTGIS_CONTAINER"
  docker ps --format '  - {{.Names}}'
  exit 1
fi

DB_USER="$(docker exec "$POSTGIS_CONTAINER" bash -lc 'echo "${POSTGRES_USER:-postgres}"')"
DB_NAME="$(docker exec "$POSTGIS_CONTAINER" bash -lc 'echo "${POSTGRES_DB:-postgres}"')"

psql_exec() {
  # usage: psql_exec "SQL..."
  docker exec -i "$POSTGIS_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "$1" >/dev/null
}

psql_file() {
  # usage: psql_file /abs/path/to/file.sql
  local f="$1"
  docker exec -i "$POSTGIS_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 < "$f" >/dev/null
}

# Ensure migration tracking table exists.
psql_exec "CREATE SCHEMA IF NOT EXISTS dashcam;"
psql_exec "CREATE TABLE IF NOT EXISTS dashcam.schema_migrations (
  script     text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);"

relpath() {
  local f="$1"
  echo "${f#$REPO_ROOT/}"
}

is_applied() {
  local script="$1"
  docker exec -i "$POSTGIS_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -tAc \
    "SELECT 1 FROM dashcam.schema_migrations WHERE script = '$(printf "%s" "$script" | sed "s/'/''/g")' LIMIT 1;" \
    | grep -qx "1"
}

mark_applied() {
  local script="$1"
  psql_exec "INSERT INTO dashcam.schema_migrations (script)
             VALUES ('$(printf "%s" "$script" | sed "s/'/''/g")')
             ON CONFLICT (script) DO NOTHING;"
}

# Build ordered list.
gather() {
  local files=()

  shopt -s nullglob
  # initdb first (if present)
  for f in "$REPO_ROOT"/initdb/[0-9][0-9]_*.sql; do files+=("$f"); done
  # db numbered
  for f in "$REPO_ROOT"/db/[0-9][0-9]_*.sql; do files+=("$f"); done
  # db/migrations
  for f in "$REPO_ROOT"/db/migrations/*.sql; do files+=("$f"); done
  shopt -u nullglob

  if (( ${#files[@]} == 0 )); then
    echo "No migration files found."
    exit 1
  fi

  for f in "${files[@]}"; do
    local p base key
    p="$(relpath "$f")"
    base="$(basename "$f")"

    # initdb ordering: 00_ then 10_ then the rest by name
    if [[ "$p" =~ ^initdb/([0-9]{2})_ ]]; then
      key="0 $(printf '%03d' "${BASH_REMATCH[1]}") $p"
    # numeric prefix ordering (db or db/migrations)
    elif [[ "$base" =~ ^([0-9]{2})_ ]]; then
      key="1 $(printf '%03d' "${BASH_REMATCH[1]}") $p"
    # date prefix ordering (only after numeric migrations)
    elif [[ "$base" =~ ^([0-9]{8})_ ]]; then
      key="2 ${BASH_REMATCH[1]} $p"
    else
      key="3 $p"
    fi

    printf '%s|%s\n' "$key" "$p"
  done | sort | sed 's/^[^|]*|//'
}

apply_one() {
  local p="$1"
  local f="$REPO_ROOT/$p"

  echo "APPLY: $p"
  psql_file "$f"
  mark_applied "$p"
}

main() {
  mapfile -t ordered < <(gather)

  local applied=0 pending=0
  for p in "${ordered[@]}"; do
    if is_applied "$p"; then
      ((applied++))
      [[ "$MODE" == "status" ]] && echo "OK       $p"
    else
      ((pending++))
      [[ "$MODE" == "status" ]] && echo "PENDING  $p"
    fi
  done

  if [[ "$MODE" == "status" ]]; then
    echo
    echo "Applied: $applied"
    echo "Pending: $pending"
    exit 0
  fi

  if (( pending == 0 )); then
    echo "No pending migrations."
    exit 0
  fi

  for p in "${ordered[@]}"; do
    if is_applied "$p"; then
      continue
    fi
    apply_one "$p"
  done

  echo "Done."
}

main
