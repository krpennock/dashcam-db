#!/usr/bin/env bash
set -euo pipefail

# Batch-ingest "ready" drive folders into the Dashcam DB.
#
# Assumptions:
# - docker compose project is the current working directory (repo root)
# - ingest service mounts host IMPORT_ROOT to /import (read-only)
# - folders are staged as:  $IMPORT_ROOT/ready/<vehicle>/<drive_folder>/manifest.json
#
# Usage:
#   ./scripts/dashcam_ingest_ready.sh camry
#   IMPORT_ROOT=/srv/dashcam/import ./scripts/dashcam_ingest_ready.sh camry
#   DRY_RUN=1 ./scripts/dashcam_ingest_ready.sh camry
#
# Behavior:
# - Ingest each folder under ready/<vehicle> that contains manifest.json
# - On success: move folder to done/<vehicle>
# - On failure: move folder to failed/<vehicle>
#
# Notes:
# - Does NOT store any credentials; relies on docker-compose environment.

VEHICLE="${1:-}"
if [[ -z "$VEHICLE" ]]; then
  echo "Usage: $0 <vehicle>"
  echo "Example: $0 camry"
  exit 2
fi

IMPORT_ROOT="${IMPORT_ROOT:-/srv/dashcam/import}"
READY_DIR="${IMPORT_ROOT}/ready/${VEHICLE}"
DONE_DIR="${IMPORT_ROOT}/done/${VEHICLE}"
FAILED_DIR="${IMPORT_ROOT}/failed/${VEHICLE}"

DRY_RUN="${DRY_RUN:-0}"
COMPOSE="${COMPOSE:-docker compose}"
SUDO="${SUDO:-}"

# Basic sanity checks
if [[ ! -d "$READY_DIR" ]]; then
  echo "ERROR: READY_DIR does not exist: $READY_DIR"
  exit 1
fi

mkdir -p "$DONE_DIR" "$FAILED_DIR"

# Confirm ingest container can see /import (mount sanity check)
# (This is cheap and prevents 'Manifest not found' loops.)
$SUDO $COMPOSE --profile tools run --rm --entrypoint sh ingest -lc 'test -d /import && exit 0 || exit 1' \
  >/dev/null 2>&1 || {
    echo "ERROR: ingest container cannot see /import. Check docker-compose bind mount for ingest volumes."
    exit 1
  }

shopt -s nullglob

count_total=0
count_ok=0
count_fail=0
count_skip=0

for d in "$READY_DIR"/*; do
  [[ -d "$d" ]] || continue
  ((++count_total))

  name="$(basename "$d")"
  host_manifest="$d/manifest.json"
  container_manifest="/import/ready/${VEHICLE}/${name}/manifest.json"

  if [[ ! -f "$host_manifest" ]]; then
    echo "SKIP (no manifest.json): $d"
    ((++count_skip))
    continue
  fi

  echo "INGEST: $container_manifest"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY_RUN=1 => would run: $SUDO $COMPOSE --profile tools run --rm ingest $container_manifest"
    continue
  fi

  if $SUDO $COMPOSE --profile tools run --rm ingest "$container_manifest"; then
    mv "$d" "$DONE_DIR/"
    ((++count_ok))
  else
    mv "$d" "$FAILED_DIR/"
    ((++count_fail))
  fi
done

echo
echo "Done."
echo "Total: $count_total  OK: $count_ok  Failed: $count_fail  Skipped: $count_skip"
