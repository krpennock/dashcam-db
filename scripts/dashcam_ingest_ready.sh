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
#   ./scripts/dashcam_ingest_ready.sh              # auto-discover all vehicles
#   ./scripts/dashcam_ingest_ready.sh --all         # explicit "process all" (same as no args)
#   ./scripts/dashcam_ingest_ready.sh camry         # process one vehicle
#   IMPORT_ROOT=/data/dashcam ./scripts/dashcam_ingest_ready.sh camry
#   DRY_RUN=1 ./scripts/dashcam_ingest_ready.sh     # dry-run all vehicles
#
# Environment:
#   IMPORT_ROOT  Host path to import tree (default: /srv/dashcam/import)
#   DRY_RUN      Set to 1 to skip actual ingest (default: 0)
#   COMPOSE      Docker compose command (default: "docker compose")
#   SUDO         Prefix command with sudo if needed (default: "")
#
# Behavior:
# - Ingest each folder under ready/<vehicle> that contains manifest.json
# - On success: move folder to done/<vehicle>
# - On failure: move folder to failed/<vehicle>
#
# Notes:
# - Does NOT store any credentials; relies on docker-compose environment.

VEHICLE="${1:-}"

# --all is an explicit alias for "no argument" (discover all vehicles)
if [[ "$VEHICLE" == "--all" ]]; then
  VEHICLE=""
fi

IMPORT_ROOT="${IMPORT_ROOT:-/srv/dashcam/import}"
DRY_RUN="${DRY_RUN:-0}"
COMPOSE="${COMPOSE:-docker compose}"
SUDO="${SUDO:-}"

# Build list of vehicles to process
shopt -s nullglob
if [[ -n "$VEHICLE" ]]; then
  # Single vehicle mode
  VEHICLES=("$VEHICLE")
else
  # Auto-discover all vehicle directories under ready/
  VEHICLES=()
  for vdir in "$IMPORT_ROOT"/ready/*/; do
    vname="$(basename "$vdir")"
    VEHICLES+=("$vname")
  done

  if [[ ${#VEHICLES[@]} -eq 0 ]]; then
    echo "No vehicle directories found under $IMPORT_ROOT/ready/"
    exit 0
  fi

  echo "Discovered ${#VEHICLES[@]} vehicle(s): ${VEHICLES[*]}"
fi

# Mount sanity check (run once, not per vehicle)
$SUDO $COMPOSE --profile tools run --rm --entrypoint sh ingest -lc 'test -d /import && exit 0 || exit 1' \
  >/dev/null 2>&1 || {
    echo "ERROR: ingest container cannot see /import. Check docker-compose bind mount for ingest volumes."
    exit 1
  }

grand_total=0
grand_ok=0
grand_fail=0
grand_skip=0

for VEHICLE in "${VEHICLES[@]}"; do
  READY_DIR="${IMPORT_ROOT}/ready/${VEHICLE}"
  DONE_DIR="${IMPORT_ROOT}/done/${VEHICLE}"
  FAILED_DIR="${IMPORT_ROOT}/failed/${VEHICLE}"

  if [[ ! -d "$READY_DIR" ]]; then
    echo "WARNING: READY_DIR does not exist, skipping: $READY_DIR"
    continue
  fi

  mkdir -p "$DONE_DIR" "$FAILED_DIR"

  count_total=0
  count_ok=0
  count_fail=0
  count_skip=0

  echo ""
  echo "=== Vehicle: $VEHICLE ==="

  for d in "$READY_DIR"/*; do
    [[ -d "$d" ]] || continue
    count_total=$((count_total + 1))

    name="$(basename "$d")"
    host_manifest="$d/manifest.json"
    container_manifest="/import/ready/${VEHICLE}/${name}/manifest.json"

    if [[ ! -f "$host_manifest" ]]; then
      echo "SKIP (no manifest.json): $d"
      count_skip=$((count_skip + 1))
      continue
    fi

    echo "INGEST: $container_manifest"

    if [[ "$DRY_RUN" == "1" ]]; then
      echo "DRY_RUN=1 => would run: $SUDO $COMPOSE --profile tools run --rm ingest $container_manifest"
      continue
    fi

    if $SUDO $COMPOSE --profile tools run --rm ingest "$container_manifest"; then
      mv "$d" "$DONE_DIR/"
      count_ok=$((count_ok + 1))
    else
      mv "$d" "$FAILED_DIR/"
      count_fail=$((count_fail + 1))
    fi
  done

  echo "Vehicle $VEHICLE -- Total: $count_total  OK: $count_ok  Failed: $count_fail  Skipped: $count_skip"

  grand_total=$((grand_total + count_total))
  grand_ok=$((grand_ok + count_ok))
  grand_fail=$((grand_fail + count_fail))
  grand_skip=$((grand_skip + count_skip))
done

echo ""
echo "Done."
if [[ ${#VEHICLES[@]} -gt 1 ]]; then
  echo "Grand total: $grand_total  OK: $grand_ok  Failed: $grand_fail  Skipped: $grand_skip"
else
  echo "Total: $grand_total  OK: $grand_ok  Failed: $grand_fail  Skipped: $grand_skip"
fi
