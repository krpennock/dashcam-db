#!/usr/bin/env bash
set -euo pipefail

# Clean up processed drive folders from the import tree.
#
# This script is intended to be run manually after verifying that
# ingested data looks good in the database.  It is intentionally
# DRY-RUN by default so you always see what will be removed first.
#
# Targets:
#   done/    — drives that were successfully ingested (safest to remove)
#   failed/  — drives that failed ingest (use --include-failed)
#   .prev_*  — collision-renamed folders from re-ingest runs (use --include-prev)
#
# Usage:
#   ./scripts/dashcam_cleanup.sh                     # dry-run, all vehicles, done/ only
#   ./scripts/dashcam_cleanup.sh camry               # dry-run, single vehicle
#   ./scripts/dashcam_cleanup.sh --confirm            # actually delete, all vehicles
#   ./scripts/dashcam_cleanup.sh --confirm camry      # actually delete, single vehicle
#   ./scripts/dashcam_cleanup.sh --include-failed     # dry-run, include failed/
#   ./scripts/dashcam_cleanup.sh --include-prev       # dry-run, include .prev_* folders
#   ./scripts/dashcam_cleanup.sh --all                # dry-run, done + failed + prev
#   ./scripts/dashcam_cleanup.sh --all --confirm      # delete everything
#
# Environment:
#   IMPORT_ROOT  Host path to import tree (default: /srv/dashcam/import)

IMPORT_ROOT="${IMPORT_ROOT:-/srv/dashcam/import}"

# --- Parse arguments ---
CONFIRM=0
INCLUDE_FAILED=0
INCLUDE_PREV=0
VEHICLE=""

for arg in "$@"; do
  case "$arg" in
    --confirm)       CONFIRM=1 ;;
    --include-failed) INCLUDE_FAILED=1 ;;
    --include-prev)  INCLUDE_PREV=1 ;;
    --all)           INCLUDE_FAILED=1; INCLUDE_PREV=1 ;;
    --help|-h)
      sed -n '3,/^$/{ s/^# \?//; p }' "$0"
      exit 0
      ;;
    -*)
      echo "Unknown option: $arg" >&2
      exit 2
      ;;
    *)
      if [[ -n "$VEHICLE" ]]; then
        echo "ERROR: multiple vehicle arguments not supported" >&2
        exit 2
      fi
      VEHICLE="$arg"
      ;;
  esac
done

# --- Discover vehicles ---
shopt -s nullglob

if [[ -n "$VEHICLE" ]]; then
  VEHICLES=("$VEHICLE")
else
  VEHICLES=()
  # Collect vehicle names from done/ and failed/ (union)
  for vdir in "$IMPORT_ROOT"/done/*/ "$IMPORT_ROOT"/failed/*/; do
    vname="$(basename "$vdir")"
    # deduplicate
    local_dup=0
    for v in "${VEHICLES[@]+"${VEHICLES[@]}"}"; do
      [[ "$v" == "$vname" ]] && local_dup=1 && break
    done
    [[ "$local_dup" -eq 0 ]] && VEHICLES+=("$vname")
  done

  if [[ ${#VEHICLES[@]} -eq 0 ]]; then
    echo "Nothing to clean up."
    exit 0
  fi
fi

# --- Build removal list ---
declare -a TO_REMOVE=()

for v in "${VEHICLES[@]}"; do
  DONE_DIR="$IMPORT_ROOT/done/$v"
  FAILED_DIR="$IMPORT_ROOT/failed/$v"
  READY_DIR="$IMPORT_ROOT/ready/$v"

  # done/ folders (always included)
  if [[ -d "$DONE_DIR" ]]; then
    for d in "$DONE_DIR"/*/; do
      [[ -d "$d" ]] || continue
      TO_REMOVE+=("done/$v/$(basename "$d")")
    done
  fi

  # failed/ folders (opt-in)
  if [[ "$INCLUDE_FAILED" -eq 1 && -d "$FAILED_DIR" ]]; then
    for d in "$FAILED_DIR"/*/; do
      [[ -d "$d" ]] || continue
      TO_REMOVE+=("failed/$v/$(basename "$d")")
    done
  fi

  # .prev_* folders in done/, failed/, and ready/ (opt-in)
  if [[ "$INCLUDE_PREV" -eq 1 ]]; then
    for search_dir in "$DONE_DIR" "$FAILED_DIR" "$READY_DIR"; do
      [[ -d "$search_dir" ]] || continue
      for d in "$search_dir"/*.prev_*/; do
        [[ -d "$d" ]] || continue
        parent="$(basename "$(dirname "$d")")"
        grandparent="$(basename "$(dirname "$(dirname "$d")")")"
        TO_REMOVE+=("$grandparent/$parent/$(basename "$d")")
      done
    done
  fi
done

if [[ ${#TO_REMOVE[@]} -eq 0 ]]; then
  echo "Nothing to clean up."
  exit 0
fi

# --- Summary ---
count_done=0
count_failed=0
count_prev=0

for entry in "${TO_REMOVE[@]}"; do
  case "$entry" in
    *.prev_*) ((++count_prev)) ;;
    failed/*) ((++count_failed)) ;;
    done/*)   ((++count_done)) ;;
  esac
done

echo "Cleanup summary:"
echo "  done/   folders:  $count_done"
[[ "$INCLUDE_FAILED" -eq 1 ]] && echo "  failed/ folders:  $count_failed"
[[ "$INCLUDE_PREV" -eq 1 ]]   && echo "  .prev_* folders:  $count_prev"
echo "  ────────────────"
echo "  Total to remove:  ${#TO_REMOVE[@]}"
echo ""

if [[ "$CONFIRM" -eq 0 ]]; then
  echo "DRY RUN — the following directories would be removed:"
  echo ""
  for entry in "${TO_REMOVE[@]}"; do
    echo "  $IMPORT_ROOT/$entry"
  done
  echo ""
  echo "Re-run with --confirm to actually delete."
  exit 0
fi

# --- Confirm interactively ---
echo "The above directories will be PERMANENTLY deleted."
read -r -p "Type YES to proceed: " answer
if [[ "$answer" != "YES" ]]; then
  echo "Aborted."
  exit 1
fi

# --- Remove ---
removed=0
for entry in "${TO_REMOVE[@]}"; do
  target="$IMPORT_ROOT/$entry"
  if [[ -d "$target" ]]; then
    rm -rf "$target"
    ((++removed))
  fi
done

echo ""
echo "Removed $removed directories."
