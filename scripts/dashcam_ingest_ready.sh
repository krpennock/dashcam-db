#!/usr/bin/env bash
#
# Load whatever is waiting in the import tree, once, and stop.
#
# This used to be the whole batch loader. The importer service now watches the
# import tree continuously and does the same work (loading each drive, filing the
# folder into done/ or failed/, and recording what happened in the database), so
# this is a thin wrapper for when you want to prod it by hand.
#
#   ./scripts/dashcam_ingest_ready.sh          # process what is waiting, then stop
#
# The importer takes a lock, so if the service is already running this exits
# quietly rather than loading anything twice.
#
# Environment:
#   IMPORT_ROOT  host path to the import tree (default: /srv/dashcam/import)
#   COMPOSE      docker compose command (default: docker compose)
#   SUDO         prefix for commands needing elevated access (default: none)
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

COMPOSE="${COMPOSE:-docker compose}"
SUDO="${SUDO:-}"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  sed -n '2,/^set -euo/{ s/^# \?//; p }' "$0" | sed '$d'
  exit 0
fi

if [[ $# -gt 0 ]]; then
  echo "This script takes no arguments now: the importer processes everything waiting." >&2
  echo "Run it with --help for the details." >&2
  exit 2
fi

# If the service is up, ask it to do a pass now by running a one-off in the same
# image; the lock keeps the two from colliding.
exec $SUDO $COMPOSE run --rm --no-deps importer --once
