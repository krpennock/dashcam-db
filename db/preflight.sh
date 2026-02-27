#!/usr/bin/env bash
set -euo pipefail
POSTGIS_CONTAINER="${POSTGIS_CONTAINER:-dashcam-postgis}"
docker exec -i "$POSTGIS_CONTAINER" bash -lc \
'psql -U "$POSTGRES_USER" -d "${POSTGRES_DB:-postgres}"' < "$(dirname "$0")/preflight.sql"
