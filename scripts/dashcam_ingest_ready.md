# dashcam_ingest_ready.sh

Batch-ingest drive folders into the Dashcam DB via the dockerized ingest service.

## Prerequisites

- Docker Compose services running (`docker compose up -d`)
- Drive folders staged under `$IMPORT_ROOT/ready/<vehicle>/`
- Each drive folder must contain a `manifest.json` (produced by `blackvue_drives_v60.py`)

## Directory Layout

```
$IMPORT_ROOT/                     # default: /srv/dashcam/import
  ready/<vehicle>/                # drives waiting to be ingested
    20260305_073746_camry_front/
      manifest.json
      *.csv
      *.nmea
      artifacts/
        clips/
        thumbs/
  done/<vehicle>/                 # drives successfully ingested (moved here)
  failed/<vehicle>/               # drives that failed ingest (moved here)
```

## Usage

```bash
# From the repo root (~/dashcam-db):

# Auto-discover all vehicles under ready/
./scripts/dashcam_ingest_ready.sh

# Process a single vehicle
./scripts/dashcam_ingest_ready.sh camry

# Dry run (show what would be ingested without running)
DRY_RUN=1 ./scripts/dashcam_ingest_ready.sh

# Custom import root
IMPORT_ROOT=/data/dashcam ./scripts/dashcam_ingest_ready.sh camry
```

## Environment Variables

| Variable      | Default                   | Description                                    |
|---------------|---------------------------|------------------------------------------------|
| `IMPORT_ROOT` | `/srv/dashcam/import`     | Host path to the import directory tree          |
| `DRY_RUN`     | `0`                       | Set to `1` to print commands without executing  |
| `COMPOSE`     | `docker compose`          | Docker Compose command                          |
| `SUDO`        | *(empty)*                 | Prefix for commands requiring elevated access   |

## Behavior

1. Discovers vehicle directories under `ready/` (or uses the one specified).
2. Runs a mount sanity check to confirm the ingest container can see `/import`.
3. For each drive folder containing `manifest.json`:
   - Invokes the ingest container with the manifest path.
   - **Success**: moves the folder to `done/<vehicle>/`.
   - **Failure**: moves the folder to `failed/<vehicle>/`.
4. Prints per-vehicle and grand-total counters.

### Collision Handling

If a folder with the same name already exists in `done/` or `failed/` (e.g., from a previous run), the old folder is renamed with a `.prev_YYYYMMDDHHMMSS` suffix rather than overwritten. This preserves previous data for debugging.

## Typical Workflow

```
                 blackvue_drives_v60.py
  Raw clips  ──────────────────────────>  Processed drive folders
  (Windows)                                (Windows)
                                              │
                                         rsync from WSL
                                              │
                                              v
                                    ready/<vehicle>/
                                              │
                                   dashcam_ingest_ready.sh
                                              │
                              ┌───────────────┴───────────────┐
                              v                               v
                       done/<vehicle>/                  failed/<vehicle>/
                                              │
                                   dashcam_cleanup.sh
                                      (when ready)
```

## Related Scripts

- `blackvue_drives_v60.py` -- processes raw dashcam clips into drive folders with manifests
- `dashcam_cleanup.sh` -- removes processed folders from `done/`, `failed/`, and `.prev_*`
