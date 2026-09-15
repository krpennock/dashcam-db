# dashcam_cleanup.sh

Remove processed drive folders from the import tree after successful ingestion.

This script is designed to be run **manually** after you have verified that ingested data looks correct in the database. It is **dry-run by default** -- you must pass `--confirm` and type `YES` to actually delete anything.

## Usage

```bash
# From the repo root (~/dashcam-db):

# Dry run -- show what would be removed (done/ folders only)
./scripts/dashcam_cleanup.sh

# Dry run for a single vehicle
./scripts/dashcam_cleanup.sh camry

# Actually delete done/ folders (interactive YES prompt)
./scripts/dashcam_cleanup.sh --confirm

# Include failed/ folders in cleanup
./scripts/dashcam_cleanup.sh --include-failed

# Include .prev_* collision-renamed folders
./scripts/dashcam_cleanup.sh --include-prev

# Include everything (done + failed + prev)
./scripts/dashcam_cleanup.sh --all

# Delete everything for a single vehicle
./scripts/dashcam_cleanup.sh --all --confirm camry
```

## Options

| Flag               | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| *(no flags)*       | Dry-run targeting `done/` folders only                             |
| `--confirm`        | Actually perform deletion (requires interactive `YES` confirmation)|
| `--include-failed` | Also target `failed/` folders                                      |
| `--include-prev`   | Also target `.prev_*` collision-renamed folders                    |
| `--all`            | Shorthand for `--include-failed --include-prev`                    |
| `--help`, `-h`     | Print help text                                                    |

A vehicle name (e.g., `camry`) can be provided as a positional argument to limit cleanup to a single vehicle. If omitted, all vehicles found in `done/` and `failed/` are discovered automatically.

## Environment Variables

| Variable      | Default               | Description                            |
|---------------|-----------------------|----------------------------------------|
| `IMPORT_ROOT` | `/srv/dashcam/import` | Host path to the import directory tree |

## What Gets Cleaned

| Category  | Source                               | When to clean                                   |
|-----------|--------------------------------------|-------------------------------------------------|
| `done/`   | Successfully ingested drive folders  | After verifying data in the DB looks correct     |
| `failed/` | Drive folders that failed ingest     | After investigating the failure and re-ingesting |
| `.prev_*` | Old folders renamed during collision | After confirming the newer ingest succeeded      |

## Safety Features

- **Dry-run by default**: always shows what would be removed before doing anything.
- **Interactive confirmation**: even with `--confirm`, you must type `YES` at the prompt.
- **Opt-in escalation**: `failed/` and `.prev_*` folders require explicit flags.
- **Summary before action**: displays counts by category so you can sanity-check.

## Example Output

```
$ ./scripts/dashcam_cleanup.sh --all

Cleanup summary:
  done/   folders:  94
  failed/ folders:  3
  .prev_* folders:  1
  ────────────────
  Total to remove:  98

DRY RUN — the following directories would be removed:

  /srv/dashcam/import/done/civic/20260106_151554_civic_front
  /srv/dashcam/import/done/civic/20260107_041909_civic_front
  ...

Re-run with --confirm to actually delete.
```

## Related Scripts

- `dashcam_ingest_ready.sh` -- ingests drive folders and moves them to `done/` or `failed/`
- `blackvue_drives_v60.py` -- processes raw dashcam clips into drive folders with manifests
