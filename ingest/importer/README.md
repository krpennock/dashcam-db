# ingest/importer/

`repair.py` is a one-off tool for two faults in drives that were loaded before
the pipeline work: wrong session identifiers, and wrong start times.

**Wrong identifiers.** The processing script used to add `_02` / `_03` to a
drive's name whenever a folder of that name already existed, and the session id
is derived from that name, so re-processing a drive created a second copy of it
instead of updating the first. 239 Camry sessions carried such an id.

**Wrong start times.** The loader set each session's start to the earliest GPS
timestamp in the drive. A receiver reports a stale time for the first seconds
after it wakes, sometimes from days earlier, so that reading is often nonsense —
and because the GPS points, accelerometer readings, snapshots and clip times are
all stored as an offset from the start, one bad start drags the whole drive with
it.

## The corrected start time

The cameras run on a fixed clock six hours behind UTC with no daylight saving,
so a drive's own name gives one answer. Separately, every GPS row with a
confirmed fix votes: its own timestamp minus how far into the drive it sits. The
median of those votes gives the other answer.

| Situation | What is used | Recorded as |
|---|---|---|
| 30 or more confirmed fixes | the median of the votes | `gnss_median` |
| …and it disagrees with the camera clock by over 15 min | still the votes, but flagged | `gnss_median_clock_mismatch` |
| fewer than 30 fixes | the drive's own name at UTC-6 | `filename_low` |
| …inside a window where the camera clock is known to have been wrong | the name plus a correction measured from neighbouring drives | `filename_estimated_clock_offset` |

Never the earliest GPS row, which is the bug being repaired.

`clock_fixes.json` holds the only manual decision: on 2026-01-06..08 the Civic
camera's clock ran 5h00m12s slow. Five drives from those days have enough fixes
to measure that; four more do not, so the measured correction is carried across
to them and marked as an estimate rather than a measurement.

## Running it

Report only (the default; it opens the database read-only and refuses to write):

```bash
cd ~/dashcam-db && set -a && . ./.env && set +a
DB_HOST=127.0.0.1 DB_PORT=5432 DB_NAME="$POSTGRES_DB" \
DB_USER="$POSTGRES_USER" DB_PASSWORD="$POSTGRES_PASSWORD" \
  ~/dashcam-db/.venv/bin/python repair.py --report-dir /srv/dashcam/import/reports
```

Apply it (take a backup first; `--confirm` is required, `--only-tag` and
`--limit` exist for a cautious first run):

```bash
  ... repair.py --report-dir /srv/dashcam/import/reports \
      --media-root /home/kpennock/dashcam-db/media --apply --confirm
```

Each session is repaired in a single transaction: the telemetry is re-timed from
`t_rel_s`, snapshots from `offset_s`, clip times from each clip's own filename
plus the drive's measured clock error, then the id is corrected (every child row
follows it), the thumbnail paths are pointed at the new id, and the events and
summary are recomputed. What happened to each drive is written onto the drive
itself, under `time_sync.repair`.

### Thumbnail folders

The folders under `media/<vehicle>/<session id>/` are created by the loader
container, so they belong to root and the user running the repair usually cannot
rename them. That rename happens after the transaction commits, so a failure
leaves the drive correctly repaired with its images temporarily unreachable, and
is reported as such rather than as a failed repair. Fix them afterwards from
inside the loader image, which owns them:

```bash
cd ~/dashcam-db && docker compose --profile tools run --rm --no-deps \
  --entrypoint python -v /home/kpennock/dcdb-tools:/tools:ro \
  ingest /tools/repair.py --media-only --media-root /media --report-dir /tmp
```

`--media-only` touches no database rows. It reads the rename each session
recorded and moves the folder to match, so it is safe to re-run.

## Migrations this depends on

* `db/migrations/27_drive_identity.sql` — adds `drive_tag` and `time_confidence`,
  and recreates the seven foreign keys with `ON UPDATE CASCADE` so a session id
  can be corrected in place at all.
* `db/migrations/28_drive_tag_unique.sql` — one session per
  `(vehicle_tag, drive_tag)`. Apply this only after the repair has filled the
  tags in.

Note that `dashcam.schema_migrations` was empty: `db/migrate.sh` has never
recorded a run, so running it would replay every migration from the beginning.
Migration 27 was therefore applied directly with `psql` and recorded by hand;
migration 28 is applied the same way, after the repair. Filling that ledger in
so `migrate.sh` is usable again is worth doing separately.
