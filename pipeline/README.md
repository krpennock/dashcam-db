# pipeline/

`blackvue_drives.py` turns a set of BlackVue clips into one folder per drive,
containing the GPS track, the accelerometer readings, still frames and a
`manifest.json` that the server's loader reads into the database.

It is the same script that used to live at `scripts/blackvue_drives_v60.py` (and
still runs by hand from `C:\dev\Dashcam\prod`), with the changes the automated
pipeline needs. Everything below is additive: the old command line still works,
and `manifest.json` is still `schema_version: 2`.

## Reading straight off the card

The database uses the GPS table, the accelerometer table, the clip list and the
stills. All of that comes from under three percent of each clip: a half-megabyte
telemetry block at the front of the file, the clip's index, and one frame every
thirty seconds. So the pipeline no longer copies a card before processing it.
Clips are read where they are, the drive folder is built on the processing disk,
and the card only has to stay in the reader until the server confirms the load.
Copying a full 256 GB card used to take about two hours before any processing
started; a five-pair drive now builds in under three seconds straight off the
card.

Two things make that possible:

* **Telemetry is read in-process.** The GPS sentences and accelerometer samples
  sit in the clip's `free` atom, each in a tagged block. The script reads that
  block and writes the same `.nmea` and `.3gf.txt` sidecars blackclue would
  have, into the drive's own work folder. Nothing is written beside the clip, so
  a card is only ever read. `--telemetry-reader blackclue` still runs the
  external tool for anyone who wants it.
* **Stills are seeked, not filtered.** Each still is taken from its own clip
  with an exact seek, which reads a few megabytes and decodes at most a second
  of video. The previous method decoded every frame of the drive through
  ffmpeg's `fps` filter and threw 899 of every 900 away; that was 97% of the
  build time, and the filter keeps the *last* frame of each window, so every
  still was about fifteen seconds later than the time recorded against it. Stills
  now land on the moment their index row says.

The pipeline keeps no video. The holding folder and the `retention` command only
look after clips copied before this change, until the holding window has let
them all go. A drive can always be re-cut from the card itself, or from a copy
made by hand.

## Running it

The hand-written `RunDrivesCamry.txt` / `RunDrivesCivic.txt` commands work
unchanged. The pipeline instead builds one known drive at a time, pointing the
script at the clips on the card:

```
python pipeline/blackvue_drives.py \
  --clips-from <list of clip paths, one per line> \
  --drive-tag 20260806_094436_camry \
  --drive-session-id <uuid the pipeline expects> \
  --vehicle camry --output F:\Dashcam\Processed_Camry \
  --camera-utc-offset -06:00 \
  --run-blackclue --telemetry-reader builtin --blackclue-view front \
  --emit-csv --emit-gpx --emit-accel \
  --emit-stills --stills-every-seconds 30 --stills-name utc --stills-geojson \
  --emit-manifest --skip-existing
```

### Options added for the pipeline

| Option | What it does |
|---|---|
| `--clips-from FILE` | Process exactly the clips listed in FILE (one path per line; blank lines and `#` comments ignored) instead of scanning `--source`. |
| `--drive-tag TAG` | Use this exact tag, and treat all the given clips as one drive. Without it the tag is `YYYYMMDD_HHMMSS_<vehicle>` from the first clip. |
| `--drive-session-id UUID` | Fail unless the drive's id works out to this, so a mismatch is caught before anything is written. |
| `--camera-utc-offset ±HH:MM` | The camera's fixed offset from UTC (default `-06:00`). Used wherever a time comes from a clip filename or the camera's own clock. |
| `--telemetry-reader builtin\|blackclue` | How telemetry is read out of a clip. `builtin` (default) reads the embedded block in-process and writes nothing beside the clip. `blackclue` runs the external tool; `--blackclue`, `--blackclue-mode` and `--telemetry-sidecar-action` apply only then. |
| `--blackclue-jobs N` | How many clips to read telemetry from at once. |
| `--stills-jobs N` | How many stills to extract at once, one ffmpeg process each (default 4). |
| `--batch-id ID` | Recorded in the manifest under `pipeline.batch_id`. |
| `--max-failed-clip-ratio R` | Give up on a drive when more than this fraction of its clips fail telemetry extraction (default `0.2`). |

### Exit codes

| Code | Meaning |
|---|---|
| 0 | At least one drive was built. |
| 3 | Nothing to do: every drive was already built, or was too short. |
| 4 | Built, but a drive has no accelerometer CSV, so the database loader will reject it. |
| 1 / 2 | Failure, or a usage error. |

## How a drive gets its name and id

The tag is `YYYYMMDD_HHMMSS_<vehicle>` from the drive's first clip, and the id is
`uuid5(NAMESPACE_URL, "blackvue-drive:<vehicle>:<tag>")`. Both are now fully
determined by the clips.

Previously the script appended `_02`, `_03` … whenever a folder of that name
already existed, and it created the working folder *before* checking whether the
drive was already processed, so the check never fired. Every re-run therefore
produced another folder and another id, and the database ended up holding the
same drive several times (239 Camry sessions carry an `_02` id).

## Re-running is safe

Each drive is written into `<output>/_work/<vehicle>/<tag>_<view>.build/`. When it
is finished, `manifest.json` is written, then `_complete.json`, and only then is
the folder renamed into `<output>/<vehicle>/<tag>_<view>/` in one atomic step. A
crash or a kill can leave a `.build` folder behind, never a half-written drive
folder that the importer might pick up. The next run deletes it and starts over.

With `--skip-existing`, a drive is skipped when `_complete.json` shows the same
clips and the same processing options. A folder built before markers existed is
"adopted" (kept, and given a marker) when its manifest lists exactly the same
clips. Anything else is rebuilt.

Taking the card out during a build is expected rather than an error. The build
fails, the pipeline puts the drive back to "planned", and the next time the card
is seen the drive is built again from the start.

## How times are worked out

Both cameras run on a fixed UTC-6 clock with no daylight saving, drifting a few
seconds. The database needs each drive's real start.

* **Session start** is the *median* of `(each fixed GNSS row's own UTC) − t_rel_s`,
  over rows whose status is `A`. Every row votes for the same instant, so the
  handful of stale rows the camera emits right after waking from parking mode —
  they report the last fix's time but still claim a valid fix — cannot move it.
  Recorded as `time_confidence: gnss_median`.
* The script used to take the **earliest** point of the largest time cluster,
  which is exactly one of those stale rows. Measured against the August Camry
  footage, that start was 8 to 80 minutes early on 9 of 23 drives, and one drive
  overlapped the previous one.
* **Fewer than 30 fixed rows** → the first clip's filename read at the camera
  offset, flagged `time_confidence: filename_low`.
* **GNSS more than 15 minutes from the filename clock** → GNSS still wins, but
  the drive is flagged `gnss_median_clock_mismatch` for review. This is the
  January 2026 case where the camera clock itself was five hours out.
* **Clip and still times** come from each clip's own filename plus the drive's
  measured clock error, using the clip's measured length. Clips are ~61 s, and a
  normal clip is cut short (sometimes to 9 s) when an event or parking clip takes
  over, so assuming 60 s made still timestamps drift and attributed them to the
  wrong clip. `offset_s` in `artifacts/thumbs/index.csv` is the real number of
  seconds from the session start, which is how the database times each still.
* **Still frames** are seeked to exactly the moment their index row records.
  Every still produced before script version 62 is from about fifteen seconds
  later than its row says (see above), which is worth remembering when looking
  at older drives in the viewer.

## Manifest additions

`drive_tag`, `clip_set_sha1`, `clips_excluded[]`, `clips[].type` (N/E/P/I/M) and
`clips[].duration_s`, `source.clock_utc_offset`, `pipeline.batch_id`,
`pipeline.script_version`, and the `session_time` diagnostics
(`anchor_method`, `gnss_valid_rows`, `filename_start_ts_utc`, `filename_delta_s`,
`clock_offset_s`).

## Tolerated problems

* A rear clip with no matching front clip is dropped and listed in
  `clips_excluded` (it used to abort the whole run).
* A clip whose telemetry cannot be extracted is dropped and listed, unless more
  than `--max-failed-clip-ratio` of the drive fails.
* A moment that cannot be decoded for a still is skipped with a warning, unless
  more than a tenth of the drive's stills fail.

## Tests

```
python -m unittest discover -s pipeline/tests -v
```
