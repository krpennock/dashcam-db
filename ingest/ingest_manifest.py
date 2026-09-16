"""Load one processed drive folder into the database.

Usable two ways, and the command line behaves as it always has:

    python ingest_manifest.py /import/<drive folder>/manifest.json

    from ingest_manifest import ingest_drive, IngestDataError
    result = ingest_drive(conn, manifest_path, media_root)

What changed, and why
---------------------
* **The start time is never taken from the earliest GPS row again.** A receiver
  reports a stale time for the first seconds after it wakes, sometimes a time
  from days earlier, and everything in a drive is stored as an offset from the
  start, so one bad reading dragged the whole drive with it. About half the
  database had to be repaired because of this. A manifest written by the current
  drives script already carries a start worked out from the median of its fixed
  GPS rows and is trusted; anything older is recomputed here the same way.
* **A missing GPS file is no longer fatal**, so a drive recorded in a car park or
  a tunnel still loads, marked as approximately timed. The accelerometer file is
  still required, because without it there is nothing to load.
* **Staging is per-run.** The shared stage tables meant two loads at once
  silently corrupted each other; each run now gets its own temporary copies.
* **Failures raise `IngestDataError`** with a short code, so a caller can record
  why a drive did not load instead of a process exiting mid-batch.
* **The session id comes from the drive's tag**, with any `_NN` re-run suffix
  removed, so re-loading a drive updates it instead of creating a second copy.
"""
from __future__ import annotations

import csv
import json
import os
import re
import shutil
import statistics
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import psycopg

# --------------------------------------------------------------------------
# Rules, kept deliberately identical to pipeline/blackvue_drives.py and
# ingest/importer/repair.py. If one of these changes, change it in all three.
# --------------------------------------------------------------------------

#: Rows with a confirmed fix a drive needs before its GPS time is trusted.
GNSS_MIN_VALID_ROWS = 30

#: GPS and the camera clock normally agree within seconds. Past this the GPS
#: still wins, but the drive is flagged for a human to look at.
GNSS_FILENAME_TOLERANCE_S = 900.0

#: The cameras run on a fixed offset from UTC with no daylight saving.
DEFAULT_CAMERA_UTC_OFFSET = "-06:00"

#: Clip length assumed when a manifest does not carry a measured one.
NOMINAL_CLIP_SECONDS = 61.0

#: Confidence values only the current drives script writes. Anything else came
#: from an older version whose start time cannot be trusted.
TRUSTED_CONFIDENCES = {
    "gnss_median",
    "gnss_median_clock_mismatch",
    "filename_low",
    "filename_estimated_clock_offset",
}

TRAILING_RUN_RE = re.compile(r"_\d{2}$")
NOTES_RE = re.compile(r"^import\s+(?P<tag>\S+?)_(?P<view>front|rear|interior|other)\s*$")
FOLDER_RE = re.compile(r"^(?P<tag>\d{8}_\d{6}_[A-Za-z0-9]+(?:_\d{2})?)_(?P<view>front|rear|interior|other)$")
DATE_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})$")
TIME_RE = re.compile(r"^([01]\d|2[0-3])([0-5]\d)([0-5]\d)(\.\d+)?$")
STAMP_RE = re.compile(r"^(\d{8})_(\d{6})")


class IngestDataError(Exception):
    """The drive folder cannot be loaded, and retrying will not help.

    Carries a short code so the caller can record a reason against the import
    without parsing prose.
    """

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def _get_path(obj: Any, *keys: str):
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
        if cur is None:
            return None
    return cur


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_utc_offset(text: Optional[str]) -> timezone:
    """Parse "-06:00" / "+0530" / "Z" into a fixed timezone."""
    s = (text or "").strip()
    if not s or s.upper() == "Z":
        return timezone.utc
    m = re.match(r"^([+-])(\d{2}):?(\d{2})$", s)
    if not m:
        return parse_utc_offset(DEFAULT_CAMERA_UTC_OFFSET)
    delta = timedelta(hours=int(m.group(2)), minutes=int(m.group(3)))
    return timezone(-delta if m.group(1) == "-" else delta)


def base_tag_of(tag: Optional[str]) -> Optional[str]:
    """Strip a trailing _NN re-run suffix, which is what split drives in two."""
    return TRAILING_RUN_RE.sub("", tag) if tag else None


def stamp_utc(name: Optional[str], camera_tz: timezone) -> Optional[datetime]:
    """The YYYYMMDD_HHMMSS at the front of a tag or clip name, as real UTC."""
    if not name:
        return None
    m = STAMP_RE.match(str(name))
    if not m:
        return None
    try:
        local = datetime.strptime(f"{m.group(1)}_{m.group(2)}", "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return local.replace(tzinfo=camera_tz).astimezone(timezone.utc)


def clean_session_id(vehicle_tag: str, base_tag: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"blackvue-drive:{vehicle_tag}:{base_tag}"))


def gnss_anchor_from_csv(path: Path) -> dict[str, Any]:
    """When the drive really started, from the GPS rows themselves.

    Every row with a confirmed fix votes: its own clock minus how far into the
    drive it sits. The median ignores the stale rows the camera emits just after
    waking from parking mode, which still claim a valid fix.
    """
    votes: list[float] = []
    rows = 0
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows += 1
                if (row.get("rmc_status") or "").strip().upper() != "A":
                    continue
                d = DATE_RE.match((row.get("date_ddmmyy") or "").strip())
                t = TIME_RE.match((row.get("time_hhmmss") or "").strip())
                if not d or not t:
                    continue
                try:
                    instant = datetime(
                        2000 + int(d.group(3)), int(d.group(2)), int(d.group(1)),
                        int(t.group(1)), int(t.group(2)), int(t.group(3)),
                        tzinfo=timezone.utc,
                    )
                    votes.append(instant.timestamp() - float(row.get("t_rel_s") or 0.0))
                except (ValueError, TypeError):
                    continue
    except OSError:
        return {"anchor": None, "valid_rows": 0, "rows": 0}

    if not votes:
        return {"anchor": None, "valid_rows": 0, "rows": rows}
    return {
        "anchor": datetime.fromtimestamp(statistics.median(votes), tz=timezone.utc),
        "valid_rows": len(votes),
        "rows": rows,
    }


def max_t_rel_s(path: Optional[Path]) -> Optional[float]:
    """How long the drive ran, from a telemetry file's own timeline."""
    if path is None or not path.exists():
        return None
    best: Optional[float] = None
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    v = float(row.get("t_rel_s") or "nan")
                except (TypeError, ValueError):
                    continue
                if v == v and (best is None or v > best):  # v == v filters NaN
                    best = v
    except OSError:
        return None
    return best


def resolve_identity(manifest: dict, folder_name: str) -> tuple[str, str, str]:
    """Return (vehicle_tag, base drive tag, view) for a drive folder."""
    vehicle = (manifest.get("vehicle_tag") or "").strip()
    tag = manifest.get("drive_tag")
    view = None

    if not tag:
        m = NOTES_RE.match(str(manifest.get("notes") or ""))
        if m:
            tag, view = m.group("tag"), m.group("view")
    if not tag:
        m = FOLDER_RE.match(folder_name)
        if m:
            tag, view = m.group("tag"), m.group("view")
    if view is None:
        m = NOTES_RE.match(str(manifest.get("notes") or "")) or FOLDER_RE.match(folder_name)
        view = m.group("view") if m else "front"

    base = base_tag_of(tag)
    if not vehicle or not base:
        raise IngestDataError(
            "NO_DRIVE_TAG",
            f"cannot work out the vehicle and drive tag for {folder_name!r}",
        )
    return vehicle, base, view


def resolve_session_time(
    manifest: dict,
    *,
    gnss_csv: Optional[Path],
    accel_csv: Optional[Path],
    base_tag: str,
    camera_tz: timezone,
) -> dict[str, Any]:
    """Decide when the drive started, and how much to trust it."""
    st = manifest.get("session_time") or {}
    manifest_conf = str(st.get("time_confidence") or "")
    manifest_start = _parse_ts(st.get("start_ts_utc"))

    clips = manifest.get("clips") or []
    first_clip = None
    for c in clips:
        if isinstance(c, dict) and str(c.get("channel")) == "front" and c.get("clip_name"):
            first_clip = str(c["clip_name"])
            break
    file_start = stamp_utc(first_clip, camera_tz) or stamp_utc(base_tag, camera_tz)

    needs_review = False
    details: dict[str, Any] = {"manifest_time_confidence": manifest_conf or None}

    if manifest_start is not None and manifest_conf in TRUSTED_CONFIDENCES:
        # Written by the current drives script, which already used the median of
        # the fixed GPS rows.
        start, confidence = manifest_start, manifest_conf
        details["source"] = "manifest"
        needs_review = manifest_conf == "gnss_median_clock_mismatch"
    else:
        info = gnss_anchor_from_csv(gnss_csv) if (gnss_csv and gnss_csv.exists()) else {"anchor": None, "valid_rows": 0, "rows": 0}
        details.update({"gnss_valid_rows": info["valid_rows"], "gnss_rows": info["rows"], "source": "recomputed"})
        if info["anchor"] is not None and info["valid_rows"] >= GNSS_MIN_VALID_ROWS:
            start, confidence = info["anchor"], "gnss_median"
            if file_start is not None:
                delta = (start - file_start).total_seconds()
                details["camera_clock_delta_s"] = round(delta, 3)
                if abs(delta) > GNSS_FILENAME_TOLERANCE_S:
                    confidence, needs_review = "gnss_median_clock_mismatch", True
        elif file_start is not None:
            start, confidence = file_start, "filename_low"
        else:
            raise IngestDataError(
                "NO_START_TIME",
                "no usable GPS rows and no timestamp in the drive tag or clip names",
            )

    duration = max(
        [v for v in (max_t_rel_s(gnss_csv), max_t_rel_s(accel_csv)) if v is not None]
        or [0.0]
    )
    if duration <= 0:
        manifest_end = _parse_ts(st.get("end_ts_utc"))
        if manifest_start and manifest_end:
            duration = (manifest_end - manifest_start).total_seconds()
        else:
            fronts = sum(1 for c in clips if isinstance(c, dict) and c.get("channel") == "front")
            duration = max(fronts, 1) * NOMINAL_CLIP_SECONDS

    return {
        "start": start,
        "end": start + timedelta(seconds=float(duration)),
        "confidence": confidence,
        "needs_review": needs_review,
        "filename_start": file_start,
        "details": details,
    }


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------

def _copy_csv(cur: psycopg.Cursor, table: str, path: Path) -> int:
    with path.open("rb") as f:
        with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                cp.write(chunk)
    cur.execute(f"SELECT count(*) FROM {table};")
    return int(cur.fetchone()[0])


def _swap_thumbs(dest_dir: Path, staged: list[tuple[Path, str]]) -> int:
    """Replace a session's thumbnails with exactly the ones in this manifest.

    Copied into a fresh folder and swapped in, so a re-load never leaves
    thumbnails from a previous version of the drive lying around.
    """
    if not staged:
        return 0
    new_dir = dest_dir.with_name(dest_dir.name + ".new")
    if new_dir.exists():
        shutil.rmtree(new_dir, ignore_errors=True)
    new_dir.mkdir(parents=True, exist_ok=True)
    for src, name in staged:
        shutil.copy2(src, new_dir / name)

    old_dir = dest_dir.with_name(dest_dir.name + ".old")
    if old_dir.exists():
        shutil.rmtree(old_dir, ignore_errors=True)
    if dest_dir.exists():
        dest_dir.rename(old_dir)
    new_dir.rename(dest_dir)
    if old_dir.exists():
        shutil.rmtree(old_dir, ignore_errors=True)
    return len(staged)


def ingest_drive(
    conn: psycopg.Connection,
    manifest_path: Path | str,
    media_root: Path | str = "/media",
    *,
    batch_id: Optional[str] = None,
) -> dict[str, Any]:
    """Load one drive folder. Everything happens in a single transaction.

    Returns a summary of what was loaded. Raises IngestDataError when the folder
    itself is the problem, which a caller should record rather than retry.
    """
    manifest_path = Path(manifest_path).resolve()
    if not manifest_path.exists():
        raise IngestDataError("NO_MANIFEST", f"manifest not found: {manifest_path}")

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise IngestDataError("BAD_MANIFEST", f"manifest is not valid JSON: {exc}") from exc

    base = manifest_path.parent
    vehicle_tag, drive_tag, view = resolve_identity(manifest, base.name)
    sid = clean_session_id(vehicle_tag, drive_tag)
    # Older manifests record no clock offset. The cameras have always run on the
    # same fixed offset, and treating a missing value as UTC would put the
    # filename clock six hours out and flag every old drive as a clock mismatch.
    camera_tz = parse_utc_offset(
        _get_path(manifest, "source", "clock_utc_offset") or DEFAULT_CAMERA_UTC_OFFSET
    )

    telem = manifest.get("telemetry") or {}
    gnss_rel = _get_path(telem, "gnss", "path") or telem.get("gnss_csv")
    accel_rel = _get_path(telem, "accel", "path") or telem.get("accel_csv")

    gnss_csv = (base / gnss_rel).resolve() if gnss_rel else None
    accel_csv = (base / accel_rel).resolve() if accel_rel else None

    warnings: list[str] = []
    if gnss_csv is None or not gnss_csv.exists():
        # Not fatal: a drive with no fix still has an accelerometer trace and
        # still belongs in the database, just approximately timed.
        warnings.append("no GPS file; the drive is timed from the camera clock")
        gnss_csv = None
    if accel_csv is None:
        raise IngestDataError("NO_ACCEL_CSV", "the manifest lists no accelerometer file")
    if not accel_csv.exists():
        raise IngestDataError("NO_ACCEL_CSV", f"accelerometer file missing: {accel_csv.name}")

    timing = resolve_session_time(
        manifest, gnss_csv=gnss_csv, accel_csv=accel_csv, base_tag=drive_tag, camera_tz=camera_tz
    )
    start, end = timing["start"], timing["end"]

    manifest_sid = str(manifest.get("drive_session_id") or "")
    if manifest_sid and manifest_sid != sid:
        warnings.append(f"manifest id {manifest_sid} replaced by {sid} derived from the drive tag")

    counts_per_g = float(_get_path(telem, "accel", "counts_per_g") or 128.0)
    derived_params = manifest.get("derived_params")
    clips = [c for c in (manifest.get("clips") or []) if isinstance(c, dict)]

    time_sync = {
        "ingest": {
            "loaded_utc": datetime.now(timezone.utc).isoformat(),
            "time_confidence": timing["confidence"],
            "needs_review": timing["needs_review"],
            "start_ts_utc": start.isoformat(),
            "filename_start_ts_utc": timing["filename_start"].isoformat() if timing["filename_start"] else None,
            "camera_utc_offset": _get_path(manifest, "source", "clock_utc_offset") or DEFAULT_CAMERA_UTC_OFFSET,
            **timing["details"],
        }
    }

    counts: dict[str, int] = {}
    with conn.transaction():
        with conn.cursor() as cur:
            # An approved manual clock correction has to survive a re-load.
            # Four January drives were re-timed by hand because the camera's own
            # clock was five hours wrong that week and they carry too few fixes
            # to show it; this loader cannot rediscover that, and would quietly
            # put them back five hours early. Only this one case is preserved.
            cur.execute(
                "SELECT start_ts_utc, time_confidence FROM dashcam.drive_session "
                "WHERE drive_session_id = %s::uuid;",
                (sid,),
            )
            existing = cur.fetchone()
            if (
                existing
                and existing[0] is not None
                and existing[1] == "filename_estimated_clock_offset"
                and timing["confidence"] == "filename_low"
            ):
                duration = end - start
                start = existing[0]
                end = start + duration
                timing["confidence"] = "filename_estimated_clock_offset"
                time_sync["ingest"]["kept_existing_start"] = True
                time_sync["ingest"]["time_confidence"] = timing["confidence"]
                time_sync["ingest"]["start_ts_utc"] = start.isoformat()
                warnings.append(
                    "kept the camera-clock correction already approved for this drive "
                    "instead of falling back to the camera clock"
                )

            # Replace this session cleanly: the child rows go, the session row
            # stays and is updated, so anything referring to it survives.
            for table in ("storyboard_frame", "derived_event", "drive_summary",
                          "accel_sample", "gnss_sample", "clip"):
                cur.execute(f"DELETE FROM dashcam.{table} WHERE drive_session_id = %s::uuid;", (sid,))

            cur.execute(
                """
                INSERT INTO dashcam.drive_session (
                    drive_session_id, vehicle_tag, drive_tag, notes,
                    start_ts_utc, end_ts_utc, time_confidence, camera_serial,
                    source_batch_id, ingested_at, time_sync
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s::uuid, now(), %s::jsonb)
                ON CONFLICT (drive_session_id) DO UPDATE SET
                    vehicle_tag = EXCLUDED.vehicle_tag,
                    drive_tag = EXCLUDED.drive_tag,
                    notes = EXCLUDED.notes,
                    start_ts_utc = EXCLUDED.start_ts_utc,
                    end_ts_utc = EXCLUDED.end_ts_utc,
                    time_confidence = EXCLUDED.time_confidence,
                    camera_serial = COALESCE(EXCLUDED.camera_serial, dashcam.drive_session.camera_serial),
                    source_batch_id = COALESCE(EXCLUDED.source_batch_id, dashcam.drive_session.source_batch_id),
                    ingested_at = EXCLUDED.ingested_at,
                    time_sync = COALESCE(dashcam.drive_session.time_sync, '{}'::jsonb) || EXCLUDED.time_sync;
                """,
                (sid, vehicle_tag, drive_tag, f"import {drive_tag}_{view}", start, end,
                 timing["confidence"], _get_path(manifest, "source", "serial"),
                 batch_id, json.dumps(time_sync)),
            )

            # --- clips -------------------------------------------------------
            clip_rows = []
            for c in clips:
                name = str(c.get("clip_name") or "")
                if not name:
                    continue
                c_start = _parse_ts(c.get("start_ts_utc")) or stamp_utc(name, camera_tz)
                dur = c.get("duration_s")
                c_end = _parse_ts(c.get("end_ts_utc"))
                if c_end is None and c_start is not None:
                    c_end = c_start + timedelta(seconds=float(dur or NOMINAL_CLIP_SECONDS))
                clip_rows.append((sid, c.get("channel"), name, c_start, c_end,
                                  _get_path(c, "video", "path")))
            if clip_rows:
                cur.executemany(
                    """
                    INSERT INTO dashcam.clip
                        (drive_session_id, channel, clip_name, start_ts_utc, end_ts_utc, video_path)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s);
                    """,
                    clip_rows,
                )
            counts["clip"] = len(clip_rows)

            # --- GPS ---------------------------------------------------------
            # Per-run temporary tables: two loads at once used to share one set
            # of staging tables and truncate each other's rows.
            counts["gnss"] = 0
            if gnss_csv is not None:
                cur.execute(
                    "CREATE TEMP TABLE gnss_stage (LIKE dashcam.gnss_csv_stage) ON COMMIT DROP;"
                )
                staged = _copy_csv(cur, "gnss_stage", gnss_csv)
                expected = _get_path(telem, "gnss", "rows")
                if expected is not None and int(expected) != staged:
                    raise IngestDataError(
                        "GNSS_ROW_MISMATCH",
                        f"the GPS file holds {staged} rows but the manifest says {expected}",
                    )
                cur.execute(
                    """
                    INSERT INTO dashcam.gnss_sample (
                      drive_session_id, device_ms, t_rel_s, ts_utc, lat, lon,
                      speed_knots, speed_mps, speed_mph, course_deg, rmc_status,
                      fix_quality, satellites, hdop, alt_m, geoid_sep_m,
                      date_ddmmyy, time_hhmmss
                    )
                    SELECT %s::uuid, s.ms, s.t_rel_s,
                           %s::timestamptz + (s.t_rel_s * interval '1 second'),
                           s.lat, s.lon, s.speed_knots, s.speed_mps, s.speed_mph,
                           s.course_deg, NULLIF(s.rmc_status,'')::char(1),
                           s.fix_quality, s.satellites, s.hdop, s.alt_m, s.geoid_sep_m,
                           -- The receiver sometimes emits a malformed sentence whose
                           -- date field holds the time instead (173441.00 where
                           -- 070126 belongs). The column is six characters wide, and
                           -- such a value means nothing anyway, so it is dropped
                           -- rather than failing the whole drive: five January drives
                           -- sat unimported for months over a handful of these. The
                           -- row's real timestamp comes from the start plus t_rel_s.
                           CASE WHEN s.date_ddmmyy ~ '^[0-9]{6}$' THEN s.date_ddmmyy END,
                           NULLIF(s.time_hhmmss,'')
                    FROM gnss_stage s
                    WHERE s.t_rel_s IS NOT NULL;
                    """,
                    (sid, start),
                )
                counts["gnss"] = cur.rowcount

            # --- accelerometer ------------------------------------------------
            cur.execute(
                "CREATE TEMP TABLE accel_stage (LIKE dashcam.accel_csv_stage) ON COMMIT DROP;"
            )
            staged = _copy_csv(cur, "accel_stage", accel_csv)
            expected = _get_path(telem, "accel", "rows")
            if expected is not None and int(expected) != staged:
                raise IngestDataError(
                    "ACCEL_ROW_MISMATCH",
                    f"the accelerometer file holds {staged} rows but the manifest says {expected}",
                )
            cur.execute(
                """
                INSERT INTO dashcam.accel_sample (
                  drive_session_id, abs_ms, t_rel_s, ts_utc,
                  ax_raw, ay_raw, az_raw, ax_g, ay_g, az_g, a_mag_g,
                  clip_name, idx, t_clip_ms
                )
                SELECT %s::uuid, s.abs_ms, s.t_rel_s,
                       %s::timestamptz + (s.t_rel_s * interval '1 second'),
                       s.ax, s.ay, s.az,
                       s.ax / %s, s.ay / %s, s.az / %s,
                       |/ ((s.ax / %s)^2 + (s.ay / %s)^2 + (s.az / %s)^2),
                       s.clip, s.idx, s.t_clip_ms
                FROM accel_stage s
                WHERE s.t_rel_s IS NOT NULL;
                """,
                (sid, start, counts_per_g, counts_per_g, counts_per_g,
                 counts_per_g, counts_per_g, counts_per_g),
            )
            counts["accel"] = cur.rowcount
            if counts["accel"] == 0:
                raise IngestDataError("NO_ACCEL_ROWS", "the accelerometer file has no usable rows")

            # --- thumbnails ---------------------------------------------------
            counts["storyboard"] = _ingest_storyboard(
                cur, sid=sid, vehicle_tag=vehicle_tag, base=base,
                media_root=Path(media_root), start=start, warnings=warnings,
            )

            # --- events and summary -------------------------------------------
            if derived_params is None:
                cur.execute("SELECT dashcam.refresh_derived(%s::uuid);", (sid,))
            else:
                cur.execute(
                    "SELECT dashcam.refresh_derived(%s::uuid, %s::jsonb);",
                    (sid, json.dumps(derived_params)),
                )
            cur.execute(
                "SELECT count(*) FROM dashcam.derived_event WHERE drive_session_id = %s::uuid;",
                (sid,),
            )
            counts["events"] = int(cur.fetchone()[0])

    return {
        "status": "ingested",
        "drive_session_id": sid,
        "vehicle_tag": vehicle_tag,
        "drive_tag": drive_tag,
        "view": view,
        "start_ts_utc": start.isoformat(),
        "end_ts_utc": end.isoformat(),
        "time_confidence": timing["confidence"],
        "needs_review": timing["needs_review"],
        "counts": counts,
        "warnings": warnings,
    }


def _ingest_storyboard(
    cur: psycopg.Cursor,
    *,
    sid: str,
    vehicle_tag: str,
    base: Path,
    media_root: Path,
    start: datetime,
    warnings: list[str],
) -> int:
    """Load the still frames and copy their images into the media folder.

    Frame times come from the offset into the drive, and positions are taken
    from the GPS rows just loaded rather than from the index file: an index
    written against a wrong start time carries positions to match.
    """
    thumbs_dir = (base / "artifacts" / "thumbs").resolve()
    index_csv = thumbs_dir / "index.csv"
    index_geojson = thumbs_dir / "index.geojson"
    if not thumbs_dir.is_dir() or not (index_csv.exists() or index_geojson.exists()):
        return 0
    if not media_root.is_dir():
        warnings.append(f"media folder {media_root} is not mounted; thumbnails were skipped")
        return 0

    rows: list[dict[str, Any]] = []
    if index_geojson.exists():
        try:
            gj = json.loads(index_geojson.read_text(encoding="utf-8"))
        except ValueError:
            gj = {}
        for feat in (gj.get("features") or []):
            props = (feat or {}).get("properties") or {}
            if props.get("file"):
                rows.append(props)
    if not rows and index_csv.exists():
        with index_csv.open("r", encoding="utf-8", newline="") as f:
            rows = [r for r in csv.DictReader(f) if r.get("file")]

    def _f(value: Any) -> Optional[float]:
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    dest_dir = (media_root / vehicle_tag / sid / "thumbs").resolve()
    staged: list[tuple[Path, str]] = []
    db_rows = []
    for r in rows:
        name = str(r.get("file") or "").strip()
        src = thumbs_dir / name
        if not name or not src.exists():
            continue
        offset = _f(r.get("offset_s"))
        ts = start + timedelta(seconds=offset) if offset is not None else _parse_ts(r.get("utc_time"))
        if ts is None:
            continue
        staged.append((src, name))
        db_rows.append((
            sid, ts, ts, offset, (r.get("source_clip") or None), _f(r.get("clip_offset_s")),
            name, f"{vehicle_tag}/{sid}/thumbs/{name}",
        ))

    if not db_rows:
        return 0

    cur.executemany(
        """
        INSERT INTO dashcam.storyboard_frame (
          drive_session_id, ts_utc, local_time, offset_s,
          source_clip, clip_offset_s, file_name, media_rel_path
        )
        VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (drive_session_id, ts_utc) DO UPDATE SET
          local_time = EXCLUDED.local_time,
          offset_s = EXCLUDED.offset_s,
          source_clip = EXCLUDED.source_clip,
          clip_offset_s = EXCLUDED.clip_offset_s,
          file_name = EXCLUDED.file_name,
          media_rel_path = EXCLUDED.media_rel_path;
        """,
        db_rows,
    )
    inserted = len(db_rows)

    # Positions from the GPS rows of this very drive, nearest in time.
    # The lateral join sits inside a subquery over a second alias of the table:
    # an UPDATE target cannot be referenced from a LATERAL in its own FROM.
    cur.execute(
        """
        UPDATE dashcam.storyboard_frame f
           SET lat = n.lat, lon = n.lon, speed_mph = n.speed_mph, course_deg = n.course_deg
          FROM (
                SELECT f2.ts_utc, g.lat, g.lon, g.speed_mph, g.course_deg
                  FROM dashcam.storyboard_frame f2
                  CROSS JOIN LATERAL (
                        SELECT s.lat, s.lon, s.speed_mph, s.course_deg
                          FROM dashcam.gnss_sample s
                         WHERE s.drive_session_id = f2.drive_session_id
                           AND s.lat IS NOT NULL AND s.lon IS NOT NULL
                         ORDER BY abs(extract(epoch FROM (s.ts_utc - f2.ts_utc)))
                         LIMIT 1
                       ) g
                 WHERE f2.drive_session_id = %s::uuid
               ) n
         WHERE f.drive_session_id = %s::uuid
           AND f.ts_utc = n.ts_utc;
        """,
        (sid, sid),
    )

    try:
        _swap_thumbs(dest_dir, staged)
    except OSError as exc:
        warnings.append(f"thumbnails could not be written: {exc}")
    return inserted


# --------------------------------------------------------------------------
# Command line (unchanged contract: one manifest path, exit 0 on success)
# --------------------------------------------------------------------------

def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: ingest_manifest.py /path/to/manifest.json", file=sys.stderr)
        return 2

    conn = psycopg.connect(
        host=env("DB_HOST", "db"),
        port=env("DB_PORT", "5432"),
        dbname=env("DB_NAME", "dashcam"),
        user=env("DB_USER", "dashcam"),
        password=env("DB_PASSWORD", "change_me_now"),
        autocommit=True,
    )
    try:
        result = ingest_drive(conn, sys.argv[1], os.getenv("MEDIA_ROOT", "/media"))
    except IngestDataError as exc:
        print(f"FAILED {exc.code}: {exc.message}", file=sys.stderr)
        return 3
    finally:
        conn.close()

    for w in result["warnings"]:
        print(f"WARNING: {w}", file=sys.stderr)
    print(
        f"OK: ingested {result['drive_tag']} ({result['vehicle_tag']}) as {result['drive_session_id']}\n"
        f"    start {result['start_ts_utc']} [{result['time_confidence']}] counts {result['counts']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
