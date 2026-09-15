"""Report on wrong drive-session ids and wrong session start times.

This tool is READ-ONLY. It runs a handful of SELECT statements, works out what
each drive session's identifier and start time *should* be, and writes two
report files. It never writes to the database.

Two faults are being reported on:

1. Wrong ids. An older version of the processing script appended ``_02`` /
   ``_03`` to a drive's tag whenever a folder of that name already existed on
   disk. Because the session id is derived from the tag, a re-run produced a
   brand new id for a drive that was already in the database. The clean id is
   derived from the tag with any trailing ``_NN`` removed.

2. Wrong start times. The loader set each session's start to the earliest GPS
   ``utc_time`` it could find, which is frequently a stale reading left over
   from days earlier. Everything downstream (GPS points, accelerometer points,
   storyboard frames, clip times, event markers) is derived from that start, so
   a wrong start drags the whole session with it.

The corrected rule for a session's true start:

* ``filename_utc`` is the timestamp in the folder tag (camera local time, a
  fixed UTC-6 clock with no daylight saving) plus six hours.
* Every GPS row with a valid fix casts a vote: the row's own date and time,
  minus how far into the drive the row sits. The median of those votes is the
  GPS anchor.
* With at least 30 valid votes the GPS anchor wins (``gps_median``). If it
  disagrees with the filename by more than 15 minutes the session is flagged
  for a human to confirm, because that means the camera's own clock was set
  wrong rather than the GPS being wrong.
* With fewer than 30 votes we fall back to the filename (``filename_low``).

Usage::

    DB_HOST=... DB_PORT=... DB_NAME=... DB_USER=... DB_PASSWORD=... \
        python repair.py --report-dir /srv/dashcam/import/reports

"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg


# --------------------------------------------------------------------------
# Tunables. These are the thresholds the corrected rule is written in terms of.
# --------------------------------------------------------------------------

#: Minimum number of valid GPS votes before the GPS anchor is trusted.
MIN_VALID_GPS_ROWS = 30

#: How far the GPS anchor may sit from the filename time before a human is
#: asked to confirm which of the two is right (seconds).
CLOCK_MISMATCH_S = 900.0

#: A session counts as "re-timed" once it moves by more than this (seconds).
RETIMED_S = 120.0

#: The cameras run on a fixed UTC-6 clock, no daylight saving.
CAMERA_UTC_OFFSET_HOURS = 6

#: Namespace and template the clean drive_session_id is built from.
ID_NAMESPACE = uuid.NAMESPACE_URL
ID_TEMPLATE = "blackvue-drive:{vehicle_tag}:{base_tag}"

DEFAULT_IMPORT_ROOT = "/srv/dashcam/import"

#: Where the thumbnails live, one folder per session id.
DEFAULT_MEDIA_ROOT = "/media"

#: Event thresholds the drives script writes into every manifest. Used when a
#: session has no parameters of its own recorded on its summary row.
DEFAULT_DERIVED_PARAMS: dict[str, Any] = {
    "hard_brake_mps2": 2.6,
    "hard_accel_mps2": 2.2,
    "harsh_turn_mps2": 3.0,
    "min_speed_brake_mph": 12,
    "min_speed_turn_mph": 20,
    "stop_min_s": 15,
    "bump_cooldown_s": 3,
}

#: storyboard_frame and event_marker both carry ts_utc in their primary key, so
#: a shift of, say, exactly 30 seconds would move one row onto another's current
#: timestamp part-way through the statement. Their timestamps are parked this far
#: out first, then written to their final values.
TEMP_SHIFT_INTERVAL = "1000 years"

#: Clip length used when rewriting clip times for drives that are already loaded.
#: The camera writes roughly 61 s clips and no measured duration was kept for
#: them, so this is a stand-in, not a measurement.
NOMINAL_CLIP_SECONDS = 61.0

#: Manual camera-clock corrections live in this file, beside this script.
CLOCK_FIXES_FILENAME = "clock_fixes.json"

#: ``notes`` looks like ``import 20251228_140500_camry_02_front``.
NOTES_RE = re.compile(
    r"^import\s+(?P<tag>\d{8}_\d{6}_[A-Za-z0-9]+(?:_\d{2})?)"
    r"_(?P<view>front|rear|interior|other)\s*$"
)

#: A drive folder looks like ``20251228_140500_camry_02_front``.
FOLDER_RE = re.compile(
    r"^(?P<tag>\d{8}_\d{6}_[A-Za-z0-9]+(?:_\d{2})?)"
    r"_(?P<view>front|rear|interior|other)$"
)

TRAILING_RUN_RE = re.compile(r"_\d{2}$")

#: GPS rows carry the date as DDMMYY and the time as HHMMSS with optional
#: fractional seconds. Some rows are junk, so both are matched strictly.
DATE_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})$")
TIME_RE = re.compile(r"^([01]\d|2[0-3])([0-5]\d)([0-5]\d)(\.\d+)?$")


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


# --------------------------------------------------------------------------
# Small parsing helpers
# --------------------------------------------------------------------------

def parse_notes_tag(notes: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Return ``(tag, view)`` from a ``drive_session.notes`` value."""
    if not notes:
        return None, None
    m = NOTES_RE.match(notes)
    if not m:
        return None, None
    return m.group("tag"), m.group("view")


def base_tag_of(tag: Optional[str]) -> Optional[str]:
    """Strip a trailing ``_NN`` re-run suffix from a tag."""
    if not tag:
        return None
    return TRAILING_RUN_RE.sub("", tag)


def clean_session_id(vehicle_tag: str, base_tag: str) -> str:
    name = ID_TEMPLATE.format(vehicle_tag=vehicle_tag, base_tag=base_tag)
    return str(uuid.uuid5(ID_NAMESPACE, name))


def filename_utc_of(tag: Optional[str]) -> Optional[datetime]:
    """Camera-local timestamp in the tag, converted to UTC."""
    if not tag or len(tag) < 15:
        return None
    try:
        local = datetime.strptime(tag[:15], "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return local.replace(tzinfo=timezone.utc) + timedelta(hours=CAMERA_UTC_OFFSET_HOURS)


def parse_gps_instant(date_ddmmyy: Optional[str], time_hhmmss: Optional[str]) -> Optional[datetime]:
    """Turn a GPS row's own date and time into a UTC instant, or ``None``."""
    if not date_ddmmyy or not time_hhmmss:
        return None
    dm = DATE_RE.match(date_ddmmyy.strip())
    tm = TIME_RE.match(time_hhmmss.strip())
    if not dm or not tm:
        return None
    dd, mm, yy = int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
    hh, mi, ss = int(tm.group(1)), int(tm.group(2)), int(tm.group(3))
    frac = float(tm.group(4)) if tm.group(4) else 0.0
    try:
        return datetime(
            2000 + yy, mm, dd, hh, mi, ss, tzinfo=timezone.utc
        ) + timedelta(seconds=frac)
    except ValueError:
        # e.g. 30 February -- a junk row.
        return None


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt is not None else None


def human_delta(seconds: Optional[float]) -> str:
    """Render a signed shift the way a person would say it."""
    if seconds is None:
        return "unknown"
    sign = "-" if seconds < 0 else "+"
    s = abs(float(seconds))
    if s < 1:
        return "no change"
    if s < 90:
        return f"{sign}{s:.0f} sec"
    if s < 5400:
        return f"{sign}{s / 60:.1f} min"
    if s < 86400:
        return f"{sign}{s / 3600:.2f} hours"
    return f"{sign}{s / 86400:.2f} days"


# --------------------------------------------------------------------------
# Database reads
# --------------------------------------------------------------------------

def connect(read_only: bool = True) -> psycopg.Connection:
    conn = psycopg.connect(
        host=env("DB_HOST", "db"),
        port=env("DB_PORT", "5432"),
        dbname=env("DB_NAME", "dashcam"),
        user=env("DB_USER", "dashcam"),
        password=env("DB_PASSWORD", "change_me_now"),
        autocommit=True,
    )
    if read_only:
        # Belt and braces: make the server itself reject any write from this tool
        # unless it was started with --apply.
        with conn.cursor() as cur:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    return conn


def identity_columns_present(conn: psycopg.Connection) -> bool:
    """Has db/migrations/27_drive_identity.sql been applied?"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM information_schema.columns
            WHERE table_schema = 'dashcam' AND table_name = 'drive_session'
              AND column_name IN ('drive_tag', 'time_confidence');
            """
        )
        return int(cur.fetchone()[0]) == 2


def fetch_summary_params(conn: psycopg.Connection) -> dict[str, dict[str, Any]]:
    """Each session's event thresholds, as recorded on its summary row.

    Re-using them keeps a repaired session's events computed the same way they
    were the first time; only sessions with none fall back to the defaults.
    """
    out: dict[str, dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT drive_session_id::text, params FROM dashcam.drive_summary;")
        for sid, params in cur.fetchall():
            if isinstance(params, (str, bytes, bytearray)):
                try:
                    params = json.loads(params)
                except ValueError:
                    params = None
            if isinstance(params, dict) and params:
                out[sid] = params
    return out


def load_clock_fixes(path: Optional[Path]) -> dict[str, Any]:
    """Manual camera-clock corrections, if a file of them was supplied.

    Some drives sit inside a window where the camera's own clock was wrong, but
    carry too few satellite fixes to measure it. The correction measured from
    their neighbours is applied to them instead, and recorded as an estimate.
    """
    if path is None:
        return {"offset_s": 0.0, "tags": set(), "source": None, "raw": None}
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {
        "offset_s": float(data.get("offset_s") or 0.0),
        "tags": {str(t) for t in (data.get("tags") or [])},
        "source": str(path),
        "raw": data,
    }


def fetch_sessions(conn: psycopg.Connection) -> list[dict[str, Any]]:
    have_identity = identity_columns_present(conn)
    extra = ", drive_tag, time_confidence" if have_identity else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT drive_session_id::text, vehicle_tag, notes,
                   start_ts_utc, end_ts_utc{extra}
            FROM dashcam.drive_session
            ORDER BY start_ts_utc NULLS LAST, drive_session_id;
            """
        )
        rows = cur.fetchall()

    sessions = []
    for row in rows:
        sid, vehicle_tag, notes, start_ts, end_ts = row[:5]
        stored_tag, stored_conf = (row[5], row[6]) if have_identity else (None, None)
        tag, view = parse_notes_tag(notes)
        base = base_tag_of(tag)
        sessions.append(
            {
                "current_id": sid,
                "vehicle_tag": vehicle_tag,
                "notes": notes,
                "tag": tag,
                "view": view,
                "base_tag": base,
                "current_start_ts_utc": start_ts,
                "current_end_ts_utc": end_ts,
                "clean_id": clean_session_id(vehicle_tag, base) if (vehicle_tag and base) else None,
                "filename_utc": filename_utc_of(tag),
                "stored_drive_tag": stored_tag,
                "stored_time_confidence": stored_conf,
                "identity_columns": have_identity,
            }
        )
    return sessions


def fetch_row_counts(conn: psycopg.Connection) -> dict[str, dict[str, int]]:
    """Per-session row counts for every table that hangs off a session."""
    tables = {
        "clip": "dashcam.clip",
        "gnss": "dashcam.gnss_sample",
        "accel": "dashcam.accel_sample",
        "storyboard": "dashcam.storyboard_frame",
        "derived_event": "dashcam.derived_event",
    }
    counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {k: 0 for k in tables}
    )
    with conn.cursor() as cur:
        for key, table in tables.items():
            cur.execute(
                f"SELECT drive_session_id::text, count(*) FROM {table} "
                f"GROUP BY 1;"
            )
            for sid, n in cur.fetchall():
                counts[sid][key] = int(n)
    return counts


def fetch_clip_names(conn: psycopg.Connection) -> dict[str, set[str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT drive_session_id::text, clip_name FROM dashcam.clip;"
        )
        out: dict[str, set[str]] = defaultdict(set)
        for sid, clip_name in cur.fetchall():
            out[sid].add(clip_name)
    return out


def fetch_gnss_span(conn: psycopg.Connection) -> dict[str, float]:
    """Longest ``t_rel_s`` per session -- how long the drive ran for."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT drive_session_id::text, max(t_rel_s)
            FROM dashcam.gnss_sample
            GROUP BY 1;
            """
        )
        return {
            sid: float(v)
            for sid, v in cur.fetchall()
            if v is not None
        }


def fetch_gps_anchors(conn: psycopg.Connection) -> dict[str, tuple[Optional[datetime], int]]:
    """Median GPS anchor and vote count for every session.

    Each GPS row with a valid fix (``rmc_status = 'A'``) whose own date and
    time parse as a real instant casts one vote for where the drive started:
    the row's instant minus how far into the drive it sits.
    """
    votes: dict[str, list[float]] = defaultdict(list)

    # A server-side cursor only survives inside a transaction, and the
    # connection is in autocommit mode, so open one explicitly. The session is
    # already marked read only, so this transaction cannot write either.
    with conn.transaction():
        with conn.cursor(name="gnss_anchor_scan") as cur:
            cur.itersize = 50_000
            cur.execute(
                """
                SELECT drive_session_id::text, t_rel_s, date_ddmmyy, time_hhmmss
                FROM dashcam.gnss_sample
                WHERE rmc_status = 'A'
                  AND t_rel_s IS NOT NULL
                  AND date_ddmmyy IS NOT NULL
                  AND time_hhmmss IS NOT NULL;
                """
            )
            for sid, t_rel_s, date_ddmmyy, time_hhmmss in cur:
                instant = parse_gps_instant(date_ddmmyy, time_hhmmss)
                if instant is None:
                    continue
                votes[sid].append(instant.timestamp() - float(t_rel_s))

    anchors: dict[str, tuple[Optional[datetime], int]] = {}
    for sid, vs in votes.items():
        median_epoch = statistics.median(vs)
        anchors[sid] = (
            datetime.fromtimestamp(median_epoch, tz=timezone.utc),
            len(vs),
        )
    return anchors


# --------------------------------------------------------------------------
# The corrected start-time rule
# --------------------------------------------------------------------------

def decide_start(session: dict[str, Any], clock_fixes: Optional[dict[str, Any]] = None) -> None:
    """Fill in proposed start, method and flags for one session, in place."""
    clock_fixes = clock_fixes or {"offset_s": 0.0, "tags": set()}
    filename_utc: Optional[datetime] = session["filename_utc"]
    anchor: Optional[datetime] = session["gps_median_anchor"]
    n_valid: int = session["n_valid"]

    gps_vs_filename = None
    if anchor is not None and filename_utc is not None:
        gps_vs_filename = (anchor - filename_utc).total_seconds()

    session["gps_vs_filename_s"] = gps_vs_filename

    if n_valid >= MIN_VALID_GPS_ROWS and anchor is not None:
        session["proposed_start_ts_utc"] = anchor
        session["method"] = "gps_median"
        session["needs_review"] = (
            gps_vs_filename is not None and abs(gps_vs_filename) > CLOCK_MISMATCH_S
        )
    else:
        # Too few fixes to trust the satellites: the camera's own clock is the
        # best there is. Where that clock is known to have been wrong, a
        # correction measured from neighbouring drives is applied instead, and
        # marked as an estimate rather than a measurement.
        proposed = filename_utc
        method = "filename_low"
        if filename_utc is not None and session.get("base_tag") in clock_fixes.get("tags", ()):
            offset = float(clock_fixes.get("offset_s") or 0.0)
            proposed = filename_utc + timedelta(seconds=offset)
            method = "filename_estimated_clock_offset"
            session["clock_fix_offset_s"] = offset
        session["proposed_start_ts_utc"] = proposed
        session["method"] = method
        session["needs_review"] = False

    current = session["current_start_ts_utc"]
    proposed = session["proposed_start_ts_utc"]
    if current is not None and proposed is not None:
        session["shift_s"] = (proposed - current).total_seconds()
    else:
        session["shift_s"] = None

    duration = session.get("duration_s")
    if proposed is not None and duration is not None:
        session["proposed_end_ts_utc"] = proposed + timedelta(seconds=duration)
    else:
        session["proposed_end_ts_utc"] = None


SHIFT_BUCKETS: list[tuple[str, float, float]] = [
    ("unchanged (under 2 sec)", 0.0, 2.0),
    ("2 sec to 1 min", 2.0, 60.0),
    ("1 to 2 min", 60.0, 120.0),
    ("2 to 15 min", 120.0, 900.0),
    ("15 min to 1 hour", 900.0, 3600.0),
    ("1 to 6 hours", 3600.0, 21600.0),
    ("more than 6 hours", 21600.0, float("inf")),
]


def bucket_for(shift_s: Optional[float]) -> str:
    if shift_s is None:
        return "no current start time"
    a = abs(shift_s)
    for label, lo, hi in SHIFT_BUCKETS:
        if lo <= a < hi:
            return label
    return SHIFT_BUCKETS[-1][0]


# --------------------------------------------------------------------------
# Review list
# --------------------------------------------------------------------------

def find_overlaps(sessions: list[dict[str, Any]],
                  clip_names: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Pairs that would overlap in time after re-timing AND share a clip set.

    Overlapping alone is normal (two cameras, or a drive that abuts the next).
    A pair that overlaps *and* lists exactly the same clips is a genuine
    duplicate of the same drive. The expectation is that there are none.
    """
    timed = [
        s for s in sessions
        if s["proposed_start_ts_utc"] is not None
        and s["proposed_end_ts_utc"] is not None
    ]
    timed.sort(key=lambda s: s["proposed_start_ts_utc"])

    out = []
    for i, a in enumerate(timed):
        a_clips = clip_names.get(a["current_id"], set())
        for b in timed[i + 1:]:
            if b["proposed_start_ts_utc"] > a["proposed_end_ts_utc"]:
                break  # sorted by start, so nothing later can overlap a
            b_clips = clip_names.get(b["current_id"], set())
            if not a_clips or not b_clips:
                continue
            if a_clips != b_clips:
                continue
            out.append(
                {
                    "session_a": a["current_id"],
                    "session_b": b["current_id"],
                    "tag_a": a["tag"],
                    "tag_b": b["tag"],
                    "vehicle_tag": a["vehicle_tag"],
                    "a_start": iso(a["proposed_start_ts_utc"]),
                    "a_end": iso(a["proposed_end_ts_utc"]),
                    "b_start": iso(b["proposed_start_ts_utc"]),
                    "b_end": iso(b["proposed_end_ts_utc"]),
                    "clip_count": len(a_clips),
                }
            )
    return out


def find_id_collisions(sessions: list[dict[str, Any]],
                       clip_names: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Two or more different current sessions that map to one clean id."""
    by_clean: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for s in sessions:
        if s["clean_id"]:
            by_clean[s["clean_id"]].append(s)

    out = []
    for clean_id, group in sorted(by_clean.items()):
        if len(group) < 2:
            continue
        out.append(
            {
                "clean_id": clean_id,
                "vehicle_tag": group[0]["vehicle_tag"],
                "base_tag": group[0]["base_tag"],
                "members": [
                    {
                        "current_id": s["current_id"],
                        "tag": s["tag"],
                        "current_start_ts_utc": iso(s["current_start_ts_utc"]),
                        "proposed_start_ts_utc": iso(s["proposed_start_ts_utc"]),
                        "method": s["method"],
                        "counts": s["counts"],
                        "clip_names": sorted(clip_names.get(s["current_id"], set())),
                    }
                    for s in group
                ],
            }
        )
    return out


# --------------------------------------------------------------------------
# Server import tree
# --------------------------------------------------------------------------

def scan_import_tree(import_root: Path,
                     clean_ids_in_db: set[str]) -> dict[str, Any]:
    """Which folders under ``incoming/`` are already loaded, and duplicated."""
    result: dict[str, Any] = {
        "root": str(import_root),
        "exists": import_root.is_dir(),
        "vehicles": {},
        "totals": {
            "incoming": 0,
            "in_database": 0,
            "not_in_database": 0,
            "also_under_done": 0,
            "also_under_failed": 0,
            "redundant_copies": 0,
        },
    }
    if not result["exists"]:
        return result

    incoming_root = import_root / "incoming"
    if not incoming_root.is_dir():
        return result

    for vehicle_dir in sorted(p for p in incoming_root.iterdir() if p.is_dir()):
        vehicle = vehicle_dir.name
        done_dir = import_root / "done" / vehicle
        failed_dir = import_root / "failed" / vehicle
        done_names = {p.name for p in done_dir.iterdir()} if done_dir.is_dir() else set()
        failed_names = {p.name for p in failed_dir.iterdir()} if failed_dir.is_dir() else set()

        entries = []
        for folder in sorted(p for p in vehicle_dir.iterdir() if p.is_dir()):
            m = FOLDER_RE.match(folder.name)
            tag = m.group("tag") if m else None
            base = base_tag_of(tag)
            cid = clean_session_id(vehicle, base) if base else None
            in_db = bool(cid and cid in clean_ids_in_db)
            in_done = folder.name in done_names
            in_failed = folder.name in failed_names
            entries.append(
                {
                    "folder": folder.name,
                    "tag": tag,
                    "base_tag": base,
                    "clean_id": cid,
                    "in_database": in_db,
                    "also_under_done": in_done,
                    "also_under_failed": in_failed,
                    "has_manifest": (folder / "manifest.json").is_file(),
                    # Redundant = the drive is already loaded and a copy of the
                    # folder is already filed under done/ or failed/.
                    "redundant_copy": in_db and (in_done or in_failed),
                }
            )

        result["vehicles"][vehicle] = {
            "incoming_count": len(entries),
            "done_count": len(done_names),
            "failed_count": len(failed_names),
            "entries": entries,
        }
        t = result["totals"]
        t["incoming"] += len(entries)
        t["in_database"] += sum(1 for e in entries if e["in_database"])
        t["not_in_database"] += sum(1 for e in entries if not e["in_database"])
        t["also_under_done"] += sum(1 for e in entries if e["also_under_done"])
        t["also_under_failed"] += sum(1 for e in entries if e["also_under_failed"])
        t["redundant_copies"] += sum(1 for e in entries if e["redundant_copy"])

    return result


# --------------------------------------------------------------------------
# Report assembly
# --------------------------------------------------------------------------

def summarise(sessions: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(sessions)
    new_id = sum(1 for s in sessions if s["id_changes"])
    retimed = sum(
        1 for s in sessions
        if s["shift_s"] is not None and abs(s["shift_s"]) > RETIMED_S
    )

    buckets: dict[str, int] = {label: 0 for label, _, _ in SHIFT_BUCKETS}
    buckets["no current start time"] = 0
    for s in sessions:
        buckets[bucket_for(s["shift_s"])] += 1

    by_method: dict[str, int] = defaultdict(int)
    for s in sessions:
        by_method[s["method"]] += 1

    per_vehicle: dict[str, dict[str, int]] = {}
    for s in sessions:
        v = s["vehicle_tag"] or "(none)"
        d = per_vehicle.setdefault(
            v,
            {
                "sessions": 0,
                "new_id": 0,
                "retimed_over_2_min": 0,
                "gps_median": 0,
                "filename_low": 0,
                "needs_review": 0,
                "no_gnss": 0,
            },
        )
        d["sessions"] += 1
        if s["id_changes"]:
            d["new_id"] += 1
        if s["shift_s"] is not None and abs(s["shift_s"]) > RETIMED_S:
            d["retimed_over_2_min"] += 1
        d[s["method"]] = d.get(s["method"], 0) + 1
        if s["needs_review"]:
            d["needs_review"] += 1
        if s["counts"]["gnss"] == 0:
            d["no_gnss"] += 1

    gps_near_filename = sum(
        1 for s in sessions
        if s["method"] == "gps_median"
        and s["gps_vs_filename_s"] is not None
        and abs(s["gps_vs_filename_s"]) <= RETIMED_S
    )

    return {
        "total_sessions": total,
        "sessions_getting_a_new_id": new_id,
        "sessions_keeping_their_id": total - new_id,
        "sessions_retimed_over_2_min": retimed,
        "shift_buckets": buckets,
        "shift_buckets_total": sum(buckets.values()),
        "by_method": dict(by_method),
        "gps_median_within_2_min_of_filename": gps_near_filename,
        "per_vehicle": per_vehicle,
    }


def collect_sessions(
    conn: psycopg.Connection, clock_fixes: Optional[dict[str, Any]] = None
) -> list[dict[str, Any]]:
    """Every session, with its clean id and its corrected start worked out."""
    sessions = fetch_sessions(conn)
    counts = fetch_row_counts(conn)
    spans = fetch_gnss_span(conn)
    anchors = fetch_gps_anchors(conn)

    for s in sessions:
        sid = s["current_id"]
        s["counts"] = counts.get(
            sid, {"clip": 0, "gnss": 0, "accel": 0, "storyboard": 0, "derived_event": 0}
        )
        anchor, n_valid = anchors.get(sid, (None, 0))
        s["gps_median_anchor"] = anchor
        s["n_valid"] = n_valid
        s["total_gnss_rows"] = s["counts"]["gnss"]

        duration = spans.get(sid)
        if duration is None and s["current_start_ts_utc"] and s["current_end_ts_utc"]:
            duration = (s["current_end_ts_utc"] - s["current_start_ts_utc"]).total_seconds()
        s["duration_s"] = duration

        decide_start(s, clock_fixes)
        s["id_changes"] = bool(s["clean_id"]) and s["clean_id"] != s["current_id"]

    return sessions


def build_report(
    conn: psycopg.Connection,
    import_root: Path,
    clock_fixes: Optional[dict[str, Any]] = None,
    sessions: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    sessions = sessions if sessions is not None else collect_sessions(conn, clock_fixes)
    clip_names = fetch_clip_names(conn)

    clock_mismatch = [s for s in sessions if s["needs_review"]]
    no_gnss = [s for s in sessions if s["counts"]["gnss"] == 0]
    unparsed = [s for s in sessions if not s["tag"]]
    overlaps = find_overlaps(sessions, clip_names)
    collisions = find_id_collisions(sessions, clip_names)

    clean_ids_in_db = {s["clean_id"] for s in sessions if s["clean_id"]}
    import_tree = scan_import_tree(import_root, clean_ids_in_db)

    return {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "report",
        "rule": {
            "min_valid_gps_rows": MIN_VALID_GPS_ROWS,
            "clock_mismatch_seconds": CLOCK_MISMATCH_S,
            "retimed_seconds": RETIMED_S,
            "camera_utc_offset_hours": CAMERA_UTC_OFFSET_HOURS,
            "id_template": ID_TEMPLATE,
            "id_namespace": "uuid.NAMESPACE_URL",
        },
        "summary": summarise(sessions),
        "review": {
            "clock_mismatch": [
                {
                    "current_id": s["current_id"],
                    "clean_id": s["clean_id"],
                    "vehicle_tag": s["vehicle_tag"],
                    "tag": s["tag"],
                    "filename_utc": iso(s["filename_utc"]),
                    "gps_median_anchor": iso(s["gps_median_anchor"]),
                    "n_valid": s["n_valid"],
                    "gps_vs_filename_s": s["gps_vs_filename_s"],
                    "proposed_start_ts_utc": iso(s["proposed_start_ts_utc"]),
                }
                for s in clock_mismatch
            ],
            "duplicate_overlaps": overlaps,
            "no_gnss_rows": [
                {
                    "current_id": s["current_id"],
                    "clean_id": s["clean_id"],
                    "vehicle_tag": s["vehicle_tag"],
                    "tag": s["tag"],
                    "current_start_ts_utc": iso(s["current_start_ts_utc"]),
                    "proposed_start_ts_utc": iso(s["proposed_start_ts_utc"]),
                    "method": s["method"],
                    "counts": s["counts"],
                }
                for s in no_gnss
            ],
            "clean_id_collisions": collisions,
            "unparsable_notes": [
                {"current_id": s["current_id"], "notes": s["notes"]} for s in unparsed
            ],
        },
        "import_tree": import_tree,
        "sessions": [
            {
                "current_drive_session_id": s["current_id"],
                "clean_drive_session_id": s["clean_id"],
                "id_changes": s["id_changes"],
                "vehicle_tag": s["vehicle_tag"],
                "tag": s["tag"],
                "base_tag": s["base_tag"],
                "notes": s["notes"],
                "current_start_ts_utc": iso(s["current_start_ts_utc"]),
                "current_end_ts_utc": iso(s["current_end_ts_utc"]),
                "filename_utc": iso(s["filename_utc"]),
                "gps_median_anchor": iso(s["gps_median_anchor"]),
                "n_valid": s["n_valid"],
                "total_gnss_rows": s["total_gnss_rows"],
                "gps_vs_filename_s": s["gps_vs_filename_s"],
                "proposed_start_ts_utc": iso(s["proposed_start_ts_utc"]),
                "proposed_end_ts_utc": iso(s["proposed_end_ts_utc"]),
                "duration_s": s["duration_s"],
                "method": s["method"],
                "needs_review": s["needs_review"],
                "shift_s": s["shift_s"],
                "shift_bucket": bucket_for(s["shift_s"]),
                "counts": s["counts"],
            }
            for s in sessions
        ],
    }


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

def render_markdown(report: dict[str, Any]) -> str:
    s = report["summary"]
    rv = report["review"]
    tree = report["import_tree"]
    L: list[str] = []
    add = L.append

    add("# Dashcam database repair report")
    add("")
    add(f"Generated {report['generated_utc']} (UTC). Nothing was changed. "
        "This is a description of what is wrong and what a repair would do.")
    add("")

    add("## What this is about")
    add("")
    add("Every drive the cameras recorded is stored as a *session*. Two things "
        "went wrong when those sessions were loaded.")
    add("")
    add("**The identifiers drifted.** The processing script used to add `_02` or "
        "`_03` to a drive's name whenever a folder of that name already existed "
        "on disk. The session's identifier is calculated from that name, so "
        "re-processing the same drive produced a brand new identifier instead of "
        "recognising the drive it already had. The fix is to calculate the "
        "identifier from the drive's real name, with the `_02` style suffix "
        "removed.")
    add("")
    add("**The start times are often wrong.** The loader took the earliest GPS "
        "timestamp it could find in the drive and used that as the moment the "
        "drive began. GPS receivers report a stale time for the first few "
        "seconds after they wake up, sometimes a time from days earlier, so that "
        "reading is frequently nonsense. Because the position, the "
        "accelerometer, the preview images and the clip times are all measured "
        "as an offset from the start, one bad start time drags the whole drive "
        "with it.")
    add("")
    add("The corrected start time is worked out two ways. The cameras run on a "
        "fixed clock six hours behind UTC that never changes for daylight "
        "saving, so the timestamp in the drive's own name gives one answer. "
        "Separately, every GPS reading that has a proper satellite fix votes for "
        "a start time, by taking its own timestamp and subtracting how far into "
        "the drive it sits; the middle value of those votes gives the other "
        "answer. When there are at least "
        f"{report['rule']['min_valid_gps_rows']} good GPS votes the GPS answer "
        "is used, because satellite time is more reliable than the camera's "
        "internal clock. Otherwise the name is used.")
    add("")

    add("## The numbers")
    add("")
    add(f"- Sessions in the database: **{s['total_sessions']}**")
    add(f"- Sessions that would get a corrected identifier: **{s['sessions_getting_a_new_id']}**")
    add(f"- Sessions that already have the right identifier: **{s['sessions_keeping_their_id']}**")
    add(f"- Sessions whose start time moves by more than two minutes: **{s['sessions_retimed_over_2_min']}**")
    add("")
    add("How the corrected start time was decided:")
    add("")
    for method, n in sorted(s["by_method"].items()):
        label = ("GPS satellite time (the reliable case)"
                 if method == "gps_median"
                 else "the drive's own name, because there were too few usable GPS readings")
        add(f"- `{method}` -- {label}: **{n}**")
    add("")
    add(f"Of the sessions decided by GPS, **{s['gps_median_within_2_min_of_filename']}** "
        "agree with the camera's own clock to within two minutes, which is the "
        "reassuring case: two independent sources telling the same story.")
    add("")
    add("How far the start times move:")
    add("")
    add("| How much the start time moves | Sessions |")
    add("| --- | ---: |")
    for label, _, _ in SHIFT_BUCKETS:
        add(f"| {label} | {s['shift_buckets'][label]} |")
    if s["shift_buckets"].get("no current start time"):
        add(f"| no current start time | {s['shift_buckets']['no current start time']} |")
    add(f"| **total** | **{s['shift_buckets_total']}** |")
    add("")
    add("Per vehicle:")
    add("")
    add("| Vehicle | Sessions | New identifier | Moved over 2 min | GPS time | Name time | Needs review | No GPS rows |")
    add("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for v, d in sorted(s["per_vehicle"].items()):
        add(f"| {v} | {d['sessions']} | {d['new_id']} | {d['retimed_over_2_min']} | "
            f"{d['gps_median']} | {d['filename_low']} | {d['needs_review']} | {d['no_gnss']} |")
    add("")

    add("## Things a person needs to look at")
    add("")

    add("### Drives where the camera clock and the satellites disagree")
    add("")
    if not rv["clock_mismatch"]:
        add("None. The camera clock and the satellites agree everywhere.")
    else:
        add(f"{len(rv['clock_mismatch'])} drive(s). In each of these the GPS has "
            "plenty of good readings, but they disagree with the camera's own "
            "clock by more than fifteen minutes. That means the camera's clock "
            "itself was set wrong at the time. The satellite time is almost "
            "certainly the correct one, but because this changes the drive's "
            "date and time noticeably, it is worth confirming before anything "
            "is applied.")
        add("")
        add("| Vehicle | Drive | Camera clock says | Satellites say | Disagreement | Good GPS readings |")
        add("| --- | --- | --- | --- | --- | ---: |")
        for r in rv["clock_mismatch"]:
            add(f"| {r['vehicle_tag']} | `{r['tag']}` | {r['filename_utc']} | "
                f"{r['gps_median_anchor']} | {human_delta(r['gps_vs_filename_s'])} | {r['n_valid']} |")
    add("")

    add("### Drives that would end up on top of each other")
    add("")
    if not rv["duplicate_overlaps"]:
        add("None, which is what we hoped for. No two drives would overlap in "
            "time while also listing exactly the same video clips, so the "
            "corrected times do not create any genuine duplicates.")
    else:
        add(f"{len(rv['duplicate_overlaps'])} pair(s) overlap in time and list "
            "exactly the same video clips, which means they are the same drive "
            "stored twice.")
        add("")
        add("| Vehicle | Drive A | Drive B | Clips |")
        add("| --- | --- | --- | ---: |")
        for r in rv["duplicate_overlaps"]:
            add(f"| {r['vehicle_tag']} | `{r['tag_a']}` ({r['a_start']}) | "
                f"`{r['tag_b']}` ({r['b_start']}) | {r['clip_count']} |")
    add("")

    add("### Drives with no GPS data at all")
    add("")
    if not rv["no_gnss_rows"]:
        add("None. Every drive has at least some GPS data.")
    else:
        add(f"{len(rv['no_gnss_rows'])} drive(s) have no GPS rows, so their start "
            "time can only come from the drive's own name.")
        add("")
        add("| Vehicle | Drive | Start time to use | Clips | Accelerometer rows |")
        add("| --- | --- | --- | ---: | ---: |")
        for r in rv["no_gnss_rows"]:
            add(f"| {r['vehicle_tag']} | `{r['tag']}` | {r['proposed_start_ts_utc']} | "
                f"{r['counts']['clip']} | {r['counts']['accel']} |")
    add("")

    add("### Two drives claiming the same corrected identifier")
    add("")
    if not rv["clean_id_collisions"]:
        add("None. Every drive maps to its own identifier, so correcting the "
            "identifiers cannot make two drives collide.")
    else:
        add(f"{len(rv['clean_id_collisions'])} identifier(s) are claimed by more "
            "than one session. No fix is proposed for these -- they need a "
            "decision about which copy is the real one.")
        for c in rv["clean_id_collisions"]:
            add("")
            add(f"**`{c['base_tag']}`** ({c['vehicle_tag']}) -> `{c['clean_id']}`")
            add("")
            add("| Current identifier | Name | Clips | GPS rows | Accel rows | Preview images | Events |")
            add("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
            for m in c["members"]:
                k = m["counts"]
                add(f"| `{m['current_id']}` | `{m['tag']}` | {k['clip']} | {k['gnss']} | "
                    f"{k['accel']} | {k['storyboard']} | {k['derived_event']} |")
    add("")

    if rv["unparsable_notes"]:
        add("### Sessions whose name could not be read")
        add("")
        for r in rv["unparsable_notes"]:
            add(f"- `{r['current_id']}` -- notes: `{r['notes']}`")
        add("")

    add("## Folders waiting on the server")
    add("")
    if not tree["exists"]:
        add(f"The import folder `{tree['root']}` was not found, so nothing could be checked.")
    else:
        t = tree["totals"]
        add(f"There are **{t['incoming']}** drive folders sitting under "
            f"`incoming/`. Of those, **{t['in_database']}** describe a drive "
            "that is already loaded into the database, and "
            f"**{t['not_in_database']}** do not.")
        add("")
        add(f"**{t['redundant_copies']}** of them are redundant copies: the drive "
            "is already in the database *and* a copy of the same folder is "
            "already filed under `done/` or `failed/`. Nothing has been moved or "
            "deleted -- this is a list, not an action.")
        add("")
        add("| Vehicle | In incoming | Already in database | Copy under done | Copy under failed | Redundant |")
        add("| --- | ---: | ---: | ---: | ---: | ---: |")
        for v, d in sorted(tree["vehicles"].items()):
            e = d["entries"]
            add(f"| {v} | {d['incoming_count']} | {sum(1 for x in e if x['in_database'])} | "
                f"{sum(1 for x in e if x['also_under_done'])} | "
                f"{sum(1 for x in e if x['also_under_failed'])} | "
                f"{sum(1 for x in e if x['redundant_copy'])} |")
        add("")
        for v, d in sorted(tree["vehicles"].items()):
            not_loaded = [x for x in d["entries"] if not x["in_database"]]
            if not_loaded:
                add(f"Folders under `incoming/{v}/` that are **not** in the "
                    f"database ({len(not_loaded)}):")
                add("")
                for x in not_loaded:
                    where = []
                    if x["also_under_done"]:
                        where.append("copy under done/")
                    if x["also_under_failed"]:
                        where.append("copy under failed/")
                    suffix = f" ({', '.join(where)})" if where else ""
                    add(f"- `{x['folder']}`{suffix}")
                add("")
            also = [x for x in d["entries"] if x["redundant_copy"]]
            if also:
                add(f"Redundant folders under `incoming/{v}/` ({len(also)}) -- "
                    "already loaded and already filed elsewhere:")
                add("")
                for x in also:
                    where = "done/" if x["also_under_done"] else "failed/"
                    add(f"- `{x['folder']}` (also under {where})")
                add("")

    add("## What happens next")
    add("")
    add("Nothing yet. This tool only reports. Applying the corrections would "
        "mean rewriting each session's identifier and start time, and shifting "
        "every GPS point, accelerometer reading, preview image, clip time and "
        "event marker that hangs off it by the same amount. That step has not "
        "been written, on purpose, so that these numbers can be checked first.")
    add("")
    return "\n".join(L)


# --------------------------------------------------------------------------
# Applying the repair
# --------------------------------------------------------------------------

def needs_work(s: dict[str, Any]) -> bool:
    """Is there anything left to do for this session?"""
    if s["clean_id"] and s["clean_id"] != s["current_id"]:
        return True
    if s["proposed_start_ts_utc"] is None:
        return False
    if s["current_start_ts_utc"] is None:
        return True
    if abs((s["proposed_start_ts_utc"] - s["current_start_ts_utc"]).total_seconds()) > 1.0:
        return True
    if s.get("stored_drive_tag") != s.get("base_tag"):
        return True
    if not s.get("stored_time_confidence"):
        return True
    return False


def clip_times(clip_name: str, clock_offset_s: float) -> tuple[Optional[datetime], Optional[datetime]]:
    """A clip's start and end, from its own filename plus the drive's clock error.

    Reading these out of the per-clip telemetry is what produced the nonsense
    already in the database: the stale pre-lock rows gave every parking clip in a
    drive the same one-second span. NOMINAL_CLIP_SECONDS stands in for the length,
    because an already-loaded drive has no measured durations to hand.
    """
    stamp = filename_utc_of(clip_name)
    if stamp is None:
        return None, None
    start = stamp + timedelta(seconds=clock_offset_s)
    return start, start + timedelta(seconds=NOMINAL_CLIP_SECONDS)


def clock_offset_for(s: dict[str, Any]) -> float:
    """How far the camera clock sat from real time for this drive, in seconds."""
    if s["method"] == "filename_low":
        return 0.0
    if s["method"] == "filename_estimated_clock_offset":
        return float(s.get("clock_fix_offset_s") or 0.0)
    return float(s.get("gps_vs_filename_s") or 0.0)


def rename_media_dir(
    media_root: Optional[Path], vehicle_tag: Optional[str], old_id: str, new_id: str
) -> str:
    """Move a session's thumbnail folder to its corrected id."""
    if media_root is None:
        return "no media root given"
    if old_id == new_id:
        return "unchanged"
    old_dir = media_root / (vehicle_tag or "unknown") / old_id
    new_dir = media_root / (vehicle_tag or "unknown") / new_id
    if not old_dir.is_dir():
        return "no folder to rename"
    if new_dir.exists():
        return "target already exists, left alone"
    old_dir.rename(new_dir)
    return "renamed"


def apply_one(
    conn: psycopg.Connection,
    s: dict[str, Any],
    *,
    media_root: Optional[Path],
    params_by_session: dict[str, dict[str, Any]],
    clip_names: dict[str, set[str]],
) -> dict[str, Any]:
    """Repair one session inside a single transaction.

    The thumbnail folder is renamed after the transaction commits: if that rename
    fails the database is still consistent, and the folder can be renamed later.
    """
    old_id = s["current_id"]
    new_id = s["clean_id"] or old_id
    start = s["proposed_start_ts_utc"]
    end = s["proposed_end_ts_utc"]
    if start is None:
        return {"tag": s["tag"], "status": "skipped", "reason": "no start time could be worked out"}

    current = s["current_start_ts_utc"]
    delta_s = (start - current).total_seconds() if current is not None else 0.0
    offset_s = clock_offset_for(s)
    params = params_by_session.get(old_id) or DEFAULT_DERIVED_PARAMS
    notes = f"import {s['base_tag']}_{s['view'] or 'front'}"

    time_sync = {
        "repair": {
            "applied_utc": datetime.now(timezone.utc).isoformat(),
            "tool": "ingest/importer/repair.py",
            "old_drive_session_id": old_id if old_id != new_id else None,
            "old_start_ts_utc": iso(current),
            "new_start_ts_utc": iso(start),
            "shift_s": round(delta_s, 3),
            "method": s["method"],
            "gps_votes": s["n_valid"],
            "gps_vs_filename_s": s["gps_vs_filename_s"],
            "camera_clock_offset_s": round(offset_s, 3),
        }
    }

    counts: dict[str, int] = {}
    with conn.transaction():
        with conn.cursor() as cur:
            # 1. Re-time the telemetry, still under the old id. Both tables carry
            #    t_rel_s (seconds from the start of the drive), so the corrected
            #    start is all that is needed.
            cur.execute(
                "UPDATE dashcam.gnss_sample SET ts_utc = %s + (t_rel_s * interval '1 second') "
                "WHERE drive_session_id = %s::uuid AND t_rel_s IS NOT NULL;",
                (start, old_id),
            )
            counts["gnss"] = cur.rowcount
            cur.execute(
                "UPDATE dashcam.accel_sample SET ts_utc = %s + (t_rel_s * interval '1 second') "
                "WHERE drive_session_id = %s::uuid AND t_rel_s IS NOT NULL;",
                (start, old_id),
            )
            counts["accel"] = cur.rowcount

            # 2. Storyboard frames carry ts_utc in their primary key, so park the
            #    timestamps clear of it first, then write the real values.
            cur.execute(
                f"UPDATE dashcam.storyboard_frame SET ts_utc = ts_utc + interval '{TEMP_SHIFT_INTERVAL}' "
                "WHERE drive_session_id = %s::uuid;",
                (old_id,),
            )
            cur.execute(
                f"""
                UPDATE dashcam.storyboard_frame
                   SET ts_utc = CASE WHEN offset_s IS NOT NULL
                                     THEN %s + (offset_s * interval '1 second')
                                     ELSE ts_utc - interval '{TEMP_SHIFT_INTERVAL}'
                                          + (%s * interval '1 second') END,
                       local_time = CASE WHEN offset_s IS NOT NULL
                                     THEN %s + (offset_s * interval '1 second')
                                     ELSE local_time + (%s * interval '1 second') END
                 WHERE drive_session_id = %s::uuid;
                """,
                (start, delta_s, start, delta_s, old_id),
            )
            counts["storyboard"] = cur.rowcount

            # 3. Hand-added event markers, same primary-key problem.
            cur.execute(
                f"UPDATE dashcam.event_marker SET ts_utc = ts_utc + interval '{TEMP_SHIFT_INTERVAL}' "
                "WHERE drive_session_id = %s::uuid;",
                (old_id,),
            )
            cur.execute(
                f"UPDATE dashcam.event_marker SET ts_utc = ts_utc - interval '{TEMP_SHIFT_INTERVAL}' "
                "+ (%s * interval '1 second') WHERE drive_session_id = %s::uuid;",
                (delta_s, old_id),
            )
            counts["event_marker"] = cur.rowcount

            # 4. Clip times, from each clip's own filename.
            rows = []
            for name in sorted(clip_names.get(old_id, ())):
                c_start, c_end = clip_times(name, offset_s)
                if c_start is not None:
                    rows.append((c_start, c_end, old_id, name))
            if rows:
                cur.executemany(
                    "UPDATE dashcam.clip SET start_ts_utc = %s, end_ts_utc = %s "
                    "WHERE drive_session_id = %s::uuid AND clip_name = %s;",
                    rows,
                )
            counts["clip"] = len(rows)

            # 5. The session row itself. Changing the id cascades into every child
            #    table, which is what migration 27 made possible.
            cur.execute(
                """
                UPDATE dashcam.drive_session
                   SET drive_session_id = %s::uuid,
                       start_ts_utc = %s,
                       end_ts_utc = %s,
                       drive_tag = %s,
                       time_confidence = %s,
                       notes = %s,
                       time_sync = coalesce(time_sync, '{}'::jsonb) || %s::jsonb
                 WHERE drive_session_id = %s::uuid;
                """,
                (new_id, start, end, s["base_tag"], s["method"], notes,
                 json.dumps(time_sync), old_id),
            )

            # 6. Thumbnail paths follow the id.
            if new_id != old_id:
                cur.execute(
                    "UPDATE dashcam.storyboard_frame "
                    "SET media_rel_path = replace(media_rel_path, %s, %s) "
                    "WHERE drive_session_id = %s::uuid AND position(%s in media_rel_path) > 0;",
                    (old_id, new_id, new_id, old_id),
                )

            # 7. Events and the summary are derived from the telemetry, so they are
            #    recomputed rather than shifted.
            cur.execute(
                "SELECT dashcam.refresh_derived(%s::uuid, %s::jsonb);",
                (new_id, json.dumps(params)),
            )

    # The database work is committed by this point. A thumbnail folder that cannot
    # be renamed (they are written by the loader container, so they can be owned
    # by root) leaves the session correctly repaired but its images unreachable
    # until --media-only is run with enough privilege. That is not a failed
    # repair, and reporting it as one would be wrong.
    try:
        media = rename_media_dir(media_root, s["vehicle_tag"], old_id, new_id)
    except OSError as exc:
        media = f"failed: {type(exc).__name__}: {exc}"

    return {
        "tag": s["tag"],
        "base_tag": s["base_tag"],
        "vehicle_tag": s["vehicle_tag"],
        "old_id": old_id,
        "new_id": new_id,
        "status": "repaired",
        "old_start_ts_utc": iso(current),
        "new_start_ts_utc": iso(start),
        "shift_s": round(delta_s, 3),
        "method": s["method"],
        "rows": counts,
        "media": media,
    }


def apply_repair(
    conn: psycopg.Connection,
    sessions: list[dict[str, Any]],
    *,
    media_root: Optional[Path],
    only_tags: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """Repair every session that still needs it, one transaction each."""
    clip_names = fetch_clip_names(conn)
    params_by_session = fetch_summary_params(conn)

    todo = [s for s in sessions if needs_work(s)]
    if only_tags:
        wanted = set(only_tags)
        todo = [s for s in todo if s["tag"] in wanted or s["base_tag"] in wanted]
    if limit:
        todo = todo[:limit]

    results: list[dict[str, Any]] = []
    for i, s in enumerate(todo, start=1):
        label = f"{s['vehicle_tag']}/{s['base_tag']}"
        try:
            r = apply_one(
                conn, s,
                media_root=media_root,
                params_by_session=params_by_session,
                clip_names=clip_names,
            )
        except Exception as exc:  # one bad session must not stop the rest
            r = {
                "tag": s["tag"], "base_tag": s["base_tag"], "vehicle_tag": s["vehicle_tag"],
                "old_id": s["current_id"], "new_id": s["clean_id"],
                "status": "failed", "error": f"{type(exc).__name__}: {exc}",
            }
        results.append(r)
        shift = f" {r['shift_s']:+.0f}s" if r.get("shift_s") is not None else ""
        err = f" -- {r['error']}" if r["status"] == "failed" else ""
        print(f"[{i}/{len(todo)}] {label}: {r['status']}{shift}{err}", flush=True)

    return {
        "attempted": len(todo),
        "repaired": sum(1 for r in results if r["status"] == "repaired"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "media_pending": sum(
            1 for r in results if str(r.get("media", "")).startswith("failed")
        ),
        "results": results,
    }


def fetch_recorded_renames(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Sessions whose repair record remembers an old id, so a folder may lag."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT drive_session_id::text, vehicle_tag, drive_tag,
                   time_sync->'repair'->>'old_drive_session_id'
            FROM dashcam.drive_session
            WHERE time_sync->'repair'->>'old_drive_session_id' IS NOT NULL
            ORDER BY drive_tag;
            """
        )
        return [
            {"new_id": new_id, "vehicle_tag": vehicle, "drive_tag": tag, "old_id": old_id}
            for new_id, vehicle, tag, old_id in cur.fetchall()
        ]


def reconcile_media(conn: psycopg.Connection, media_root: Path) -> dict[str, Any]:
    """Rename any thumbnail folder still sitting under a session's old id.

    Reads nothing but the repair record each session already carries, so it can
    be re-run, and can be run from somewhere with more privilege than the process
    that did the repair (the folders belong to the loader container).
    """
    moves = fetch_recorded_renames(conn)
    renamed = already = missing = 0
    failed: list[dict[str, str]] = []

    for m in moves:
        base = media_root / (m["vehicle_tag"] or "unknown")
        old_dir, new_dir = base / m["old_id"], base / m["new_id"]
        if new_dir.is_dir():
            already += 1
            continue
        if not old_dir.is_dir():
            missing += 1
            continue
        try:
            old_dir.rename(new_dir)
            renamed += 1
            print(f"renamed {m['drive_tag']}: {m['old_id']} -> {m['new_id']}", flush=True)
        except OSError as exc:
            failed.append({"drive_tag": m["drive_tag"], "error": f"{type(exc).__name__}: {exc}"})

    return {
        "sessions_with_a_recorded_rename": len(moves),
        "renamed": renamed,
        "already_correct": already,
        "no_folder_to_rename": missing,
        "failed": failed,
    }


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Report on wrong drive-session ids and start times (read-only).",
    )
    p.add_argument(
        "--report-dir",
        required=True,
        help="Directory the report files are written to.",
    )
    p.add_argument(
        "--import-root",
        default=DEFAULT_IMPORT_ROOT,
        help=f"Root of the server import tree (default: {DEFAULT_IMPORT_ROOT}).",
    )
    p.add_argument(
        "--media-root",
        default=DEFAULT_MEDIA_ROOT,
        help=f"Folder holding <vehicle>/<session id>/thumbs (default: {DEFAULT_MEDIA_ROOT}). "
             "Only used with --apply, to rename a session's thumbnails to its corrected id.",
    )
    p.add_argument(
        "--clock-fixes",
        default=None,
        help=f"JSON file of manual camera-clock corrections (default: {CLOCK_FIXES_FILENAME} "
             "beside this script, when it exists).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Write the repair to the database. Requires --confirm.",
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        help="Required alongside --apply.",
    )
    p.add_argument(
        "--only-tag",
        action="append",
        dest="only_tags",
        default=None,
        help="Repair only this drive tag (e.g. 20260406_081807_civic). Repeatable.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Repair at most this many sessions, for a cautious first run.",
    )
    p.add_argument(
        "--media-only",
        action="store_true",
        help="Touch no database rows: only rename thumbnail folders that are still "
             "under a session's old id, using the repair record on each session. "
             "Run this from wherever has permission to rename them.",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    if args.apply and not args.confirm:
        raise SystemExit(
            "--apply also needs --confirm: it rewrites session ids and every "
            "timestamp derived from them. Take a database backup first."
        )

    if args.media_only:
        conn = connect(read_only=True)
        try:
            outcome = reconcile_media(conn, Path(args.media_root))
        finally:
            conn.close()
        print(
            f"media: renamed={outcome['renamed']} already_correct={outcome['already_correct']} "
            f"no_folder={outcome['no_folder_to_rename']} failed={len(outcome['failed'])} "
            f"of {outcome['sessions_with_a_recorded_rename']} recorded rename(s)"
        )
        for f in outcome["failed"]:
            print(f"  {f['drive_tag']}: {f['error']}")
        return 1 if outcome["failed"] else 0

    clock_path = Path(args.clock_fixes) if args.clock_fixes else Path(__file__).with_name(CLOCK_FIXES_FILENAME)
    clock_fixes = load_clock_fixes(clock_path if clock_path.is_file() else None)
    if clock_fixes["tags"]:
        print(f"Clock corrections from {clock_fixes['source']}: "
              f"{len(clock_fixes['tags'])} drive(s), {clock_fixes['offset_s']:+.1f}s")

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    conn = connect(read_only=not args.apply)
    try:
        if args.apply and not identity_columns_present(conn):
            raise SystemExit(
                "drive_session has no drive_tag / time_confidence columns. "
                "Apply db/migrations/27_drive_identity.sql first."
            )

        # The report always describes the state before anything is written.
        sessions = collect_sessions(conn, clock_fixes)
        report = build_report(conn, Path(args.import_root), clock_fixes, sessions=sessions)

        outcome = None
        if args.apply:
            outcome = apply_repair(
                conn,
                sessions,
                media_root=Path(args.media_root) if args.media_root else None,
                only_tags=args.only_tags,
                limit=args.limit,
            )
            report["apply"] = outcome
    finally:
        conn.close()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = "repair_applied" if args.apply else "repair"
    json_path = report_dir / f"{prefix}_{stamp}.json"
    md_path = report_dir / f"{prefix}_{stamp}.md"

    markdown = render_markdown(report)
    if outcome is not None:
        markdown += (
            "\n## What was applied\n\n"
            f"- Sessions needing work: **{outcome['attempted']}**\n"
            f"- Repaired: **{outcome['repaired']}**\n"
            f"- Skipped: **{outcome['skipped']}**\n"
            f"- Failed: **{outcome['failed']}**\n\n"
            "The per-session detail, including the old identifier and the exact "
            "shift applied to each drive, is in the JSON file beside this one. "
            "Every drive also carries its own record under `time_sync.repair` in "
            "the database.\n"
        )
        failed = [r for r in outcome["results"] if r["status"] == "failed"]
        if failed:
            markdown += "\n### Sessions that failed\n\n"
            for r in failed:
                markdown += f"- `{r.get('base_tag')}` -- {r.get('error')}\n"

    json_path.write_text(json.dumps(report, indent=2, sort_keys=False), encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")

    s = report["summary"]
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(
        f"sessions={s['total_sessions']} new_id={s['sessions_getting_a_new_id']} "
        f"retimed>2min={s['sessions_retimed_over_2_min']} "
        f"methods={s['by_method']} "
        f"needs_review={len(report['review']['clock_mismatch'])} "
        f"no_gnss={len(report['review']['no_gnss_rows'])} "
        f"collisions={len(report['review']['clean_id_collisions'])} "
        f"dup_overlaps={len(report['review']['duplicate_overlaps'])}"
    )
    if outcome is not None:
        print(
            f"applied: attempted={outcome['attempted']} repaired={outcome['repaired']} "
            f"skipped={outcome['skipped']} failed={outcome['failed']}"
        )
        return 1 if outcome["failed"] else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
