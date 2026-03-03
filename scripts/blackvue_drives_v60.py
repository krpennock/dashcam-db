#!/usr/bin/env python3
"""
blackvue_drives.py

Cross-platform (Windows/macOS/Linux) drive stitcher for BlackVue-style dashcam clips.

What it does:
- Scans a source folder for MP4s whose filename starts with YYYYMMDD_HHMMSS_...
  and whose stem ends with "F" (front) or "R" (rear),
  e.g. 20251229_211524_NF.mp4 / 20251229_211524_NR.mp4
- Groups timestamps into "drives" based on a gap threshold (default 120s)
- Writes ffmpeg concat lists per drive
- Optionally stitches:
    * front -> drive_XXX_front_edit.mp4 (video stream copied; audio normalized to AAC 48kHz stereo)
    * rear  -> drive_XXX_rear_noaudio.mp4 (video stream copied; no audio)
- Optionally runs blackclue per clip (complete/raw/both), then stitches per-clip .nmea files into a drive-level .nmea
- Optionally converts stitched .nmea into CSV and/or GPX per drive

Notes:
- blackclue typically writes sidecars next to each MP4 (.nmea/.3gf/.jpg/..). stdout may be empty.
- If your output folder is inside your source folder and you use --recurse,
  exclude it (default excludes "Processed" and the output dir name).

Requirements:
- ffmpeg installed (or provide --ffmpeg path)
- blackclue installed (or provide --blackclue path)

Typical usage:
  python blackvue_drives.py --source Z:\\Dashcam --output F:\\Dashcam\\Processed --recurse --run-ffmpeg --for-resolve --run-blackclue --blackclue-mode complete --emit-csv --emit-gpx
"""
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as _dt
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

def _try_zoneinfo(tz_name: str) -> Optional[ZoneInfo]:
    """Best-effort ZoneInfo lookup.

    On Windows, Python may not ship the IANA timezone database; installing the 'tzdata' pip package fixes this.
    Returns None when the timezone cannot be resolved.
    """
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return None
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


TS_RE = re.compile(r"^(?P<date>\d{8})_(?P<time>\d{6})_")
_NMEA_LINE_RE = re.compile(r"^\[(?P<ms>\d+)\](?P<rest>.*)$")

# --- Database-import manifest defaults ---
# These thresholds are intended for downstream event/feature extraction.
DEFAULT_DERIVED_PARAMS: Dict[str, object] = {
    "hard_brake_mps2": 2.6,
    "hard_accel_mps2": 2.2,
    "harsh_turn_mps2": 3.0,
    "min_speed_brake_mph": 12,
    "min_speed_turn_mph": 20,
    "stop_min_s": 15,
    "bump_cooldown_s": 3,
}


@dataclass(frozen=True)
class ClipPair:
    ts_key: str              # "YYYYMMDD_HHMMSS"
    ts_dt: _dt.datetime
    front: Optional[Path]    # stem endswith 'F'
    rear: Optional[Path]     # stem endswith 'R'


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def which_or_fail(exe: str) -> str:
    """Resolve an executable either from an explicit path or PATH."""
    p = Path(exe)
    if p.parent and p.exists():
        return str(p)
    found = shutil.which(exe)
    if not found:
        raise FileNotFoundError(f"Executable not found: {exe}")
    return found


def parse_timestamp_from_name(name: str) -> Optional[Tuple[str, _dt.datetime]]:
    """Return (ts_key, ts_dt) from filenames starting with YYYYMMDD_HHMMSS_..."""
    m = TS_RE.match(name)
    if not m:
        return None
    ts_key = f"{m.group('date')}_{m.group('time')}"
    try:
        ts_dt = _dt.datetime.strptime(ts_key, "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return ts_key, ts_dt



def sanitize_tag(text: str) -> str:
    """Convert an arbitrary label into a filesystem-friendly tag."""
    t = (text or "").strip()
    if not t:
        return "vehicle"
    out_chars = []
    for ch in t:
        if ch.isalnum() or ch in ("-", "_"):
            out_chars.append(ch)
        elif ch.isspace():
            out_chars.append("-")
        # drop everything else (slashes, colons, etc.)
    out = "".join(out_chars)
    out = re.sub(r"-{2,}", "-", out)
    out = out.strip("-_")
    return out.lower() or "vehicle"

def render_manifest_filename(template: str, *, tag: str, view: str) -> str:
    """Render --manifest-filename. Supports {tag} and {view}."""
    t = (template or "manifest.json").strip()
    if not t:
        t = "manifest.json"
    try:
        return t.format(tag=tag, view=view)
    except Exception:
        # If user passed a filename with braces that aren't placeholders, fall back to literal.
        return t


def make_unique_drive_tag(base: str, *, final_dir: Path, work_root: Path, tele_root: Path) -> str:
    """Return a unique tag derived from *base*.

    We check for collisions across:
      - the final composed output in final_dir (tag.mp4)
      - any legacy final output name (tag_preview.mp4)
      - the working/staging folder (work_root/tag)
      - telemetry folder (tele_root/tag)
    """
    tag = base
    n = 1
    while True:
        candidates = [
            final_dir / f"{tag}.mp4",
            final_dir / f"{tag}_preview.mp4",
            work_root / tag,
            tele_root / tag,
        ]
        if not any(p.exists() for p in candidates):
            return tag
        n += 1
        tag = f"{base}_{n:02d}"

def is_front_or_rear(mp4_path: Path) -> Optional[str]:
    """Determine view based on last character of the stem: ...F -> 'F', ...R -> 'R'."""
    stem = mp4_path.stem
    if not stem:
        return None
    v = stem[-1].upper()
    return v if v in ("F", "R") else None


def _is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.resolve().relative_to(base.resolve())
        return True
    except Exception:
        return False


def iter_mp4s(
    source: Path,
    recurse: bool,
    exclude_dirs: Sequence[str],
    exclude_paths: Sequence[Path] | None = None,
) -> Iterable[Path]:
    """Yield MP4 files under source.

    - exclude_dirs: directory *names* to skip (case-insensitive)
    - exclude_paths: full directory paths to skip (anything under them is ignored)
    """
    exclude = {d.lower() for d in exclude_dirs if d}
    ex_paths = [(p.resolve() if isinstance(p, Path) else Path(p).resolve()) for p in (exclude_paths or [])]

    def _skip(p: Path) -> bool:
        # Full-path exclusions first (more precise than name-based exclusions).
        for ex in ex_paths:
            if _is_relative_to(p, ex):
                return True
        # Then name-based exclusions.
        return any(part.lower() in exclude for part in p.parts)

    if recurse:
        for p in source.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() != ".mp4":
                continue
            if _skip(p):
                continue
            yield p
    else:
        for p in source.iterdir():
            if not p.is_file():
                continue
            if p.suffix.lower() != ".mp4":
                continue
            if _skip(p):
                continue
            yield p


def scan_pairs(source: Path, recurse: bool, exclude_dirs: Sequence[str], exclude_paths: Sequence[Path] | None = None) -> List[ClipPair]:
    by_ts: Dict[str, Dict[str, Path]] = {}
    by_dt: Dict[str, _dt.datetime] = {}

    for mp4 in iter_mp4s(source, recurse, exclude_dirs, exclude_paths=exclude_paths):
        if not mp4.is_file():
            continue
        ts = parse_timestamp_from_name(mp4.name)
        if not ts:
            continue
        ts_key, ts_dt = ts

        view = is_front_or_rear(mp4)
        if not view:
            continue

        by_ts.setdefault(ts_key, {})[view] = mp4
        by_dt[ts_key] = ts_dt

    pairs: List[ClipPair] = []
    for ts_key, d in by_ts.items():
        pairs.append(
            ClipPair(
                ts_key=ts_key,
                ts_dt=by_dt[ts_key],
                front=d.get("F"),
                rear=d.get("R"),
            )
        )

    pairs.sort(key=lambda p: p.ts_dt)
    return pairs


# --- Manifest (per drive session) ---

def _stable_drive_session_id(*, vehicle_tag: str, drive_tag: str) -> str:
    """Return a stable UUID for a drive session (re-running should not create a new ID)."""
    key = f"blackvue-drive:{vehicle_tag}:{drive_tag}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _dt_to_iso_utc(x: Optional[_dt.datetime]) -> Optional[str]:
    if x is None:
        return None
    if x.tzinfo is None:
        # treat naive as UTC to avoid silently shifting
        x = x.replace(tzinfo=_dt.timezone.utc)
    return x.astimezone(_dt.timezone.utc).isoformat()


def _clip_bounds_from_sidecar_nmea(nmea_path: Path) -> Tuple[Optional[_dt.datetime], Optional[_dt.datetime]]:
    """Best-effort (start,end) UTC timestamps for a clip from its blackclue .nmea sidecar.

    Prefers parsed NMEA RMC-derived utc_time; falls back to the bracketed epoch ms when needed.
    """
    try:
        txt = nmea_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return (None, None)

    try:
        recs = nmea_to_records(txt)
    except Exception:
        recs = []

    if not recs:
        return (None, None)

    def _parse_iso_any(s: str) -> Optional[_dt.datetime]:
        s = (s or "").strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                return _dt.datetime.fromisoformat(s[:-1] + "+00:00")
            return _dt.datetime.fromisoformat(s)
        except Exception:
            return None

    dts: List[_dt.datetime] = []
    for r in recs:
        utc_s = (r.get("utc_time") or "").strip()
        dt = _parse_iso_any(utc_s) if utc_s else None

        if dt is None:
            try:
                ms_i = int(r.get("ms"))
                dt = _dt.datetime.fromtimestamp(ms_i / 1000.0, tz=_dt.timezone.utc)
            except Exception:
                dt = None

        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            dts.append(dt.astimezone(_dt.timezone.utc))

    if not dts:
        return (None, None)

    return (min(dts), max(dts))


def _parse_clip_bounds_from_sidecar(nmea_path: Path) -> Tuple[Optional[str], Optional[str]]:
    """Return (start_iso, end_iso) from a blackclue .nmea sidecar.

    The underlying parser returns timezone-aware datetimes. We emit ISO 8601 strings
    with an explicit UTC offset ("+00:00") so the manifest is portable.
    """

    start_dt, end_dt = _clip_bounds_from_sidecar_nmea(nmea_path)
    if start_dt is None:
        return (None, None)
    if end_dt is None:
        end_dt = start_dt
    return (start_dt.isoformat(), end_dt.isoformat())



def _utc_now_iso_z() -> str:
    """Return current UTC timestamp like 2026-02-13T01:23:45Z."""
    return (
        _dt.datetime.now(_dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _sha256_hex(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _csv_info(path: Path, *, want_first_last_col: Optional[str] = None) -> Tuple[int, List[str], Optional[str], Optional[str]]:
    """Return (rows, columns, first_val, last_val) for a CSV.

    If want_first_last_col is provided and exists in the header, first_val/last_val are
    taken from that column across all data rows.
    """
    rows = 0
    cols: List[str] = []
    first_val: Optional[str] = None
    last_val: Optional[str] = None

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return 0, [], None, None
        cols = [c.strip() for c in header]
        col_idx = None
        if want_first_last_col and want_first_last_col in cols:
            col_idx = cols.index(want_first_last_col)

        for row in reader:
            if not row:
                continue
            rows += 1
            if col_idx is not None and col_idx < len(row):
                v = row[col_idx].strip()
                if v:
                    if first_val is None:
                        first_val = v
                    last_val = v

    return rows, cols, first_val, last_val



def _largest_time_cluster(dts: List[_dt.datetime], *, gap_seconds: float = 2 * 3600.0) -> List[_dt.datetime]:
    """Return the largest cluster of timestamps where adjacent times are within gap_seconds.

    This is used to reject outlier clock/GNSS values (e.g., pre-lock, bad RTC) that can
    otherwise distort min/max bounds.
    """
    if not dts:
        return []
    # Normalize to UTC aware
    norm: List[_dt.datetime] = []
    for dt in dts:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        norm.append(dt.astimezone(_dt.timezone.utc))
    norm.sort()

    clusters: List[List[_dt.datetime]] = []
    cur: List[_dt.datetime] = [norm[0]]
    for dt in norm[1:]:
        if (dt - cur[-1]).total_seconds() <= gap_seconds:
            cur.append(dt)
        else:
            clusters.append(cur)
            cur = [dt]
    clusters.append(cur)

    # Pick cluster with most points; tie-breaker: widest span.
    def _key(c: List[_dt.datetime]) -> Tuple[int, float]:
        span = (c[-1] - c[0]).total_seconds() if len(c) > 1 else 0.0
        return (len(c), span)

    best = max(clusters, key=_key)
    return best


def _parse_iso_utc_any(s: str) -> Optional[_dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dt = _dt.datetime.fromisoformat(s[:-1] + "+00:00")
        else:
            dt = _dt.datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc)


def _analyze_gnss_csv(path: Path, *, cluster_gap_seconds: float = 2 * 3600.0) -> dict:
    """Analyze a GNSS CSV to derive both raw and clustered UTC bounds plus a robust UTC anchor.

    Returns a dict with keys:
      rows, columns
      gnss_raw_min_ts_utc, gnss_raw_max_ts_utc
      gnss_cluster_start_ts_utc, gnss_cluster_end_ts_utc
      gnss_points_total, gnss_points_valid, gnss_cluster_points
      cluster_gap_seconds
      gnss_anchor_ts_utc, gnss_anchor_t_rel_s
      t_rel_s_max
    """
    rows = 0
    cols: List[str] = []

    # All parseable UTC timestamps (raw bounds, includes pre-lock / bogus)
    raw_dts: List[_dt.datetime] = []

    # "Valid fix" points for clustering + anchoring: (dt_utc, t_rel_s)
    valid_pts: List[Tuple[_dt.datetime, Optional[float]]] = []

    # Track max timeline seconds (best-effort) for estimating total video duration.
    t_rel_s_max: Optional[float] = None

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return {
                "rows": 0,
                "columns": [],
                "gnss_raw_min_ts_utc": None,
                "gnss_raw_max_ts_utc": None,
                "gnss_cluster_start_ts_utc": None,
                "gnss_cluster_end_ts_utc": None,
                "gnss_points_total": 0,
                "gnss_points_valid": 0,
                "gnss_cluster_points": 0,
                "cluster_gap_seconds": float(cluster_gap_seconds),
                "gnss_anchor_ts_utc": None,
                "gnss_anchor_t_rel_s": None,
                "t_rel_s_max": None,
            }

        cols = [c.strip() for c in header]

        def _idx(name: str) -> Optional[int]:
            try:
                return cols.index(name)
            except ValueError:
                return None

        i_utc = _idx("utc_time")
        i_rmc = _idx("rmc_status")
        i_fix = _idx("fix_quality")
        i_lat = _idx("lat")
        i_lon = _idx("lon")
        i_sat = _idx("satellites")
        i_trel = _idx("t_rel_s")
        i_ms = _idx("ms")

        for row in reader:
            if not row:
                continue
            rows += 1

            # timeline max
            trel: Optional[float] = None
            if i_trel is not None and i_trel < len(row):
                try:
                    trel = float((row[i_trel] or "").strip() or "0")
                except Exception:
                    trel = None
            if trel is None and i_ms is not None and i_ms < len(row):
                # Some firmwares populate ms (relative) but not t_rel_s; approximate.
                try:
                    trel = float((row[i_ms] or "").strip() or "0") / 1000.0
                except Exception:
                    trel = None
            if trel is not None:
                if t_rel_s_max is None or trel > t_rel_s_max:
                    t_rel_s_max = trel

            if i_utc is None or i_utc >= len(row):
                continue
            utc_s = (row[i_utc] or "").strip()
            dt = _parse_iso_utc_any(utc_s)
            if dt is None:
                continue

            dt = dt.astimezone(_dt.timezone.utc)
            raw_dts.append(dt)

            # Validity heuristic: require either a valid RMC status "A" or nonzero fix_quality/sats,
            # and (when lat/lon are present) non-zero coordinates.
            rmc = row[i_rmc].strip().upper() if i_rmc is not None and i_rmc < len(row) else ""
            fixq = 0
            if i_fix is not None and i_fix < len(row):
                try:
                    fixq = int(float((row[i_fix] or "").strip() or "0"))
                except Exception:
                    fixq = 0

            sats = 0
            if i_sat is not None and i_sat < len(row):
                try:
                    sats = int(float((row[i_sat] or "").strip() or "0"))
                except Exception:
                    sats = 0

            lat_ok = True
            lon_ok = True
            if i_lat is not None and i_lat < len(row):
                try:
                    lat = float((row[i_lat] or "").strip() or "0")
                    lat_ok = (-90.0 <= lat <= 90.0) and abs(lat) > 1e-6
                except Exception:
                    lat_ok = True
            if i_lon is not None and i_lon < len(row):
                try:
                    lon = float((row[i_lon] or "").strip() or "0")
                    lon_ok = (-180.0 <= lon <= 180.0) and abs(lon) > 1e-6
                except Exception:
                    lon_ok = True

            valid = False
            if rmc == "A" or fixq > 0 or sats > 0:
                valid = True
            if not (lat_ok and lon_ok):
                valid = False

            if valid:
                valid_pts.append((dt, trel))

    def _iso_z(dt: Optional[_dt.datetime]) -> Optional[str]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    raw_min = min(raw_dts) if raw_dts else None
    raw_max = max(raw_dts) if raw_dts else None

    # Cluster on "valid" points only (pre-lock/bogus often fails validity anyway).
    valid_pts.sort(key=lambda t: t[0])
    clusters: List[List[Tuple[_dt.datetime, Optional[float]]]] = []
    if valid_pts:
        cur: List[Tuple[_dt.datetime, Optional[float]]] = [valid_pts[0]]
        for dt, trel in valid_pts[1:]:
            prev_dt = cur[-1][0]
            if (dt - prev_dt).total_seconds() > float(cluster_gap_seconds):
                clusters.append(cur)
                cur = [(dt, trel)]
            else:
                cur.append((dt, trel))
        clusters.append(cur)

    best_cluster: List[Tuple[_dt.datetime, Optional[float]]] = []
    if clusters:
        # pick largest; tie -> choose the one with the latest end (more likely the real drive)
        clusters.sort(key=lambda c: (len(c), c[-1][0]))
        best_cluster = clusters[-1]

    cluster_min = min((dt for dt, _ in best_cluster), default=None)
    cluster_max = max((dt for dt, _ in best_cluster), default=None)

    # Anchor = earliest UTC in best cluster; use its t_rel_s if available.
    anchor_dt: Optional[_dt.datetime] = None
    anchor_trel: Optional[float] = None
    if best_cluster:
        # earliest by dt
        best_cluster.sort(key=lambda t: t[0])
        anchor_dt = best_cluster[0][0]
        anchor_trel = best_cluster[0][1]
        # if multiple rows share same dt, pick the smallest trel (closer to video start)
        same = [trel for dt, trel in best_cluster if dt == anchor_dt and trel is not None]
        if same:
            anchor_trel = min(same)

    return {
        "rows": rows,
        "columns": cols,
        "gnss_raw_min_ts_utc": _iso_z(raw_min),
        "gnss_raw_max_ts_utc": _iso_z(raw_max),
        "gnss_cluster_start_ts_utc": _iso_z(cluster_min),
        "gnss_cluster_end_ts_utc": _iso_z(cluster_max),
        "gnss_points_total": len(raw_dts),
        "gnss_points_valid": len(valid_pts),
        "gnss_cluster_points": len(best_cluster),
        "cluster_gap_seconds": float(cluster_gap_seconds),
        "gnss_anchor_ts_utc": _iso_z(anchor_dt),
        "gnss_anchor_t_rel_s": float(anchor_trel) if anchor_trel is not None else None,
        "t_rel_s_max": float(t_rel_s_max) if t_rel_s_max is not None else None,
    }


def _max_t_rel_s_from_csv(path: Path, *, col_name: str = "t_rel_s") -> Optional[float]:
    """Return max value of a t_rel_s-like column in a CSV (best-effort)."""
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return None
            cols = [c.strip() for c in header]
            try:
                idx = cols.index(col_name)
            except ValueError:
                return None
            mx: Optional[float] = None
            for row in reader:
                if not row or idx >= len(row):
                    continue
                try:
                    v = float((row[idx] or "").strip() or "0")
                except Exception:
                    continue
                if mx is None or v > mx:
                    mx = v
            return mx
    except Exception:
        return None
def _nmea_info(path: Path) -> Tuple[int, str]:
    """Return (rows, sha256_hex) for an NMEA text file."""
    sha = _sha256_hex(path)
    rows = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.strip():
                    rows += 1
    except Exception:
        rows = 0
    return rows, sha
def _parse_iso_z(s: str) -> _dt.datetime:
    # Accept "...Z" or full offset.
    if s.endswith("Z"):
        return _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return _dt.datetime.fromisoformat(s)


def write_manifest_json(
    manifest_path: Path,
    drive_tag: str,
    vehicle_tag: str,
    notes: str,
    telemetry_gnss_csv: Optional[Path],
    telemetry_accel_csv: Optional[Path],
    clip_entries: List[Dict[str, object]],
    derived_params: Dict[str, object],
    *,
    telemetry_nmea: Optional[Path] = None,
    timezone: str = "America/Chicago",
    source_model: Optional[str] = None,
    source_serial: Optional[str] = None,
    source_firmware: Optional[str] = None,
    camera_front_enabled: bool = True,
    camera_rear_enabled: bool = True,
    include_sha256: bool = True,
    dry_run: bool = False,
    # Manifest v2 extensions
    clip_seconds: Optional[float] = None,
    stills_view: Optional[str] = None,
    stills_every_seconds: Optional[float] = None,
) -> None:
    """Write schema v2 manifest.json.

    All paths in the manifest are relative to manifest_path.parent.
    """

    manifest_dir = manifest_path.parent
    drive_session_id = _stable_drive_session_id(vehicle_tag=vehicle_tag, drive_tag=drive_tag)
    created_utc = _utc_now_iso_z()

    def _rel(p: Optional[Path]) -> Optional[str]:
        if not p:
            return None
        try:
            return str(p.resolve().relative_to(manifest_dir.resolve()))
        except Exception:
            return p.name

    def _norm_utc_str(s: Optional[str]) -> Optional[str]:
        s = (s or "").strip()
        if not s:
            return None
        try:
            if s.endswith("+00:00"):
                return s[:-6] + "Z"
        except Exception:
            pass
        return s

    telemetry: Dict[str, object] = {
        "source_channel": "front",
    }

    gnss_start: Optional[str] = None
    gnss_end: Optional[str] = None
    gnss_info: Optional[dict] = None

    if telemetry_gnss_csv and telemetry_gnss_csv.exists():
        gnss_info = _analyze_gnss_csv(telemetry_gnss_csv)
        rows, cols = int(gnss_info.get('rows') or 0), list(gnss_info.get('columns') or [])
        gnss_start = gnss_info.get('gnss_cluster_start_ts_utc')
        gnss_end = gnss_info.get('gnss_cluster_end_ts_utc')
        telemetry["gnss"] = {
            "format": "blackvue_csv_v1",
            "path": _rel(telemetry_gnss_csv),
            "rows": rows,
            "columns": cols,
        }
        if include_sha256:
            telemetry["gnss"]["sha256"] = _sha256_hex(telemetry_gnss_csv)

    # IMPORTANT: include accel when file exists, regardless of --emit-accel.
    if telemetry_accel_csv and telemetry_accel_csv.exists():
        rows, cols, _, _ = _csv_info(telemetry_accel_csv)
        telemetry["accel"] = {
            "format": "blackvue_accel_csv_v1",
            "path": _rel(telemetry_accel_csv),
            "rows": rows,
            "columns": cols,
            "units": {
                "ax": "raw",
                "ay": "raw",
                "az": "raw",
                "abs_ms": "milliseconds",
                "t_clip_ms": "milliseconds",
            },
        }
        if include_sha256:
            telemetry["accel"]["sha256"] = _sha256_hex(telemetry_accel_csv)

    if telemetry_nmea and telemetry_nmea.exists():
        nmea_rows = 0
        nmea_sha = None
        if include_sha256:
            nmea_rows, nmea_sha = _nmea_info(telemetry_nmea)
        else:
            try:
                with open(telemetry_nmea, "r", encoding="utf-8", errors="replace") as _f:
                    for _line in _f:
                        if _line.strip():
                            nmea_rows += 1
            except Exception:
                nmea_rows = 0
        telemetry["nmea"] = {
            "format": "nmea0183",
            "path": _rel(telemetry_nmea),
            "rows": nmea_rows,
            "columns": [],
        }
        if include_sha256 and nmea_sha:
            telemetry["nmea"]["sha256"] = nmea_sha

    # --- Session time (must be non-null whenever possible) ---
    session_start: Optional[str] = None
    session_end: Optional[str] = None
    time_conf: str = "unknown"

    
    # 1) Prefer GNSS CSV (clustered) and extrapolate to cover the whole stitched video when possible.
    #    We anchor absolute time using the first point in the largest GNSS time cluster + its t_rel_s,
    #    then compute video_start_utc = anchor_utc - t_rel_s_anchor.
    session_time_extra: Dict[str, object] = {}
    if gnss_info and gnss_info.get("gnss_cluster_start_ts_utc") and gnss_info.get("gnss_cluster_end_ts_utc"):
        session_time_extra.update({
            "gnss_raw_min_ts_utc": gnss_info.get("gnss_raw_min_ts_utc"),
            "gnss_raw_max_ts_utc": gnss_info.get("gnss_raw_max_ts_utc"),
            "gnss_cluster_start_ts_utc": gnss_info.get("gnss_cluster_start_ts_utc"),
            "gnss_cluster_end_ts_utc": gnss_info.get("gnss_cluster_end_ts_utc"),
            "gnss_cluster_points": int(gnss_info.get("gnss_cluster_points") or 0),
            "gnss_points_total": int(gnss_info.get("gnss_points_total") or 0),
            "cluster_gap_seconds": float(gnss_info.get("cluster_gap_seconds") or (2 * 3600.0)),
        })

        anchor_iso = gnss_info.get("gnss_anchor_ts_utc")
        anchor_trel = gnss_info.get("gnss_anchor_t_rel_s")
        if anchor_iso:
            session_time_extra["gnss_anchor_ts_utc"] = anchor_iso
        if anchor_trel is not None:
            session_time_extra["gnss_anchor_t_rel_s"] = float(anchor_trel)

        # Compute video_start_utc from the GNSS anchor when we have t_rel_s.
        video_start_dt: Optional[_dt.datetime] = None
        if anchor_iso and anchor_trel is not None:
            try:
                adt = _parse_iso_z_dt(str(anchor_iso))
                if adt is not None:
                    video_start_dt = adt.astimezone(_dt.timezone.utc) - _dt.timedelta(seconds=float(anchor_trel))
            except Exception:
                video_start_dt = None

        # Estimate stitched-video duration using telemetry (preferred) or clip-count * clip_seconds.
        duration_candidates: List[float] = []
        trel_max = gnss_info.get("t_rel_s_max")
        if trel_max is not None:
            try:
                duration_candidates.append(float(trel_max))
            except Exception:
                pass

        if telemetry_accel_csv and telemetry_accel_csv.exists():
            mx = _max_t_rel_s_from_csv(telemetry_accel_csv, col_name="t_rel_s")
            if mx is not None:
                duration_candidates.append(float(mx))

        if clip_seconds is not None:
            # Approximate stitched duration from front clips count.
            try:
                n_front = sum(1 for c in clip_entries if str(c.get("channel") or "").lower() == "front")
                duration_candidates.append(float(n_front) * float(clip_seconds))
            except Exception:
                pass

        if video_start_dt is not None:
            # If we can't estimate duration from telemetry, fall back to GNSS cluster end.
            if not duration_candidates and gnss_info.get("gnss_cluster_end_ts_utc"):
                try:
                    cend = _parse_iso_z_dt(str(gnss_info.get("gnss_cluster_end_ts_utc")))
                    if cend is not None:
                        duration_candidates.append(max(0.0, (cend - video_start_dt).total_seconds()))
                except Exception:
                    pass

            if duration_candidates:
                dur = max(duration_candidates)
                session_start = video_start_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                session_end = (video_start_dt + _dt.timedelta(seconds=float(dur))).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                time_conf = "gnss_cluster_anchor"
            else:
                # If duration is unknowable, fall back to clustered min/max bounds.
                session_start, session_end = gnss_start, gnss_end
                time_conf = "gnss_cluster_min_max"
        else:
            # No usable t_rel_s anchor; fall back to clustered min/max.
            session_start, session_end = gnss_start, gnss_end
            time_conf = "gnss_cluster_min_max"

    # 2) Fallback: stitched NMEA bounds (UTC time or epoch ms)
    if not (session_start and session_end) and telemetry_nmea and telemetry_nmea.exists():
        try:
            txt = telemetry_nmea.read_text(encoding="utf-8", errors="replace")
            recs = nmea_to_records(txt)
            dts: List[_dt.datetime] = []

            def _parse_iso_any(s: str) -> Optional[_dt.datetime]:
                s = (s or "").strip()
                if not s:
                    return None
                try:
                    if s.endswith("Z"):
                        return _dt.datetime.fromisoformat(s[:-1] + "+00:00")
                    return _dt.datetime.fromisoformat(s)
                except Exception:
                    return None

            for r in recs:
                dt = _parse_iso_any(r.get("utc_time") or "")
                if dt is None:
                    try:
                        ms_i = int(r.get("ms"))
                        dt = _dt.datetime.fromtimestamp(ms_i / 1000.0, tz=_dt.timezone.utc)
                    except Exception:
                        dt = None
                if dt is not None:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=_dt.timezone.utc)
                    dts.append(dt.astimezone(_dt.timezone.utc))

            if dts:
                use = _largest_time_cluster(dts, gap_seconds=2 * 3600.0) or dts
                session_start = min(use).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                session_end = max(use).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                time_conf = "nmea_bounds"
        except Exception:
            pass

    # 3) Fallback: per-clip NMEA sidecar bounds
    if not (session_start and session_end):
        starts: List[_dt.datetime] = []
        ends: List[_dt.datetime] = []
        for c in clip_entries:
            s = c.get("start_ts_utc")
            e = c.get("end_ts_utc")
            if isinstance(s, str) and isinstance(e, str) and s and e:
                try:
                    starts.append(_parse_iso_z(s))
                    ends.append(_parse_iso_z(e))
                except Exception:
                    pass
        if starts and ends:
            session_start = min(starts).astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            session_end = max(ends).astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            time_conf = "clip_nmea_bounds"

    # 4) Final fallback: parse local timestamps from clip filenames + timezone + clip_seconds
    if not (session_start and session_end) and clip_seconds is not None:
        tz = _try_zoneinfo(timezone)
        if tz is not None:
            clip_keys: List[_dt.datetime] = []
            for c in clip_entries:
                nm = str(c.get("clip_name") or "")
                parsed = parse_timestamp_from_name(nm)
                if not parsed:
                    continue
                _, naive = parsed
                clip_keys.append(naive.replace(tzinfo=tz))
            if clip_keys:
                start_local = min(clip_keys)
                end_local = max(clip_keys) + _dt.timedelta(seconds=float(clip_seconds))
                session_start = start_local.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                session_end = end_local.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                time_conf = "filename_local_bounds"

    # --- Clips (keep previous shape) ---
    clips_v1: List[Dict[str, object]] = []
    for c in clip_entries:
        entry: Dict[str, object] = {
            "channel": c.get("channel"),
            "clip_name": c.get("clip_name"),
            "role": c.get("role") or "normal",
            "start_ts_utc": c.get("start_ts_utc"),
            "end_ts_utc": c.get("end_ts_utc"),
        }
        entry["video"] = {"path": c.get("video_path")}
        clips_v1.append(entry)

    # --- Artifacts (storyboard required when stills exist) ---
    artifacts: Dict[str, object] = {
        "archive": [],
        "previews": [],
        "exports": [],
    }

    thumbs_index_csv = manifest_dir / "artifacts" / "thumbs" / "index.csv"
    if thumbs_index_csv.exists():
        sb: Dict[str, object] = {
            "type": "stills_index_v1",
            "path": "artifacts/thumbs",
            "index_csv": "artifacts/thumbs/index.csv",
            "every_seconds": float(stills_every_seconds) if stills_every_seconds is not None else None,
            "view": (stills_view or "").strip().lower() or None,
        }
        thumbs_index_geojson = manifest_dir / "artifacts" / "thumbs" / "index.geojson"
        if thumbs_index_geojson.exists():
            sb["index_geojson"] = "artifacts/thumbs/index.geojson"
        artifacts["storyboard"] = sb

    manifest = {
        "schema_version": 2,
        "drive_session_id": str(drive_session_id),
        "vehicle_tag": vehicle_tag,
        "notes": notes,
        "created_utc": created_utc,
        "source": {
            "vendor": "blackvue",
            "model": source_model,
            "serial": source_serial,
            "timezone": timezone,
            "firmware": source_firmware,
            "camera": {
                "front": {"enabled": bool(camera_front_enabled)},
                "rear": {"enabled": bool(camera_rear_enabled)},
            },
        },
        "telemetry": telemetry,
        
        "session_time": dict({
            "start_ts_utc": session_start,
            "end_ts_utc": session_end,
            "time_confidence": time_conf,
        }, **session_time_extra),
        "clips": clips_v1,
        "derived_params": derived_params,
        "artifacts": artifacts,
    }

    if dry_run:
        return

    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

def split_into_drives(pairs: Sequence[ClipPair], gap_seconds: int) -> List[List[ClipPair]]:
    drives: List[List[ClipPair]] = []
    cur: List[ClipPair] = []

    prev_dt: Optional[_dt.datetime] = None
    for p in pairs:
        if prev_dt is not None:
            gap = (p.ts_dt - prev_dt).total_seconds()
            if gap > gap_seconds:
                if cur:
                    drives.append(cur)
                cur = []
        cur.append(p)
        prev_dt = p.ts_dt

    if cur:
        drives.append(cur)
    return drives


def ffmpeg_quote_for_concat(p: Path) -> str:
    """
    Concat demuxer lines look like: file 'C:\\path with spaces\\clip.mp4'
    Escape single quotes conservatively.
    """
    s = str(p.resolve())
    s = s.replace("'", r"'\''")
    return f"file '{s}'"


def write_concat_list(path: Path, files: Sequence[Path], dry_run: bool) -> None:
    if dry_run:
        print(f"[DRY] Would write concat list: {path} ({len(files)} entries)")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for fp in files:
            f.write(ffmpeg_quote_for_concat(fp))
            f.write("\n")


def run_subprocess(args: Sequence[str], dry_run: bool, verbose: bool) -> int:
    if dry_run:
        print("[DRY] " + " ".join(args))
        return 0
    if verbose:
        print(" ".join(args))
    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    if p.returncode != 0:
        eprint(p.stdout)
    return p.returncode


def run_cmd(args: Sequence[str], dry_run: bool, verbose: bool = True) -> int:
    """Back-compat wrapper used by some call sites."""
    return run_subprocess(list(args), dry_run=dry_run, verbose=verbose)


def run_ffmpeg_concat(
    ffmpeg: str,
    list_file: Path,
    out_file: Path,
    keep_audio: bool,
    for_resolve: bool,
    resolve_audio_bitrate_kbps: int,
    dry_run: bool,
    verbose: bool
) -> None:
    out_file.parent.mkdir(parents=True, exist_ok=True)

    args: List[str] = [ffmpeg, "-hide_banner", "-y", "-f", "concat", "-safe", "0", "-i", str(list_file)]

    # Map streams explicitly.
    args += ["-map", "0:v:0"]
    if keep_audio:
        args += ["-map", "0:a:0?"]
    else:
        args += ["-an"]

    # Video: copy (no generational loss)
    args += ["-c:v", "copy"]

    if keep_audio and for_resolve:
        # Normalize audio into something Resolve reliably ingests.
        args += ["-c:a", "aac", "-b:a", f"{resolve_audio_bitrate_kbps}k", "-ar", "48000", "-ac", "2"]
    elif keep_audio:
        # If not for Resolve, preserve audio as-is (best effort).
        args += ["-c:a", "copy"]

    args += ["-movflags", "+faststart", str(out_file)]

    rc = run_subprocess(args, dry_run=dry_run, verbose=verbose)
    if rc != 0:
        raise RuntimeError(f"ffmpeg failed (exit {rc}) for output: {out_file}")

def _interval_tag(seconds: float) -> str:
    # 1.0 -> "1", 0.5 -> "0p5"
    if seconds is None:
        return "0"
    try:
        s = float(seconds)
    except Exception:
        return "0"
    if abs(s - round(s)) < 1e-9:
        return str(int(round(s)))
    txt = f"{s:.3f}".rstrip("0").rstrip(".")
    return txt.replace(".", "p")


def _ffmpeg_input_args(*, prefer_mp4: Optional[Path], list_file: Path) -> List[str]:
    """
    Prefer a single stitched MP4 when available; otherwise use concat-demuxer list.
    """
    if prefer_mp4 is not None and prefer_mp4.exists() and prefer_mp4.is_file():
        return ["-i", str(prefer_mp4)]
    return ["-f", "concat", "-safe", "0", "-i", str(list_file)]



def _ffmpeg_hwaccel_args(hwaccel: str | None) -> List[str]:
    ha = (hwaccel or "").strip().lower()
    if not ha or ha == "none":
        return []
    return ["-hwaccel", ha]


def _simple_vcodec_args(
    *,
    codec: str,
    crf: int,
    preset: str,
    nvenc_cq: int | None,
    nvenc_preset: str,
    nvenc_rc: str,
    nvenc_target_mbps: float | None,
    nvenc_maxrate_mult: float,
    nvenc_bufsize_mult: float,
    tag_hvc1: bool,
) -> List[str]:
    codec_key = (codec or "").lower().strip()
    if codec_key in ("x265", "hevc", "libx265"):
        args = ["-c:v", "libx265", "-crf", str(crf), "-preset", preset]
        if tag_hvc1:
            args += ["-tag:v", "hvc1"]
        return args
    if codec_key in ("x264", "h264", "libx264"):
        return ["-c:v", "libx264", "-crf", str(crf), "-preset", preset]
    if codec_key in ("hevc_nvenc", "nvenc", "nvenc_hevc"):
        if nvenc_target_mbps is not None and nvenc_target_mbps > 0:
            tgt = float(nvenc_target_mbps)
            maxr = max(tgt, tgt * float(nvenc_maxrate_mult))
            buf = max(tgt, tgt * float(nvenc_bufsize_mult))
            args = [
                "-c:v", "hevc_nvenc",
                "-preset", nvenc_preset,
                "-rc", "vbr_hq",
                "-b:v", f"{tgt:.3f}M",
                "-maxrate", f"{maxr:.3f}M",
                "-bufsize", f"{buf:.3f}M",
            ]
        else:
            cq = nvenc_cq if nvenc_cq is not None else max(0, min(51, int(crf) + 6))
            args = ["-c:v", "hevc_nvenc", "-preset", nvenc_preset, "-rc", nvenc_rc, "-cq:v", str(cq), "-b:v", "0"]
        if tag_hvc1:
            args += ["-tag:v", "hvc1"]
        return args
    if codec_key in ("h264_nvenc", "nvenc_h264"):
        if nvenc_target_mbps is not None and nvenc_target_mbps > 0:
            tgt = float(nvenc_target_mbps)
            maxr = max(tgt, tgt * float(nvenc_maxrate_mult))
            buf = max(tgt, tgt * float(nvenc_bufsize_mult))
            return [
                "-c:v", "h264_nvenc",
                "-preset", nvenc_preset,
                "-rc", "vbr_hq",
                "-b:v", f"{tgt:.3f}M",
                "-maxrate", f"{maxr:.3f}M",
                "-bufsize", f"{buf:.3f}M",
            ]
        cq = nvenc_cq if nvenc_cq is not None else max(0, min(51, int(crf) + 4))
        return ["-c:v", "h264_nvenc", "-preset", nvenc_preset, "-rc", nvenc_rc, "-cq:v", str(cq), "-b:v", "0"]
    raise ValueError(f"Unsupported codec: {codec!r} (use x264/x265, hevc_nvenc/h264_nvenc, or nvenc)")


def run_ffmpeg_timelapse(
    *,
    ffmpeg_exe: str,
    hwaccel: str | None,
    prefer_mp4: Optional[Path],
    list_file: Path,
    out_mp4: Path,
    every_seconds: float,
    playback_fps: int,
    width: int,
    codec: str,
    crf: int,
    preset: str,
    nvenc_cq: int | None,
    nvenc_target_mbps: float | None,
    nvenc_preset: str,
    nvenc_rc: str,
    nvenc_maxrate_mult: float,
    nvenc_bufsize_mult: float,
    tag_hvc1: bool,
    dry_run: bool,
    verbose: bool,
) -> None:
    out_mp4.parent.mkdir(parents=True, exist_ok=True)

    # Sample frames at 1/every_seconds, then accelerate to playback_fps.
    # The setpts expression re-timestamps frames to constant-frame-rate output.
    vf_parts: List[str] = [f"fps=1/{every_seconds:g}"]
    if width and width > 0:
        vf_parts.append(f"scale={int(width)}:-2")
    vf_parts.append(f"setpts=N/({int(playback_fps)}*TB)")
    vf = ",".join(vf_parts)

    cmd: List[str] = [ffmpeg_exe, "-hide_banner", "-y", *(_ffmpeg_hwaccel_args(hwaccel)), *(_ffmpeg_input_args(prefer_mp4=prefer_mp4, list_file=list_file))]
    cmd += ["-an", "-vf", vf]
    cmd += _simple_vcodec_args(
        codec=codec,
        crf=int(crf),
        preset=str(preset),
        nvenc_cq=nvenc_cq,
        nvenc_preset=nvenc_preset,
        nvenc_rc=nvenc_rc,
        nvenc_target_mbps=nvenc_target_mbps,
        nvenc_maxrate_mult=nvenc_maxrate_mult,
        nvenc_bufsize_mult=nvenc_bufsize_mult,
        tag_hvc1=tag_hvc1,
    )
    cmd += ["-pix_fmt", "yuv420p", "-vsync", "cfr", "-r", str(int(playback_fps)), "-movflags", "+faststart", str(out_mp4)]

    rc = run_subprocess(cmd, dry_run=dry_run, verbose=verbose)
    if rc != 0:
        raise RuntimeError(f"ffmpeg timelapse failed (exit {rc}) for output: {out_mp4}")


def run_ffmpeg_stills(
    *,
    ffmpeg_exe: str,
    hwaccel: str | None,
    prefer_mp4: Optional[Path],
    list_file: Path,
    out_dir: Path,
    every_seconds: float,
    width: int,
    fmt: str,
    jpg_quality: int,
    max_frames: int,
    dry_run: bool,
    verbose: bool,
) -> None:
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    vf_parts: List[str] = [f"fps=1/{every_seconds:g}"]
    if width and width > 0:
        vf_parts.append(f"scale={int(width)}:-2")
    vf = ",".join(vf_parts)

    ext = (fmt or "jpg").lower().strip()
    if ext not in ("jpg", "png"):
        raise ValueError(f"Unsupported stills format: {fmt!r} (use jpg or png)")

    pattern = out_dir / f"%06d.{ext}"

    cmd: List[str] = [ffmpeg_exe, "-hide_banner", "-y", *(_ffmpeg_hwaccel_args(hwaccel)), *(_ffmpeg_input_args(prefer_mp4=prefer_mp4, list_file=list_file))]
    cmd += ["-an", "-vf", vf, "-vsync", "vfr"]
    if max_frames and int(max_frames) > 0:
        cmd += ["-frames:v", str(int(max_frames))]
    if ext == "jpg":
        q = max(2, min(31, int(jpg_quality)))
        cmd += ["-q:v", str(q)]
    cmd += [str(pattern)]

    rc = run_subprocess(cmd, dry_run=dry_run, verbose=verbose)
    if rc != 0:
        raise RuntimeError(f"ffmpeg still extraction failed (exit {rc}) into: {out_dir}")

def _segments_for_paths(
    *,
    paths: List[Path],
    tz_name: str,
    clip_seconds: float,
) -> List[Tuple[float, _dt.datetime, Path]]:
    """Return list of (concat_start_s, real_start_local_dt, clip_path) for mapping concat offsets -> real time."""
    tz = _try_zoneinfo(tz_name)
    if tz is None:
        # Fallback to the system local timezone (works on Windows without IANA tzdata).
        tz = _dt.datetime.now().astimezone().tzinfo or _dt.timezone.utc
    segs: List[Tuple[float, _dt.datetime, Path]] = []
    t_concat = 0.0
    for p in paths:
        ts = parse_timestamp_from_name(p.name)
        if not ts:
            continue
        _, naive = ts
        real_local = naive.replace(tzinfo=tz)
        segs.append((t_concat, real_local, p))
        t_concat += float(clip_seconds)
    return segs


def _real_time_for_offset(
    *,
    segs: List[Tuple[float, _dt.datetime, Path]],
    offset_s: float,
    clip_seconds: float,
) -> Tuple[_dt.datetime, Path, float]:
    """Map a concat timeline offset (seconds) to a local datetime using clip boundaries."""
    if not segs:
        raise ValueError("No segments available for still timestamp mapping.")
    # Find last segment with start <= offset_s
    last = segs[0]
    for s in segs:
        if s[0] <= offset_s:
            last = s
        else:
            break
    seg_start_s, real_start_local, clip_path = last
    within = max(0.0, float(offset_s) - float(seg_start_s))
    # Clamp within a clip; extraction won't exceed but protect anyway.
    within = min(within, float(clip_seconds))
    return (real_start_local + _dt.timedelta(seconds=within), clip_path, within)


def _parse_iso_z_dt(s: str) -> Optional[_dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return _dt.datetime.fromisoformat(s[:-1] + "+00:00")
        return _dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _compute_video_start_utc(
    *,
    gnss_csv_path: Optional[Path] = None,
    nmea_path: Optional[Path] = None,
) -> Optional[str]:
    """Compute video t=0 UTC timestamp from GNSS CSV anchor or stitched NMEA.

    Uses the same anchor logic as manifest session_time: anchor_utc - anchor_t_rel_s.
    Returns ISO 8601 string with Z suffix, or None.
    """
    # 1) Prefer GNSS CSV clustered anchor (most robust)
    if gnss_csv_path is not None and gnss_csv_path.exists():
        try:
            info = _analyze_gnss_csv(gnss_csv_path)
            a_iso = info.get("gnss_anchor_ts_utc")
            a_t = info.get("gnss_anchor_t_rel_s")
            if a_iso and a_t is not None:
                adt = _parse_iso_z_dt(str(a_iso))
                if adt is not None:
                    vs = adt.astimezone(_dt.timezone.utc) - _dt.timedelta(seconds=float(a_t))
                    return vs.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            pass

    # 2) Derive from stitched NMEA: first valid record's utc_time - t_rel_s
    if nmea_path is not None and nmea_path.exists():
        try:
            txt = nmea_path.read_text(encoding="utf-8", errors="replace")
            recs = nmea_to_records(txt)
            for r in recs:
                utc_s = r.get("utc_time")
                trel = r.get("t_rel_s")
                if utc_s and trel is not None:
                    adt = _parse_iso_z_dt(utc_s)
                    if adt is not None:
                        vs = adt.astimezone(_dt.timezone.utc) - _dt.timedelta(seconds=float(trel))
                        return vs.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            pass

    return None


def _gps_series_from_gnss_csv(gnss_csv: Path) -> List[Tuple[_dt.datetime, float, float, dict]]:
    """Return sorted list of (utc_dt, lat, lon, row_dict) from a GNSS CSV for interpolation."""
    try:
        with open(gnss_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            out: List[Tuple[_dt.datetime, float, float, dict]] = []
            for row in reader:
                if not row:
                    continue
                dt = _parse_iso_utc_any((row.get("utc_time") or "").strip())
                if dt is None:
                    # Fall back to ms epoch timestamp (dashcam RTC)
                    ms_s = (row.get("ms") or "").strip()
                    if ms_s:
                        try:
                            dt = _dt.datetime.fromtimestamp(int(ms_s) / 1000.0, tz=_dt.timezone.utc)
                        except Exception:
                            pass
                if dt is None:
                    continue
                lat_s = row.get("lat")
                lon_s = row.get("lon")
                if lat_s is None or lon_s is None:
                    continue
                try:
                    lat = float(str(lat_s).strip() or "0")
                    lon = float(str(lon_s).strip() or "0")
                except Exception:
                    continue
                if abs(lat) < 1e-6 and abs(lon) < 1e-6:
                    continue
                out.append((dt.astimezone(_dt.timezone.utc), lat, lon, row))
            out.sort(key=lambda t: t[0])
            return out
    except Exception:
        return []

def _gps_series_from_stitched_nmea(nmea_path: Path) -> List[Tuple[_dt.datetime, float, float, dict]]:
    """Return sorted list of (utc_dt, lat, lon, rec) for interpolation."""
    try:
        txt = nmea_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    recs = nmea_to_records(txt)
    out: List[Tuple[_dt.datetime, float, float, dict]] = []
    for r in recs:
        utc_s = r.get("utc_time") or ""
        dt = _parse_iso_z_dt(utc_s)
        lat = r.get("lat")
        lon = r.get("lon")
        if dt is None or lat is None or lon is None:
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except Exception:
            continue
        out.append((dt.astimezone(_dt.timezone.utc), lat_f, lon_f, r))
    out.sort(key=lambda t: t[0])
    return out


def _interp_gps_at(series: List[Tuple[_dt.datetime, float, float, dict]], utc_dt: _dt.datetime) -> Optional[Tuple[float, float, dict]]:
    """Linear interpolate lat/lon at utc_dt; returns (lat, lon, props_src)."""
    if not series:
        return None
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=_dt.timezone.utc)
    t = utc_dt.astimezone(_dt.timezone.utc)

    times = [p[0] for p in series]
    i = bisect.bisect_left(times, t)
    if i <= 0:
        dt0, lat0, lon0, r0 = series[0]
        return (lat0, lon0, r0)
    if i >= len(series):
        dt1, lat1, lon1, r1 = series[-1]
        return (lat1, lon1, r1)

    dt0, lat0, lon0, r0 = series[i - 1]
    dt1, lat1, lon1, r1 = series[i]
    span = (dt1 - dt0).total_seconds()
    if span <= 0:
        return (lat1, lon1, r1)
    w = (t - dt0).total_seconds() / span
    w = 0.0 if w < 0 else (1.0 if w > 1 else w)
    lat = lat0 + (lat1 - lat0) * w
    lon = lon0 + (lon1 - lon0) * w
    use = r0 if w < 0.5 else r1
    return (lat, lon, use)


def rename_stills_with_timestamps(
    *,
    thumbs_dir: Path,
    paths: List[Path],
    every_seconds: float,
    tz_name: str,
    clip_seconds: float,
    name_mode: str,  # sequence|local|utc
    write_index: bool,
    nmea_path: Optional[Path],
    gnss_csv_path: Optional[Path],
    session_start_ts_utc: Optional[str],
    write_geojson: bool,
    dry_run: bool,
) -> None:
    """Rename %06d.jpg/png stills to timestamp-based names, write thumbs/index.csv, and optionally thumbs/index.geojson.

    Contract (dashcam-db): when mode == "utc" and an anchor is available, still UTC timestamps must be derived
    from the SAME anchor as manifest.session_time.start_ts_utc (video t=0), i.e.:

        utc_time = session_start_ts_utc + offset_s

    If session_start_ts_utc is not provided, we derive a robust video-start anchor from GNSS CSV (clustered anchor).
    """
    mode = (name_mode or "sequence").strip().lower()
    if mode == "sequence" and not write_index:
        return

    _seq_pat = re.compile(r"^(?P<n>\d{6})(?:\.__tmp__)?$", re.IGNORECASE)

    # Only process sequence-numbered stills produced by ffmpeg (e.g. 000001.jpg) and
    # any leftover temp stills from a prior crash (e.g. 000001.__tmp__.jpg).
    seq_items: List[Tuple[int, Path]] = []
    try:
        for p in thumbs_dir.iterdir():
            if not (p.is_file() and p.suffix.lower() in (".jpg", ".jpeg", ".png")):
                continue
            m = _seq_pat.match(p.stem)
            if not m:
                continue
            n = int(m.group("n"))
            seq_items.append((n, p))
    except FileNotFoundError:
        return

    if not seq_items:
        return

    seq_items.sort(key=lambda t: t[0])
    ext_files = [p for _, p in seq_items]

    # Determine UTC anchor for the stills timeline.
    # Prefer explicit session_start_ts_utc (manifest anchor). Otherwise derive from GNSS CSV clustered anchor.
    utc_anchor_dt: Optional[_dt.datetime] = None

    def _parse_iso_z_any(s: Optional[str]) -> Optional[_dt.datetime]:
        if not s:
            return None
        try:
            return _parse_iso_z_dt(str(s))
        except Exception:
            return None

    # 1) Explicit session start (video t=0 UTC)
    utc_anchor_dt = _parse_iso_z_any(session_start_ts_utc)

    # 2) Derive from GNSS CSV using clustered anchor: video_start_utc = anchor_utc - anchor_t_rel_s
    if utc_anchor_dt is None and gnss_csv_path is not None and gnss_csv_path.exists():
        try:
            info = _analyze_gnss_csv(gnss_csv_path)
            a_iso = info.get("gnss_anchor_ts_utc")
            a_t = info.get("gnss_anchor_t_rel_s")
            if a_iso and a_t is not None:
                adt = _parse_iso_z_any(str(a_iso))
                if adt is not None:
                    utc_anchor_dt = adt.astimezone(_dt.timezone.utc) - _dt.timedelta(seconds=float(a_t))
        except Exception:
            utc_anchor_dt = None

    # 3) Last resort: stitched NMEA series start (may drift; use only if nothing else)
    if utc_anchor_dt is None and (nmea_path is not None and nmea_path.exists()):
        try:
            series_tmp = _gps_series_from_stitched_nmea(nmea_path)
            if series_tmp:
                utc_anchor_dt = series_tmp[0][0].astimezone(_dt.timezone.utc)
        except Exception:
            utc_anchor_dt = None

    # Determine naming granularity (milliseconds when interval < 1s)
    need_ms = float(every_seconds) < 1.0

    # For local/sequence modes we use the existing filename-based segment model (may require tzdb).
    segs = None
    if mode != "utc":
        try:
            segs = _segments_for_paths(paths=paths, tz_name=tz_name, clip_seconds=clip_seconds)
        except Exception:
            segs = None
        if not segs and (mode in ("local", "sequence")) and write_index:
            # We'll still try to build an index, but local timestamps may be empty.
            pass

    def _clip_for_offset(off_s: float) -> Tuple[str, float]:
        """Best-effort mapping from stitched-video offset to source clip name and offset within clip."""
        if not paths:
            return ("", float(off_s))
        try:
            idx = int(float(off_s) // float(clip_seconds))
        except Exception:
            idx = 0
        if idx < 0:
            idx = 0
        if idx >= len(paths):
            idx = len(paths) - 1
        within = float(off_s) - float(idx) * float(clip_seconds)
        if within < 0:
            within = 0.0
        return (paths[idx].name, within)

    def _local_from_utc(utc_dt: _dt.datetime) -> Optional[_dt.datetime]:
        # Prefer manifest timezone; fall back to system local tz if tzdb is unavailable.
        try:
            tz = ZoneInfo(str(tz_name))
            return utc_dt.astimezone(tz)
        except Exception:
            try:
                sys_tz = _dt.datetime.now().astimezone().tzinfo or _dt.timezone.utc
                return utc_dt.astimezone(sys_tz)
            except Exception:
                return None

    rows: List[Tuple[str, str, str, float, str, float]] = []
    renames: List[Tuple[Path, Path]] = []
    reserved: set[str] = set()

    for fp in ext_files:
        m = _seq_pat.match(fp.stem)
        i = (int(m.group("n")) - 1) if m else 0
        off = float(i) * float(every_seconds)

        src_clip_name, within = _clip_for_offset(off)

        utc_dt: Optional[_dt.datetime] = None
        local_dt: Optional[_dt.datetime] = None

        if mode == "utc":
            if utc_anchor_dt is not None:
                utc_dt = (utc_anchor_dt + _dt.timedelta(seconds=float(off))).astimezone(_dt.timezone.utc)
                local_dt = _local_from_utc(utc_dt)
        else:
            # Local/sequence: derive local_dt from filename/segment model
            if segs:
                try:
                    ldt, clip_path, within2 = _real_time_for_offset(segs=segs, offset_s=off, clip_seconds=clip_seconds)
                    local_dt = ldt
                    src_clip_name = clip_path.name
                    within = within2
                except Exception:
                    pass
            if mode == "local":
                # no utc conversion required unless for index
                pass
            else:
                # sequence
                pass

        # Filename base
        if mode == "utc":
            if utc_dt is None:
                base = fp.stem  # fallback
            else:
                base = utc_dt.strftime("%Y%m%d_%H%M%S")
                if need_ms:
                    base += f"_{int(utc_dt.microsecond/1000):03d}"
                base += "Z"
        else:
            if local_dt is None:
                base = fp.stem
            else:
                base = local_dt.strftime("%Y%m%d_%H%M%S")
                if need_ms:
                    base += f"_{int(local_dt.microsecond/1000):03d}"

        new_name = f"{base}{fp.suffix.lower()}"
        new_path = thumbs_dir / new_name

        # Avoid collisions deterministically:
        # - track names reserved in this run (two-phase rename means exists() checks are unreliable)
        # - allow overwriting existing timestamped files from a prior run (idempotent reruns)
        if new_path.name in reserved:
            n = 1
            while True:
                cand = thumbs_dir / f"{base}_{n:02d}{fp.suffix.lower()}"
                if cand.name not in reserved and not cand.exists():
                    new_path = cand
                    break
                n += 1
        reserved.add(new_path.name)

        if mode != "sequence":
            renames.append((fp, new_path))

        # index row
        if mode == "utc" and utc_dt is not None:
            utc_iso = utc_dt.astimezone(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
            local_iso = local_dt.isoformat() if local_dt is not None else ""
        else:
            utc_iso = ""
            if local_dt is not None and local_dt.tzinfo is not None:
                try:
                    utc_iso = local_dt.astimezone(_dt.timezone.utc).isoformat().replace("+00:00", "Z")
                except Exception:
                    utc_iso = ""
            local_iso = local_dt.isoformat() if local_dt is not None else ""
        rows.append((new_path.name if mode != "sequence" else fp.name, local_iso, utc_iso, off, src_clip_name, float(within)))

    # Apply renames (two-phase to avoid conflicts)
    if mode != "sequence":
        if dry_run:
            for a, b in renames[:5]:
                print(f"[DRY] rename still: {a.name} -> {b.name}")
        else:
            tmp_pairs: List[Tuple[Path, Path]] = []
            for src, dst in renames:
                tmp = src.with_name(src.stem + ".__tmp2__" + src.suffix)
                src.rename(tmp)
                tmp_pairs.append((tmp, dst))
            for tmp, dst in tmp_pairs:
                # Use replace() so reruns don't crash if dst already exists.
                tmp.replace(dst)

    do_index = write_index or (mode in ("local", "utc"))
    if do_index:
        idx_path = thumbs_dir / "index.csv"
        header = "file,local_time,utc_time,offset_s,source_clip,clip_offset_s\n"
        if dry_run:
            print(f"[DRY] Would write stills index: {idx_path}")
        else:
            with idx_path.open("w", encoding="utf-8", newline="") as f:
                f.write(header)
                for r in rows:
                    f.write(f"{r[0]},{r[1]},{r[2]},{r[3]:.3f},{r[4]},{r[5]:.3f}\n")

    # Optional: write thumbs/index.geojson for mapping apps.
    do_geo = bool(write_geojson) and do_index
    if do_geo:
        series: List[Tuple[_dt.datetime, float, float, dict]] = []
        if gnss_csv_path is not None and gnss_csv_path.exists():
            series = _gps_series_from_gnss_csv(gnss_csv_path)
        if not series and (nmea_path is not None and nmea_path.exists()):
            series = _gps_series_from_stitched_nmea(nmea_path)

        if series:
            gj_path = thumbs_dir / "index.geojson"
            features = []
            for (fname, local_iso, utc_iso, off_s, src_clip, clip_off) in rows:
                dt = _parse_iso_z_dt(utc_iso)
                if dt is None:
                    continue
                interp = _interp_gps_at(series, dt)
                if not interp:
                    continue
                lat, lon, rec = interp
                props = {
                    "file": fname,
                    "local_time": local_iso,
                    "utc_time": utc_iso,
                    "offset_s": round(float(off_s), 3),
                    "source_clip": src_clip,
                    "clip_offset_s": round(float(clip_off), 3),
                }
                for k in ("speed_mph", "speed_mps", "course_deg", "alt_m", "fix_quality", "satellites", "hdop"):
                    try:
                        v = rec.get(k)
                    except Exception:
                        v = None
                    if v is not None and v != "":
                        props[k] = v
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": props,
                })

            fc = {"type": "FeatureCollection", "features": features}
            if dry_run:
                print(f"[DRY] Would write thumbs geojson: {gj_path} ({len(features)} features)")
            else:
                gj_path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
def _deg_wrap_180(d: float) -> float:
    """Wrap degrees into [-180, 180]."""
    x = (d + 180.0) % 360.0 - 180.0
    return x


def derive_events_from_nmea_records(
    records: List[dict],
    params: Dict[str, object],
    *,
    video_start_ts_utc: Optional[str] = None,
) -> List[dict]:
    """Derive simple 'events' from stitched NMEA records.

    This is intentionally lightweight and conservative:
    - hard brake / accel: based on dv/dt from GPS speed
    - harsh turn: based on v * d(heading)/dt (approx lateral accel)
    - stop: sustained near-zero speed

    Returns a list of event dicts with at least:
      {type, ms, utc_time, lat, lon, speed_mps, value_mps2, course_deg}
    """
    if not records:
        return []

    # thresholds / params
    hard_brake = float(params.get("hard_brake_mps2", 2.6))
    hard_accel = float(params.get("hard_accel_mps2", 2.2))
    harsh_turn = float(params.get("harsh_turn_mps2", 3.0))
    min_speed_brake_mph = float(params.get("min_speed_brake_mph", 12))
    min_speed_turn_mph = float(params.get("min_speed_turn_mph", 20))
    stop_min_s = float(params.get("stop_min_s", 15))
    cooldown_s = float(params.get("bump_cooldown_s", 3))

    KNOT_TO_MPS = 0.514444
    MPH_TO_MPS = 0.44704

    min_speed_brake_mps = min_speed_brake_mph * MPH_TO_MPS
    min_speed_turn_mps = min_speed_turn_mph * MPH_TO_MPS

    # sanitize + sort by ms
    recs = [r for r in records if isinstance(r.get("ms"), int)]
    recs.sort(key=lambda r: r["ms"])
    if len(recs) < 2:
        return []

    
    # Prefer a manifest-aligned video start anchor when provided (covers pre-lock video period).
    video_start_dt = _parse_iso_z_dt(video_start_ts_utc) if video_start_ts_utc else None
    if video_start_dt is not None:
        video_start_dt = video_start_dt.astimezone(_dt.timezone.utc)
        try:
            drive_start_ms = int(video_start_dt.timestamp() * 1000)
        except Exception:
            drive_start_ms = int(recs[0]["ms"])
    else:
        drive_start_ms = int(recs[0]["ms"])

    # helpers
    def _speed_mps(r: dict) -> Optional[float]:
        spd_kn = r.get("speed_knots")
        if spd_kn is None:
            return None
        try:
            return float(spd_kn) * KNOT_TO_MPS
        except Exception:
            return None

    def _course_deg(r: dict) -> Optional[float]:
        c = r.get("course_deg")
        if c is None:
            return None
        try:
            return float(c)
        except Exception:
            return None

    events: List[dict] = []
    seg = {"brake": None, "accel": None, "turn": None}
    stop_start = None
    stop_last_ms = None
    last_event_ms_by_type: Dict[str, int] = {}

    def _emit_event(ev_type: str, peak_rec: dict, peak_val: float) -> None:
        if peak_rec is None:
            return
        ms = int(peak_rec.get("ms"))
        ev_dt = _parse_iso_z_dt(str(peak_rec.get("utc_time") or ""))
        if ev_dt is not None:
            ev_dt = ev_dt.astimezone(_dt.timezone.utc)
        last_ms = last_event_ms_by_type.get(ev_type)
        if last_ms is not None and (ms - last_ms) < int(cooldown_s * 1000):
            return
        last_event_ms_by_type[ev_type] = ms

        events.append({
            "type": ev_type,
            "ms": ms,
            "drive_start_ms": drive_start_ms,
            "offset_ms": int(((ev_dt - video_start_dt).total_seconds() * 1000.0)) if (video_start_dt is not None and ev_dt is not None) else (ms - drive_start_ms),
            "offset_s": ((ev_dt - video_start_dt).total_seconds()) if (video_start_dt is not None and ev_dt is not None) else ((ms - drive_start_ms) / 1000.0),
            "utc_time": peak_rec.get("utc_time"),
            "lat": peak_rec.get("lat"),
            "lon": peak_rec.get("lon"),
            "speed_mps": _speed_mps(peak_rec),
            "course_deg": _course_deg(peak_rec),
            "value_mps2": float(peak_val),
        })

    prev = recs[0]
    prev_v = _speed_mps(prev)
    prev_course = _course_deg(prev)

    for cur in recs[1:]:
        cur_v = _speed_mps(cur)
        if cur_v is None or prev_v is None:
            prev, prev_v, prev_course = cur, cur_v, _course_deg(cur)
            continue

        dt = (int(cur["ms"]) - int(prev["ms"])) / 1000.0
        if dt <= 0:
            prev, prev_v, prev_course = cur, cur_v, _course_deg(cur)
            continue

        accel = (cur_v - prev_v) / dt

        # brake
        if cur_v >= min_speed_brake_mps and accel <= -hard_brake:
            mag = abs(accel)
            s = seg["brake"]
            if s is None or mag > s["peak_val"]:
                seg["brake"] = {"peak_rec": cur, "peak_val": mag}
        else:
            if seg["brake"] is not None:
                _emit_event("hard_brake", seg["brake"]["peak_rec"], seg["brake"]["peak_val"])
                seg["brake"] = None

        # accel
        if cur_v >= min_speed_brake_mps and accel >= hard_accel:
            mag = abs(accel)
            s = seg["accel"]
            if s is None or mag > s["peak_val"]:
                seg["accel"] = {"peak_rec": cur, "peak_val": mag}
        else:
            if seg["accel"] is not None:
                _emit_event("hard_accel", seg["accel"]["peak_rec"], seg["accel"]["peak_val"])
                seg["accel"] = None

        # turn (approx lat accel)
        cur_course = _course_deg(cur)
        if cur_course is not None and prev_course is not None:
            ddeg = _deg_wrap_180(cur_course - prev_course)
            omega = math.radians(ddeg) / dt
            a_lat = cur_v * omega
            mag = abs(a_lat)
            if cur_v >= min_speed_turn_mps and mag >= harsh_turn:
                s = seg["turn"]
                if s is None or mag > s["peak_val"]:
                    seg["turn"] = {"peak_rec": cur, "peak_val": mag}
            else:
                if seg["turn"] is not None:
                    _emit_event("harsh_turn", seg["turn"]["peak_rec"], seg["turn"]["peak_val"])
                    seg["turn"] = None

        # stop
        if cur_v <= 0.5:
            if stop_start is None:
                stop_start = cur
            stop_last_ms = int(cur["ms"])
        else:
            if stop_start is not None and stop_last_ms is not None:
                dur_s = (stop_last_ms - int(stop_start["ms"])) / 1000.0
                if dur_s >= stop_min_s:
                    _emit_event("stop", stop_start, dur_s)
                stop_start = None
                stop_last_ms = None

        prev, prev_v, prev_course = cur, cur_v, cur_course

    # flush segments
    if seg["brake"] is not None:
        _emit_event("hard_brake", seg["brake"]["peak_rec"], seg["brake"]["peak_val"])
    if seg["accel"] is not None:
        _emit_event("hard_accel", seg["accel"]["peak_rec"], seg["accel"]["peak_val"])
    if seg["turn"] is not None:
        _emit_event("harsh_turn", seg["turn"]["peak_rec"], seg["turn"]["peak_val"])
    if stop_start is not None and stop_last_ms is not None:
        dur_s = (stop_last_ms - int(stop_start["ms"])) / 1000.0
        if dur_s >= stop_min_s:
            _emit_event("stop", stop_start, dur_s)

    events.sort(key=lambda e: int(e.get("ms", 0)))
    return events


def write_events_index(events: List[dict], events_dir: Path, *, dry_run: bool) -> None:
    """Write artifacts/events/index.csv and index.geojson."""
    if dry_run:
        return
    events_dir.mkdir(parents=True, exist_ok=True)

    csv_path = events_dir / "index.csv"
    fields = ["type","ms","drive_start_ms","offset_ms","offset_s","utc_time","lat","lon","speed_mps","course_deg","value_mps2"]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for e in events:
            w.writerow({k: e.get(k) for k in fields})

    features = []
    for e in events:
        lat = e.get("lat")
        lon = e.get("lon")
        if lat is None or lon is None:
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except Exception:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon_f, lat_f]},
            "properties": dict(e),
        })

    geo = {"type":"FeatureCollection","features":features}
    (events_dir / "index.geojson").write_text(json.dumps(geo, indent=2), encoding="utf-8")


def extract_event_stills(
    *,
    ffmpeg_exe: str,
    video_path: Path,
    events: List[dict],
    out_dir: Path,
    fmt: str,
    quality: int,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Extract a still at each event time from a stitched drive video."""
    if dry_run:
        return
    out_dir.mkdir(parents=True, exist_ok=True)

    fmt = (fmt or "jpg").lower()
    if fmt not in ("jpg","png"):
        fmt = "jpg"

    for e in events:
        # Use drive-relative offset seconds when available; epoch timestamps cannot be used as -ss.
        sec = e.get("offset_s")
        if sec is None:
            ms = e.get("ms")
            start_ms = e.get("drive_start_ms")
            if ms is None:
                continue
            try:
                if start_ms is not None:
                    sec = (float(ms) - float(start_ms)) / 1000.0
                else:
                    sec = float(ms) / 1000.0
            except Exception:
                continue
        try:
            sec = float(sec)
        except Exception:
            continue
        if sec < 0:
            continue

        utc = (e.get("utc_time") or "").replace(":", "").replace("-", "").replace("T", "_").replace("Z", "Z")
        base = utc if utc else f"ms{int(ms):012d}"
        ev_type = str(e.get("type") or "event")
        out_path = out_dir / f"{base}_{ev_type}.{fmt}"

        cmd = [
            ffmpeg_exe,
            "-hide_banner",
            "-loglevel", "error" if not verbose else "info",
            "-ss", f"{sec:.3f}",
            "-i", str(video_path),
            "-frames:v", "1",
        ]
        if fmt == "jpg":
            cmd += ["-q:v", str(int(quality))]
            # Avoid MJPEG strict compliance failures on limited-range sources.
            cmd += ["-pix_fmt", "yuvj420p", "-strict", "-1"]
        cmd += [str(out_path)]

        if verbose:
            print("    " + " ".join(cmd))
        subprocess.run(cmd, check=False)
def collect_sidecars(mp4: Path) -> List[Path]:
    """
    blackclue typically produces files adjacent to the MP4:
      - <base>.nmea
      - <base>.3gf
      - <base>.3gf.txt
      - <base>.jpg
      - <mp4>-*-free.bin   (pattern varies; capture via glob)
    """
    out: List[Path] = []
    base = mp4.with_suffix("")  # removes .mp4
    for ext in (".nmea", ".3gf", ".3gf.txt", ".jpg"):
        p = Path(str(base) + ext)
        if p.exists():
            out.append(p)

    for p in mp4.parent.glob(mp4.name + "-*-free.bin"):
        if p.exists():
            out.append(p)

    return out



def run_blackclue(blackclue: str, mode: str, mp4: Path, dry_run: bool, verbose: bool) -> None:
    """
    mode: "complete" -> -c, "raw" -> -r
    blackclue writes sidecars next to the input mp4; stdout may be empty.
    """
    args: List[str] = [blackclue]
    if mode == "complete":
        args.append("-c")
    elif mode == "raw":
        args.append("-r")
    else:
        raise ValueError(mode)

    args.append(str(mp4))
    rc = run_subprocess(args, dry_run=dry_run, verbose=verbose)
    if rc != 0:
        raise RuntimeError(f"blackclue failed (exit {rc}) on: {mp4}")


def copy_or_move_files(files: Sequence[Path], dest_dir: Path, action: str, dry_run: bool) -> List[Path]:
    """Copy/move sidecar files to dest_dir and return destination paths.

    When action == 'none', no filesystem changes are made and an empty list is returned.
    """
    dests: List[Path] = []
    if action == "none":
        return dests
    dest_dir.mkdir(parents=True, exist_ok=True)

    for src in files:
        dest = dest_dir / src.name
        dests.append(dest)
        if dry_run:
            print(f"[DRY] {action} {src} -> {dest}")
            continue
        if action == "copy":
            shutil.copy2(src, dest)
        elif action == "move":
            shutil.move(str(src), str(dest))
        else:
            raise ValueError(f"Unknown telemetry sidecar action: {action}")

    return dests

def stitch_nmea(nmea_files: Sequence[Path], out_path: Path, dry_run: bool) -> None:
    """
    Stitch per-clip .nmea files into one drive-level .nmea.
    De-dupes by (ms, sentence_type) where possible.
    Removes blank lines for cleaner downstream parsing.
    """
    if dry_run:
        print(f"[DRY] Would stitch {len(nmea_files)} NMEA file(s) -> {out_path}")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    seen: set[Tuple[str, str]] = set()

    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        for nf in nmea_files:
            if not nf.exists():
                continue
            for raw_line in nf.read_text(encoding="utf-8", errors="replace").splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                m = _NMEA_LINE_RE.match(line)
                if m:
                    ms = m.group("ms")
                    rest = m.group("rest")
                    if rest.startswith("$"):
                        stype = rest[1:].split(",", 1)[0]
                        key = (ms, stype)
                    elif rest == "":
                        key = (ms, "MARK")
                    else:
                        key = (ms, rest[:24])
                    if key in seen:
                        continue
                    seen.add(key)
                out.write(line)
                out.write("\n")


def _nmea_ddmm_to_decimal(coord: str) -> Optional[float]:
    """
    Convert ddmm.mmmm or dddmm.mmmm to decimal degrees (unsigned).
    """
    if not coord or "." not in coord:
        return None
    intpart = coord.split(".", 1)[0]
    if len(intpart) < 3:
        return None
    deg_digits = len(intpart) - 2
    deg = float(coord[:deg_digits])
    minutes = float(coord[deg_digits:])
    return deg + minutes / 60.0


def _parse_hhmmss(time_str: str) -> Optional[_dt.time]:
    if not time_str:
        return None
    # accepts HHMMSS or HHMMSS.SS
    try:
        hh = int(time_str[0:2])
        mm = int(time_str[2:4])
        ss = int(time_str[4:6])
        micro = 0
        if len(time_str) > 6 and time_str[6] == ".":
            frac = time_str[7:]
            if frac.isdigit():
                micro = int((frac + "000000")[:6])
        return _dt.time(hh, mm, ss, micro)
    except Exception:
        return None


def _parse_ddmmyy(date_str: str) -> Optional[_dt.date]:
    if not date_str or len(date_str) != 6 or not date_str.isdigit():
        return None
    dd = int(date_str[0:2])
    mm = int(date_str[2:4])
    yy = int(date_str[4:6])
    # NMEA dates are usually 19xx/20xx; assume 2000-2099 for yy<80 else 1900s
    year = 2000 + yy if yy < 80 else 1900 + yy
    try:
        return _dt.date(year, mm, dd)
    except Exception:
        return None


def _iso_utc(dt: Optional[_dt.datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def nmea_to_records(nmea_text: str) -> List[dict]:
    """
    Parse stitched NMEA with leading [epoch_ms] markers into a list of records.
    Merges RMC and GGA by the bracketed ms timestamp.
    """
    records: Dict[int, dict] = {}

    def _safe_int(s: str | None) -> Optional[int]:
        if s is None:
            return None
        s = s.strip()
        if not s:
            return None
        try:
            return int(s)
        except Exception:
            return None

    def _safe_float(s: str | None) -> Optional[float]:
        if s is None:
            return None
        s = s.strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None


    for raw in nmea_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = _NMEA_LINE_RE.match(line)
        if not m:
            continue
        ms = int(m.group("ms"))
        rest = m.group("rest")
        if rest == "":
            continue
        if not rest.startswith("$"):
            continue

        rec = records.setdefault(ms, {"ms": ms})

        # Remove checksum and split fields
        sentence = rest
        if "*" in sentence:
            sentence = sentence.split("*", 1)[0]
        parts = sentence.split(",")
        stype = parts[0].lstrip("$").upper()

        if stype.endswith("RMC") and len(parts) >= 10:
            t = _parse_hhmmss(parts[1])
            status = parts[2]
            lat = _nmea_ddmm_to_decimal(parts[3])
            lat_hemi = parts[4].upper() if parts[4] else ""
            lon = _nmea_ddmm_to_decimal(parts[5])
            lon_hemi = parts[6].upper() if parts[6] else ""
            spd_kn = float(parts[7]) if parts[7] else None
            course = _safe_float(parts[8])
            d = _parse_ddmmyy(parts[9])

            if lat is not None and lat_hemi == "S":
                lat = -lat
            if lon is not None and lon_hemi == "W":
                lon = -lon

            dt_utc = None
            if d and t:
                dt_utc = _dt.datetime.combine(d, t).replace(tzinfo=_dt.timezone.utc)

            rec.update({
                "rmc_status": status,
                "utc_time": _iso_utc(dt_utc),
                "lat": lat,
                "lon": lon,
                "speed_knots": spd_kn,
                "course_deg": course,
                "date_ddmmyy": parts[9],
                "time_hhmmss": parts[1],
            })

        elif stype.endswith("GGA") and len(parts) >= 10:
            t = _parse_hhmmss(parts[1])
            lat = _nmea_ddmm_to_decimal(parts[2])
            lat_hemi = parts[3].upper() if parts[3] else ""
            lon = _nmea_ddmm_to_decimal(parts[4])
            lon_hemi = parts[5].upper() if parts[5] else ""
            fix = int(parts[6]) if parts[6].isdigit() else None
            sats = int(parts[7]) if parts[7].isdigit() else None
            if len(parts) > 8 and parts[8].upper() == "M":
                # Malformed / shifted GGA (hdop should never be "M"); ignore this sentence.
                continue
            hdop = _safe_float(parts[8])
            alt = _safe_float(parts[9])
            geoid = _safe_float(parts[11]) if len(parts) >= 12 else None
            if lat is not None and lat_hemi == "S":
                lat = -lat
            if lon is not None and lon_hemi == "W":
                lon = -lon

            rec.update({
                "fix_quality": fix,
                "satellites": sats,
                "hdop": hdop,
                "alt_m": alt,
                "geoid_sep_m": geoid,
                "gga_time_hhmmss": parts[1],
            })

            # If RMC missing lat/lon, use GGA.
            if rec.get("lat") is None and lat is not None:
                rec["lat"] = lat
            if rec.get("lon") is None and lon is not None:
                rec["lon"] = lon

    # Convert to list sorted by ms
    out = [records[k] for k in sorted(records.keys())]

    # Add relative time and derived speed units
    if out:
        ms0 = out[0]["ms"]
        for r in out:
            r["t_rel_s"] = (r["ms"] - ms0) / 1000.0
            spd_kn = r.get("speed_knots")
            if spd_kn is not None:
                r["speed_mps"] = spd_kn * 0.514444
                r["speed_mph"] = spd_kn * 1.150779
            else:
                r["speed_mps"] = None
                r["speed_mph"] = None

    # Backfill utc_time for pre-lock records that have ms but no GNSS-derived timestamp.
    # Uses the first valid GNSS UTC anchor + ms delta to extrapolate backwards (and forwards
    # through any gaps). The dashcam ms timestamps have consistent relative timing from the
    # same RTC, so the delta is reliable even if the absolute clock drifts slightly.
    if out:
        anchor_ms: Optional[int] = None
        anchor_dt: Optional[_dt.datetime] = None
        for r in out:
            utc_s = r.get("utc_time")
            if utc_s:
                anchor_dt = _parse_iso_z_dt(utc_s)
                if anchor_dt is not None:
                    anchor_ms = r["ms"]
                    break
        if anchor_ms is not None and anchor_dt is not None:
            for r in out:
                if not r.get("utc_time"):
                    delta_ms = r["ms"] - anchor_ms
                    backfilled = anchor_dt + _dt.timedelta(milliseconds=delta_ms)
                    r["utc_time"] = _iso_utc(backfilled)

    return out




def nmea_first_epoch_ms(nmea_path: Path) -> Optional[int]:
    """
    Return the first epoch timestamp (milliseconds) found in a BlackVue .nmea file.
    Lines look like: [1767042925880]
    """
    try:
        with nmea_path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if len(line) >= 3 and line[0] == "[" and line[-1] == "]":
                    inner = line[1:-1].strip()
                    if inner.isdigit():
                        return int(inner)
    except FileNotFoundError:
        return None
    return None


def parse_3gf_txt(threegf_txt: Path) -> List[dict]:
    """
    Parse blackclue-decoded .3gf.txt into rows.

    Example line:
      0 00000413 007e fffc 0013 1043 126 -4 19

    We emit:
      {"idx": 0, "t_ms": 1043, "ax": 126, "ay": -4, "az": 19}
    """
    rows: List[dict] = []
    with threegf_txt.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            parts = s.split()
            if len(parts) < 9:
                continue
            try:
                idx = int(parts[0])
                # prefer the explicit decimal columns (last 4)
                t_ms = int(parts[5])
                ax = int(parts[6])
                ay = int(parts[7])
                az = int(parts[8])
            except ValueError:
                continue
            rows.append({"idx": idx, "t_ms": t_ms, "ax": ax, "ay": ay, "az": az})
    return rows


def write_accel_csv(out_csv: Path, samples: List[dict]) -> None:
    """
    Write accelerometer samples as CSV.

    Each sample dict should contain:
      abs_ms, t_rel_s, ax, ay, az, clip, idx, t_clip_ms
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["abs_ms", "t_rel_s", "ax", "ay", "az", "clip", "idx", "t_clip_ms"]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in samples:
            w.writerow(row)


def build_drive_accel_samples(
    clip_mp4_paths: List[Path],
    tele_clips_dir: Path,
) -> Tuple[Optional[int], List[dict]]:
    """
    Build a drive-level accelerometer timeline from per-clip .3gf.txt + .nmea.

    We anchor each clip by the FIRST epoch-ms bracket in that clip's .nmea file.
    For each 3gf sample (t_ms relative within the clip), we compute:

      abs_ms  = clip_epoch_ms + t_ms
      t_rel_s = (abs_ms - drive_epoch0_ms)/1000

    Returns (drive_epoch0_ms, samples).
    """
    drive_epoch0_ms: Optional[int] = None
    samples: List[dict] = []

    for mp4 in clip_mp4_paths:
        stem = mp4.stem
        clip_nmea = tele_clips_dir / f"{stem}.nmea"
        clip_3gf_txt = tele_clips_dir / f"{stem}.3gf.txt"

        clip_epoch_ms = nmea_first_epoch_ms(clip_nmea)
        if clip_epoch_ms is None:
            # Can't time-anchor this clip; skip accel rows (still keep raw files).
            continue

        if drive_epoch0_ms is None:
            drive_epoch0_ms = clip_epoch_ms

        if not clip_3gf_txt.exists():
            continue

        for r in parse_3gf_txt(clip_3gf_txt):
            abs_ms = clip_epoch_ms + r["t_ms"]
            t_rel_s = (abs_ms - drive_epoch0_ms) / 1000.0 if drive_epoch0_ms is not None else 0.0
            samples.append(
                {
                    "abs_ms": abs_ms,
                    "t_rel_s": round(t_rel_s, 3),
                    "ax": r["ax"],
                    "ay": r["ay"],
                    "az": r["az"],
                    "clip": stem,
                    "idx": r["idx"],
                    "t_clip_ms": r["t_ms"],
                }
            )

    # Sort by absolute time so DB import is deterministic.
    samples.sort(key=lambda d: (d["abs_ms"], d["clip"], d["idx"]))
    return drive_epoch0_ms, samples
def write_csv(records: Sequence[dict], out_path: Path, dry_run: bool) -> None:
    if dry_run:
        print(f"[DRY] Would write CSV: {out_path} ({len(records)} rows)")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # stable column order
    fieldnames = [
        "ms", "t_rel_s", "utc_time",
        "lat", "lon",
        "speed_knots", "speed_mps", "speed_mph",
        "course_deg",
        "rmc_status",
        "fix_quality", "satellites", "hdop",
        "alt_m", "geoid_sep_m",
        "date_ddmmyy", "time_hhmmss"
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in records:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def write_gpx(records: Sequence[dict], out_path: Path, dry_run: bool, name: str) -> None:
    """
    Minimal GPX 1.1 track. Uses utc_time when present; otherwise omits <time>.
    """
    if dry_run:
        print(f"[DRY] Would write GPX: {out_path} ({len(records)} pts)")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)

    def esc(s: str) -> str:
        return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                 .replace('"', "&quot;").replace("'", "&apos;"))

    pts = []
    for r in records:
        lat = r.get("lat")
        lon = r.get("lon")
        if lat is None or lon is None:
            continue
        t = r.get("utc_time") or ""
        ele = r.get("alt_m")
        pt = f'<trkpt lat="{lat:.8f}" lon="{lon:.8f}">'
        if ele is not None:
            pt += f"<ele>{ele:.2f}</ele>"
        if t:
            pt += f"<time>{esc(t)}</time>"
        pt += "</trkpt>"
        pts.append(pt)

    gpx = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<gpx version="1.1" creator="blackvue_drives.py" xmlns="http://www.topografix.com/GPX/1/1">\n'
        f"  <trk><name>{esc(name)}</name><trkseg>\n"
        + "\n".join("    " + p for p in pts)
        + "\n  </trkseg></trk>\n</gpx>\n"
    )
    out_path.write_text(gpx, encoding="utf-8")



def _ensure_even(n: int) -> int:
    return n if n % 2 == 0 else n - 1


def _parse_wxh(s: str) -> Tuple[int, int]:
    m = re.match(r"^\s*(\d+)\s*[xX]\s*(\d+)\s*$", s or "")
    if not m:
        raise ValueError(f"Invalid size '{s}'. Expected WxH, e.g. 640x360")
    return int(m.group(1)), int(m.group(2))


def _resolve_ffprobe(ffprobe_arg: str, ffmpeg_exe: str) -> str:
    if ffprobe_arg:
        return ffprobe_arg
    if ffmpeg_exe and Path(ffmpeg_exe).exists():
        cand = Path(ffmpeg_exe).with_name("ffprobe.exe")
        if cand.exists():
            return str(cand)
    return "ffprobe"


@dataclass
class VideoInfo:
    width: int
    height: int
    duration_s: float


def ffprobe_video_info(ffprobe_exe: str, video_path: Path) -> VideoInfo:
    # Return width/height/duration for a video file using ffprobe.
    cmd = [
        ffprobe_exe,
        "-v", "error",
        "-show_entries", "stream=width,height:format=duration",
        "-of", "json",
        str(video_path),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"ffprobe failed ({p.returncode}) for {video_path}:\n{p.stderr.strip()}")
    data = json.loads(p.stdout)
    streams = data.get("streams") or []
    if not streams:
        raise RuntimeError(f"ffprobe returned no streams for {video_path}")
    w = int(streams[0].get("width") or 0)
    h = int(streams[0].get("height") or 0)
    dur = float((data.get("format") or {}).get("duration") or 0.0)
    if w <= 0 or h <= 0 or dur <= 0:
        # Some MP4 report duration=0 when fragmented; try an alternate query.
        cmd2 = [ffprobe_exe, "-v", "error", "-of", "default=nw=1:nk=1", "-show_entries", "format=duration", str(video_path)]
        p2 = subprocess.run(cmd2, capture_output=True, text=True)
        if p2.returncode == 0:
            try:
                dur = float(p2.stdout.strip())
            except Exception:
                pass
    return VideoInfo(width=w, height=h, duration_s=dur)


def render_map_gpx_animator(
    *,
    java_exe: str,
    jar_path: str,
    gpx_path: Path,
    out_mp4: Path,
    width: int,
    height: int,
    fps: int,
    total_time_ms: int,
    zoom: int,
    tms_url_template: str,
    tms_user_agent: str,
    dry_run: bool,
) -> None:
    # Render an MP4 map animation from a GPX file using GPX Animator (Java).
    if not jar_path:
        raise ValueError("--gpx-animator-jar is required when --run-map-render is set")
    jar = Path(jar_path)
    if not jar.exists():
        raise FileNotFoundError(f"GPX Animator jar not found: {jar}")
    cmd: List[str] = [
        java_exe, "-jar", str(jar),
        "--input", str(gpx_path),
        "--output", str(out_mp4),
        "--width", str(width),
        "--height", str(height),
        "--fps", str(fps),
        "--total-time", str(total_time_ms),
        "--speedup", "1",
        "--skip-idle", "false",
    ]
    if zoom and zoom > 0:
        cmd += ["--zoom", str(zoom)]
    if tms_url_template:
        cmd += ["--tms-url-template", tms_url_template]
    if tms_user_agent:
        cmd += ["--tms-user-agent", tms_user_agent]

    if dry_run:
        print("[DRY] " + " ".join(cmd))
        return

    out_mp4.parent.mkdir(parents=True, exist_ok=True)
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            f"GPX Animator failed ({p.returncode}) for {gpx_path}:\nSTDOUT:\n{p.stdout.strip()}\n\nSTDERR:\n{p.stderr.strip()}"
        )
    if not out_mp4.exists():
        raise RuntimeError(f"GPX Animator reported success but did not create: {out_mp4}")


def compose_preview_ffmpeg(
    *,
    ffmpeg_exe: str,
    front_mp4: Path,
    rear_mp4: Path,
    out_mp4: Path,
    pip_w: int,
    pip_h: int,
    margin: int,
    matte: bool,
    matte_pad: int,
    matte_color: str,
    codec: str,
    crf: int,
    preset: str = "medium",
    nvenc_cq: int | None = None,
    nvenc_preset: str = "p7",
    nvenc_rc: str = "vbr_hq",
    nvenc_target_mbps: float | None = None,
    nvenc_maxrate_mult: float = 1.5,
    nvenc_bufsize_mult: float = 2.0,
    tag_hvc1: bool = True,
    audio_bitrate_kbps: int,
    map_mp4: Path | None = None,
    base_w: int | None = None,
    base_h: int | None = None,
    duration_s: float | None = None,
    dry_run: bool = False,
) -> None:
    """
    Compose a "preview" composite:
      - Front video is the base canvas (input 0).
      - Rear is PiP bottom-right (input 1), scaled to pip_w x pip_h.
      - Optional map video PiP bottom-left (input 2), scaled to pip_w x pip_h.

    IMPORTANT: This function must NOT extend duration. If duration_s is provided, output is trimmed to that.
    """
    vcodec_args = _simple_vcodec_args(
        codec=codec,
        crf=crf,
        preset=preset,
        nvenc_cq=nvenc_cq,
        nvenc_preset=nvenc_preset,
        nvenc_rc=nvenc_rc,
        nvenc_target_mbps=nvenc_target_mbps,
        nvenc_maxrate_mult=nvenc_maxrate_mult,
        nvenc_bufsize_mult=nvenc_bufsize_mult,
        tag_hvc1=tag_hvc1,
    )

    # Build positions (expressions evaluated by ffmpeg)
    if base_w is None or base_h is None:
        raise ValueError("compose_preview_ffmpeg requires base_w and base_h (base canvas dimensions)")

    if matte:
        outer_w = pip_w + (2 * matte_pad)
        outer_h = pip_h + (2 * matte_pad)

        rear_box_x = max(0, base_w - outer_w - margin)
        rear_box_y = max(0, base_h - outer_h - margin)

        rear_x = max(0, base_w - pip_w - margin - matte_pad)
        rear_y = max(0, base_h - pip_h - margin - matte_pad)

        map_box_x = margin
        map_box_y = max(0, base_h - outer_h - margin)

        map_x = margin + matte_pad
        map_y = max(0, base_h - pip_h - margin - matte_pad)
    else:
        outer_w = pip_w
        outer_h = pip_h

        rear_box_x = max(0, base_w - outer_w - margin)
        rear_box_y = max(0, base_h - outer_h - margin)

        rear_x = max(0, base_w - pip_w - margin)
        rear_y = max(0, base_h - pip_h - margin)

        map_box_x = margin
        map_box_y = max(0, base_h - outer_h - margin)

        map_x = margin
        map_y = max(0, base_h - pip_h - margin)

    # Rear chain: keep AR, then pad to exact pip size (transparent pad is fine; matte is drawn on base)
    rear_chain = (
        f"[1:v]setpts=PTS-STARTPTS,"
        f"scale={pip_w}:{pip_h}:force_original_aspect_ratio=decrease,"
        f"pad={pip_w}:{pip_h}:(ow-iw)/2:(oh-ih)/2:color=black@0"
        f"[rear];"
    )

    # Optional map chain
    map_chain = ""
    if map_mp4 is not None:
        map_chain = (
            f"[2:v]setpts=PTS-STARTPTS,"
            f"scale={pip_w}:{pip_h}:force_original_aspect_ratio=decrease,"
            f"pad={pip_w}:{pip_h}:(ow-iw)/2:(oh-ih)/2:color=black@0"
            f"[map];"
        )

    # Base / boxes
    base_chain = "[0:v]setpts=PTS-STARTPTS[base];"
    boxed_label = "base"
    if matte:
        # Draw matte rectangles on base where PiPs will go (behind the actual overlays).
        outer_w = pip_w + (2 * matte_pad)
        outer_h = pip_h + (2 * matte_pad)
        draw = (
            f"[base]"
            f"drawbox=x={rear_box_x}:y={rear_box_y}:w={outer_w}:h={outer_h}:color={matte_color}:t=fill"
        )
        if map_mp4 is not None:
            draw += (
                f",drawbox=x={map_box_x}:y={map_box_y}:w={outer_w}:h={outer_h}:color={matte_color}:t=fill"
            )
        draw += "[boxed];"
        base_chain += draw
        boxed_label = "boxed"

    # Overlay chain
    if map_mp4 is not None:
        overlay_chain = (
            f"[{boxed_label}][map]overlay=x={map_x}:y={map_y}:eof_action=pass[tmp1];"
            f"[tmp1][rear]overlay=x={rear_x}:y={rear_y}:eof_action=pass,format=yuv420p[vout]"
        )
    else:
        overlay_chain = (
            f"[{boxed_label}][rear]overlay=x={rear_x}:y={rear_y}:eof_action=pass,format=yuv420p[vout]"
        )

    filter_complex = base_chain + rear_chain + map_chain + overlay_chain

    cmd: list[str] = [ffmpeg_exe, "-hide_banner", "-y", "-i", str(front_mp4), "-i", str(rear_mp4)]
    if map_mp4 is not None:
        cmd += ["-i", str(map_mp4)]

    cmd += [
        "-filter_complex",
        filter_complex,
        "-map",
        "[vout]",
        "-map",
        "0:a:0?",
        *vcodec_args,
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        f"{audio_bitrate_kbps}k",
        "-ar",
        "48000",
        "-ac",
        "2",
        "-movflags",
        "+faststart",
    ]

    # Hard trim to base duration if we know it (prevents accidental long outputs).
    if duration_s is not None and duration_s > 0:
        cmd += ["-t", f"{duration_s:.3f}"]
    else:
        # Safety: if we couldn't determine duration, do not allow any stream to
        # accidentally extend the output (e.g., bogus container duration).
        cmd += ["-shortest"]

    cmd += [str(out_mp4)]

    run_cmd(cmd, dry_run=dry_run)

def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="BlackVue drive stitcher (ffmpeg + blackclue + NMEA stitching)")
    ap.add_argument("--source", required=True, type=Path, help="Source directory containing MP4 files")
    ap.add_argument("--output", required=True, type=Path, help="Output directory (Processed root)")
    ap.add_argument("--vehicle", default=None, help="Vehicle tag for naming (default: last folder name of --source)")

    ap.add_argument(
        "--output-layout",
        choices=["import", "legacy"],
        default="import",
        help=(
            "Output folder layout. "
            "import=<output>/<vehicle>/<drive_tag>_<view>/..., "
            "legacy=output/Drives + output/Work + output/Telemetry. "
            "Default: import"
        ),
    )
    ap.add_argument(
        "--manifest-filename",
        default="manifest.json",
        help="Manifest filename (supports {tag} and {view}). Default: manifest.json",
    )
    ap.add_argument("--drives-layout", choices=["flat", "date"], default="flat",
                    help="Where to place final MP4 outputs under output/Drives: flat=all in Drives/, date=Drives/YYYYMMDD/. Default: flat")
    ap.add_argument("--telemetry-date-source", choices=["filename", "nmea"], default="filename",
                    help="Telemetry date-folder scheme. filename=use clip timestamp (local-style), nmea=use GPS/NMEA UTC date when available. Default: filename")
    ap.add_argument("--recurse", action="store_true", help="Recurse into subfolders")
    ap.add_argument("--front-only", action="store_true", help="Front-only mode: process only front (F) clips; error if any rear (R) clip exists without a matching front")
    ap.add_argument("--exclude-dir", action="append", default=[], help="Exclude directory name (repeatable)")
    ap.add_argument("--gap-seconds", type=int, default=120, help="Gap threshold to split drives (seconds)")
    # short-drive filtering (garage shuffles, etc.)
    ap.add_argument("--min-drive-seconds", type=int, default=0,
                    help="Skip drives shorter than this many seconds (approx; 0=disabled)")
    ap.add_argument("--min-drive-clips", type=int, default=0,
                    help="Skip drives with fewer than this many timestamp clips (0=disabled)")
    ap.add_argument("--clip-seconds", type=int, default=60,
                    help="Assumed clip length (seconds) for estimating drive duration (default: 60)")

    # lightweight archive outputs (timelapse / still frames)
    ap.add_argument("--emit-timelapse", action="store_true",
                    help="Create per-drive timelapse MP4(s) using ffmpeg (does not require --run-ffmpeg)")
    ap.add_argument("--timelapse-every-seconds", type=float, default=1.0,
                    help="Sample one frame every N seconds for timelapse (default: 1.0)")
    ap.add_argument("--timelapse-playback-fps", type=int, default=30,
                    help="Playback FPS for timelapse output (default: 30)")
    ap.add_argument("--timelapse-width", type=int, default=1280,
                    help="Scale timelapse output to this width (0=keep source width). Default: 1280")
    ap.add_argument("--timelapse-view", choices=("front","rear","both"), default="front",
                    help="Which view(s) to generate timelapse for (default: front)")
    ap.add_argument("--timelapse-codec", choices=("x265","x264","hevc_nvenc","h264_nvenc","nvenc"), default="x265",
                    help="Timelapse encoder (default: x265). NVENC options require NVIDIA GPU + ffmpeg build with nvenc.")
    ap.add_argument("--timelapse-crf", type=int, default=28,
                    help="CRF for x264/x265 timelapse encode (default: 28; higher=smaller/worse)")
    ap.add_argument("--timelapse-preset", default="medium",
                    help="Preset for x264/x265 timelapse encode (default: medium)")
    ap.add_argument("--timelapse-nvenc-cq", type=int, default=None,
                    help="NVENC CQ for timelapse (lower=better). Default derives from --timelapse-crf.")
    ap.add_argument("--timelapse-nvenc-target-mbps", type=float, default=None,
                    help="NVENC target Mbps for timelapse (VBR_HQ). Overrides --timelapse-nvenc-cq if set.")

    ap.add_argument("--emit-stills", action="store_true",
                    help="Extract periodic still frames (screen captures) per drive using ffmpeg")
    ap.add_argument("--stills-every-seconds", type=float, default=30.0,
                    help="Capture one frame every N seconds (default: 30.0)")
    ap.add_argument("--stills-width", type=int, default=1920,
                    help="Scale still frames to this width (0=keep source width). Default: 1920")
    ap.add_argument("--stills-format", choices=("jpg","png"), default="jpg",
                    help="Output format for still frames (default: jpg)")
    ap.add_argument("--stills-quality", type=int, default=3,
                    help="JPEG quality for ffmpeg -q:v (2=best, 31=worst). Ignored for png. Default: 3")
    ap.add_argument("--stills-max", type=int, default=0,
                    help="Max still frames per drive (0=unlimited)")
    ap.add_argument("--stills-name", choices=("sequence","local","utc"), default="sequence",
                    help="Still filename scheme (default: sequence). local/utc rename frames using capture timestamps and write thumbs/index.csv.")
    ap.add_argument("--stills-index", action="store_true",
                    help="Write thumbs/index.csv mapping still filenames to capture timestamps (also enabled automatically when --stills-name is local/utc).")
    ap.add_argument("--stills-geojson", action="store_true",
                    help="Write artifacts/thumbs/index.geojson with a GPS point per still (requires stitched per-drive .nmea; best with --stills-name utc or --stills-index).")
    ap.add_argument("--stills-view", choices=("front","rear","both"), default="front",
                    help="Which view(s) to extract stills for (default: front)")


    # event extraction (derived from stitched NMEA; optional)
    ap.add_argument("--emit-events", action="store_true",
                    help="Detect simple driving events from stitched per-drive NMEA and write artifacts/events/index.csv + index.geojson")
    ap.add_argument("--emit-event-stills", action="store_true",
                    help="When used with --emit-events and a stitched video, extract a still at each event time into artifacts/events/stills/")
    ap.add_argument("--event-stills-format", choices=("jpg","png"), default="jpg",
                    help="Image format for event stills (default: jpg)")
    ap.add_argument("--event-stills-quality", type=int, default=3,
                    help="JPEG quality for event stills (ffmpeg qscale: 2-31; lower is better; default: 3)")

    ap.add_argument("--dry-run", action="store_true", help="Print actions without writing/execing")
    ap.add_argument("--verbose", action="store_true", help="Print external commands as they run")

    # ffmpeg / Resolve
    ap.add_argument("--run-ffmpeg", action="store_true", help="Run ffmpeg stitching")
    ap.add_argument("--ffmpeg", default="ffmpeg", help="ffmpeg executable or path")
    ap.add_argument("--ffmpeg-hwaccel", default="none", choices=("none","auto","cuda","dxva2","d3d11va","qsv","vaapi"),
                    help="Optional ffmpeg hardware decode acceleration for stills/timelapse (default: none). On Windows, auto is usually safest.")
    ap.add_argument("--for-resolve", action="store_true", help="Normalize audio for DaVinci Resolve import")
    ap.add_argument(
        "--stitched-dest",
        default="auto",
        choices=("auto", "video_root", "drive_root", "drive_artifacts"),
        help="Where to place stitched MP4s. auto=video_root for legacy layout, drive_artifacts for --output-layout import.",
    )

    ap.add_argument("--resolve-audio-bitrate-kbps", type=int, default=192)

    # blackclue / telemetry
    ap.add_argument("--run-blackclue", action="store_true", help="Run blackclue extraction")
    ap.add_argument("--blackclue", default="blackclue", help="blackclue executable or path")
    ap.add_argument("--blackclue-mode", choices=("complete", "raw", "both"), default="raw")
    ap.add_argument("--blackclue-view", choices=("front", "rear", "both"), default="front")
    ap.add_argument(
        "--blackclue-jobs",
        type=int,
        default=1,
        help=(
            "Parallel jobs for per-clip blackclue extraction (default: 1). "
            "Increase to speed up telemetry extraction."
        ),
    )
    ap.add_argument("--telemetry-sidecar-action", choices=("none", "copy", "move"), default="none",
                    help="What to do with blackclue-generated sidecars after extraction")
    ap.add_argument("--no-stitch-nmea", action="store_true", help="Do not stitch NMEA into per-drive files")

    # conversions
    ap.add_argument("--emit-csv", action="store_true", help="Convert stitched NMEA -> CSV per drive/view")
    ap.add_argument("--emit-gpx", action="store_true", help="Convert stitched NMEA -> GPX per drive/view")
    ap.add_argument("--emit-accel", action="store_true", help="Emit per-drive accelerometer CSV (requires blackclue)")
    ap.add_argument(
        "--emit-manifest",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Emit per-drive manifest JSON for database import (default: enabled). "
            "Disable with --no-emit-manifest."
        ),
    )
    ap.add_argument(
        "--manifest-notes",
        default="",
        help="Optional notes string to include in manifest.json (default: auto)",
    )

    ap.add_argument("--timezone", default="America/Chicago", help="Timezone name stored in manifest (IANA, e.g. America/Chicago).")
    ap.add_argument("--source-model", default=None, help="Dashcam model string for manifest (optional).")
    ap.add_argument("--source-serial", default=None, help="Dashcam serial for manifest (optional).")
    ap.add_argument("--source-firmware", default=None, help="Dashcam firmware string for manifest (optional).")
    _ms = ap.add_mutually_exclusive_group()
    _ms.add_argument("--manifest-sha256", dest="manifest_sha256", action="store_true", default=True,
                     help="Compute sha256 for telemetry files in manifest (default; may be slower).")
    _ms.add_argument("--no-manifest-sha256", dest="manifest_sha256", action="store_false",
                     help="Disable sha256 computation in manifest.")

    # map overlay rendering (GPX Animator)
    ap.add_argument("--run-map-render", action="store_true", help="Render map overlay video from GPX using GPX Animator (Java)")
    ap.add_argument("--java", default="java", help="Java executable (e.g., java.exe) for running GPX Animator")
    ap.add_argument("--gpx-animator-jar", default="", help="Path to gpx-animator.jar (required when --run-map-render)")
    ap.add_argument("--ffprobe", default="", help="Optional ffprobe executable. If omitted, will use ffprobe next to --ffmpeg, or 'ffprobe'")
    ap.add_argument("--map-view", choices=("front","rear"), default="front", help="Which view's telemetry to use for the map")
    ap.add_argument("--map-fps", type=int, default=15, help="FPS for rendered map video")
    ap.add_argument("--map-zoom", type=int, default=0, help="Optional fixed zoom level (0=auto)")
    ap.add_argument("--tms-url-template", default="", help="Optional tile server URL template for GPX Animator")
    ap.add_argument("--tms-user-agent", default="BlackVueDrivePipeline/1.0", help="User-Agent for tile downloads (OSM policy expects a descriptive UA)")

    # picture-in-picture composition preview (ffmpeg)
    ap.add_argument("--compose", dest="compose_preview", action="store_true", help="Compose final output: rear PiP bottom-right over front")
    ap.add_argument("--compose-preview", dest="compose_preview", action="store_true", help=argparse.SUPPRESS)  # backwards-compat
    ap.add_argument("--pip-scale", type=float, default=0.25, help="PiP scale relative to base width (ignored if --pip-size is provided)")
    ap.add_argument("--pip-size", default="", help="PiP size WxH (e.g. 640x360). Overrides --pip-scale")
    ap.add_argument("--pip-margin", type=int, default=40, help="Margin (px) from border for PiP boxes")
    ap.add_argument("--pip-matte", action="store_true", help="Draw a solid-color matte behind PiP boxes")
    ap.add_argument("--pip-matte-pad", type=int, default=10, help="Padding (px) around PiP inside the matte")
    ap.add_argument("--pip-matte-color", default="black@0.6", help="FFmpeg color for the matte (e.g. black@0.6)")
    ap.add_argument("--compose-codec", choices=("x265","x264","hevc_nvenc","h264_nvenc","nvenc"), default="x265", help="Compose encoder for composed preview output (base=front). Use x265/hevc_nvenc for HEVC; 'nvenc' is an alias for hevc_nvenc.")
    ap.add_argument("--compose-crf", type=int, default=22, help="CRF for x264/x265 preview composite; also used to derive default NVENC CQ if --nvenc-cq is not set.")
    ap.add_argument("--compose-preset", default="medium", help="Preset for libx264/libx265 (e.g., slow, medium, fast).")
    ap.add_argument("--nvenc-cq", type=int, default=None, help="NVENC constant-quality (lower=better). Default is a rough mapping from --compose-crf (CQ=CRF+6).")
    ap.add_argument("--nvenc-target-mbps", type=float, default=None, help="NVENC target average video bitrate (Mbps) using VBR_HQ. If set, overrides --nvenc-cq and produces more predictable file sizes.")
    ap.add_argument("--nvenc-match-file", default=None, help="Path to a reference file (e.g., an x265 output). If provided, script will compute its average bitrate via ffprobe and use that as --nvenc-target-mbps.")
    ap.add_argument("--nvenc-maxrate-mult", type=float, default=1.5, help="Multiplier for NVENC maxrate vs target bitrate (only when --nvenc-target-mbps or --nvenc-match-file is used).")
    ap.add_argument("--nvenc-bufsize-mult", type=float, default=2.0, help="Multiplier for NVENC bufsize vs target bitrate (only when --nvenc-target-mbps or --nvenc-match-file is used).")
    ap.add_argument("--nvenc-preset", default="p7", help="NVENC preset (e.g., p1..p7; higher=slower/better quality).")
    ap.add_argument("--nvenc-rc", default="vbr_hq", help="NVENC rate control (e.g., vbr, vbr_hq, cbr, constqp).")
    ap.add_argument("--no-hvc1-tag", action="store_true", help="Do not set -tag:v hvc1 for HEVC MP4 outputs (some players prefer it).")
    # optional limiting for testing
    ap.add_argument("--limit-drives", type=int, default=0, help="Process only the first N drives (0=all)")
    ap.add_argument("--skip-existing", action="store_true", help="Skip processing a drive/view if the manifest file already exists in the output folder (resume mode).")

    args = ap.parse_args(argv)

    if args.telemetry_date_source == "nmea" and not args.run_blackclue:
        raise SystemExit("--telemetry-date-source nmea requires --run-blackclue (otherwise there is no NMEA timestamp to derive date folders).")

    if args.emit_timelapse:
        if args.timelapse_every_seconds <= 0:
            raise SystemExit("--timelapse-every-seconds must be > 0")
        if args.timelapse_playback_fps <= 0:
            raise SystemExit("--timelapse-playback-fps must be > 0")

    if args.emit_stills:
        if args.stills_every_seconds <= 0:
            raise SystemExit("--stills-every-seconds must be > 0")

    if args.front_only:
        if args.emit_timelapse and args.timelapse_view in ("rear", "both"):
            raise SystemExit("--front-only: --timelapse-view must be 'front'")
        if args.emit_stills and args.stills_view in ("rear", "both"):
            raise SystemExit("--front-only: --stills-view must be 'front'")

    if args.output_layout == "import":
        # Import layout uses a per-view drive folder name, so require a single view.
        if args.run_blackclue and args.blackclue_view == "both":
            raise SystemExit("--output-layout import requires --blackclue-view front|rear (run twice for both).")
        if args.emit_timelapse and args.timelapse_view == "both":
            raise SystemExit("--output-layout import requires --timelapse-view front|rear (folder is view-specific).")
        if args.emit_stills and args.stills_view == "both":
            raise SystemExit("--output-layout import requires --stills-view front|rear (folder is view-specific).")

        # If multiple view knobs are in play, require them to match to avoid ambiguous folder naming.
        chosen_views = []
        if args.run_blackclue:
            chosen_views.append(args.blackclue_view)
        if args.emit_timelapse:
            chosen_views.append(args.timelapse_view)
        if args.emit_stills:
            chosen_views.append(args.stills_view)
        chosen_views = [v for v in chosen_views if v in ("front", "rear")]
        if chosen_views and len(set(chosen_views)) != 1:
            raise SystemExit(
                "--output-layout import: --blackclue-view / --timelapse-view / --stills-view must all match (front or rear)."
            )

    source = args.source.resolve()
    output = args.output.resolve()

    vehicle_tag = sanitize_tag(args.vehicle or source.name)

    if args.output_layout == "import":
        # Import layout: <output>/<vehicle>/<drive_tag>_<view>/...
        # Keep work/tmp outputs out of the import tree.
        drives_root = output / "_video" / vehicle_tag
        work_root = output / "_work" / vehicle_tag
        tele_root = output / vehicle_tag
        tele_tmp_root = work_root / "_tele_tmp"
    else:
        drives_root = output / "Drives"
        work_root = output / "Work"
        tele_root = output / "Telemetry"
        tele_tmp_root = tele_root / "_tmp"

    if not args.dry_run:
        # Only create the video output root if we might write stitched MP4s.
        if args.output_layout != "import" or args.run_ffmpeg or args.compose_preview:
            drives_root.mkdir(parents=True, exist_ok=True)
        work_root.mkdir(parents=True, exist_ok=True)
        tele_root.mkdir(parents=True, exist_ok=True)
        tele_tmp_root.mkdir(parents=True, exist_ok=True)
    # default exclusions: "Processed" plus anything user added
    exclude_dirs = set([d for d in args.exclude_dir if d])
    exclude_dirs.add("Processed")

    # IMPORTANT: do NOT exclude by output directory *name* globally.
    # That can accidentally exclude legitimate input folders that share a common name (e.g. "Interesting").
    # Instead, if the output path is inside the source path, exclude the output *path* precisely.
    exclude_paths: List[Path] = []
    if args.recurse:
        try:
            output.resolve().relative_to(source.resolve())
            exclude_paths.append(output.resolve())
        except Exception:
            pass

    pairs = scan_pairs(source, args.recurse, sorted(exclude_dirs), exclude_paths=exclude_paths)
    if not pairs:
        print("No matching MP4 clips found.")
        return 1

    # Validate data integrity: a rear clip without a matching front clip is almost always a partial copy/corruption.
    orphan_rears = [p.rear for p in pairs if p.rear is not None and p.front is None]
    if orphan_rears:
        sample = "\n".join(f"  - {p}" for p in orphan_rears[:10])
        more = "" if len(orphan_rears) <= 10 else f"\n  ... and {len(orphan_rears) - 10} more"
        raise SystemExit(f"Rear clip(s) found without matching front clip:\n{sample}{more}")

    if args.front_only:
        # Keep only pairs that have a front clip; ignore any paired rears entirely.
        pairs = [p for p in pairs if p.front is not None]

        if args.run_blackclue and args.blackclue_view in ("rear", "both"):
            raise SystemExit("--front-only: --blackclue-view must be 'front'")
        if args.run_map_render and args.map_view != "front":
            raise SystemExit("--front-only: --map-view must be 'front' (or omit --run-map-render)")
    drives = split_into_drives(pairs, args.gap_seconds)

    total_clips = sum((1 if p.front else 0) + (1 if p.rear else 0) for p in pairs)
    print(f"Found {total_clips} clips across {len(pairs)} timestamps. Split into {len(drives)} drive(s).")

    need_ffmpeg = bool(args.run_ffmpeg or args.compose_preview or args.emit_timelapse or args.emit_stills)
    ffmpeg_exe = which_or_fail(args.ffmpeg) if need_ffmpeg else None
    blackclue_exe = which_or_fail(args.blackclue) if args.run_blackclue else None

    # NVENC file-size matching: optional global target bitrate derived from a reference file.
    nvenc_target_mbps_global: float | None = args.nvenc_target_mbps
    nvenc_match_file = Path(args.nvenc_match_file).expanduser() if getattr(args, "nvenc_match_file", "") else None
    nvenc_match_derived = False

    max_drives = args.limit_drives if args.limit_drives and args.limit_drives > 0 else len(drives)

    for i, drive in enumerate(drives[:max_drives], start=1):
        drive_id = f"drive_{i:03d}"
        start_dt = drive[0].ts_dt
        end_dt = drive[-1].ts_dt
        approx_duration_s = (end_dt - start_dt).total_seconds() + float(args.clip_seconds)

        # Skip short drives (garage shuffle). Useful to avoid clutter in archives.
        if (args.min_drive_seconds and args.min_drive_seconds > 0 and approx_duration_s < float(args.min_drive_seconds)) or (
            args.min_drive_clips and args.min_drive_clips > 0 and len(drive) < int(args.min_drive_clips)
        ):
            why = []
            if args.min_drive_seconds and args.min_drive_seconds > 0:
                why.append(f"duration~{approx_duration_s:.0f}s<{int(args.min_drive_seconds)}s")
            if args.min_drive_clips and args.min_drive_clips > 0:
                why.append(f"clips={len(drive)}<{int(args.min_drive_clips)}")
            print(f"{drive_id} ({start_dt:%Y%m%d_%H%M%S}_{vehicle_tag}): skipped short drive ({', '.join(why)})")
            continue

        # Video output folders (legacy behavior). In import layout, these are under <output>/_video/<vehicle>/...
        date_dir = drives_root if args.drives_layout == "flat" else (drives_root / f"{start_dt:%Y%m%d}")
        work_date_root = work_root if args.drives_layout == "flat" else (work_root / f"{start_dt:%Y%m%d}")

        if args.output_layout == "import":
            tele_date_dir_guess = tele_root
            tele_proc_root = tele_root
            use_tele_tmp = False
        else:
            tele_date_dir_guess = tele_root if args.drives_layout == "flat" else (tele_root / f"{start_dt:%Y%m%d}")
            tele_tmp_root_date = tele_tmp_root if args.drives_layout == "flat" else (tele_tmp_root / f"{start_dt:%Y%m%d}")
            use_tele_tmp = (args.run_blackclue and (args.telemetry_date_source != "filename") and (args.drives_layout != "flat"))
            tele_proc_root = tele_tmp_root_date if use_tele_tmp else tele_date_dir_guess

        if not args.dry_run:
            # Work root is always created; date subdirs are optional.
            work_date_root.mkdir(parents=True, exist_ok=True)

            # Create telemetry roots.
            tele_proc_root.mkdir(parents=True, exist_ok=True)
            if (args.output_layout != "import") and use_tele_tmp:
                tele_date_dir_guess.mkdir(parents=True, exist_ok=True)

            # Only create the video dir if we may write stitched MP4s.
            if args.output_layout != "import" or args.run_ffmpeg or args.compose_preview:
                date_dir.mkdir(parents=True, exist_ok=True)

        base_tag = f"{start_dt:%Y%m%d_%H%M%S}_{vehicle_tag}"

        # In import layout, the drive folder includes the selected view suffix.
        primary_view = (
            args.blackclue_view if args.run_blackclue else
            (args.timelapse_view if args.emit_timelapse else
             (args.stills_view if args.emit_stills else "front"))
        )

        if args.output_layout == "import":
            tag = base_tag
            n = 1
            while True:
                drive_folder = tele_proc_root / f"{tag}_{primary_view}"
                work_folder = work_date_root / f"{tag}_{primary_view}"
                # (Optional) also avoid collisions with video outputs if they exist.
                vid_candidate = (date_dir / f"{tag}.mp4")
                if not (drive_folder.exists() or work_folder.exists() or vid_candidate.exists()):
                    break
                n += 1
                tag = f"{base_tag}_{n:02d}"
        else:
            tag = make_unique_drive_tag(base_tag, final_dir=date_dir, work_root=work_date_root, tele_root=tele_date_dir_guess)

        # Working/staging files (concat lists, stitched intermediates, etc.)
        work_dir = work_date_root / (f"{tag}_{primary_view}" if args.output_layout == "import" else tag)
        if not args.dry_run:
            work_dir.mkdir(parents=True, exist_ok=True)

        tele_dir = tele_proc_root / (f"{tag}_{primary_view}" if args.output_layout == "import" else tag)


        # Resume support: optionally skip drives already processed (manifest exists).
        if getattr(args, "skip_existing", False):
            tele_view_for_manifest = args.blackclue_view if args.run_blackclue else primary_view
            manifest_exists_path = tele_dir / render_manifest_filename(args.manifest_filename, tag=tag, view=tele_view_for_manifest)
            if manifest_exists_path.exists():
                print(f"{drive_id} ({tag}_{tele_view_for_manifest}): skipped (manifest exists)")
                continue
        tele_clips_dir = (tele_dir / "artifacts" / "clips") if args.output_layout == "import" else (tele_dir / "Clips")
        tele_start_ms = None  # earliest GPS epoch-ms seen in this drive (used for telemetry date folders)
        derived_params = dict(DEFAULT_DERIVED_PARAMS)

        front_paths: List[Path] = []
        rear_paths: List[Path] = []
        missing_front = 0
        missing_rear = 0

        for p in drive:
            if p.front:
                front_paths.append(p.front)
            else:
                missing_front += 1
            if p.rear:
                rear_paths.append(p.rear)
            else:
                missing_rear += 1

        rear_present = len(rear_paths)
        if args.front_only:
            # In front-only mode, ignore missing rear clips (expected) and do not process rear clips.
            missing_rear = 0
            print(f"{drive_id} ({tag}): front={len(front_paths)} (missing {missing_front}), rear=IGNORED ({rear_present} present)")
            rear_paths_effective: List[Path] = []
        else:
            print(f"{drive_id} ({tag}): front={len(front_paths)} (missing {missing_front}), rear={len(rear_paths)} (missing {missing_rear})")
            rear_paths_effective = rear_paths

        rear_paths = rear_paths_effective

        front_list = work_dir / f"{tag}_front.txt"
        rear_list = work_dir / f"{tag}_rear.txt"
        write_concat_list(front_list, front_paths, dry_run=args.dry_run)
        write_concat_list(rear_list, rear_paths, dry_run=args.dry_run)

        front_nmea_files: List[Path] = []
        rear_nmea_files: List[Path] = []

        if args.run_blackclue:
            if args.dry_run:
                print(f"[DRY] Would create telemetry dir: {tele_dir}")
                print(f"[DRY] Would create telemetry clips dir: {tele_clips_dir}")
            else:
                tele_dir.mkdir(parents=True, exist_ok=True)
                tele_clips_dir.mkdir(parents=True, exist_ok=True)

            def process_view(paths: Sequence[Path]) -> List[Path]:
                modes = ["complete", "raw"] if args.blackclue_mode == "both" else [args.blackclue_mode]

                def one_clip(mp4: Path) -> Optional[Path]:
                    for m in modes:
                        run_blackclue(blackclue_exe, m, mp4, dry_run=args.dry_run, verbose=args.verbose)

                    nmea_src = mp4.with_suffix(".nmea")
                    sidecars = collect_sidecars(mp4)
                    if sidecars:
                        copy_or_move_files(
                            sidecars,
                            dest_dir=tele_clips_dir,
                            action=args.telemetry_sidecar_action,
                            dry_run=args.dry_run,
                        )

                    if args.telemetry_sidecar_action != "none":
                        nmea_use = tele_clips_dir / nmea_src.name
                    else:
                        nmea_use = nmea_src
                    return nmea_use if nmea_use.exists() else None

                nmea_list: List[Path] = []
                jobs = max(1, int(getattr(args, "blackclue_jobs", 1) or 1))
                if jobs == 1 or len(paths) <= 1:
                    for mp4 in paths:
                        out = one_clip(mp4)
                        if out is not None:
                            nmea_list.append(out)
                    return nmea_list

                # Parallelize blackclue invocation per clip. This is I/O + subprocess bound, so threads work well.
                with ThreadPoolExecutor(max_workers=jobs) as ex:
                    futs = {ex.submit(one_clip, mp4): mp4 for mp4 in paths}
                    for fut in as_completed(futs):
                        res = fut.result()  # propagate any exception
                        if res is not None:
                            nmea_list.append(res)
                # Keep outputs deterministic-ish (by name) for downstream stitching.
                nmea_list.sort(key=lambda p: p.name)
                return nmea_list


            if args.blackclue_view in ("front", "both"):
                front_nmea_files = process_view(front_paths)
            if args.blackclue_view in ("rear", "both"):
                rear_nmea_files = process_view(rear_paths)

        stitched_front: Optional[Path] = None
        stitched_rear: Optional[Path] = None

        if args.run_blackclue and not args.no_stitch_nmea:
            if front_nmea_files:
                stitched_front = tele_dir / f"{tag}_front.nmea"
                stitch_nmea(front_nmea_files, stitched_front, dry_run=args.dry_run)
            if rear_nmea_files:
                stitched_rear = tele_dir / f"{tag}_rear.nmea"
                stitch_nmea(rear_nmea_files, stitched_rear, dry_run=args.dry_run)

        if args.emit_csv or args.emit_gpx or args.telemetry_date_source == "nmea":
            def convert_one(stitched: Optional[Path], view: str) -> None:
                nonlocal tele_start_ms
                if not stitched or args.dry_run:
                    return
                txt = stitched.read_text(encoding="utf-8", errors="replace")
                recs = nmea_to_records(txt)
                if args.telemetry_date_source == "nmea" and recs:
                    ms0 = recs[0].get("ms")
                    if isinstance(ms0, int):
                        tele_start_ms = ms0 if tele_start_ms is None else min(tele_start_ms, ms0)
                if args.emit_csv:
                    write_csv(recs, stitched.with_suffix(".csv"), dry_run=args.dry_run)
                if args.emit_gpx:
                    write_gpx(recs, stitched.with_suffix(".gpx"), dry_run=args.dry_run, name=f"{tag}_{view}")
            convert_one(stitched_front, "front")
            convert_one(stitched_rear, "rear")

            # Optional: build drive-level accelerometer timeline from .3gf.txt sidecars.
            # This does NOT attempt to interpret units (g vs counts); it preserves raw integer axes.
            if (args.emit_accel or args.emit_csv) and (not args.dry_run):
                try:
                    if args.blackclue_view in ("front", "both") and front_paths:
                        _, accel = build_drive_accel_samples(front_paths, tele_clips_dir)
                        if accel:
                            out_accel = tele_dir / f"{tag}_front_accel.csv"
                            write_accel_csv(out_accel, accel)
                    if args.blackclue_view in ("rear", "both") and rear_paths:
                        _, accel = build_drive_accel_samples(rear_paths, tele_clips_dir)
                        if accel:
                            out_accel = tele_dir / f"{tag}_rear_accel.csv"
                            write_accel_csv(out_accel, accel)
                except Exception as e:
                    print(f"  [WARN] accel export failed for {tag}: {e}")

        front_out: Optional[Path] = None
        rear_out: Optional[Path] = None

        if args.run_ffmpeg:
            # Determine where stitched MP4s should live.
            stitched_dest = args.stitched_dest
            if stitched_dest == "auto":
                stitched_dest = "drive_artifacts" if args.output_layout == "import" else "video_root"

            if args.output_layout == "import" and stitched_dest in ("drive_artifacts", "drive_root"):
                out_dir = (tele_dir / "artifacts") if stitched_dest == "drive_artifacts" else tele_dir
                if not args.dry_run:
                    out_dir.mkdir(parents=True, exist_ok=True)
                front_name = "front_edit.mp4" if args.for_resolve else "front.mp4"
                rear_name = "rear.mp4"  # rear is no-audio by design
                front_out = out_dir / front_name
                rear_out = out_dir / rear_name
            else:
                # Legacy/video-root behavior (stitched MP4s live under <output>/Drives or <output>/_video)
                front_out = date_dir / (f"{tag}_front_edit.mp4" if args.for_resolve else f"{tag}_front.mp4")
                rear_out = date_dir / f"{tag}_rear_noaudio.mp4"

            print(f"  ffmpeg front -> {front_out.name}")
            run_ffmpeg_concat(
                ffmpeg=ffmpeg_exe,
                list_file=front_list,
                out_file=front_out,
                keep_audio=True,
                for_resolve=args.for_resolve,
                dry_run=args.dry_run,
                resolve_audio_bitrate_kbps=args.resolve_audio_bitrate_kbps,
                verbose=args.verbose,
            )

            if rear_paths:
                print(f"  ffmpeg rear  -> {rear_out.name}")
                run_ffmpeg_concat(
                    ffmpeg=ffmpeg_exe,
                    list_file=rear_list,
                    out_file=rear_out,
                    keep_audio=False,
                    for_resolve=args.for_resolve,
                    dry_run=args.dry_run,
                    resolve_audio_bitrate_kbps=args.resolve_audio_bitrate_kbps,
                    verbose=args.verbose,
                )
            else:
                print("  ffmpeg rear  -> (skipped; no rear clips found for this drive)")


        # Timelapse and/or periodic still frames for lightweight archiving.
        if args.emit_timelapse or args.emit_stills:
            if args.output_layout == "import":
                artifacts_dir = tele_dir / "artifacts"
                thumbs_dir = artifacts_dir / "thumbs"
                if args.dry_run:
                    print(f"[DRY] Would create artifacts dir: {artifacts_dir}")
                else:
                    artifacts_dir.mkdir(parents=True, exist_ok=True)
                    thumbs_dir.mkdir(parents=True, exist_ok=True)
            else:
                if args.dry_run:
                    print(f"[DRY] Would create preview dir: {tele_dir / 'preview'}")
                else:
                    tele_dir.mkdir(parents=True, exist_ok=True)
                    (tele_dir / "preview").mkdir(parents=True, exist_ok=True)
                preview_dir = tele_dir / "preview"

            prefer_front = front_out if (front_out is not None and front_out.exists()) else None
            prefer_rear = rear_out if (rear_out is not None and rear_out.exists()) else None

            if args.emit_timelapse:
                itag = _interval_tag(args.timelapse_every_seconds)

                def do_timelapse(view: str) -> None:
                    if view == "rear" and (args.front_only or not rear_paths):
                        return
                    list_file = front_list if view == "front" else rear_list
                    prefer_mp4 = prefer_front if view == "front" else prefer_rear

                    if args.output_layout == "import":
                        # Folder is view-specific; use a simple stable filename.
                        out_name = "preview_1fps.mp4" if float(args.timelapse_every_seconds) == 1.0 else f"preview_{itag}s.mp4"
                        out_mp4 = (tele_dir / "artifacts" / out_name)
                    else:
                        out_mp4 = preview_dir / f"{view}_{itag}s.mp4"

                    print(f"  timelapse {view} -> {out_mp4.relative_to(output)}")
                    run_ffmpeg_timelapse(
                        ffmpeg_exe=ffmpeg_exe,
                        hwaccel=str(args.ffmpeg_hwaccel),
                        prefer_mp4=prefer_mp4,
                        list_file=list_file,
                        out_mp4=out_mp4,
                        every_seconds=float(args.timelapse_every_seconds),
                        playback_fps=int(args.timelapse_playback_fps),
                        width=int(args.timelapse_width),
                        codec=str(args.timelapse_codec),
                        crf=int(args.timelapse_crf),
                        preset=str(args.timelapse_preset),
                        nvenc_cq=args.timelapse_nvenc_cq,
                        nvenc_target_mbps=args.timelapse_nvenc_target_mbps,
                        nvenc_preset=str(args.nvenc_preset),
                        nvenc_rc=str(args.nvenc_rc),
                        nvenc_maxrate_mult=float(args.nvenc_maxrate_mult),
                        nvenc_bufsize_mult=float(args.nvenc_bufsize_mult),
                        tag_hvc1=(not args.no_hvc1_tag),
                        dry_run=args.dry_run,
                        verbose=args.verbose,
                    )

                if args.timelapse_view in ("front", "both"):
                    do_timelapse("front")
                if args.timelapse_view in ("rear", "both"):
                    do_timelapse("rear")

            if args.emit_stills:
                itag = _interval_tag(args.stills_every_seconds)

                def do_stills(view: str) -> None:
                    if view == "rear" and (args.front_only or not rear_paths):
                        return
                    list_file = front_list if view == "front" else rear_list
                    prefer_mp4 = prefer_front if view == "front" else prefer_rear

                    if args.output_layout == "import":
                        out_dir = tele_dir / "artifacts" / "thumbs"
                    else:
                        out_dir = preview_dir / f"{view}_frames_{itag}s"

                    print(f"  stills {view} -> {out_dir.relative_to(output)}")
                    run_ffmpeg_stills(
                        ffmpeg_exe=ffmpeg_exe,
                        hwaccel=str(args.ffmpeg_hwaccel),
                        prefer_mp4=prefer_mp4,
                        list_file=list_file,
                        out_dir=out_dir,
                        every_seconds=float(args.stills_every_seconds),
                        width=int(args.stills_width),
                        fmt=str(args.stills_format),
                        jpg_quality=int(args.stills_quality),
                        max_frames=int(args.stills_max),
                        dry_run=args.dry_run,
                        verbose=args.verbose,
                    )


                    # Optionally rename stills with timestamps and write an index.csv for later synchronization.
                    name_mode = str(getattr(args, "stills_name", "sequence"))
                    write_index = bool(getattr(args, "stills_index", False)) or (name_mode.lower() in ("local", "utc"))
                    if (name_mode.lower() != "sequence") or write_index:
                        paths_for_view = front_paths if view == "front" else rear_paths
                        # Stills timestamps must use the SAME absolute-time anchor as the manifest (telemetry source_channel = front).
                        # We intentionally prefer the primary_view (typically "front") GNSS/NMEA series even when extracting stills from rear video.
                        gnss_csv_path = tele_dir / f"{tag}_{primary_view}.csv"
                        nmea_anchor_path = tele_dir / f"{tag}_{primary_view}.nmea"

                        # Compute video t=0 UTC anchor for stills GPS synchronization.
                        # This MUST match the manifest's session_time.start_ts_utc.
                        _stills_session_start = _compute_video_start_utc(
                            gnss_csv_path=gnss_csv_path,
                            nmea_path=nmea_anchor_path,
                        )

                        rename_stills_with_timestamps(
                            thumbs_dir=out_dir,
                            paths=list(paths_for_view),
                            every_seconds=float(args.stills_every_seconds),
                            tz_name=str(args.timezone),
                            clip_seconds=float(args.clip_seconds),
                            name_mode=name_mode,
                            write_index=write_index,
                            nmea_path=nmea_anchor_path,
                            gnss_csv_path=gnss_csv_path,
                            session_start_ts_utc=_stills_session_start,
                            write_geojson=bool(getattr(args, "stills_geojson", False)),
                            dry_run=args.dry_run,
                        )

                if args.stills_view in ("front", "both"):
                    do_stills("front")
                if args.stills_view in ("rear", "both"):
                    do_stills("rear")


        # Optional: derive driving events from stitched NMEA and optionally extract stills at event instants.
        if args.emit_events:
            ev_nmea = tele_dir / f"{tag}_{primary_view}.nmea"
            if not ev_nmea.exists():
                print("  [WARN] --emit-events requested but no stitched .nmea found for this drive.")
            else:
                try:
                    ev_records = nmea_to_records(ev_nmea.read_text(encoding="utf-8", errors="replace"))
                    
                    video_start_ts_utc = None
                    try:
                        gnss_candidate = tele_dir / f"{tag}_{primary_view}.csv"
                        if gnss_candidate.exists():
                            gi = _analyze_gnss_csv(gnss_candidate)
                            a_iso = gi.get("gnss_anchor_ts_utc")
                            a_t = gi.get("gnss_anchor_t_rel_s")
                            if a_iso and a_t is not None:
                                adt = _parse_iso_z_dt(str(a_iso))
                                if adt is not None:
                                    vs = adt.astimezone(_dt.timezone.utc) - _dt.timedelta(seconds=float(a_t))
                                    video_start_ts_utc = vs.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                    except Exception:
                        video_start_ts_utc = None

                    events = derive_events_from_nmea_records(ev_records, derived_params, video_start_ts_utc=video_start_ts_utc)
                except Exception as e:
                    print(f"  [WARN] event extraction failed for {tag}: {e}")
                    events = []

                if events:
                    ev_dir = (tele_dir / "artifacts" / "events") if args.output_layout == "import" else (tele_dir / "Events")
                    if args.dry_run:
                        print(f"[DRY] Would write events -> {ev_dir}")
                    else:
                        write_events_index(events, ev_dir, dry_run=args.dry_run)
                        print(f"  events -> artifacts/events ({len(events)} found)")
                        if args.emit_event_stills:
                            if front_out is None or not Path(front_out).exists():
                                print("  [WARN] --emit-event-stills requested but no stitched front video is available (enable --run-ffmpeg).")
                            else:
                                extract_event_stills(
                                    ffmpeg_exe=ffmpeg_exe,
                                    video_path=Path(front_out),
                                    events=events,
                                    out_dir=(ev_dir / "stills"),
                                    fmt=args.event_stills_format,
                                    quality=int(args.event_stills_quality),
                                    dry_run=args.dry_run,
                                    verbose=args.verbose,
                                )
                else:
                    print("  events -> (none)")

# optional map rendering (GPX Animator) and/or composite preview
        if args.run_map_render or args.compose_preview:
            ffprobe_exe = _resolve_ffprobe(args.ffprobe, ffmpeg_exe)

            # Determine base video for probing size/duration.
            probe_path = front_out if (args.run_ffmpeg and (not args.dry_run) and front_out.exists()) else front_paths[0]
            try:
                base_info = ffprobe_video_info(ffprobe_exe, probe_path)
            except Exception:
                if args.dry_run:
                    # Best-effort fallback for printing commands in dry-run mode.
                    base_info = VideoInfo(width=2560, height=1440, duration_s=0.0)
                else:
                    raise

            if args.pip_size:
                pip_w, pip_h = _parse_wxh(args.pip_size)
            else:
                pip_w = _ensure_even(int(base_info.width * args.pip_scale))
                pip_h = _ensure_even(int(base_info.height * args.pip_scale))
            pip_w = max(2, pip_w)
            pip_h = max(2, pip_h)

            # Compute target duration (ms) for map render.
            target_ms = 0
            if base_info.duration_s and base_info.duration_s > 0:
                target_ms = int(round(base_info.duration_s * 1000))

            map_mp4 = tele_dir / f"{tag}_map.mp4"

            if args.run_map_render:
                map_view = args.map_view
                gpx_path = tele_dir / f"{tag}_{map_view}_track.gpx"

                # Prefer the stitched NMEA created earlier in the pipeline (Telemetry\YYYYMMDD\<tag>\<tag>_front.nmea / <tag>_rear.nmea)
                stitched_nmea = tele_dir / f"{tag}_{map_view}.nmea"

                if not stitched_nmea.exists():
                    # Try to stitch from sidecar .nmea files next to clips (produced by BlackClue -c).
                    view_clips = front_paths if map_view == "front" else rear_paths
                    nmea_files = [c.with_suffix(".nmea") for c in view_clips if c.with_suffix(".nmea").exists()]
                    if nmea_files:
                        stitch_nmea(nmea_files, stitched_nmea, dry_run=args.dry_run)
                    else:
                        raise RuntimeError(
                            f"No NMEA sidecars found for {tag} ({map_view}). "
                            "Run with --run-blackclue --blackclue-mode complete first."
                        )

                # Ensure GPX exists
                if not gpx_path.exists():
                    nmea_text = stitched_nmea.read_text(encoding="utf-8", errors="ignore")
                    recs = nmea_to_records(nmea_text)
                    if not recs:
                        raise RuntimeError(f"No usable GPS records found in {stitched_nmea}")
                    write_gpx(recs, gpx_path, dry_run=args.dry_run, name=f"{tag}_map")
                    if target_ms <= 0:
                        target_ms = int(recs[-1].ms - recs[0].ms)

                if target_ms <= 0:
                    # Fallback to stitched NMEA timestamps
                    nmea_text = stitched_nmea.read_text(encoding="utf-8", errors="ignore")
                    recs = nmea_to_records(nmea_text)
                    if recs:
                        target_ms = int(recs[-1].ms - recs[0].ms)

                if target_ms <= 0:
                    raise RuntimeError(
                        "Unable to determine target duration for map render. "
                        "Ensure ffprobe is available or stitched NMEA exists."
                    )

                render_map_gpx_animator(
                    java_exe=args.java,
                    jar_path=args.gpx_animator_jar,
                    gpx_path=gpx_path,
                    out_mp4=map_mp4,
                    width=pip_w,
                    height=pip_h,
                    fps=args.map_fps,
                    total_time_ms=target_ms,
                    zoom=args.map_zoom,
                    tms_url_template=args.tms_url_template,
                    tms_user_agent=args.tms_user_agent,
                    dry_run=args.dry_run,
                )
                print(f"  map render    -> {map_mp4.relative_to(output)}")

            if args.compose_preview:
                if not args.run_ffmpeg:
                    raise RuntimeError("--compose-preview requires --run-ffmpeg (to create base/rear videos).")
                # Only require a rendered map if the user explicitly requested map rendering.
                if args.run_map_render and (not args.dry_run) and (not map_mp4.exists()):
                    raise RuntimeError(f"Map video not found for compose: {map_mp4} (run with --run-map-render).")

                final_out = (date_dir / f"{tag}.mp4")

                if not args.dry_run:
                    date_dir.mkdir(parents=True, exist_ok=True)

                # Optional: derive a global NVENC target bitrate from a reference output file.
                codec_norm = (args.compose_codec or "").strip().lower()
                nvenc_target_mbps = nvenc_target_mbps_global
                if nvenc_target_mbps is None and nvenc_match_file is not None and codec_norm in ("hevc_nvenc", "nvenc", "nvenc_hevc", "h264_nvenc"):
                    if not nvenc_match_file.exists():
                        raise RuntimeError(f"--nvenc-match-file does not exist: {nvenc_match_file}")
                    if ffprobe_exe is None:
                        raise RuntimeError("--nvenc-match-file requires --ffprobe")
                    ref_info = ffprobe_video_info(ffprobe_exe, nvenc_match_file)
                    ref_bytes = nvenc_match_file.stat().st_size
                    nvenc_target_mbps = (ref_bytes * 8.0) / max(0.001, ref_info.duration_s) / 1e6
                    nvenc_target_mbps_global = nvenc_target_mbps
                if rear_out is not None and rear_out.exists():
                    # compose_preview_ffmpeg expects explicit PIP dimensions and a duration cap.
                    compose_preview_ffmpeg(
                        ffmpeg_exe=ffmpeg_exe,
                        front_mp4=front_out,
                        rear_mp4=rear_out,
                        out_mp4=final_out,
                        duration_s=base_info.duration_s,
                        pip_w=pip_w,
                        pip_h=pip_h,
                        base_w=base_info.width,
                        base_h=base_info.height,
                        margin=args.pip_margin,
                        matte=args.pip_matte,
                        matte_pad=args.pip_matte_pad,
                        matte_color=args.pip_matte_color,
                        codec=args.compose_codec,
                        crf=args.compose_crf,
                        preset=args.compose_preset,
                        nvenc_target_mbps=nvenc_target_mbps,
                        audio_bitrate_kbps=args.resolve_audio_bitrate_kbps,
                        dry_run=args.dry_run,
                    )
                else:
                    # Front-only drive: no PiP. Create a preview by copying the stitched front.
                    if args.dry_run:
                        print(f"[DRY] Would create front-only final: {final_out}")
                    else:
                        shutil.copy2(front_out, final_out)
                print(f"  compose -> {final_out.relative_to(output)}")


        # Optionally re-home Telemetry/YYYYMMDD based on the actual GPS/NMEA start time (UTC).
        if (args.output_layout != "import") and args.run_blackclue and args.telemetry_date_source == "nmea" and tele_start_ms is not None:
            tele_ymd = _dt.datetime.fromtimestamp(tele_start_ms / 1000.0, tz=_dt.timezone.utc).strftime("%Y%m%d")
            tele_date_dir_final = tele_root / tele_ymd
            final_tele_dir = tele_date_dir_final / tag
            if tele_dir != final_tele_dir:
                if not args.dry_run:
                    tele_date_dir_final.mkdir(parents=True, exist_ok=True)
                    if final_tele_dir.exists():
                        suffix = 2
                        while (tele_date_dir_final / f"{tag}_{suffix}").exists():
                            suffix += 1
                        final_tele_dir = tele_date_dir_final / f"{tag}_{suffix}"
                    shutil.move(str(tele_dir), str(final_tele_dir))
                tele_dir = final_tele_dir

        # Emit a per-drive manifest.json (portable, paths are relative to the manifest file).
        if args.run_blackclue and args.emit_manifest:

            tele_view = args.blackclue_view
            manifest_path = tele_dir / render_manifest_filename(args.manifest_filename, tag=tag, view=tele_view)

            # Include telemetry files in the manifest whenever they exist on disk, even if the current
            # invocation did not emit them (dashcam-db contract requirement).
            gnss_csv_candidate = tele_dir / f"{tag}_{tele_view}.csv"
            gnss_csv = gnss_csv_candidate if gnss_csv_candidate.exists() else None

            accel_csv_candidate = tele_dir / f"{tag}_{tele_view}_accel.csv"
            accel_csv = accel_csv_candidate if accel_csv_candidate.exists() else None

            nmea_candidate = tele_dir / f"{tag}_{tele_view}.nmea"
            nmea_path = nmea_candidate if nmea_candidate.exists() else None

            # Build clip entries. We include rear clips too, but timestamps may be null
            # if we did not run blackclue for that channel.
            clip_entries: List[Dict[str, object]] = []
            for ch, paths in (("front", front_paths), ("rear", rear_paths)):
                for mp4 in paths:
                    if args.telemetry_sidecar_action != "none":
                        sidecar_nmea = tele_clips_dir / f"{mp4.stem}.nmea"
                    else:
                        sidecar_nmea = mp4.with_suffix(".nmea")
                    start_iso, end_iso = _parse_clip_bounds_from_sidecar(sidecar_nmea)
                    clip_entries.append(
                        {
                            "channel": ch,
                            "clip_name": mp4.stem,
                            "start_ts_utc": start_iso,
                            "end_ts_utc": end_iso,
                            "video_path": None,
                        }
                    )

            notes = args.manifest_notes.strip() if args.manifest_notes.strip() else f"import {tag}_{tele_view}"
            if args.dry_run:
                print(f"[DRY] Would write manifest -> {manifest_path.relative_to(output)}")
            else:
                write_manifest_json(
                    manifest_path=manifest_path,
                    drive_tag=tag,
                    vehicle_tag=vehicle_tag,
                    notes=notes,
                    telemetry_gnss_csv=gnss_csv,
                    telemetry_accel_csv=accel_csv,
                    clip_entries=clip_entries,
                    derived_params=derived_params,
                    telemetry_nmea=nmea_path,
                    timezone=args.timezone,
                    source_model=args.source_model,
                    source_serial=args.source_serial,
                    source_firmware=args.source_firmware,
                    camera_front_enabled=True,
                    camera_rear_enabled=(not args.front_only and len(rear_paths) > 0),
                    include_sha256=args.manifest_sha256,
                    clip_seconds=float(args.clip_seconds),
                    stills_view=args.stills_view,
                    stills_every_seconds=float(args.stills_every_seconds),
                    dry_run=args.dry_run,
                )
                print(f"  manifest -> {manifest_path.relative_to(output)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
