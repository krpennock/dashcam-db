import os
import json
import sys
import shutil
import csv
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, timedelta

import psycopg


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


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
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: ingest_manifest.py /import/<folder>/<manifest.json>")

    manifest_path = Path(sys.argv[1]).resolve()
    if not manifest_path.exists():
        raise SystemExit(f"Manifest not found: {manifest_path}")

    m = json.loads(manifest_path.read_text(encoding="utf-8"))

    sid = m.get("drive_session_id")
    if not sid:
        raise SystemExit("Manifest missing drive_session_id")

    vehicle_tag = m.get("vehicle_tag")
    notes = m.get("notes")
    derived_params = m.get("derived_params")

    base = manifest_path.parent

    telem = m.get("telemetry", {}) or {}
    # v2 style
    gnss_rel = _get_path(telem, "gnss", "path")
    accel_rel = _get_path(telem, "accel", "path")

    # legacy style fallback
    if not gnss_rel and isinstance(telem, dict):
        gnss_rel = telem.get("gnss_csv")
    if not accel_rel and isinstance(telem, dict):
        accel_rel = telem.get("accel_csv")

    if not gnss_rel:
        raise SystemExit("Manifest missing GNSS CSV (expected telemetry.gnss.path or telemetry.gnss_csv)")
    if not accel_rel:
        raise SystemExit("Manifest missing ACCEL CSV (expected telemetry.accel.path or telemetry.accel_csv)")

    gnss_path = (base / gnss_rel).resolve()
    accel_path = (base / accel_rel).resolve()

    if not gnss_path.exists():
        raise SystemExit(f"GNSS CSV not found: {gnss_path}")
    if not accel_path.exists():
        raise SystemExit(f"Accel CSV not found: {accel_path}")

    # Session times (optional)
    start_ts_manifest = _parse_ts(_get_path(m, "session_time", "start_ts_utc"))
    end_ts_manifest = _parse_ts(_get_path(m, "session_time", "end_ts_utc"))

    clips = m.get("clips", []) or []

    conn = psycopg.connect(
        host=env("DB_HOST", "db"),
        port=env("DB_PORT", "5432"),
        dbname=env("DB_NAME", "dashcam"),
        user=env("DB_USER", "dashcam"),
        password=env("DB_PASSWORD", "change_me_now"),
        autocommit=False,
    )

    with conn:
        with conn.cursor() as cur:
            # Idempotent replace for this session
            cur.execute("DELETE FROM dashcam.storyboard_frame WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.derived_event WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.drive_summary  WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.accel_sample   WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.gnss_sample    WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.clip           WHERE drive_session_id = %s::uuid;", (sid,))

            # Upsert drive_session (we will normalize start/end after GNSS staging)
            cur.execute(
                """
                INSERT INTO dashcam.drive_session (drive_session_id, vehicle_tag, notes, start_ts_utc, end_ts_utc)
                VALUES (%s::uuid, %s, %s, %s, %s)
                ON CONFLICT (drive_session_id)
                DO UPDATE SET vehicle_tag = EXCLUDED.vehicle_tag,
                              notes = EXCLUDED.notes;
                """,
                (sid, vehicle_tag, notes, start_ts_manifest, end_ts_manifest),
            )

            # Insert clips
            if clips:
                cur.executemany(
                    """
                    INSERT INTO dashcam.clip (drive_session_id, channel, clip_name, start_ts_utc, end_ts_utc, video_path)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s);
                    """,
                    [
                        (
                            sid,
                            c.get("channel"),
                            c.get("clip_name"),
                            _parse_ts(c.get("start_ts_utc")) if isinstance(c.get("start_ts_utc"), str) else c.get("start_ts_utc"),
                            _parse_ts(c.get("end_ts_utc")) if isinstance(c.get("end_ts_utc"), str) else c.get("end_ts_utc"),
                            (c.get("video") or {}).get("path") if isinstance(c.get("video"), dict) else c.get("video_path"),
                        )
                        for c in clips
                    ],
                )

            # --- GNSS stage load ---
            cur.execute("TRUNCATE dashcam.gnss_csv_stage;")
            with gnss_path.open("rb") as f:
                with cur.copy("COPY dashcam.gnss_csv_stage FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

            # Canonical session anchor:
            # Prefer GNSS min utc_time; fallback to manifest start.
            cur.execute(
                """
                SELECT
                  min(NULLIF(utc_time,'')::timestamptz) AS min_utc,
                  max(t_rel_s) AS max_rel_s
                FROM dashcam.gnss_csv_stage;
                """
            )
            min_utc, max_rel_s = cur.fetchone()

            start_anchor = min_utc or start_ts_manifest
            if start_anchor is None:
                raise SystemExit("Unable to determine session start_ts_utc (no GNSS utc_time and no manifest session_time.start_ts_utc)")

            if start_ts_manifest and min_utc:
                delta = abs((start_ts_manifest - min_utc).total_seconds())
                if delta > 2.0:
                    print(
                        f"WARNING: manifest start_ts_utc differs from GNSS min utc_time by {delta:.3f}s; using GNSS min utc_time",
                        file=sys.stderr,
                    )

            end_anchor = None
            if max_rel_s is not None:
                end_anchor = start_anchor + timedelta(seconds=float(max_rel_s))
            else:
                end_anchor = end_ts_manifest

            cur.execute(
                """
                UPDATE dashcam.drive_session
                SET start_ts_utc = %s::timestamptz,
                    end_ts_utc   = %s::timestamptz
                WHERE drive_session_id = %s::uuid;
                """,
                (start_anchor, end_anchor, sid),
            )

            # --- GNSS insert ---
            # Canonical ts_utc computed from start + t_rel_s (NOT device utc_time)
            cur.execute(
                """
                INSERT INTO dashcam.gnss_sample (
                  drive_session_id,
                  device_ms,
                  t_rel_s,
                  ts_utc,
                  lat, lon,
                  speed_knots, speed_mps, speed_mph,
                  course_deg,
                  rmc_status,
                  fix_quality, satellites, hdop,
                  alt_m, geoid_sep_m,
                  date_ddmmyy, time_hhmmss
                )
                SELECT
                  %s::uuid,
                  s.ms,
                  s.t_rel_s,
                  (SELECT ds.start_ts_utc FROM dashcam.drive_session ds WHERE ds.drive_session_id = %s::uuid)
                    + (s.t_rel_s * interval '1 second'),
                  s.lat, s.lon,
                  s.speed_knots, s.speed_mps, s.speed_mph,
                  s.course_deg,
                  NULLIF(s.rmc_status,'')::char(1),
                  s.fix_quality, s.satellites, s.hdop,
                  s.alt_m, s.geoid_sep_m,
                  NULLIF(s.date_ddmmyy,''), NULLIF(s.time_hhmmss,'')
                FROM dashcam.gnss_csv_stage s;
                """,
                (sid, sid),
            )

            # --- Accel stage load ---
            cur.execute("TRUNCATE dashcam.accel_csv_stage;")
            with accel_path.open("rb") as f:
                with cur.copy("COPY dashcam.accel_csv_stage FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

            # --- Accel insert ---
            # ts_utc computed from drive_session.start_ts_utc + t_rel_s (canonical)
            # g-force computed from raw counts (BlackVue: 128 counts/g)
            accel_meta = _get_path(telem, "accel", "counts_per_g")
            counts_per_g = float(accel_meta) if accel_meta else 128.0

            cur.execute(
                """
                INSERT INTO dashcam.accel_sample (
                  drive_session_id,
                  abs_ms,
                  t_rel_s,
                  ts_utc,
                  ax_raw, ay_raw, az_raw,
                  ax_g, ay_g, az_g, a_mag_g,
                  clip_name,
                  idx,
                  t_clip_ms
                )
                SELECT
                  %s::uuid,
                  s.abs_ms,
                  s.t_rel_s,
                  (SELECT ds.start_ts_utc
                     FROM dashcam.drive_session ds
                    WHERE ds.drive_session_id = %s::uuid) + (s.t_rel_s * interval '1 second'),
                  s.ax, s.ay, s.az,
                  s.ax / %s, s.ay / %s, s.az / %s,
                  |/ ((s.ax / %s)^2 + (s.ay / %s)^2 + (s.az / %s)^2),
                  s.clip,
                  s.idx,
                  s.t_clip_ms
                FROM dashcam.accel_csv_stage s;
                """,
                (sid, sid, counts_per_g, counts_per_g, counts_per_g,
                 counts_per_g, counts_per_g, counts_per_g),
            )

            # --- Storyboard thumbs ---
            # Prefer artifacts/thumbs/index.geojson (has geometry), fallback to index.csv.
            thumbs_dir = (base / "artifacts" / "thumbs").resolve()
            thumbs_csv = (thumbs_dir / "index.csv").resolve()
            thumbs_geojson = (thumbs_dir / "index.geojson").resolve()

            media_root = Path("/media").resolve()

            if media_root.is_dir() and (thumbs_geojson.exists() or thumbs_csv.exists()):
                vehicle_safe = (vehicle_tag or "unknown").strip() or "unknown"
                dest_dir = (media_root / vehicle_safe / str(sid) / "thumbs").resolve()
                dest_dir.mkdir(parents=True, exist_ok=True)

                # start_dt for canonical storyboard timestamps
                cur.execute("SELECT start_ts_utc FROM dashcam.drive_session WHERE drive_session_id = %s::uuid;", (sid,))
                start_dt = cur.fetchone()[0]
                if start_dt is None:
                    raise SystemExit("drive_session.start_ts_utc is NULL; cannot compute storyboard ts_utc")

                rows_to_insert = []

                def _t(x):
                    x = (x or "").strip()
                    return x or None

                def _f(x):
                    x = (x or "").strip()
                    return float(x) if x else None

                if thumbs_geojson.exists():
                    gj = json.loads(thumbs_geojson.read_text(encoding="utf-8"))
                    for feat in (gj.get("features") or []):
                        geom = feat.get("geometry") or {}
                        props = feat.get("properties") or {}
                        coords = geom.get("coordinates")

                        if not (isinstance(coords, list) and len(coords) == 2):
                            continue
                        lon, lat = coords[0], coords[1]

                        file_name = _t(props.get("file"))
                        if not file_name:
                            continue

                        src_file = (thumbs_dir / file_name).resolve()
                        if not src_file.exists():
                            continue

                        offset_s = props.get("offset_s")
                        offset_s = float(offset_s) if offset_s is not None else None

                        # Canonical ts_utc from start + offset_s
                        if offset_s is not None:
                            ts_utc = start_dt + timedelta(seconds=float(offset_s))
                        else:
                            ts_utc = _parse_ts(props.get("utc_time"))
                            if ts_utc is None:
                                continue

                        dst_file = (dest_dir / file_name).resolve()
                        shutil.copy2(src_file, dst_file)

                        media_rel_path = f"{vehicle_safe}/{sid}/thumbs/{file_name}"

                        rows_to_insert.append(
                            (
                                sid,
                                ts_utc,
                                _parse_ts(_t(props.get("local_time"))),
                                offset_s,
                                _t(props.get("source_clip")),
                                float(props.get("clip_offset_s")) if props.get("clip_offset_s") is not None else None,
                                file_name,
                                media_rel_path,
                                float(lat) if lat is not None else None,
                                float(lon) if lon is not None else None,
                                float(props.get("speed_mph")) if props.get("speed_mph") is not None else None,
                                float(props.get("course_deg")) if props.get("course_deg") is not None else None,
                            )
                        )
                else:
                    # CSV fallback: no geometry; we still compute ts_utc from start+offset_s when present.
                    with thumbs_csv.open("r", encoding="utf-8", newline="") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            file_name = _t(row.get("file"))
                            if not file_name:
                                continue

                            src_file = (thumbs_dir / file_name).resolve()
                            if not src_file.exists():
                                continue

                            offset_s = _f(row.get("offset_s"))
                            if offset_s is not None:
                                ts_utc = start_dt + timedelta(seconds=float(offset_s))
                            else:
                                ts_utc = _parse_ts(_t(row.get("utc_time")))
                                if ts_utc is None:
                                    continue

                            dst_file = (dest_dir / file_name).resolve()
                            shutil.copy2(src_file, dst_file)

                            media_rel_path = f"{vehicle_safe}/{sid}/thumbs/{file_name}"

                            rows_to_insert.append(
                                (
                                    sid,
                                    ts_utc,
                                    _parse_ts(_t(row.get("local_time"))),
                                    offset_s,
                                    _t(row.get("source_clip")),
                                    _f(row.get("clip_offset_s")),
                                    file_name,
                                    media_rel_path,
                                    None,
                                    None,
                                    None,
                                    None,
                                )
                            )

                if rows_to_insert:
                    cur.executemany(
                        """
                        INSERT INTO dashcam.storyboard_frame (
                          drive_session_id, ts_utc, local_time, offset_s,
                          source_clip, clip_offset_s, file_name, media_rel_path,
                          lat, lon, speed_mph, course_deg
                        )
                        VALUES (%s::uuid, %s::timestamptz, %s::timestamptz, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s)
                        ON CONFLICT (drive_session_id, ts_utc)
                        DO UPDATE SET
                          local_time     = EXCLUDED.local_time,
                          offset_s       = EXCLUDED.offset_s,
                          source_clip    = EXCLUDED.source_clip,
                          clip_offset_s  = EXCLUDED.clip_offset_s,
                          file_name      = EXCLUDED.file_name,
                          media_rel_path = EXCLUDED.media_rel_path,
                          lat            = EXCLUDED.lat,
                          lon            = EXCLUDED.lon,
                          speed_mph      = EXCLUDED.speed_mph,
                          course_deg     = EXCLUDED.course_deg;
                        """,
                        rows_to_insert,
                    )

            # Derived refresh
            if derived_params is None:
                cur.execute("SELECT dashcam.refresh_derived(%s::uuid);", (sid,))
            else:
                cur.execute("SELECT dashcam.refresh_derived(%s::uuid, %s::jsonb);", (sid, json.dumps(derived_params)))

    print(f"OK: ingested {sid}")
    conn.close()


if __name__ == "__main__":
    main()
