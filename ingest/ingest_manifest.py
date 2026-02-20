import os
import json
import sys
import shutil
import csv
from pathlib import Path
from typing import Any

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
    nmea_rel = _get_path(telem, "nmea", "path")  # optional; not ingested right now

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

    # Session times (optional in manifest; we'll backfill from GNSS if absent)
    start_ts_utc = _get_path(m, "session_time", "start_ts_utc")
    end_ts_utc = _get_path(m, "session_time", "end_ts_utc")

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

            # Upsert drive_session (store manifest start/end if present)
            cur.execute(
                """
                INSERT INTO dashcam.drive_session (drive_session_id, vehicle_tag, notes, start_ts_utc, end_ts_utc)
                VALUES (%s::uuid, %s, %s, %s, %s)
                ON CONFLICT (drive_session_id)
                DO UPDATE SET vehicle_tag = EXCLUDED.vehicle_tag,
                              notes = EXCLUDED.notes,
                              start_ts_utc = COALESCE(EXCLUDED.start_ts_utc, dashcam.drive_session.start_ts_utc),
                              end_ts_utc   = COALESCE(EXCLUDED.end_ts_utc,   dashcam.drive_session.end_ts_utc);
                """,
                (sid, vehicle_tag, notes, start_ts_utc, end_ts_utc),
            )

            # Insert clips (timestamps may be null; that's fine)
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
                            c.get("start_ts_utc"),
                            c.get("end_ts_utc"),
                            (c.get("video") or {}).get("path") if isinstance(c.get("video"), dict) else c.get("video_path"),
                        )
                        for c in clips
                    ],
                )

            # --- GNSS stage load ---
            # --- Storyboard thumbs (time-based) ---
            # Expected: <drive_folder>/artifacts/thumbs/index.csv + jpg files
            # Copies: /import -> /media/<vehicle_tag>/<sid>/thumbs/<file>
            # Inserts: dashcam.storyboard_frame rows with media_rel_path
            thumbs_dir = (base / 'artifacts' / 'thumbs').resolve()
            thumbs_index = (thumbs_dir / 'index.csv').resolve()

            media_root = Path('/media').resolve()
            if thumbs_index.exists() and media_root.is_dir():
                vehicle_safe = (vehicle_tag or 'unknown').strip() or 'unknown'
                dest_dir = (media_root / vehicle_safe / str(sid) / 'thumbs').resolve()
                dest_dir.mkdir(parents=True, exist_ok=True)

                rows_to_insert = []

                def _f(x):
                    x = (x or '').strip()
                    return float(x) if x else None

                def _t(x):
                    x = (x or '').strip()
                    return x or None

                with thumbs_index.open('r', encoding='utf-8', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        file_name = _t(row.get('file'))
                        ts_utc = _t(row.get('utc_time'))
                        if not file_name or not ts_utc:
                            continue

                        src_file = (thumbs_dir / file_name).resolve()
                        if not src_file.exists():
                            continue

                        dst_file = (dest_dir / file_name).resolve()
                        shutil.copy2(src_file, dst_file)

                        media_rel_path = f"{vehicle_safe}/{sid}/thumbs/{file_name}"

                        rows_to_insert.append((
                            sid,
                            ts_utc,
                            _t(row.get('local_time')),
                            _f(row.get('offset_s')),
                            _t(row.get('source_clip')),
                            _f(row.get('clip_offset_s')),
                            file_name,
                            media_rel_path,
                        ))

                if rows_to_insert:
                    cur.executemany(
                        """
                        INSERT INTO dashcam.storyboard_frame (
                          drive_session_id, ts_utc, local_time, offset_s,
                          source_clip, clip_offset_s, file_name, media_rel_path
                        )
                        VALUES (%s::uuid, %s::timestamptz, %s::timestamptz, %s,
                                %s, %s, %s, %s)
                        ON CONFLICT (drive_session_id, ts_utc)
                        DO UPDATE SET
                          local_time    = EXCLUDED.local_time,
                          offset_s      = EXCLUDED.offset_s,
                          source_clip   = EXCLUDED.source_clip,
                          clip_offset_s = EXCLUDED.clip_offset_s,
                          file_name     = EXCLUDED.file_name,
                          media_rel_path= EXCLUDED.media_rel_path;
                        """,
                        rows_to_insert,
                    )
            cur.execute("TRUNCATE dashcam.gnss_csv_stage;")
            with gnss_path.open("rb") as f:
                with cur.copy("COPY dashcam.gnss_csv_stage FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

            # --- GNSS insert ---
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
                  NULLIF(s.utc_time,'')::timestamptz,
                  s.lat, s.lon,
                  s.speed_knots, s.speed_mps, s.speed_mph,
                  s.course_deg,
                  NULLIF(s.rmc_status,'')::char(1),
                  s.fix_quality, s.satellites, s.hdop,
                  s.alt_m, s.geoid_sep_m,
                  NULLIF(s.date_ddmmyy,''), NULLIF(s.time_hhmmss,'')
                FROM dashcam.gnss_csv_stage s;
                """,
                (sid,),
            )

            # If session_time was missing in manifest, backfill start/end from GNSS
            cur.execute(
                """
                UPDATE dashcam.drive_session ds
                SET start_ts_utc = COALESCE(ds.start_ts_utc, g.min_ts),
                    end_ts_utc   = COALESCE(ds.end_ts_utc,   g.max_ts)
                FROM (
                  SELECT min(ts_utc) AS min_ts, max(ts_utc) AS max_ts
                  FROM dashcam.gnss_sample
                  WHERE drive_session_id = %s::uuid
                ) g
                WHERE ds.drive_session_id = %s::uuid;
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
            # accel_csv_stage expected columns: abs_ms,t_rel_s,ax,ay,az,clip,idx,t_clip_ms
            # ts_utc computed from drive_session.start_ts_utc + t_rel_s
            cur.execute(
                """
                INSERT INTO dashcam.accel_sample (
                  drive_session_id,
                  abs_ms,
                  t_rel_s,
                  ts_utc,
                  ax_raw, ay_raw, az_raw,
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
                  s.clip,
                  s.idx,
                  s.t_clip_ms
                FROM dashcam.accel_csv_stage s;
                """,
                (sid, sid),
            )

            # Derived refresh (summary, events, geo, etc.)
            if derived_params is None:
                cur.execute("SELECT dashcam.refresh_derived(%s::uuid);", (sid,))
            else:
                cur.execute("SELECT dashcam.refresh_derived(%s::uuid, %s::jsonb);", (sid, json.dumps(derived_params)))

    print(f"OK: ingested {sid}")
    conn.close()


if __name__ == "__main__":
    main()
