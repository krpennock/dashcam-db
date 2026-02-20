import os
import json
import sys
from pathlib import Path

import psycopg


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: ingest_manifest.py /import/<folder>/<manifest.json>")

    manifest_path = Path(sys.argv[1]).resolve()
    if not manifest_path.exists():
        raise SystemExit(f"Manifest not found: {manifest_path}")

    m = json.loads(manifest_path.read_text(encoding="utf-8"))
    sid = m["drive_session_id"]
    vehicle_tag = m.get("vehicle_tag")
    notes = m.get("notes")
    derived_params = m.get("derived_params")

    base = manifest_path.parent
    telem = m.get("telemetry", {})
    gnss_csv = telem.get("gnss_csv")
    accel_csv = telem.get("accel_csv")

    if not gnss_csv or not accel_csv:
        raise SystemExit("Manifest missing telemetry.gnss_csv and/or telemetry.accel_csv")

    gnss_path = (base / gnss_csv).resolve()
    accel_path = (base / accel_csv).resolve()

    if not gnss_path.exists():
        raise SystemExit(f"GNSS CSV not found: {gnss_path}")
    if not accel_path.exists():
        raise SystemExit(f"Accel CSV not found: {accel_path}")

    clips = m.get("clips", [])

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
            cur.execute("DELETE FROM dashcam.derived_event WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.drive_summary  WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.accel_sample  WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.gnss_sample   WHERE drive_session_id = %s::uuid;", (sid,))
            cur.execute("DELETE FROM dashcam.clip          WHERE drive_session_id = %s::uuid;", (sid,))

            # Upsert drive_session
            cur.execute(
                """
                INSERT INTO dashcam.drive_session (drive_session_id, vehicle_tag, notes)
                VALUES (%s::uuid, %s, %s)
                ON CONFLICT (drive_session_id)
                DO UPDATE SET vehicle_tag = EXCLUDED.vehicle_tag,
                              notes = EXCLUDED.notes;
                """,
                (sid, vehicle_tag, notes),
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
                            c.get("video_path"),
                        )
                        for c in clips
                    ],
                )

            # GNSS stage load
            cur.execute("TRUNCATE dashcam.gnss_csv_stage;")
            with gnss_path.open("rb") as f:
                with cur.copy("COPY dashcam.gnss_csv_stage FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

            # GNSS insert
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

            # Accel stage load
            cur.execute("TRUNCATE dashcam.accel_csv_stage;")
            with accel_path.open("rb") as f:
                with cur.copy("COPY dashcam.accel_csv_stage FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

            # Accel insert (matches your accel_sample schema)
            # accel_csv_stage: abs_ms,t_rel_s,ax,ay,az,clip,idx,t_clip_ms
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
                  NULL,
                  s.ax, s.ay, s.az,
                  s.clip,
                  s.idx,
                  s.t_clip_ms
                FROM dashcam.accel_csv_stage s;
                """,
                (sid,),
            )

            # Derived refresh (time sync, summary, events, geo)
            if derived_params is None:
                cur.execute("SELECT dashcam.refresh_derived(%s::uuid);", (sid,))
            else:
                cur.execute("SELECT dashcam.refresh_derived(%s::uuid, %s::jsonb);", (sid, json.dumps(derived_params)))

    print(f"OK: ingested {sid}")
    conn.close()


if __name__ == "__main__":
    main()
