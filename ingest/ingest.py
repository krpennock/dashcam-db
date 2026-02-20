import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg


DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "dashcam")
DB_USER = os.getenv("DB_USER", "dashcam")
DB_PASSWORD = os.getenv("DB_PASSWORD", "change_me_now")


def die(msg: str, code: int = 2) -> None:
    print(f"[ingest] ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def load_manifest(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"Failed to read manifest {path}: {e}")
    if not isinstance(data, dict):
        die("Manifest must be a JSON object")
    return data


def regproc_exists(cur: psycopg.Cursor, regproc: str) -> bool:
    # regproc examples: "dashcam.refresh_derived(uuid,jsonb)"
    cur.execute("SELECT to_regprocedure(%s) IS NOT NULL;", (regproc,))
    return bool(cur.fetchone()[0])


def main() -> None:
    if len(sys.argv) < 2:
        die("Usage: ingest.py /path/to/manifest.json")

    manifest_path = Path(sys.argv[1]).expanduser().resolve()
    if not manifest_path.exists():
        die(f"Manifest not found: {manifest_path}")

    m = load_manifest(manifest_path)

    sid = m.get("drive_session_id")
    if not sid or not isinstance(sid, str):
        die("manifest.drive_session_id is required (uuid string)")

    vehicle_tag = m.get("vehicle_tag")
    notes = m.get("notes")

    telemetry = m.get("telemetry") or {}
    if not isinstance(telemetry, dict):
        die("manifest.telemetry must be an object")

    gnss_rel = telemetry.get("gnss_csv")
    accel_rel = telemetry.get("accel_csv")

    base_dir = manifest_path.parent
    gnss_csv = (base_dir / gnss_rel).resolve() if gnss_rel else None
    accel_csv = (base_dir / accel_rel).resolve() if accel_rel else None

    if gnss_csv and not gnss_csv.exists():
        die(f"GNSS CSV not found: {gnss_csv}")
    if accel_csv and not accel_csv.exists():
        die(f"Accel CSV not found: {accel_csv}")

    clips = m.get("clips") or []
    if not isinstance(clips, list):
        die("manifest.clips must be an array")

    derived_params = m.get("derived_params") or {}
    if not isinstance(derived_params, dict):
        die("manifest.derived_params must be an object")

    dsn = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}"
    print(f"[ingest] Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER}")

    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            # ---- Ensure drive_session exists (upsert) ----
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

            # ---- Clear existing session data (idempotent) ----
            # Order: derived first (if present), then samples, then clips
            for sql in [
                "DELETE FROM dashcam.derived_event WHERE drive_session_id=%s::uuid;",
                "DELETE FROM dashcam.drive_summary WHERE drive_session_id=%s::uuid;",
                "DELETE FROM dashcam.accel_sample WHERE drive_session_id=%s::uuid;",
                "DELETE FROM dashcam.gnss_sample WHERE drive_session_id=%s::uuid;",
                "DELETE FROM dashcam.clip WHERE drive_session_id=%s::uuid;",
            ]:
                try:
                    cur.execute(sql, (sid,))
                except Exception:
                    # Some tables might not exist in early schema states; ignore safely
                    con.rollback()
                    con.autocommit = False
                    cur = con.cursor()

            # ---- Load GNSS ----
            if gnss_csv:
                print(f"[ingest] Loading GNSS: {gnss_csv.name}")
                cur.execute(
                    """
                    CREATE TEMP TABLE gnss_stage (
                      ms           bigint,
                      t_rel_s      double precision,
                      utc_time     text,
                      lat          double precision,
                      lon          double precision,
                      speed_knots  double precision,
                      speed_mps    double precision,
                      speed_mph    double precision,
                      course_deg   double precision,
                      rmc_status   text,
                      fix_quality  integer,
                      satellites   integer,
                      hdop         double precision,
                      alt_m        double precision,
                      geoid_sep_m  double precision,
                      date_ddmmyy  text,
                      time_hhmmss  text
                    ) ON COMMIT DROP;
                    """
                )
                copy_sql = """
                COPY gnss_stage (
                  ms,t_rel_s,utc_time,lat,lon,speed_knots,speed_mps,speed_mph,course_deg,
                  rmc_status,fix_quality,satellites,hdop,alt_m,geoid_sep_m,date_ddmmyy,time_hhmmss
                )
                FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');
                """
                with cur.copy(copy_sql) as cp, open(gnss_csv, "rb") as f:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

                cur.execute(
                    """
                    INSERT INTO dashcam.gnss_sample (
                      drive_session_id,
                      device_ms, t_rel_s, ts_utc,
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
                      ms,
                      t_rel_s,
                      NULLIF(utc_time,'')::timestamptz,
                      lat, lon,
                      speed_knots, speed_mps, speed_mph,
                      course_deg,
                      NULLIF(rmc_status,'')::char(1),
                      fix_quality, satellites, hdop,
                      alt_m, geoid_sep_m,
                      NULLIF(date_ddmmyy,''), NULLIF(time_hhmmss,'')
                    FROM gnss_stage;
                    """,
                    (sid,),
                )

                # Fit time_sync (linear regression) from GNSS
                cur.execute(
                    """
                    WITH base AS (
                      SELECT
                        device_ms::double precision AS x,
                        (extract(epoch from ts_utc) * 1000.0)::double precision AS y
                      FROM dashcam.gnss_sample
                      WHERE drive_session_id=%s::uuid
                        AND ts_utc IS NOT NULL
                    ),
                    model AS (
                      SELECT
                        regr_slope(y, x)    AS slope,
                        regr_intercept(y,x) AS intercept_ms,
                        count(*)            AS n
                      FROM base
                    ),
                    resid AS (
                      SELECT
                        abs(b.y - (m.slope*b.x + m.intercept_ms)) AS r_ms
                      FROM base b
                      CROSS JOIN model m
                      WHERE m.slope IS NOT NULL AND m.intercept_ms IS NOT NULL
                    )
                    SELECT
                      m.n,
                      m.slope,
                      m.intercept_ms,
                      percentile_cont(0.50) WITHIN GROUP (ORDER BY r_ms) AS p50,
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY r_ms) AS p95
                    FROM model m
                    LEFT JOIN resid r ON true
                    GROUP BY m.n, m.slope, m.intercept_ms;
                    """,
                    (sid,),
                )
                row = cur.fetchone()
                if not row or row[1] is None or row[2] is None:
                    die("Unable to compute time_sync from GNSS (missing ts_utc/device_ms?)")
                n, slope, intercept_ms, p50, p95 = row
                print(f"[ingest] time_sync slope={slope:.12f} intercept_ms={intercept_ms:.3f} p95_ms={float(p95):.2f}")

                # Store time_sync JSON (matches what your UI/API expects)
                cur.execute(
                    """
                    UPDATE dashcam.drive_session
                    SET time_sync = jsonb_build_object(
                      'n', %s,
                      'model', 'linear',
                      'slope', %s,
                      'intercept_ms', %s,
                      'source', 'gnss_sample.ts_utc',
                      'residual_p50_ms', %s,
                      'residual_p95_ms', %s
                    )
                    WHERE drive_session_id=%s::uuid;
                    """,
                    (int(n), float(slope), float(intercept_ms), float(p50 or 0.0), float(p95 or 0.0), sid),
                )
            else:
                slope = None
                intercept_ms = None

            # ---- Load ACCEL (requires time_sync to compute ts_utc) ----
            if accel_csv:
                if slope is None or intercept_ms is None:
                    die("Accel ingest requires GNSS-derived time_sync (no GNSS loaded / time_sync missing)")

                print(f"[ingest] Loading ACCEL: {accel_csv.name}")
                cur.execute(
                    """
                    CREATE TEMP TABLE accel_stage (
                      abs_ms   bigint,
                      t_rel_s  double precision,
                      ax       double precision,
                      ay       double precision,
                      az       double precision,
                      clip     text,
                      idx      integer,
                      t_clip_ms integer
                    ) ON COMMIT DROP;
                    """
                )

                copy_sql = """
                COPY accel_stage (abs_ms,t_rel_s,ax,ay,az,clip,idx,t_clip_ms)
                FROM STDIN WITH (FORMAT csv, HEADER true, NULL '');
                """
                with cur.copy(copy_sql) as cp, open(accel_csv, "rb") as f:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        cp.write(chunk)

                # Insert accel samples; ts_utc computed from slope/intercept
                cur.execute(
                    """
                    INSERT INTO dashcam.accel_sample (
                      drive_session_id,
                      device_ms, t_rel_s, ts_utc,
                      ax_raw, ay_raw, az_raw,
                      clip_name, sample_idx, t_clip_ms
                    )
                    SELECT
                      %s::uuid,
                      abs_ms,
                      t_rel_s,
                      to_timestamp(((%s * abs_ms) + %s) / 1000.0),
                      ax, ay, az,
                      clip, idx, t_clip_ms
                    FROM accel_stage;
                    """,
                    (sid, float(slope), float(intercept_ms)),
                )

                # Update drive_session start/end from accel ts_utc (authoritative timeline)
                cur.execute(
                    """
                    UPDATE dashcam.drive_session ds
                    SET start_ts_utc = a.min_ts,
                        end_ts_utc   = a.max_ts
                    FROM (
                      SELECT drive_session_id,
                             min(ts_utc) AS min_ts,
                             max(ts_utc) AS max_ts
                      FROM dashcam.accel_sample
                      WHERE drive_session_id=%s::uuid AND ts_utc IS NOT NULL
                      GROUP BY drive_session_id
                    ) a
                    WHERE ds.drive_session_id = a.drive_session_id;
                    """,
                    (sid,),
                )

            # ---- Load CLIPS (delete/reinsert) ----
            if clips:
                print(f"[ingest] Loading clips: {len(clips)}")
                for c in clips:
                    cur.execute(
                        """
                        INSERT INTO dashcam.clip (drive_session_id, channel, clip_name, start_ts_utc, end_ts_utc, video_path)
                        VALUES (%s::uuid, %s, %s, %s::timestamptz, %s::timestamptz, %s);
                        """,
                        (
                            sid,
                            c.get("channel"),
                            c.get("clip_name"),
                            c.get("start_ts_utc"),
                            c.get("end_ts_utc"),
                            c.get("video_path"),
                        ),
                    )

            # ---- refresh_derived (summary + events) ----
            if regproc_exists(cur, "dashcam.refresh_derived(uuid,jsonb)"):
                print("[ingest] Running dashcam.refresh_derived(...)")
                cur.execute(
                    "SELECT dashcam.refresh_derived(%s::uuid, %s::jsonb);",
                    (sid, json.dumps(derived_params)),
                )
            else:
                print("[ingest] NOTE: dashcam.refresh_derived(uuid,jsonb) not found; skipping derived refresh")

        con.commit()

    print("[ingest] Done.")
    print(f"[ingest] Session: {sid}")


if __name__ == "__main__":
    main()
