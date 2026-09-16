import os
import json
import uuid
from datetime import datetime
from typing import Optional, List

import asyncpg
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "dashcam")
DB_USER = os.getenv("DB_USER", "dashcam")
DB_PASSWORD = os.getenv("DB_PASSWORD", "change_me_now")

app = FastAPI(title="Dashcam Viewer API", version="0.4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1024)

# Host-mounted media (storyboard images, etc.)
# docker-compose mounts ./media -> /media
if os.path.isdir("/media"):
    app.mount("/media", StaticFiles(directory="/media"), name="media")

pool: asyncpg.Pool | None = None


@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=1,
        max_size=10,
        command_timeout=60,
    )


@app.on_event("shutdown")
async def shutdown():
    global pool
    if pool:
        await pool.close()


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if ts is None:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timestamp '{ts}'. Use ISO8601, e.g. 2026-01-30T15:40:00Z",
        )


def _coerce_json(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


@app.get("/health")
async def health():
    assert pool is not None
    async with pool.acquire() as con:
        v = await con.fetchval("SELECT 1;")
    return {"ok": True, "db": v}


@app.get("/vehicles")
async def list_vehicles():
    q = """
    SELECT DISTINCT vehicle_tag
    FROM dashcam.drive_session
    WHERE vehicle_tag IS NOT NULL
    ORDER BY vehicle_tag;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q)
    return [r["vehicle_tag"] for r in rows]


@app.get("/sessions")
async def list_sessions(
    limit: int = Query(500, ge=1, le=5000),
    vehicle: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
):
    after_ts = _parse_ts(after)
    before_ts = _parse_ts(before)
    q = """
    SELECT d.drive_session_id::text, d.vehicle_tag, d.start_ts_utc, d.end_ts_utc,
           d.drive_tag, d.time_confidence,
           s.duration_s, s.distance_mi, s.max_speed_mph, s.avg_speed_mph
    FROM dashcam.drive_session d
    LEFT JOIN dashcam.drive_summary s USING (drive_session_id)
    WHERE ($1::text IS NULL OR d.vehicle_tag = $1)
      AND ($2::timestamptz IS NULL OR d.start_ts_utc >= $2)
      AND ($3::timestamptz IS NULL OR d.start_ts_utc <= $3)
    ORDER BY d.start_ts_utc DESC NULLS LAST
    LIMIT $4;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, vehicle, after_ts, before_ts, limit)
    return [dict(r) for r in rows]


@app.get("/sessions/{session_id}")
async def get_session(session_id: str):
    q = """
    SELECT drive_session_id::text, vehicle_tag, start_ts_utc, end_ts_utc, notes, time_sync,
           drive_tag, time_confidence
    FROM dashcam.drive_session
    WHERE drive_session_id = $1::uuid;
    """
    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow(q, session_id)

    if not row:
        raise HTTPException(status_code=404, detail="Session not found")

    data = dict(row)
    data["time_sync"] = _coerce_json(data.get("time_sync"))
    return data


@app.get("/sessions/{session_id}/summary")
async def get_summary(session_id: str):
    q = """
    SELECT *
    FROM dashcam.drive_summary
    WHERE drive_session_id = $1::uuid;
    """
    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow(q, session_id)

    if not row:
        raise HTTPException(status_code=404, detail="Summary not found (run refresh_derived for this session)")
    data = dict(row)
    data["params"] = _coerce_json(data.get("params"))
    return data


@app.get("/sessions/{session_id}/storyboard")
async def get_storyboard(session_id: str, limit: int = Query(5000, ge=1, le=20000)):
    q = """
    SELECT ts_utc, local_time, offset_s, source_clip, clip_offset_s, file_name, media_rel_path,
           lat, lon, speed_mph, course_deg
    FROM dashcam.storyboard_frame
    WHERE drive_session_id = $1::uuid
    ORDER BY ts_utc
    LIMIT $2;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, session_id, limit)

    out = []
    for r in rows:
        d = dict(r)
        rel = d.get("media_rel_path")
        d["url"] = f"/media/{rel}" if rel else None
        out.append(d)
    return out


@app.get("/sessions/{session_id}/events")
async def get_events(
    session_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    types: Optional[str] = None,  # comma-separated
    limit: int = Query(2000, ge=1, le=20000),
):
    start_dt = _parse_ts(start)
    end_dt = _parse_ts(end)

    type_list: Optional[List[str]] = None
    if types:
        type_list = [t.strip() for t in types.split(",") if t.strip()]
        if not type_list:
            type_list = None

    q = """
    SELECT event_id::text, ts_utc, event_type, severity, details, lat, lon
    FROM dashcam.derived_event
    WHERE drive_session_id = $1::uuid
      AND ($2::timestamptz IS NULL OR ts_utc >= $2::timestamptz)
      AND ($3::timestamptz IS NULL OR ts_utc <= $3::timestamptz)
      AND ($4::text[] IS NULL OR event_type = ANY($4::text[]))
    ORDER BY ts_utc
    LIMIT $5;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, session_id, start_dt, end_dt, type_list, limit)

    out = []
    for r in rows:
        d = dict(r)
        d["details"] = _coerce_json(d.get("details"))
        out.append(d)
    return out


@app.get("/sessions/{session_id}/clips")
async def get_clips(session_id: str):
    q = """
    SELECT channel, clip_name, start_ts_utc, end_ts_utc, video_path
    FROM dashcam.clip
    WHERE drive_session_id = $1::uuid
    ORDER BY start_ts_utc;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, session_id)
    return [dict(r) for r in rows]


@app.get("/sessions/{session_id}/gnss")
async def get_gnss_points(
    session_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(20000, ge=1, le=200000),
):
    start_dt = _parse_ts(start)
    end_dt = _parse_ts(end)

    q = """
    SELECT ts_utc, lat, lon, speed_mph, course_deg, fix_quality, satellites, hdop
    FROM dashcam.v_gnss_valid
    WHERE drive_session_id = $1::uuid
      AND ($2::timestamptz IS NULL OR ts_utc >= $2::timestamptz)
      AND ($3::timestamptz IS NULL OR ts_utc <= $3::timestamptz)
    ORDER BY ts_utc
    LIMIT $4;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, session_id, start_dt, end_dt, limit)
    return [dict(r) for r in rows]


@app.get("/sessions/{session_id}/accel")
async def get_accel_5hz(
    session_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(20000, ge=1, le=200000),
):
    start_dt = _parse_ts(start)
    end_dt = _parse_ts(end)

    q = """
    SELECT ts_utc, ax, ay, az, a_mag_raw
    FROM dashcam.v_accel_5hz
    WHERE drive_session_id = $1::uuid
      AND ($2::timestamptz IS NULL OR ts_utc >= $2::timestamptz)
      AND ($3::timestamptz IS NULL OR ts_utc <= $3::timestamptz)
    ORDER BY ts_utc
    LIMIT $4;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, session_id, start_dt, end_dt, limit)
    return [dict(r) for r in rows]


@app.get("/sessions/{session_id}/track")
async def get_track_geojson(session_id: str):
    q = """
    SELECT ST_AsGeoJSON(geom_line) AS geojson
    FROM dashcam.v_track_linestring
    WHERE drive_session_id = $1::uuid;
    """
    assert pool is not None
    async with pool.acquire() as con:
        geojson = await con.fetchval(q, session_id)

    if not geojson:
        raise HTTPException(status_code=404, detail="No track available (no valid GNSS points?)")

    geom = json.loads(geojson) if isinstance(geojson, str) else geojson
    return {"type": "Feature", "geometry": geom, "properties": {"drive_session_id": session_id}}


@app.get("/sessions/{session_id}/nearest")
async def nearest(session_id: str, ts: str):
    target = _parse_ts(ts)
    if target is None:
        raise HTTPException(status_code=400, detail="ts is required")

    q_gnss = """
    WITH prev AS (
      SELECT ts_utc, lat, lon, speed_mph, course_deg
      FROM dashcam.v_gnss_valid
      WHERE drive_session_id = $1::uuid AND ts_utc <= $2::timestamptz
      ORDER BY ts_utc DESC
      LIMIT 1
    ),
    next AS (
      SELECT ts_utc, lat, lon, speed_mph, course_deg
      FROM dashcam.v_gnss_valid
      WHERE drive_session_id = $1::uuid AND ts_utc >= $2::timestamptz
      ORDER BY ts_utc ASC
      LIMIT 1
    )
    SELECT *
    FROM (
      SELECT * FROM prev
      UNION ALL
      SELECT * FROM next
    ) t
    ORDER BY abs(extract(epoch from (t.ts_utc - $2::timestamptz)))
    LIMIT 1;
    """

    q_accel = """
    WITH prev AS (
      SELECT ts_utc, ax, ay, az, a_mag_raw
      FROM dashcam.v_accel_5hz
      WHERE drive_session_id = $1::uuid AND ts_utc <= $2::timestamptz
      ORDER BY ts_utc DESC
      LIMIT 1
    ),
    next AS (
      SELECT ts_utc, ax, ay, az, a_mag_raw
      FROM dashcam.v_accel_5hz
      WHERE drive_session_id = $1::uuid AND ts_utc >= $2::timestamptz
      ORDER BY ts_utc ASC
      LIMIT 1
    )
    SELECT *
    FROM (
      SELECT * FROM prev
      UNION ALL
      SELECT * FROM next
    ) t
    ORDER BY abs(extract(epoch from (t.ts_utc - $2::timestamptz)))
    LIMIT 1;
    """

    q_clip = """
    SELECT channel, clip_name, start_ts_utc, end_ts_utc, video_path
    FROM dashcam.clip
    WHERE drive_session_id = $1::uuid
      AND start_ts_utc <= $2::timestamptz
      AND end_ts_utc   >  $2::timestamptz
    ORDER BY start_ts_utc DESC
    LIMIT 1;
    """

    assert pool is not None
    async with pool.acquire() as con:
        gnss = await con.fetchrow(q_gnss, session_id, target)
        accel = await con.fetchrow(q_accel, session_id, target)
        clip = await con.fetchrow(q_clip, session_id, target)

    return {
        "ts": target.isoformat(),
        "gnss": dict(gnss) if gnss else None,
        "accel": dict(accel) if accel else None,
        "clip": dict(clip) if clip else None,
    }


@app.get("/imports")
async def list_imports(
    limit: int = Query(100, ge=1, le=1000),
    vehicle: Optional[str] = None,
    status: Optional[str] = None,
):
    q = """
    SELECT b.batch_id::text, b.source_kind, b.source_key, b.vehicle_tag,
           b.camera_serial, b.camera_model, b.camera_firmware,
           b.clips_on_source, b.clips_new, b.bytes_new,
           b.detected_at, b.acquired_at, b.processed_at, b.received_at, b.completed_at,
           b.status, b.attempts, b.error,
           COALESCE(i.item_count, 0)     AS item_count,
           COALESCE(i.recorded_count, 0) AS recorded_count,
           COALESCE(i.problem_count, 0)  AS problem_count
    FROM dashcam.import_batch b
    LEFT JOIN (
      SELECT batch_id,
             count(*) AS item_count,
             -- Deliberately not loaded, and no cause for concern: a recording
             -- too short to be a drive, one made while parked, or one replaced
             -- by a better copy. Counting these as "not loaded" made a clean
             -- import look like a partial failure.
             count(*) FILTER (
               WHERE status IN ('skipped_short', 'parking_only', 'superseded')
             ) AS recorded_count,
             -- Everything else that did not load: these genuinely want a look.
             count(*) FILTER (
               WHERE status NOT IN
                 ('ingested', 'skipped_short', 'parking_only', 'superseded')
             ) AS problem_count
      FROM dashcam.import_item
      GROUP BY batch_id
    ) i ON i.batch_id = b.batch_id
    WHERE ($1::text IS NULL OR b.vehicle_tag = $1)
      AND ($2::text IS NULL OR b.status = $2)
    ORDER BY b.received_at DESC
    LIMIT $3;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, vehicle, status, limit)
    return [dict(r) for r in rows]


# Declared before /imports/{batch_id} so "items" is not read as a batch id.
@app.get("/imports/items")
async def list_import_items(
    limit: int = Query(200, ge=1, le=2000),
    vehicle: Optional[str] = None,
    status: Optional[str] = None,
):
    q = """
    SELECT i.item_id, i.batch_id::text, i.vehicle_tag, i.drive_tag, i.view,
           i.drive_session_id::text, i.clip_pairs, i.first_clip, i.last_clip,
           i.status, i.reason_code, i.reason, i.counts, i.time_confidence,
           i.created_at, i.started_at, i.finished_at,
           ds.start_ts_utc AS session_start_ts_utc,
           b.received_at, b.source_kind, b.status AS batch_status
    FROM dashcam.import_item i
    JOIN dashcam.import_batch b ON b.batch_id = i.batch_id
    LEFT JOIN dashcam.drive_session ds ON ds.drive_session_id = i.drive_session_id
    WHERE ($1::text IS NULL OR i.status = $1)
      AND ($2::text IS NULL OR i.vehicle_tag = $2)
    ORDER BY i.created_at DESC, i.item_id DESC
    LIMIT $3;
    """
    assert pool is not None
    async with pool.acquire() as con:
        rows = await con.fetch(q, status, vehicle, limit)

    out = []
    for r in rows:
        d = dict(r)
        d["counts"] = _coerce_json(d.get("counts"))
        out.append(d)
    return out


@app.get("/imports/{batch_id}")
async def get_import(batch_id: str):
    try:
        uuid.UUID(batch_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Import not found")

    q_batch = """
    SELECT batch_id::text, source_kind, source_key, vehicle_tag,
           camera_serial, camera_model, camera_firmware,
           clips_on_source, clips_new, bytes_new,
           detected_at, acquired_at, processed_at, received_at, completed_at,
           status, attempts, error, pc_summary
    FROM dashcam.import_batch
    WHERE batch_id = $1::uuid;
    """

    q_items = """
    SELECT i.item_id, i.batch_id::text, i.vehicle_tag, i.drive_tag, i.view,
           i.drive_session_id::text, i.clip_pairs, i.first_clip, i.last_clip,
           i.status, i.reason_code, i.reason, i.counts, i.time_confidence,
           i.created_at, i.started_at, i.finished_at,
           ds.start_ts_utc AS session_start_ts_utc
    FROM dashcam.import_item i
    LEFT JOIN dashcam.drive_session ds ON ds.drive_session_id = i.drive_session_id
    WHERE i.batch_id = $1::uuid
    ORDER BY i.drive_tag, i.view;
    """

    assert pool is not None
    async with pool.acquire() as con:
        row = await con.fetchrow(q_batch, batch_id)
        if not row:
            raise HTTPException(status_code=404, detail="Import not found")
        items = await con.fetch(q_items, batch_id)

    data = dict(row)
    data["pc_summary"] = _coerce_json(data.get("pc_summary"))

    out_items = []
    for r in items:
        d = dict(r)
        d["counts"] = _coerce_json(d.get("counts"))
        out_items.append(d)
    data["items"] = out_items
    return data
