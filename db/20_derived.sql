BEGIN;

-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Derived event markers (telemetry-first; no dependency on video)
CREATE TABLE IF NOT EXISTS dashcam.derived_event (
  event_id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  drive_session_id uuid NOT NULL REFERENCES dashcam.drive_session(drive_session_id) ON DELETE CASCADE,
  ts_utc           timestamptz NOT NULL,
  event_type       text NOT NULL, -- hard_brake, hard_accel, harsh_turn, stop, bump
  severity         double precision NULL,
  details          jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS derived_event_session_ts_idx
  ON dashcam.derived_event(drive_session_id, ts_utc);

CREATE INDEX IF NOT EXISTS derived_event_session_type_idx
  ON dashcam.derived_event(drive_session_id, event_type);

-- 2) Drive summary table (one row per session)
CREATE TABLE IF NOT EXISTS dashcam.drive_summary (
  drive_session_id        uuid PRIMARY KEY REFERENCES dashcam.drive_session(drive_session_id) ON DELETE CASCADE,
  start_ts_utc            timestamptz NULL,
  end_ts_utc              timestamptz NULL,
  duration_s              double precision NULL,
  distance_m              double precision NULL,
  distance_mi             double precision NULL,
  max_speed_mph           double precision NULL,
  avg_speed_mph           double precision NULL,
  moving_avg_speed_mph    double precision NULL,
  stops_count             integer NOT NULL DEFAULT 0,
  hard_brake_count        integer NOT NULL DEFAULT 0,
  hard_accel_count        integer NOT NULL DEFAULT 0,
  harsh_turn_count        integer NOT NULL DEFAULT 0,
  bump_count              integer NOT NULL DEFAULT 0,
  params                  jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at              timestamptz NOT NULL DEFAULT now()
);

-- Helper: great-circle-ish distance using PostGIS geography
CREATE OR REPLACE VIEW dashcam.v_session_distance AS
SELECT
  g.drive_session_id,
  CASE
    WHEN count(*) < 2 THEN NULL
    ELSE ST_Length(
      (ST_MakeLine(ST_SetSRID(ST_MakePoint(g.lon, g.lat), 4326) ORDER BY g.ts_utc))::geography
    )
  END AS distance_m
FROM dashcam.v_gnss_valid g
GROUP BY g.drive_session_id;

-- Helper: GNSS-derived kinematics at 1Hz-ish using lag
-- (works even without accelerometer axis calibration)
CREATE OR REPLACE VIEW dashcam.v_gnss_kinematics AS
WITH base AS (
  SELECT
    g.drive_session_id,
    g.ts_utc,
    g.speed_mph,
    (g.speed_mph * 0.44704)::double precision AS speed_mps,
    g.course_deg,
    lag(g.ts_utc)      OVER w AS ts_prev,
    lag(g.speed_mph)   OVER w AS speed_prev_mph,
    lag(g.course_deg)  OVER w AS course_prev
  FROM dashcam.v_gnss_valid g
  WINDOW w AS (PARTITION BY g.drive_session_id ORDER BY g.ts_utc)
),
d AS (
  SELECT
    drive_session_id,
    ts_utc,
    speed_mph,
    speed_mps,
    course_deg,
    ts_prev,
    speed_prev_mph,
    course_prev,
    EXTRACT(epoch FROM (ts_utc - ts_prev))::double precision AS dt_s,
    ((speed_mph - speed_prev_mph) * 0.44704)::double precision AS dv_mps
  FROM base
)
SELECT
  drive_session_id,
  ts_utc,
  speed_mph,
  speed_mps,
  course_deg,
  dt_s,
  CASE WHEN dt_s IS NULL OR dt_s <= 0 OR dt_s > 2.0 THEN NULL ELSE (dv_mps / dt_s) END AS a_long_mps2,

  -- course delta normalized to [-180, +180]
  CASE
    WHEN dt_s IS NULL OR dt_s <= 0 OR dt_s > 2.0 OR course_prev IS NULL THEN NULL
    ELSE (
      (
        (course_deg - course_prev + 540.0)
        - floor((course_deg - course_prev + 540.0) / 360.0) * 360.0
      ) - 180.0
    )
  END AS d_course_deg,

  CASE
    WHEN dt_s IS NULL OR dt_s <= 0 OR dt_s > 2.0 OR course_prev IS NULL THEN NULL
    ELSE radians(
      (
        (course_deg - course_prev + 540.0)
        - floor((course_deg - course_prev + 540.0) / 360.0) * 360.0
      ) - 180.0
    ) / dt_s
  END AS yaw_rate_rad_s,

  CASE
    WHEN dt_s IS NULL OR dt_s <= 0 OR dt_s > 2.0 OR course_prev IS NULL THEN NULL
    ELSE (speed_mps * (radians(
      (
        (course_deg - course_prev + 540.0)
        - floor((course_deg - course_prev + 540.0) / 360.0) * 360.0
      ) - 180.0
    ) / dt_s))
  END AS a_lat_mps2

FROM d;

-- 3) Refresh events for a session (delete + recompute)
CREATE OR REPLACE FUNCTION dashcam.refresh_events(p_session uuid, p_params jsonb DEFAULT '{}'::jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  -- GNSS-derived thresholds (m/s^2)
  t_hard_brake double precision := COALESCE((p_params->>'hard_brake_mps2')::double precision, 4.0);  -- >= 0.4g
  t_hard_accel double precision := COALESCE((p_params->>'hard_accel_mps2')::double precision, 3.0);  -- >= 0.3g
  t_harsh_turn double precision := COALESCE((p_params->>'harsh_turn_mps2')::double precision, 2.5);  -- lateral approx

  min_speed_brake double precision := COALESCE((p_params->>'min_speed_brake_mph')::double precision, 10.0);
  min_speed_turn  double precision := COALESCE((p_params->>'min_speed_turn_mph')::double precision, 15.0);

  -- stop detection
  stop_speed_mph  double precision := COALESCE((p_params->>'stop_speed_mph')::double precision, 1.0);
  stop_min_s      integer := COALESCE((p_params->>'stop_min_s')::int, 10);

  -- bump detection (adaptive using percentiles on accel magnitude)
  bump_p99  double precision;
  bump_p999 double precision;
  bump_thr  double precision;
BEGIN
  DELETE FROM dashcam.derived_event WHERE drive_session_id = p_session;

  -- A) hard brake / hard accel / harsh turn from GNSS kinematics (1Hz)
  WITH k AS (
    SELECT *
    FROM dashcam.v_gnss_kinematics
    WHERE drive_session_id = p_session
      AND a_long_mps2 IS NOT NULL
  ),
  flags AS (
    SELECT
      k.*,
      (k.speed_mph >= min_speed_brake AND k.a_long_mps2 <= -t_hard_brake) AS is_brake,
      (k.speed_mph >= min_speed_brake AND k.a_long_mps2 >=  t_hard_accel) AS is_accel,
      (k.speed_mph >= min_speed_turn  AND k.a_lat_mps2  IS NOT NULL AND abs(k.a_lat_mps2) >= t_harsh_turn) AS is_turn,
      lag((k.speed_mph >= min_speed_brake AND k.a_long_mps2 <= -t_hard_brake)) OVER (PARTITION BY k.drive_session_id ORDER BY k.ts_utc) AS prev_brake,
      lag((k.speed_mph >= min_speed_brake AND k.a_long_mps2 >=  t_hard_accel)) OVER (PARTITION BY k.drive_session_id ORDER BY k.ts_utc) AS prev_accel,
      lag((k.speed_mph >= min_speed_turn  AND k.a_lat_mps2  IS NOT NULL AND abs(k.a_lat_mps2) >= t_harsh_turn)) OVER (PARTITION BY k.drive_session_id ORDER BY k.ts_utc) AS prev_turn
    FROM k
  )
  INSERT INTO dashcam.derived_event(drive_session_id, ts_utc, event_type, severity, details)
  SELECT p_session, ts_utc, event_type, severity, details
  FROM (
    SELECT
      ts_utc,
      'hard_brake'::text AS event_type,
      abs(a_long_mps2) AS severity,
      jsonb_build_object('a_long_mps2', a_long_mps2, 'speed_mph', speed_mph) AS details,
      (is_brake AND COALESCE(prev_brake,false)=false) AS emit
    FROM flags
    UNION ALL
    SELECT
      ts_utc,
      'hard_accel'::text,
      abs(a_long_mps2),
      jsonb_build_object('a_long_mps2', a_long_mps2, 'speed_mph', speed_mph),
      (is_accel AND COALESCE(prev_accel,false)=false)
    FROM flags
    UNION ALL
    SELECT
      ts_utc,
      'harsh_turn'::text,
      abs(a_lat_mps2),
      jsonb_build_object('a_lat_mps2', a_lat_mps2, 'yaw_rate_rad_s', yaw_rate_rad_s, 'speed_mph', speed_mph),
      (is_turn AND COALESCE(prev_turn,false)=false)
    FROM flags
  ) e
  WHERE e.emit;

  -- B) stop events from GNSS speed (runs of speed < stop_speed_mph lasting >= stop_min_s)
  WITH s AS (
    SELECT
      drive_session_id, ts_utc, speed_mph,
      (speed_mph < stop_speed_mph) AS is_stop
    FROM dashcam.v_gnss_valid
    WHERE drive_session_id = p_session
  ),
  runs AS (
    SELECT
      *,
      sum(CASE WHEN is_stop AND COALESCE(lag(is_stop) OVER (PARTITION BY drive_session_id ORDER BY ts_utc), false)=false THEN 1 ELSE 0 END)
        OVER (PARTITION BY drive_session_id ORDER BY ts_utc) AS grp
    FROM s
  ),
  agg AS (
    SELECT
      drive_session_id,
      min(ts_utc) AS start_ts,
      max(ts_utc) AS end_ts,
      count(*) AS n_seconds
    FROM runs
    WHERE is_stop
    GROUP BY drive_session_id, grp
    HAVING count(*) >= stop_min_s
  )
  INSERT INTO dashcam.derived_event(drive_session_id, ts_utc, event_type, severity, details)
  SELECT
    p_session,
    start_ts,
    'stop',
    n_seconds::double precision,
    jsonb_build_object('duration_s', n_seconds, 'end_ts', end_ts)
  FROM agg;

  -- C) bump events from accelerometer magnitude (per-second max > adaptive threshold)
  SELECT
    percentile_cont(0.99)  WITHIN GROUP (ORDER BY a_mag_raw),
    percentile_cont(0.999) WITHIN GROUP (ORDER BY a_mag_raw)
  INTO bump_p99, bump_p999
  FROM dashcam.v_accel_5hz
  WHERE drive_session_id = p_session;

  IF bump_p99 IS NOT NULL AND bump_p999 IS NOT NULL THEN
    bump_thr := GREATEST(bump_p999, bump_p99 + 15.0); -- adaptive; tune later

    WITH b AS (
      SELECT
        drive_session_id,
        date_trunc('second', ts_utc) AS ts_s,
        max(a_mag_raw) AS max_mag
      FROM dashcam.v_accel_5hz
      WHERE drive_session_id = p_session
      GROUP BY drive_session_id, date_trunc('second', ts_utc)
    ),
    f AS (
      SELECT
        *,
        (max_mag > bump_thr) AS is_bump,
        lag((max_mag > bump_thr)) OVER (PARTITION BY drive_session_id ORDER BY ts_s) AS prev_bump
      FROM b
    )
    INSERT INTO dashcam.derived_event(drive_session_id, ts_utc, event_type, severity, details)
    SELECT
      p_session,
      ts_s,
      'bump',
      (max_mag - bump_p99),
      jsonb_build_object('max_mag', max_mag, 'thr', bump_thr, 'p99', bump_p99, 'p999', bump_p999)
    FROM f
    WHERE is_bump AND COALESCE(prev_bump,false)=false;
  END IF;

END;
$$;

-- 4) Refresh drive summary for a session (upsert)
CREATE OR REPLACE FUNCTION dashcam.refresh_summary(p_session uuid, p_params jsonb DEFAULT '{}'::jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_start timestamptz;
  v_end   timestamptz;
  v_dur_s double precision;
  v_dist_m double precision;
  v_dist_mi double precision;
  v_max_speed double precision;
  v_avg_speed double precision;
  v_moving_avg double precision;
BEGIN
  SELECT start_ts_utc, end_ts_utc
  INTO v_start, v_end
  FROM dashcam.drive_session
  WHERE drive_session_id = p_session;

  IF v_start IS NOT NULL AND v_end IS NOT NULL THEN
    v_dur_s := EXTRACT(epoch FROM (v_end - v_start))::double precision;
  ELSE
    v_dur_s := NULL;
  END IF;

  SELECT distance_m INTO v_dist_m
  FROM dashcam.v_session_distance
  WHERE drive_session_id = p_session;

  IF v_dist_m IS NOT NULL THEN
    v_dist_mi := v_dist_m / 1609.344;
  ELSE
    v_dist_mi := NULL;
  END IF;

  SELECT max(speed_mph) INTO v_max_speed
  FROM dashcam.v_gnss_valid
  WHERE drive_session_id = p_session;

  IF v_dur_s IS NOT NULL AND v_dur_s > 0 AND v_dist_mi IS NOT NULL THEN
    v_avg_speed := v_dist_mi / (v_dur_s / 3600.0);
  ELSE
    v_avg_speed := NULL;
  END IF;

  -- Moving average speed: compute moving time as seconds where speed >= 1 mph
  WITH s AS (
    SELECT ts_utc, speed_mph,
           lag(ts_utc) OVER (ORDER BY ts_utc) AS ts_prev
    FROM dashcam.v_gnss_valid
    WHERE drive_session_id = p_session
  ),
  t AS (
    SELECT
      CASE
        WHEN ts_prev IS NULL THEN 0
        WHEN speed_mph >= 1 THEN EXTRACT(epoch FROM (ts_utc - ts_prev))::double precision
        ELSE 0
      END AS moving_dt_s
    FROM s
  )
  SELECT
    CASE
      WHEN sum(moving_dt_s) > 0 AND v_dist_mi IS NOT NULL THEN (v_dist_mi / (sum(moving_dt_s) / 3600.0))
      ELSE NULL
    END
  INTO v_moving_avg
  FROM t;

  INSERT INTO dashcam.drive_summary(
    drive_session_id, start_ts_utc, end_ts_utc, duration_s,
    distance_m, distance_mi,
    max_speed_mph, avg_speed_mph, moving_avg_speed_mph,
    stops_count, hard_brake_count, hard_accel_count, harsh_turn_count, bump_count,
    params, updated_at
  )
  SELECT
    p_session,
    v_start, v_end, v_dur_s,
    v_dist_m, v_dist_mi,
    v_max_speed, v_avg_speed, v_moving_avg,
    COALESCE((SELECT count(*) FROM dashcam.derived_event WHERE drive_session_id=p_session AND event_type='stop'), 0),
    COALESCE((SELECT count(*) FROM dashcam.derived_event WHERE drive_session_id=p_session AND event_type='hard_brake'), 0),
    COALESCE((SELECT count(*) FROM dashcam.derived_event WHERE drive_session_id=p_session AND event_type='hard_accel'), 0),
    COALESCE((SELECT count(*) FROM dashcam.derived_event WHERE drive_session_id=p_session AND event_type='harsh_turn'), 0),
    COALESCE((SELECT count(*) FROM dashcam.derived_event WHERE drive_session_id=p_session AND event_type='bump'), 0),
    p_params,
    now()
  ON CONFLICT (drive_session_id) DO UPDATE SET
    start_ts_utc = EXCLUDED.start_ts_utc,
    end_ts_utc = EXCLUDED.end_ts_utc,
    duration_s = EXCLUDED.duration_s,
    distance_m = EXCLUDED.distance_m,
    distance_mi = EXCLUDED.distance_mi,
    max_speed_mph = EXCLUDED.max_speed_mph,
    avg_speed_mph = EXCLUDED.avg_speed_mph,
    moving_avg_speed_mph = EXCLUDED.moving_avg_speed_mph,
    stops_count = EXCLUDED.stops_count,
    hard_brake_count = EXCLUDED.hard_brake_count,
    hard_accel_count = EXCLUDED.hard_accel_count,
    harsh_turn_count = EXCLUDED.harsh_turn_count,
    bump_count = EXCLUDED.bump_count,
    params = EXCLUDED.params,
    updated_at = now();
END;
$$;

-- 5) Convenience function: refresh everything for a session
CREATE OR REPLACE FUNCTION dashcam.refresh_derived(p_session uuid, p_params jsonb DEFAULT '{}'::jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM dashcam.refresh_events(p_session, p_params);
  PERFORM dashcam.refresh_summary(p_session, p_params);
END;
$$;

COMMIT;
