BEGIN;

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
  -- FIX: split lag() and sum() into separate steps (no nested window functions)
  WITH s AS (
    SELECT
      drive_session_id, ts_utc, speed_mph,
      (speed_mph < stop_speed_mph) AS is_stop
    FROM dashcam.v_gnss_valid
    WHERE drive_session_id = p_session
  ),
  s2 AS (
    SELECT
      s.*,
      lag(is_stop) OVER (PARTITION BY drive_session_id ORDER BY ts_utc) AS prev_is_stop
    FROM s
  ),
  runs AS (
    SELECT
      s2.*,
      sum(
        CASE
          WHEN is_stop AND COALESCE(prev_is_stop,false)=false THEN 1
          ELSE 0
        END
      ) OVER (PARTITION BY drive_session_id ORDER BY ts_utc) AS grp
    FROM s2
  ),
  agg AS (
    SELECT
      drive_session_id,
      min(ts_utc) AS start_ts,
      max(ts_utc) AS end_ts,
      count(*)::int AS n_seconds
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

COMMIT;
