BEGIN;

-- Update v_accel_5hz to include ts_utc, ax, ay, az columns
-- required by the API /accel endpoint.
-- Previous view only had: drive_session_id, t_rel_s_5hz, a_mag_raw
CREATE OR REPLACE VIEW dashcam.v_accel_5hz AS
SELECT
  drive_session_id,
  (floor(t_rel_s * 5)::int / 5.0) AS t_rel_s_5hz,
  min(ts_utc)      AS ts_utc,
  avg(ax_g)        AS ax,
  avg(ay_g)        AS ay,
  avg(az_g)        AS az,
  avg(a_mag_g)     AS a_mag_raw
FROM dashcam.accel_sample
WHERE t_rel_s IS NOT NULL
GROUP BY drive_session_id, (floor(t_rel_s * 5)::int / 5.0);

COMMIT;
