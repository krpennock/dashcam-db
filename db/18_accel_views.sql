-- Required by refresh_events()/refresh_derived().
-- 5 Hz accel magnitude view built from accel_sample.a_mag_g.

CREATE OR REPLACE VIEW dashcam.v_accel_5hz AS
SELECT
  drive_session_id,
  (floor(t_rel_s * 5)::int / 5.0) AS t_rel_s_5hz,
  avg(a_mag_g) AS a_mag_raw
FROM dashcam.accel_sample
WHERE t_rel_s IS NOT NULL
GROUP BY drive_session_id, (floor(t_rel_s * 5)::int / 5.0);
