BEGIN;

-- Per-session GPS track as a single PostGIS LineString (SRID 4326).
-- Depends on: dashcam.v_gnss_valid (19_gnss_views.sql)
CREATE OR REPLACE VIEW dashcam.v_track_linestring AS
SELECT
  drive_session_id,
  ST_MakeLine(
    ST_SetSRID(ST_MakePoint(lon, lat), 4326)
    ORDER BY ts_utc
  ) AS geom_line
FROM dashcam.v_gnss_valid
GROUP BY drive_session_id
HAVING COUNT(*) >= 2;

COMMIT;
