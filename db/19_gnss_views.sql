-- Minimal stable input view used by derived layer.
CREATE OR REPLACE VIEW dashcam.v_gnss_valid AS
SELECT *
FROM dashcam.gnss_sample
WHERE geom IS NOT NULL
  AND ts_utc IS NOT NULL;
