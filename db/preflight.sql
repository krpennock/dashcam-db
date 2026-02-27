-- Dashcam DB preflight: required relations for ingest + derived refresh.
WITH req(name, kind) AS (
  VALUES
    -- base tables
    ('dashcam.device','table'),
    ('dashcam.drive_session','table'),
    ('dashcam.clip','table'),
    ('dashcam.gnss_sample','table'),
    ('dashcam.accel_sample','table'),
    ('dashcam.sensor_calibration','table'),
    ('dashcam.event_marker','table'),

    -- ingest staging
    ('dashcam.gnss_csv_stage','table'),
    ('dashcam.accel_csv_stage','table'),

    -- derived/migrations
    ('dashcam.derived_event','table'),
    ('dashcam.storyboard_frame','table'),

    -- views required by derived refresh
    ('dashcam.v_gnss_valid','view'),
    ('dashcam.v_accel_5hz','view')
),
found AS (
  SELECT r.name, r.kind, to_regclass(r.name) AS reg, c.relkind
  FROM req r
  LEFT JOIN pg_class c ON c.oid = to_regclass(r.name)
)
SELECT kind, name,
  CASE
    WHEN reg IS NULL THEN 'MISSING'
    WHEN kind='table' AND relkind <> 'r' THEN 'WRONG_KIND'
    WHEN kind='view'  AND relkind <> 'v' THEN 'WRONG_KIND'
    ELSE 'OK'
  END AS status
FROM found
ORDER BY status DESC, kind, name;
