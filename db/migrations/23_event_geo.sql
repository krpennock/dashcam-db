BEGIN;

-- 1) Add persisted coordinates + PostGIS point
ALTER TABLE dashcam.derived_event
  ADD COLUMN IF NOT EXISTS lat double precision,
  ADD COLUMN IF NOT EXISTS lon double precision,
  ADD COLUMN IF NOT EXISTS geom geometry(Point,4326);

-- 2) Useful indexes (list view + map)
CREATE INDEX IF NOT EXISTS derived_event_session_type_ts_idx
  ON dashcam.derived_event (drive_session_id, event_type, ts_utc);

CREATE INDEX IF NOT EXISTS derived_event_geom_gix
  ON dashcam.derived_event USING gist (geom);

-- 3) Trigger function to populate lat/lon/geom at INSERT/UPDATE time
CREATE OR REPLACE FUNCTION dashcam.derived_event_set_geo()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE g RECORD;
BEGIN
  -- If already populated, don't do extra work.
  IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL AND NEW.geom IS NOT NULL THEN
    RETURN NEW;
  END IF;

  IF NEW.ts_utc IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT v.lat, v.lon
    INTO g
  FROM dashcam.v_gnss_valid v
  WHERE v.drive_session_id = NEW.drive_session_id
    AND v.lat IS NOT NULL AND v.lon IS NOT NULL
  ORDER BY abs(EXTRACT(epoch FROM (v.ts_utc - NEW.ts_utc))) ASC
  LIMIT 1;

  IF g.lat IS NOT NULL AND g.lon IS NOT NULL THEN
    NEW.lat := g.lat;
    NEW.lon := g.lon;
    NEW.geom := ST_SetSRID(ST_MakePoint(g.lon, g.lat), 4326);
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_derived_event_set_geo ON dashcam.derived_event;

CREATE TRIGGER trg_derived_event_set_geo
BEFORE INSERT OR UPDATE OF ts_utc, drive_session_id
ON dashcam.derived_event
FOR EACH ROW
EXECUTE FUNCTION dashcam.derived_event_set_geo();

-- 4) Backfill existing events (CTE + LATERAL where e is in FROM)
WITH todo AS (
  SELECT
    e.event_id,
    g.lat,
    g.lon
  FROM dashcam.derived_event e
  CROSS JOIN LATERAL (
    SELECT v.lat, v.lon
    FROM dashcam.v_gnss_valid v
    WHERE v.drive_session_id = e.drive_session_id
      AND v.lat IS NOT NULL AND v.lon IS NOT NULL
    ORDER BY abs(EXTRACT(epoch FROM (v.ts_utc - e.ts_utc))) ASC
    LIMIT 1
  ) g
  WHERE e.ts_utc IS NOT NULL
    AND (e.lat IS NULL OR e.lon IS NULL OR e.geom IS NULL)
)
UPDATE dashcam.derived_event e
SET lat  = todo.lat,
    lon  = todo.lon,
    geom = ST_SetSRID(ST_MakePoint(todo.lon, todo.lat), 4326)
FROM todo
WHERE e.event_id = todo.event_id;

COMMIT;
