BEGIN;

-- One simplified route per drive.
--
-- The viewer's overview map and its "drives through this area" filter need
-- every drive's route in one query. Building that from v_track_linestring
-- means grouping every GPS row in the database each time; this table holds
-- the answer instead, thinned to about ten metres so 700 drives are a small
-- download, and indexed so a bounding-box test is cheap.

CREATE TABLE IF NOT EXISTS dashcam.drive_track (
  drive_session_id uuid PRIMARY KEY
    REFERENCES dashcam.drive_session(drive_session_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  geom_line   geometry(LineString, 4326) NOT NULL,
  point_count integer NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS drive_track_geom_gix
  ON dashcam.drive_track USING gist (geom_line);

COMMENT ON TABLE dashcam.drive_track IS
  'Route of each drive, simplified to ~10 m (ST_SimplifyPreserveTopology 0.0001 deg). '
  'Rebuilt by dashcam.refresh_track(); refresh_derived() calls it.';

-- Rebuild one drive's row. A drive with fewer than two fixes has no route.
CREATE OR REPLACE FUNCTION dashcam.refresh_track(p_session uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_line geometry;
  v_n    integer;
BEGIN
  SELECT
    ST_SimplifyPreserveTopology(
      ST_MakeLine(ST_SetSRID(ST_MakePoint(lon, lat), 4326) ORDER BY ts_utc),
      0.0001),
    count(*)
  INTO v_line, v_n
  FROM dashcam.v_gnss_valid
  WHERE drive_session_id = p_session;

  IF v_n IS NULL OR v_n < 2 OR v_line IS NULL
     OR GeometryType(v_line) <> 'LINESTRING' THEN
    DELETE FROM dashcam.drive_track WHERE drive_session_id = p_session;
    RETURN;
  END IF;

  INSERT INTO dashcam.drive_track (drive_session_id, geom_line, point_count, updated_at)
  VALUES (p_session, v_line, v_n, now())
  ON CONFLICT (drive_session_id) DO UPDATE SET
    geom_line   = EXCLUDED.geom_line,
    point_count = EXCLUDED.point_count,
    updated_at  = now();
END;
$$;

-- refresh_derived() is what the importer runs for a new drive; the track
-- now comes along with the events and the summary.
CREATE OR REPLACE FUNCTION dashcam.refresh_derived(p_session uuid, p_params jsonb DEFAULT '{}'::jsonb)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM dashcam.refresh_events(p_session, p_params);
  PERFORM dashcam.refresh_summary(p_session, p_params);
  PERFORM dashcam.refresh_track(p_session);
END;
$$;

-- Backfill every existing drive. Each one reads its own GPS rows through the
-- primary key, so this is a minute or two, not a full-table grind.
SELECT count(*) AS tracks_built
FROM (
  SELECT dashcam.refresh_track(drive_session_id)
  FROM dashcam.drive_session
) b;

COMMIT;
