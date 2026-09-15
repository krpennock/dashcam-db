BEGIN;

-- Drive identity and time confidence.
--
-- drive_tag is the YYYYMMDD_HHMMSS_<vehicle> name a drive is built from, and the
-- drive_session_id is uuid5 of it. Storing the tag is what lets a re-processed
-- drive be recognised as the same drive instead of becoming a second copy.
--
-- The unique index on (vehicle_tag, drive_tag) is deliberately NOT created here:
-- every existing row still has a NULL tag, and 239 sessions still carry an id
-- from the old _02 naming bug. It is added in a later migration, once the repair
-- has filled the tags in.
ALTER TABLE dashcam.drive_session ADD COLUMN IF NOT EXISTS drive_tag text;
ALTER TABLE dashcam.drive_session ADD COLUMN IF NOT EXISTS time_confidence text;

COMMENT ON COLUMN dashcam.drive_session.drive_tag IS
  'YYYYMMDD_HHMMSS_<vehicle> tag this session is built from, with no _NN re-run suffix.';

COMMENT ON COLUMN dashcam.drive_session.time_confidence IS
  'How start_ts_utc was established: gnss_median (median of the fixed GNSS rows), '
  'gnss_median_clock_mismatch (GNSS used, but the camera clock disagreed by more '
  'than 15 minutes), filename_low (too few fixes, camera clock used), or '
  'filename_estimated_clock_offset (camera clock used plus a correction measured '
  'from neighbouring drives).';

-- Allow a session id to be corrected in place.
--
-- Every child table already cascades deletes, but none cascaded updates, so
-- renaming a session id would be rejected by the foreign keys. The 239 sessions
-- that carry an _02 style id need exactly that rename, and the rows underneath
-- them have to follow.
--
-- Written as a loop so re-running the migration is harmless: a key that already
-- cascades updates is left alone. Recreating a key re-validates it, which scans
-- the child table (accel_sample is the big one), so this takes a short lock.
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT c.conrelid::regclass AS child,
           c.conname,
           a.attname AS col
    FROM pg_constraint c
    JOIN pg_attribute a
      ON a.attrelid = c.conrelid
     AND a.attnum = c.conkey[1]
    WHERE c.confrelid = 'dashcam.drive_session'::regclass
      AND c.contype = 'f'
      AND c.confupdtype <> 'c'      -- 'c' = already ON UPDATE CASCADE
  LOOP
    EXECUTE format(
      'ALTER TABLE %s DROP CONSTRAINT %I, '
      'ADD CONSTRAINT %I FOREIGN KEY (%I) '
      'REFERENCES dashcam.drive_session(drive_session_id) '
      'ON UPDATE CASCADE ON DELETE CASCADE',
      r.child, r.conname, r.conname, r.col
    );
    RAISE NOTICE 'recreated % on % with ON UPDATE CASCADE', r.conname, r.child;
  END LOOP;
END$$;

COMMIT;
